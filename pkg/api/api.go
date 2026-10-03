// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package api

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/llm"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/scrape"
	"github.com/glidea/zenfeed/pkg/scrape/scraper"
	"github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
	telemetry "github.com/glidea/zenfeed/pkg/telemetry"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	hashutil "github.com/glidea/zenfeed/pkg/util/hash"
	jsonschema "github.com/glidea/zenfeed/pkg/util/json_schema"
)

// --- Interface code block ---
type API interface {
	component.Component
	config.Watcher

	QueryAppConfigSchema(
		ctx context.Context,
		req *QueryAppConfigSchemaRequest,
	) (resp *QueryAppConfigSchemaResponse, err error)
	QueryAppConfig(ctx context.Context, req *QueryAppConfigRequest) (resp *QueryAppConfigResponse, err error)
	ApplyAppConfig(ctx context.Context, req *ApplyAppConfigRequest) (resp *ApplyAppConfigResponse, err error)

	QueryRSSHubCategories(
		ctx context.Context,
		req *QueryRSSHubCategoriesRequest,
	) (resp *QueryRSSHubCategoriesResponse, err error)
	QueryRSSHubWebsites(
		ctx context.Context,
		req *QueryRSSHubWebsitesRequest,
	) (resp *QueryRSSHubWebsitesResponse, err error)
	QueryRSSHubRoutes(ctx context.Context, req *QueryRSSHubRoutesRequest) (resp *QueryRSSHubRoutesResponse, err error)
	QuerySourceStatuses(ctx context.Context, req *QuerySourceStatusesRequest) (resp *QuerySourceStatusesResponse, err error)
	RefreshSource(ctx context.Context, req *RefreshSourceRequest) (resp *RefreshSourceResponse, err error)

	Write(ctx context.Context, req *WriteRequest) (resp *WriteResponse, err error) // WARN: beta!!!
	UpdateFeedLabels(
		ctx context.Context,
		req *UpdateFeedLabelsRequest,
	) (resp *UpdateFeedLabelsResponse, err error)
	Query(ctx context.Context, req *QueryRequest) (resp *QueryResponse, err error)
}

type Config struct {
	RSSHubEndpoint   string
	LLM              string
	DisabledSources  []string
	SourceCategories map[string]string
}

func (c *Config) Validate() error {
	c.RSSHubEndpoint = strings.TrimSuffix(c.RSSHubEndpoint, "/")

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.RSSHubEndpoint = app.Scrape.RSSHubEndpoint
	c.LLM = app.API.LLM
	c.DisabledSources = make([]string, 0, len(app.Scrape.Sources))
	c.SourceCategories = make(map[string]string, len(app.Scrape.Sources))
	for _, source := range app.Scrape.Sources {
		if !source.IsEnabled() {
			c.DisabledSources = append(c.DisabledSources, source.Name)
		}
		if category := canonicalCategory(source.Labels["category"]); category != "" {
			c.SourceCategories[source.Name] = category
		}
	}

	return c
}

type Dependencies struct {
	ConfigManager config.Manager
	FeedStorage   feed.Storage
	LLMFactory    llm.Factory
	SourceManager scrape.Manager
}

type QueryAppConfigSchemaRequest struct{}

type QueryAppConfigSchemaResponse map[string]any

type QueryAppConfigRequest struct{}

type QueryAppConfigResponse struct {
	// Revision is an opaque token for optimistic config updates.
	Revision   string `yaml:"_revision" json:"_revision"`
	config.App `yaml:",inline" json:",inline"`
}

type ApplyAppConfigRequest struct {
	// Revision is mandatory and must match the current config.
	Revision   *string `yaml:"_revision,omitempty" json:"_revision,omitempty"`
	config.App `yaml:",inline" json:",inline"`
}

type ApplyAppConfigResponse struct{}

type QuerySourceStatusesRequest struct{}

type QuerySourceStatusesResponse struct {
	Sources []scraper.Status `json:"sources"`
}

type RefreshSourceRequest struct {
	Name string `json:"name"`
}

type RefreshSourceResponse struct {
	Accepted bool `json:"accepted"`
}

type QueryRSSHubCategoriesRequest struct{}

type QueryRSSHubCategoriesResponse struct {
	Categories []string `json:"categories,omitempty"`
}

type QueryRSSHubWebsitesRequest struct {
	Category string `json:"category,omitempty"`
}

type QueryRSSHubWebsitesResponse struct {
	Websites []RSSHubWebsite `json:"websites,omitempty"`
}

type RSSHubWebsite struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Categories  []string `json:"categories,omitempty"`
}

type QueryRSSHubRoutesRequest struct {
	WebsiteID string `json:"website_id,omitempty"`
}

type QueryRSSHubRoutesResponse struct {
	Routes []RSSHubRoute `json:"routes,omitempty"`
}

type RSSHubRoute struct {
	Name        string         `json:"name,omitempty"`
	Description string         `json:"description,omitempty"`
	Path        any            `json:"path,omitempty"`
	Example     string         `json:"example,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Features    map[string]any `json:"features,omitempty"`
}

type WriteRequest struct { // Beta.
	Feeds []*model.Feed `json:"feeds"`
}

const (
	maxWriteFeeds      = 1000
	maxWriteLabelBytes = 6 << 20
	maxRSSHubBodyBytes = 8 << 20
)

func (r *WriteRequest) Validate() error {
	if r == nil || len(r.Feeds) == 0 {
		return errors.New("feeds is required")
	}
	if len(r.Feeds) > maxWriteFeeds {
		return errors.Errorf("feeds must contain at most %d entries", maxWriteFeeds)
	}
	totalBytes := 0
	for i, feed := range r.Feeds {
		if feed == nil {
			return errors.Errorf("feed %d is required", i)
		}
		if err := feed.Validate(); err != nil {
			return errors.Wrapf(err, "validate feed %d", i)
		}
		for _, label := range feed.Labels {
			totalBytes += len(label.Key) + len(label.Value)
			if totalBytes > maxWriteLabelBytes {
				return errors.Errorf("feed label content must total at most %d bytes", maxWriteLabelBytes)
			}
		}
	}

	return nil
}

type WriteResponse struct{}

type UpdateFeedLabelsRequest struct {
	Source string            `json:"source"`
	Link   string            `json:"link"`
	Time   time.Time         `json:"time,omitempty"`
	Labels map[string]string `json:"labels"`
}

func (r *UpdateFeedLabelsRequest) Validate() error {
	if r == nil {
		return errors.New("request is required")
	}
	if strings.TrimSpace(r.Source) == "" {
		return errors.New("source is required")
	}
	if strings.TrimSpace(r.Link) == "" {
		return errors.New("link is required")
	}
	if len(r.Labels) == 0 {
		return errors.New("labels are required")
	}
	if len(r.Labels) > 16 {
		return errors.New("labels must contain at most 16 entries")
	}

	return nil
}

type UpdateFeedLabelsResponse struct{}

type QueryRequest struct {
	Query        string             `json:"query,omitempty"`
	Threshold    float32            `json:"threshold,omitempty"`
	LabelFilters []string           `json:"label_filters,omitempty"`
	Summarize    bool               `json:"summarize,omitempty"`
	Limit        int                `json:"limit,omitempty"`
	Cursor       *block.QueryCursor `json:"cursor,omitempty"`
	Categories   []string           `json:"categories,omitempty"`
	SkipStats    bool               `json:"skip_stats,omitempty"`
	Start        time.Time          `json:"start,omitempty"`
	End          time.Time          `json:"end,omitempty"`
}

func (r *QueryRequest) Validate() error { //nolint:cyclop
	if r.Query != "" && utf8.RuneCountInString(r.Query) > 64 {
		return errors.New("query must be at most 64 characters")
	}
	if r.Threshold == 0 {
		r.Threshold = 0.5
	}
	if r.Threshold < 0 || r.Threshold > 1 {
		return errors.New("threshold must be between 0 and 1")
	}
	if r.Limit < 1 {
		r.Limit = 10
	}
	if r.Limit > block.MaxQueryLimit {
		r.Limit = block.MaxQueryLimit
	}
	if r.Cursor != nil && r.Cursor.Time.IsZero() {
		return errors.New("cursor time is required")
	}
	for _, category := range r.Categories {
		switch category {
		case "ai-paper", "ai-blog", "social-network", "ai-podcast", "podcast", "image":
		default:
			return errors.Errorf("unsupported category %q", category)
		}
	}
	if r.Start.IsZero() {
		r.Start = time.Now().Add(-24 * time.Hour)
	}
	if r.End.IsZero() {
		r.End = time.Now()
	}
	if !r.End.After(r.Start) {
		return errors.New("end must be after start")
	}

	return nil
}

type QueryRequestSemanticFilter struct {
	Query     string  `json:"query,omitempty"`
	Threshold float32 `json:"threshold,omitempty"`
}

type QueryResponse struct {
	Summary    string             `json:"summary,omitempty"`
	Feeds      []*block.FeedVO    `json:"feeds"`
	Count      int                `json:"count"`
	HasMore    bool               `json:"has_more"`
	NextCursor *block.QueryCursor `json:"next_cursor,omitempty"`
	Stats      *QueryStats        `json:"stats,omitempty"`
}

type QueryStats struct {
	Total      int            `json:"total"`
	Sources    map[string]int `json:"sources"`
	Categories map[string]int `json:"categories"`
	Tags       map[string]int `json:"tags"`
}

func newQueryStats() *QueryStats {
	return &QueryStats{
		Sources:    make(map[string]int),
		Categories: make(map[string]int),
		Tags:       make(map[string]int),
	}
}

func (s *QueryStats) Add(feed *block.FeedVO, category string) {
	s.Total++
	if source := strings.TrimSpace(feed.Labels.Get(model.LabelSource)); source != "" {
		s.Sources[source]++
	}
	if category != "" {
		s.Categories[category]++
	}
	seenTags := make(map[string]struct{})
	for _, tag := range strings.FieldsFunc(feed.Labels.Get("tags"), func(r rune) bool {
		return r == ',' || r == '，' || r == ';' || r == '；' || r == '|'
	}) {
		if tag = strings.TrimSpace(tag); tag != "" {
			identity := strings.ToLower(tag)
			if _, exists := seenTags[identity]; exists {
				continue
			}
			seenTags[identity] = struct{}{}
			s.Tags[tag]++
		}
	}
}

func feedCategory(feed *block.FeedVO) string {
	switch strings.ToLower(strings.TrimSpace(feed.Labels.Get(model.LabelSource))) {
	case "cool paper", "huggingface daily papers":
		return "ai-paper"
	case "latent.space":
		return "ai-blog"
	case "nat geo photo of the day":
		return "image"
	default:
		if strings.HasSuffix(strings.ToLower(strings.TrimSpace(feed.Labels.Get(model.LabelSource))), "updates on arxiv.org") {
			return "ai-paper"
		}
	}

	return canonicalCategory(feed.Labels.Get("category"))
}

func canonicalCategory(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "papers", "ai-paper":
		return "ai-paper"
	case "ai-blog", "cn-tech":
		return "ai-blog"
	case "x-twitter", "social-network":
		return "social-network"
	case "photography", "image":
		return "image"
	default:
		return ""
	}
}

func (a *api) effectiveFeedCategory(feed *block.FeedVO) string {
	if category, ok := a.Config().SourceCategories[feed.Labels.Get(model.LabelSource)]; ok {
		return category
	}

	return feedCategory(feed)
}

func (a *api) applyEffectiveCategory(feed *block.FeedVO) *block.FeedVO {
	category, ok := a.Config().SourceCategories[feed.Labels.Get(model.LabelSource)]
	if !ok {
		return feed
	}
	clonedFeed := *feed.Feed
	clonedFeed.Labels = append(model.Labels(nil), feed.Labels...)
	clonedFeed.Labels.Put("category", category, false)
	cloned := *feed
	cloned.Feed = &clonedFeed

	return &cloned
}

func isGeneralPodcast(feed *block.FeedVO) bool {
	if strings.EqualFold(strings.TrimSpace(feed.Labels.Get(model.LabelSource)), "Latent.Space") {
		return false
	}
	category := strings.ToLower(strings.TrimSpace(feed.Labels.Get("category")))

	return strings.TrimSpace(feed.Labels.Get("podcast_url")) != "" ||
		category == "podcast" || category == "ai-podcast"
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e Error) Error() string {
	return e.Message
}

func newError(code int, err error) Error {
	return Error{
		Code:    code,
		Message: err.Error(),
	}
}

var (
	ErrBadRequest           = func(err error) Error { return newError(http.StatusBadRequest, err) }
	ErrConflict             = func(err error) Error { return newError(http.StatusConflict, err) }
	ErrPreconditionRequired = func(err error) Error { return newError(http.StatusPreconditionRequired, err) }
	ErrNotFound             = func(err error) Error { return newError(http.StatusNotFound, err) }
	ErrUnavailable          = func(err error) Error { return newError(http.StatusServiceUnavailable, err) }
	ErrInternal             = func(err error) Error { return newError(http.StatusInternalServerError, err) }
)

// --- Factory code block ---
type Factory component.Factory[API, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[API, config.App, Dependencies](
			func(instance string, app *config.App, dependencies Dependencies) (API, error) {
				m := &mockAPI{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[API, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (API, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	api := &api{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "API",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		hc: &http.Client{Timeout: 30 * time.Second},
	}

	return api, nil
}

// --- Implementation code block ---
type api struct {
	*component.Base[Config, Dependencies]

	hc *http.Client
}

func (a *api) Reload(app *config.App) error {
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "validate config")
	}
	a.SetConfig(newConfig)

	return nil
}

func (a *api) QueryAppConfigSchema(
	ctx context.Context,
	req *QueryAppConfigSchemaRequest,
) (resp *QueryAppConfigSchemaResponse, err error) {
	schema, err := jsonschema.ForType(reflect.TypeOf(ApplyAppConfigRequest{}))
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "query app config schema"))
	}
	if definitions, ok := schema["definitions"].(map[string]any); ok {
		if requestSchema, ok := definitions["ApplyAppConfigRequest"].(map[string]any); ok {
			requestSchema["required"] = []string{"_revision"}
		}
	}

	return (*QueryAppConfigSchemaResponse)(&schema), nil
}

func (a *api) QueryAppConfig(
	ctx context.Context,
	req *QueryAppConfigRequest,
) (resp *QueryAppConfigResponse, err error) {
	c, revision := a.Dependencies().ConfigManager.AppConfigSnapshot()

	return &QueryAppConfigResponse{Revision: revision, App: *config.RedactedAppConfig(c)}, nil
}

func (a *api) ApplyAppConfig(
	ctx context.Context,
	req *ApplyAppConfigRequest,
) (resp *ApplyAppConfigResponse, err error) {
	if req == nil || req.Revision == nil || strings.TrimSpace(*req.Revision) == "" {
		return nil, ErrPreconditionRequired(errors.New(
			"_revision is required; query the current config immediately before applying changes",
		))
	}
	if err := a.Dependencies().ConfigManager.SaveAppConfig(&req.App, req.Revision); err != nil {
		var conflict *config.RevisionConflictError
		if errors.As(err, &conflict) {
			return nil, ErrConflict(errors.Wrap(err, "save app config"))
		}

		return nil, ErrBadRequest(errors.Wrap(err, "save app config"))
	}

	return &ApplyAppConfigResponse{}, nil
}

func (a *api) QuerySourceStatuses(
	ctx context.Context,
	req *QuerySourceStatusesRequest,
) (*QuerySourceStatusesResponse, error) {
	if a.Dependencies().SourceManager == nil {
		return nil, ErrUnavailable(errors.New("source manager is unavailable"))
	}

	return &QuerySourceStatusesResponse{
		Sources: a.Dependencies().SourceManager.Statuses(ctx),
	}, nil
}

func (a *api) RefreshSource(
	ctx context.Context,
	req *RefreshSourceRequest,
) (*RefreshSourceResponse, error) {
	if req == nil || strings.TrimSpace(req.Name) == "" {
		return nil, ErrBadRequest(errors.New("source name is required"))
	}
	if a.Dependencies().SourceManager == nil {
		return nil, ErrUnavailable(errors.New("source manager is unavailable"))
	}
	if err := a.Dependencies().SourceManager.Refresh(strings.TrimSpace(req.Name)); err != nil {
		switch {
		case errors.Is(err, scrape.ErrSourceNotFound):
			return nil, ErrNotFound(err)
		case errors.Is(err, scrape.ErrManagerNotReady):
			return nil, ErrUnavailable(err)
		case errors.Is(err, scrape.ErrSourceDisabled), errors.Is(err, scraper.ErrAlreadyRunning):
			return nil, ErrConflict(err)
		default:
			return nil, ErrInternal(err)
		}
	}

	return &RefreshSourceResponse{Accepted: true}, nil
}

func (a *api) QueryRSSHubCategories(
	ctx context.Context,
	req *QueryRSSHubCategoriesRequest,
) (resp *QueryRSSHubCategoriesResponse, err error) {
	url := a.Config().RSSHubEndpoint + "/api/namespace"

	// New request.
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "new request"))
	}

	// Do request.
	forwardRespIO, err := a.hc.Do(forwardReq)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "query rss hub websites"))
	}
	defer func() { _ = forwardRespIO.Body.Close() }()

	if forwardRespIO.StatusCode < http.StatusOK || forwardRespIO.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrInternal(errors.Errorf("RSSHub returned HTTP %d", forwardRespIO.StatusCode))
	}

	// Parse response.
	var forwardResp map[string]RSSHubWebsite
	if err := decodeLimitedJSON(forwardRespIO.Body, &forwardResp); err != nil {
		return nil, ErrInternal(errors.Wrap(err, "parse response"))
	}

	// Convert to response.
	categories := make(map[string]struct{}, len(forwardResp))
	for _, website := range forwardResp {
		for _, category := range website.Categories {
			categories[category] = struct{}{}
		}
	}
	result := make([]string, 0, len(categories))
	for category := range categories {
		result = append(result, category)
	}
	resp = &QueryRSSHubCategoriesResponse{Categories: result}

	return resp, nil
}

func (a *api) QueryRSSHubWebsites(
	ctx context.Context, req *QueryRSSHubWebsitesRequest,
) (resp *QueryRSSHubWebsitesResponse, err error) {
	if req.Category == "" {
		return nil, ErrBadRequest(errors.New("category is required"))
	}

	url := a.Config().RSSHubEndpoint + "/api/category/" + req.Category

	// New request.
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "new request"))
	}

	// Do request.
	forwardRespIO, err := a.hc.Do(forwardReq)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "query rss hub routes"))
	}
	defer func() { _ = forwardRespIO.Body.Close() }()

	// Parse response.
	if forwardRespIO.StatusCode < http.StatusOK || forwardRespIO.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrInternal(errors.Errorf("RSSHub returned HTTP %d", forwardRespIO.StatusCode))
	}
	body, err := readLimitedBody(forwardRespIO.Body)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "read response"))
	}
	if len(body) == 0 {
		// Hack for RSSHub...
		// Consider cache category ids for validate by self to remove this shit code.
		return nil, ErrBadRequest(errors.New("category id is invalid"))
	}
	var forwardResp map[string]RSSHubWebsite
	if err := json.Unmarshal(body, &forwardResp); err != nil {
		return nil, ErrInternal(errors.Wrap(err, "parse response"))
	}

	// Convert to response.
	resp = &QueryRSSHubWebsitesResponse{Websites: make([]RSSHubWebsite, 0, len(forwardResp))}
	for id, website := range forwardResp {
		website.ID = id
		website.Description = website.Name + " - " + website.Description
		website.Name = "" // Avoid AI confusion of ID and Name.
		resp.Websites = append(resp.Websites, website)
	}

	return resp, nil
}

func (a *api) QueryRSSHubRoutes(
	ctx context.Context,
	req *QueryRSSHubRoutesRequest,
) (resp *QueryRSSHubRoutesResponse, err error) {
	if req.WebsiteID == "" {
		return nil, ErrBadRequest(errors.New("website id is required"))
	}

	url := a.Config().RSSHubEndpoint + "/api/namespace/" + req.WebsiteID

	// New request.
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "new request"))
	}

	// Do request.
	forwardRespIO, err := a.hc.Do(forwardReq)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "query rss hub routes"))
	}
	defer func() { _ = forwardRespIO.Body.Close() }()

	// Parse response.
	if forwardRespIO.StatusCode < http.StatusOK || forwardRespIO.StatusCode >= http.StatusMultipleChoices {
		return nil, ErrInternal(errors.Errorf("RSSHub returned HTTP %d", forwardRespIO.StatusCode))
	}
	body, err := readLimitedBody(forwardRespIO.Body)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "read response"))
	}
	if len(body) == 0 {
		return nil, ErrBadRequest(errors.New("website id is invalid"))
	}

	var forwardResp struct {
		Routes map[string]RSSHubRoute `json:"routes"`
	}
	if err := json.Unmarshal(body, &forwardResp); err != nil {
		return nil, ErrInternal(errors.Wrap(err, "parse response"))
	}

	// Convert to response.
	resp = &QueryRSSHubRoutesResponse{Routes: make([]RSSHubRoute, 0, len(forwardResp.Routes))}
	for _, route := range forwardResp.Routes {
		resp.Routes = append(resp.Routes, route)
	}

	return resp, nil
}

func readLimitedBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxRSSHubBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxRSSHubBodyBytes {
		return nil, errors.Errorf("response exceeds %d bytes", maxRSSHubBodyBytes)
	}

	return body, nil
}

func decodeLimitedJSON(r io.Reader, dst any) error {
	body, err := readLimitedBody(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, dst)
}

func (a *api) Write(ctx context.Context, req *WriteRequest) (resp *WriteResponse, err error) {
	ctx = telemetry.StartWith(ctx, append(a.TelemetryLabels(), telemetrymodel.KeyOperation, "Write")...)
	defer func() { telemetry.End(ctx, err) }()

	if err := req.Validate(); err != nil {
		return nil, ErrBadRequest(err)
	}
	for _, feed := range req.Feeds {
		feed.ID = rand.Uint64()
		feed.Labels.Put(model.LabelType, "api", false)
	}
	if err := a.Dependencies().FeedStorage.Append(ctx, req.Feeds...); err != nil {
		return nil, ErrInternal(errors.Wrap(err, "append"))
	}

	return &WriteResponse{}, nil
}

func (a *api) UpdateFeedLabels(
	ctx context.Context,
	req *UpdateFeedLabelsRequest,
) (resp *UpdateFeedLabelsResponse, err error) {
	ctx = telemetry.StartWith(ctx, append(a.TelemetryLabels(), telemetrymodel.KeyOperation, "UpdateFeedLabels")...)
	defer func() { telemetry.End(ctx, err) }()
	if err := req.Validate(); err != nil {
		return nil, ErrBadRequest(err)
	}

	id := hashutil.Sum64s([]string{req.Source, req.Link})
	if err := a.Dependencies().FeedStorage.UpdateLabels(ctx, id, req.Time, req.Labels); err != nil {
		return nil, ErrInternal(errors.Wrap(err, "update feed labels"))
	}

	return &UpdateFeedLabelsResponse{}, nil
}

func (a *api) Query(ctx context.Context, req *QueryRequest) (resp *QueryResponse, err error) {
	ctx = telemetry.StartWith(ctx, append(a.TelemetryLabels(), telemetrymodel.KeyOperation, "Query")...)
	defer func() { telemetry.End(ctx, err) }()

	// Validate request.
	if err := req.Validate(); err != nil {
		return nil, ErrBadRequest(errors.Wrap(err, "validate"))
	}

	queryOptions := block.QueryOptions{
		Query:           req.Query,
		Threshold:       req.Threshold,
		LabelFilters:    req.LabelFilters,
		ExcludedSources: a.Config().DisabledSources,
		Limit:           req.Limit + 1,
		Cursor:          req.Cursor,
		Start:           req.Start,
		End:             req.End,
	}
	queryOptions.MatchFeed = func(feed *block.FeedVO) bool {
		return !isGeneralPodcast(feed)
	}
	if len(req.Categories) > 0 {
		categories := make(map[string]struct{}, len(req.Categories))
		for _, category := range req.Categories {
			if category == "podcast" || category == "ai-podcast" {
				category = "ai-blog"
			}
			categories[category] = struct{}{}
		}
		queryOptions.MatchFeed = func(feed *block.FeedVO) bool {
			if isGeneralPodcast(feed) {
				return false
			}
			_, matched := categories[a.effectiveFeedCategory(feed)]

			return matched
		}
	}
	var stats *QueryStats
	var statsMu sync.Mutex
	if req.Cursor == nil && req.Query == "" && !req.SkipStats {
		stats = newQueryStats()
		queryOptions.OnMatch = func(feed *block.FeedVO) {
			if isGeneralPodcast(feed) {
				return
			}
			statsMu.Lock()
			stats.Add(feed, a.effectiveFeedCategory(feed))
			statsMu.Unlock()
		}
	}

	// Forward to storage.
	feeds, err := a.Dependencies().FeedStorage.Query(ctx, queryOptions)
	if err != nil {
		return nil, ErrInternal(errors.Wrap(err, "query"))
	}
	if len(feeds) == 0 {
		return &QueryResponse{Feeds: []*block.FeedVO{}, Stats: stats}, nil
	}

	hasMore := len(feeds) > req.Limit
	if hasMore {
		feeds = feeds[:req.Limit]
	}
	for i, feed := range feeds {
		feeds[i] = a.applyEffectiveCategory(feed)
	}
	var nextCursor *block.QueryCursor
	if hasMore {
		last := feeds[len(feeds)-1]
		nextCursor = &block.QueryCursor{
			Score: last.Score,
			Time:  last.Time,
			ID:    last.ID,
		}
	}

	// Summarize feeds.
	var summary string
	if req.Summarize {
		var sb strings.Builder
		for _, feed := range feeds {
			sb.WriteString(feed.Labels.Get(model.LabelContent) + "\n")
		}

		q := []string{
			"You are a helpful assistant that summarizes the following feeds.",
			sb.String(),
		}
		if req.Query != "" {
			q = append(q, "And my specific question & requirements are: "+req.Query)
			q = append(q, "Respond in query's original language.")
		}

		summary, err = a.Dependencies().LLMFactory.Get(a.Config().LLM).String(ctx, q)
		if err != nil {
			summary = err.Error()
		}
	}

	// Convert to response.
	for _, feed := range feeds {
		feed.Time = feed.Time.In(time.Local)
	}

	return &QueryResponse{
		Summary:    summary,
		Feeds:      feeds,
		Count:      len(feeds),
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Stats:      stats,
	}, nil
}

type mockAPI struct {
	component.Mock
}

func (m *mockAPI) Reload(app *config.App) error {
	return m.Called(app).Error(0)
}

func (m *mockAPI) QueryAppConfigSchema(
	ctx context.Context,
	req *QueryAppConfigSchemaRequest,
) (resp *QueryAppConfigSchemaResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryAppConfigSchemaResponse), args.Error(1)
}

func (m *mockAPI) QueryAppConfig(
	ctx context.Context,
	req *QueryAppConfigRequest,
) (resp *QueryAppConfigResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryAppConfigResponse), args.Error(1)
}

func (m *mockAPI) ApplyAppConfig(
	ctx context.Context,
	req *ApplyAppConfigRequest,
) (resp *ApplyAppConfigResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*ApplyAppConfigResponse), args.Error(1)
}

func (m *mockAPI) QuerySourceStatuses(
	ctx context.Context,
	req *QuerySourceStatusesRequest,
) (*QuerySourceStatusesResponse, error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QuerySourceStatusesResponse), args.Error(1)
}

func (m *mockAPI) RefreshSource(
	ctx context.Context,
	req *RefreshSourceRequest,
) (*RefreshSourceResponse, error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*RefreshSourceResponse), args.Error(1)
}

func (m *mockAPI) QueryRSSHubCategories(
	ctx context.Context,
	req *QueryRSSHubCategoriesRequest,
) (resp *QueryRSSHubCategoriesResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryRSSHubCategoriesResponse), args.Error(1)
}

func (m *mockAPI) QueryRSSHubWebsites(
	ctx context.Context,
	req *QueryRSSHubWebsitesRequest,
) (resp *QueryRSSHubWebsitesResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryRSSHubWebsitesResponse), args.Error(1)
}

func (m *mockAPI) QueryRSSHubRoutes(
	ctx context.Context,
	req *QueryRSSHubRoutesRequest,
) (resp *QueryRSSHubRoutesResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryRSSHubRoutesResponse), args.Error(1)
}

func (m *mockAPI) Query(ctx context.Context, req *QueryRequest) (resp *QueryResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*QueryResponse), args.Error(1)
}

func (m *mockAPI) Write(ctx context.Context, req *WriteRequest) (resp *WriteResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*WriteResponse), args.Error(1)
}

func (m *mockAPI) UpdateFeedLabels(
	ctx context.Context,
	req *UpdateFeedLabelsRequest,
) (resp *UpdateFeedLabelsResponse, err error) {
	args := m.Called(ctx, req)

	return args.Get(0).(*UpdateFeedLabelsResponse), args.Error(1)
}

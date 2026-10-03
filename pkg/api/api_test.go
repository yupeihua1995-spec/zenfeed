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
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/model"
	feedstorage "github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
)

func TestAPIQueryAppConfig_BitsUT(t *testing.T) {
	app := &config.App{Timezone: "UTC"}
	app.LLMs = []config.LLM{{Name: "primary", APIKey: "llm-secret"}}
	app.Jina.Token = "jina-secret"
	app.Scrape.RSSHubAccessKey = "rsshub-secret"
	app.Storage.Object.AccessKeyID = "storage-id"
	app.Storage.Object.SecretAccessKey = "storage-secret"
	app.Notify.Channels.Email = &config.NotifyChannelEmail{Password: "smtp-secret"}
	app.Notify.Receivers = []config.NotifyReceiver{{
		Name: "webhook",
		Webhook: &config.NotifyReceiverWebhook{
			URL: "https://hooks.example.test/private-token",
		},
	}}
	mgr := &stubConfigManager{app: app, revision: "revision-1"}
	svc := newTestAPI(t, mgr)

	resp, err := svc.QueryAppConfig(context.Background(), &QueryAppConfigRequest{})

	require.NoError(t, err)
	assert.Equal(t, "UTC", resp.Timezone)
	assert.Equal(t, "revision-1", resp.Revision)
	b, marshalErr := json.Marshal(resp)
	require.NoError(t, marshalErr)
	for _, secret := range []string{
		"llm-secret", "jina-secret", "rsshub-secret", "storage-id",
		"storage-secret", "smtp-secret", "private-token",
	} {
		assert.NotContains(t, string(b), secret)
	}
}

func TestAPIApplyAppConfig_BitsUT(t *testing.T) {
	t.Run("passes the expected revision to the manager", func(t *testing.T) {
		mgr := &stubConfigManager{}
		svc := newTestAPI(t, mgr)
		revision := "revision-1"

		resp, err := svc.ApplyAppConfig(context.Background(), &ApplyAppConfigRequest{
			Revision: revisionPtr(revision),
			App:      config.App{Timezone: "UTC"},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, mgr.expectedRevision)
		assert.Equal(t, revision, *mgr.expectedRevision)
		assert.Equal(t, "UTC", mgr.savedApp.Timezone)
	})

	t.Run("rejects omitted revision as a precondition failure", func(t *testing.T) {
		mgr := &stubConfigManager{}
		svc := newTestAPI(t, mgr)

		_, err := svc.ApplyAppConfig(context.Background(), &ApplyAppConfigRequest{
			App: config.App{Timezone: "UTC"},
		})

		var apiErr Error
		require.True(t, errors.As(err, &apiErr))
		assert.Equal(t, http.StatusPreconditionRequired, apiErr.Code)
		assert.Contains(t, apiErr.Message, "_revision")
		assert.Nil(t, mgr.savedApp)
	})

	t.Run("rejects an empty revision as a precondition failure", func(t *testing.T) {
		mgr := &stubConfigManager{}
		svc := newTestAPI(t, mgr)

		_, err := svc.ApplyAppConfig(context.Background(), &ApplyAppConfigRequest{
			Revision: revisionPtr("  "),
		})

		var apiErr Error
		require.True(t, errors.As(err, &apiErr))
		assert.Equal(t, http.StatusPreconditionRequired, apiErr.Code)
		assert.Nil(t, mgr.savedApp)
	})

	t.Run("maps revision conflict to HTTP 409 API error", func(t *testing.T) {
		mgr := &stubConfigManager{saveErr: &config.RevisionConflictError{Expected: "old", Actual: "new"}}
		svc := newTestAPI(t, mgr)

		_, err := svc.ApplyAppConfig(context.Background(), &ApplyAppConfigRequest{
			Revision: revisionPtr("old"),
		})

		var apiErr Error
		require.True(t, errors.As(err, &apiErr))
		assert.Equal(t, http.StatusConflict, apiErr.Code)
		assert.Contains(t, apiErr.Message, "config revision conflict")
	})

	t.Run("keeps non-conflict save failures as bad requests", func(t *testing.T) {
		mgr := &stubConfigManager{saveErr: errors.New("invalid config")}
		svc := newTestAPI(t, mgr)

		_, err := svc.ApplyAppConfig(context.Background(), &ApplyAppConfigRequest{Revision: revisionPtr("revision-1")})

		var apiErr Error
		require.True(t, errors.As(err, &apiErr))
		assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	})
}

func TestAPIQueryAppConfigSchemaRequiresRevision_BitsUT(t *testing.T) {
	svc := newTestAPI(t, &stubConfigManager{})

	schema, err := svc.QueryAppConfigSchema(context.Background(), &QueryAppConfigSchemaRequest{})

	require.NoError(t, err)
	definitions := (*schema)["definitions"].(map[string]any)
	requestSchema := definitions["ApplyAppConfigRequest"].(map[string]any)
	assert.Equal(t, []string{"_revision"}, requestSchema["required"])
}

func TestWriteRequestValidate_BitsUT(t *testing.T) {
	t.Run("requires at least one feed", func(t *testing.T) {
		require.Error(t, (&WriteRequest{}).Validate())
	})

	t.Run("rejects too many feeds", func(t *testing.T) {
		req := &WriteRequest{Feeds: make([]*model.Feed, maxWriteFeeds+1)}
		require.ErrorContains(t, req.Validate(), "at most")
	})

	t.Run("rejects oversized label content", func(t *testing.T) {
		req := &WriteRequest{Feeds: []*model.Feed{{Labels: model.Labels{{
			Key: model.LabelContent, Value: strings.Repeat("x", maxWriteLabelBytes+1),
		}}}}}
		require.ErrorContains(t, req.Validate(), "label content")
	})
}

func TestQueryRequestPagination_BitsUT(t *testing.T) {
	now := time.Now()

	t.Run("caps the public page size", func(t *testing.T) {
		req := &QueryRequest{Limit: block.MaxQueryLimit + 100, Start: now.Add(-time.Hour), End: now}

		require.NoError(t, req.Validate())
		assert.Equal(t, block.MaxQueryLimit, req.Limit)
	})

	t.Run("requires cursor time", func(t *testing.T) {
		req := &QueryRequest{
			Limit:  100,
			Cursor: &block.QueryCursor{ID: 42},
			Start:  now.Add(-time.Hour),
			End:    now,
		}

		require.ErrorContains(t, req.Validate(), "cursor time")
	})

	t.Run("rejects unsupported content categories", func(t *testing.T) {
		req := &QueryRequest{
			Limit:      20,
			Categories: []string{"unknown"},
			Start:      now.Add(-time.Hour),
			End:        now,
		}

		require.ErrorContains(t, req.Validate(), "unsupported category")
	})

	t.Run("serializes cursor IDs without losing uint64 precision", func(t *testing.T) {
		cursor := &block.QueryCursor{Score: 0.75, Time: now, ID: ^uint64(0)}
		encoded, err := json.Marshal(&QueryResponse{NextCursor: cursor})
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"id":"18446744073709551615"`)

		var request QueryRequest
		require.NoError(t, json.Unmarshal([]byte(`{"cursor":{"score":0.75,"time":"`+now.Format(time.RFC3339Nano)+`","id":"18446744073709551615"}}`), &request))
		require.NotNil(t, request.Cursor)
		assert.Equal(t, ^uint64(0), request.Cursor.ID)
	})
}

func TestAPIQueryReturnsNextCursor_BitsUT(t *testing.T) {
	now := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	disabled := false
	storage := &queryStorageStub{feeds: []*block.FeedVO{
		{Feed: &model.Feed{ID: 3, Time: now}, Score: 0.9},
		{Feed: &model.Feed{ID: 2, Time: now.Add(-time.Minute)}, Score: 0.8},
		{Feed: &model.Feed{ID: 1, Time: now.Add(-2 * time.Minute)}, Score: 0.7},
	}}
	app := &config.App{Scrape: config.Scrape{Sources: []config.ScrapeSource{
		{Name: "disabled-source", Enabled: &disabled},
	}}}
	got, err := new("test-pagination", app, Dependencies{
		ConfigManager: &stubConfigManager{},
		FeedStorage:   storage,
	})
	require.NoError(t, err)
	svc := got.(*api)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	resp, err := svc.Query(context.Background(), &QueryRequest{
		Limit: 2,
		Start: now.Add(-time.Hour),
		End:   now.Add(time.Hour),
	})

	require.NoError(t, err)
	require.Len(t, resp.Feeds, 2)
	assert.True(t, resp.HasMore)
	require.NotNil(t, resp.NextCursor)
	assert.Equal(t, uint64(2), resp.NextCursor.ID)
	assert.Equal(t, 3, storage.query.Limit)
	assert.Equal(t, []string{"disabled-source"}, storage.query.ExcludedSources)
	require.NotNil(t, resp.Stats)
	assert.Equal(t, 3, resp.Stats.Total)
}

func TestAPIConfigTracksDisabledSources_BitsUT(t *testing.T) {
	disabled := false
	enabled := true
	app := &config.App{Scrape: config.Scrape{Sources: []config.ScrapeSource{
		{Name: "legacy-default"},
		{Name: "explicit-enabled", Enabled: &enabled},
		{Name: "disabled", Enabled: &disabled},
	}}}

	var got Config
	got.From(app)

	assert.Equal(t, []string{"disabled"}, got.DisabledSources)
}

func TestAPIQueryFiltersCanonicalCategory_BitsUT(t *testing.T) {
	now := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	storage := &queryStorageStub{feeds: []*block.FeedVO{
		{Feed: &model.Feed{ID: 2, Time: now, Labels: model.Labels{{Key: "category", Value: "ai-blog"}}}},
		{Feed: &model.Feed{ID: 1, Time: now.Add(-time.Minute), Labels: model.Labels{{Key: "category", Value: "image"}}}},
	}}
	got, err := new("test-category", &config.App{}, Dependencies{
		ConfigManager: &stubConfigManager{},
		FeedStorage:   storage,
	})
	require.NoError(t, err)
	svc := got.(*api)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	resp, err := svc.Query(context.Background(), &QueryRequest{
		Limit:      20,
		Categories: []string{"image"},
		SkipStats:  true,
		Start:      now.Add(-time.Hour),
		End:        now.Add(time.Hour),
	})

	require.NoError(t, err)
	require.Len(t, resp.Feeds, 1)
	assert.Equal(t, uint64(1), resp.Feeds[0].ID)
}

func TestAPIQueryAppliesCurrentSourceCategoryToHistoricalFeeds_BitsUT(t *testing.T) {
	now := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	storage := &queryStorageStub{feeds: []*block.FeedVO{{
		Feed: &model.Feed{
			ID:   1,
			Time: now,
			Labels: model.Labels{
				{Key: model.LabelSource, Value: "Configured source"},
				{Key: "category", Value: "ai-blog"},
			},
		},
	}}}
	app := &config.App{Scrape: config.Scrape{Sources: []config.ScrapeSource{{
		Name:   "Configured source",
		Labels: map[string]string{"category": "image"},
	}}}}
	got, err := new("test-category-override", app, Dependencies{
		ConfigManager: &stubConfigManager{},
		FeedStorage:   storage,
	})
	require.NoError(t, err)
	svc := got.(*api)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	resp, err := svc.Query(context.Background(), &QueryRequest{
		Limit:      20,
		Categories: []string{"image"},
		Start:      now.Add(-time.Hour),
		End:        now.Add(time.Hour),
	})

	require.NoError(t, err)
	require.Len(t, resp.Feeds, 1)
	assert.Equal(t, "image", resp.Feeds[0].Labels.Get("category"))
	require.NotNil(t, resp.Stats)
	assert.Equal(t, 1, resp.Stats.Categories["image"])
}

func TestFeedCategory_BitsUT(t *testing.T) {
	tests := []struct {
		name     string
		labels   model.Labels
		expected string
	}{
		{name: "canonical category", labels: model.Labels{{Key: "category", Value: "ai-blog"}}, expected: "ai-blog"},
		{name: "legacy category", labels: model.Labels{{Key: "category", Value: "photography"}}, expected: "image"},
		{name: "legacy source", labels: model.Labels{{Key: model.LabelSource, Value: "cs.CV updates on arXiv.org"}}, expected: "ai-paper"},
		{name: "latent space", labels: model.Labels{{Key: model.LabelSource, Value: "Latent.Space"}, {Key: "category", Value: "podcast"}}, expected: "ai-blog"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, feedCategory(&block.FeedVO{Feed: &model.Feed{Labels: tt.labels}}))
		})
	}
}

func TestGeneralPodcastExclusion_BitsUT(t *testing.T) {
	assert.True(t, isGeneralPodcast(&block.FeedVO{Feed: &model.Feed{Labels: model.Labels{
		{Key: model.LabelSource, Value: "硅谷101"},
		{Key: "podcast_url", Value: "https://example.com/audio.mp3"},
	}}}))
	assert.True(t, isGeneralPodcast(&block.FeedVO{Feed: &model.Feed{Labels: model.Labels{
		{Key: model.LabelSource, Value: "Other Podcast"},
		{Key: "category", Value: "podcast"},
	}}}))
	assert.False(t, isGeneralPodcast(&block.FeedVO{Feed: &model.Feed{Labels: model.Labels{
		{Key: model.LabelSource, Value: "Latent.Space"},
		{Key: "category", Value: "podcast"},
	}}}))
}

func TestAPIOutboundHTTPBounds_BitsUT(t *testing.T) {
	svc := newTestAPI(t, &stubConfigManager{})
	assert.Equal(t, 30*time.Second, svc.hc.Timeout)

	got, err := readLimitedBody(strings.NewReader("1234"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1234"), got)

	_, err = readLimitedBody(strings.NewReader(strings.Repeat("x", maxRSSHubBodyBytes+1)))
	require.ErrorContains(t, err, "exceeds")
}

func revisionPtr(revision string) *string {
	return &revision
}

func newTestAPI(t *testing.T, mgr config.Manager) *api {
	t.Helper()
	got, err := new("test", &config.App{}, Dependencies{ConfigManager: mgr})
	require.NoError(t, err)
	svc, ok := got.(*api)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })

	return svc
}

type queryStorageStub struct {
	component.Mock
	feeds []*block.FeedVO
	query block.QueryOptions
}

var _ feedstorage.Storage = (*queryStorageStub)(nil)

func (s *queryStorageStub) Reload(*config.App) error { return nil }

func (s *queryStorageStub) Append(context.Context, ...*model.Feed) error { return nil }

func (s *queryStorageStub) Query(_ context.Context, query block.QueryOptions) ([]*block.FeedVO, error) {
	s.query = query
	result := make([]*block.FeedVO, 0, len(s.feeds))
	if query.OnMatch != nil {
		for _, feed := range s.feeds {
			query.OnMatch(feed)
		}
	}
	for _, feed := range s.feeds {
		if query.MatchFeed == nil || query.MatchFeed(feed) {
			result = append(result, feed)
		}
	}

	return result, nil
}

func (s *queryStorageStub) Exists(context.Context, uint64, time.Time) (bool, error) {
	return false, nil
}

func (s *queryStorageStub) UpdateLabels(context.Context, uint64, time.Time, map[string]string) error {
	return nil
}

type stubConfigManager struct {
	app              *config.App
	revision         string
	savedApp         *config.App
	expectedRevision *string
	saveErr          error
}

func (m *stubConfigManager) Name() string             { return "stubConfigManager" }
func (m *stubConfigManager) Instance() string         { return "test" }
func (m *stubConfigManager) Run() error               { return nil }
func (m *stubConfigManager) Ready() <-chan struct{}   { return nil }
func (m *stubConfigManager) Close() error             { return nil }
func (m *stubConfigManager) AppConfig() *config.App   { return m.app }
func (m *stubConfigManager) Subscribe(config.Watcher) {}
func (m *stubConfigManager) AppConfigSnapshot() (*config.App, string) {
	return m.app, m.revision
}
func (m *stubConfigManager) SaveAppConfig(app *config.App, expectedRevision *string) error {
	m.savedApp = app
	m.expectedRevision = expectedRevision

	return m.saveErr
}

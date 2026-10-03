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

package scraper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/storage/kv"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	hashutil "github.com/glidea/zenfeed/pkg/util/hash"
	"github.com/glidea/zenfeed/pkg/util/retry"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
)

var clk = clock.New()

// --- Interface code block ---
type Scraper interface {
	component.Component
	Config() *Config
	Status() Status
	Trigger() error
}

type StatusState string

const (
	StatusNever    StatusState = "never"
	StatusIdle     StatusState = "idle"
	StatusRunning  StatusState = "running"
	StatusError    StatusState = "error"
	StatusDisabled StatusState = "disabled"
)

var ErrAlreadyRunning = stderrors.New("scrape is already running")

type Status struct {
	Name                string      `json:"name"`
	State               StatusState `json:"state"`
	LastAttemptAt       *time.Time  `json:"last_attempt_at,omitempty"`
	LastSuccessAt       *time.Time  `json:"last_success_at,omitempty"`
	NextRunAt           *time.Time  `json:"next_run_at,omitempty"`
	LastReceivedCount   int         `json:"last_received_count"`
	LastAppendedCount   int         `json:"last_appended_count"`
	ConsecutiveFailures int         `json:"consecutive_failures"`
	LastError           string      `json:"last_error,omitempty"`
}

type persistedStatus struct {
	Fingerprint string `json:"fingerprint"`
	Status
}

type Config struct {
	Past     time.Duration
	Interval time.Duration
	Name     string
	Labels   model.Labels
	RSS      *ScrapeSourceRSS
}

const maxPast = 15 * 24 * time.Hour

func (c *Config) Validate() error {
	if c.Past <= 0 {
		c.Past = timeutil.Day
	}
	if c.Past > maxPast {
		c.Past = maxPast
	}
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.Interval < 10*time.Minute {
		c.Interval = 10 * time.Minute
	}
	if c.Name == "" {
		return errors.New("name cannot be empty")
	}
	if c.RSS != nil {
		if err := c.RSS.Validate(); err != nil {
			return errors.Wrap(err, "invalid RSS config")
		}
	}

	return nil
}

type Dependencies struct {
	FeedStorage feed.Storage
	KVStorage   kv.Storage
}

// --- Factory code block ---
type Factory component.Factory[Scraper, Config, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Scraper, Config, Dependencies](
			func(instance string, config *Config, dependencies Dependencies) (Scraper, error) {
				m := &mockScraper{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Scraper, Config, Dependencies](new)
}

func new(instance string, config *Config, dependencies Dependencies) (Scraper, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid scraper config")
	}

	source, err := newReader(config)
	if err != nil {
		return nil, errors.Wrap(err, "creating source")
	}

	return &scraper{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "Scraper",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		source:  source,
		trigger: make(chan struct{}, 1),
		status:  Status{Name: config.Name, State: StatusNever},
	}, nil
}

// --- Implementation code block ---

type scraper struct {
	*component.Base[Config, Dependencies]

	source   reader
	trigger  chan struct{}
	busy     atomic.Bool
	statusMu sync.RWMutex
	status   Status
}

func (s *scraper) Run() (err error) {
	ctx := telemetry.StartWith(s.Context(), append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()

	// Add random offset to avoid synchronized scraping.
	offset := timeutil.Random(time.Minute)
	log.Debug(ctx, "computed scrape offset", "offset", offset)
	s.loadStatus()
	s.setNextRun(clk.Now().Add(offset))

	timer := time.NewTimer(offset)
	defer timer.Stop()
	s.MarkReady()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if s.busy.CompareAndSwap(false, true) {
				s.scrapeUntilSuccess(ctx)
				s.busy.Store(false)
			}
			timer.Reset(s.Config().Interval)
			s.setNextRun(clk.Now().Add(s.Config().Interval))
		case <-s.trigger:
			s.scrapeOnceAndRecord(ctx)
			s.busy.Store(false)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.Config().Interval)
			s.setNextRun(clk.Now().Add(s.Config().Interval))
		}
	}
}

func (s *scraper) scrapeUntilSuccess(ctx context.Context) {
	_ = retry.Backoff(ctx, func() error {
		return s.scrapeOnceAndRecord(ctx)
	}, &retry.Options{
		MinInterval: time.Minute,
		MaxInterval: 16 * time.Minute,
		MaxAttempts: retry.InfAttempts,
	})
}

func (s *scraper) scrapeOnceAndRecord(ctx context.Context) (err error) {
	s.setStatusState(StatusRunning)
	attemptedAt := clk.Now()
	received, appended := 0, 0
	defer func() { s.recordAttempt(attemptedAt, received, appended, err) }()

	opCtx := telemetry.StartWith(ctx, append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "scrape")...)
	defer func() { telemetry.End(opCtx, err) }()
	opCtx, cancel := context.WithTimeout(opCtx, 20*time.Minute)
	defer cancel()

	feeds, err := s.source.Read(opCtx)
	if err != nil {
		return errors.Wrap(err, "reading source feeds")
	}
	received = len(feeds)
	log.Debug(opCtx, "reading source feeds success", "count", received)

	processed := s.processFeeds(opCtx, feeds)
	appended = len(processed)
	log.Debug(opCtx, "processed feeds", "count", appended)
	if appended == 0 {
		return nil
	}
	if err := s.Dependencies().FeedStorage.Append(opCtx, processed...); err != nil {
		return errors.Wrap(err, "saving feeds")
	}
	log.Debug(opCtx, "appending feeds success")

	return nil
}

func (s *scraper) Trigger() error {
	if !s.busy.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	s.setStatusState(StatusRunning)
	select {
	case s.trigger <- struct{}{}:
		return nil
	default:
		s.busy.Store(false)
		return ErrAlreadyRunning
	}
}

func (s *scraper) Status() Status {
	s.statusMu.RLock()
	status := s.status
	s.statusMu.RUnlock()

	return status
}

func (s *scraper) setStatusState(state StatusState) {
	s.statusMu.Lock()
	s.status.State = state
	s.statusMu.Unlock()
}

func (s *scraper) setNextRun(next time.Time) {
	s.statusMu.Lock()
	s.status.NextRunAt = &next
	s.statusMu.Unlock()
}

func (s *scraper) recordAttempt(attemptedAt time.Time, received, appended int, attemptErr error) {
	s.statusMu.Lock()
	s.status.LastAttemptAt = &attemptedAt
	s.status.LastReceivedCount = received
	s.status.LastAppendedCount = appended
	if attemptErr != nil {
		s.status.State = StatusError
		s.status.ConsecutiveFailures++
		s.status.LastError = sanitizeStatusError(attemptErr)
	} else {
		s.status.State = StatusIdle
		succeededAt := clk.Now()
		s.status.LastSuccessAt = &succeededAt
		s.status.ConsecutiveFailures = 0
		s.status.LastError = ""
	}
	status := s.status
	s.statusMu.Unlock()
	s.persistStatus(status)
}

func (s *scraper) statusFingerprint() string {
	return configFingerprint(s.Config())
}

func configFingerprint(config *Config) string {
	b, _ := json.Marshal(config)
	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:])
}

func (s *scraper) statusKey() []byte {
	return persistedStatusKey(s.Config().Name)
}

func persistedStatusKey(name string) []byte {
	sum := sha256.Sum256([]byte(name))

	return []byte("scraper.status." + hex.EncodeToString(sum[:]))
}

func (s *scraper) persistStatus(status Status) {
	b, err := json.Marshal(persistedStatus{Fingerprint: s.statusFingerprint(), Status: status})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Dependencies().KVStorage.Set(ctx, s.statusKey(), b, 0); err != nil {
		log.Error(s.Context(), errors.Wrap(err, "persist scraper status"))
	}
}

func (s *scraper) loadStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status := LoadPersistedStatus(ctx, s.Dependencies().KVStorage, s.Config())
	if status.LastAttemptAt == nil {
		return
	}
	s.statusMu.Lock()
	s.status = status
	s.statusMu.Unlock()
}

func LoadPersistedStatus(ctx context.Context, storage kv.Storage, config *Config) Status {
	status := Status{Name: config.Name, State: StatusNever}
	b, err := storage.Get(ctx, persistedStatusKey(config.Name))
	if err != nil {
		return status
	}
	var persisted persistedStatus
	if json.Unmarshal(b, &persisted) != nil || persisted.Fingerprint != configFingerprint(config) {
		return status
	}
	persisted.Status.Name = config.Name
	persisted.Status.NextRunAt = nil
	if persisted.Status.LastAttemptAt == nil {
		persisted.Status.State = StatusNever
	} else if persisted.Status.LastError != "" {
		persisted.Status.State = StatusError
	} else {
		persisted.Status.State = StatusIdle
	}

	return persisted.Status
}

func sanitizeStatusError(err error) string {
	parts := strings.Fields(err.Error())
	for i, part := range parts {
		trimmed := strings.Trim(part, "\"'(),:")
		parsed, parseErr := url.Parse(trimmed)
		if parseErr != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			continue
		}
		parsed.User = nil
		parsed.RawQuery = ""
		parsed.Fragment = ""
		parts[i] = strings.Replace(part, trimmed, parsed.String(), 1)
	}
	message := strings.Join(parts, " ")
	if runes := []rune(message); len(runes) > 300 {
		message = string(runes[:300])
	}

	return message
}

func (s *scraper) processFeeds(ctx context.Context, feeds []*model.Feed) []*model.Feed {
	feeds = s.filterPasted(feeds)
	feeds = s.addAdditionalMetaLabels(feeds)
	feeds = s.fillIDs(feeds)
	feeds = s.filterExists(ctx, feeds)

	return feeds
}

func (s *scraper) filterPasted(feeds []*model.Feed) (filtered []*model.Feed) {
	now := clk.Now()
	for _, feed := range feeds {
		t := timeutil.MustParse(feed.Labels.Get(model.LabelPubTime))
		if timeutil.InRange(t, now.Add(-s.Config().Past), now) {
			filtered = append(filtered, feed)
		}
	}

	return filtered
}

func (s *scraper) fillIDs(feeds []*model.Feed) []*model.Feed {
	for _, feed := range feeds {
		// We can not use the pub time to join the hash,
		// because the pub time is dynamic for some sources.
		//
		// title may be changed for some sources... so...
		source := feed.Labels.Get(model.LabelSource)
		link := feed.Labels.Get(model.LabelLink)
		feed.ID = hashutil.Sum64s([]string{source, link})
	}

	return feeds
}

const (
	keyPrefix = "scraper.feed.try-append."
	ttl       = maxPast + time.Minute // Ensure the key is always available util the feed is pasted.
)

func (s *scraper) filterExists(ctx context.Context, feeds []*model.Feed) (filtered []*model.Feed) {
	appendToResult := func(feed *model.Feed) {
		key := keyPrefix + strconv.FormatUint(feed.ID, 10)
		value := timeutil.Format(feed.Time)
		if err := s.Dependencies().KVStorage.Set(ctx, []byte(key), []byte(value), ttl); err != nil {
			log.Error(ctx, err, "set last try store time")
		}
		filtered = append(filtered, feed)
	}

	for _, feed := range feeds {
		key := keyPrefix + strconv.FormatUint(feed.ID, 10)

		lastTryStored, err := s.Dependencies().KVStorage.Get(ctx, []byte(key))
		switch {
		default:
			log.Error(ctx, err, "get last stored time, fallback to continue writing")
			appendToResult(feed)

		case errors.Is(err, kv.ErrNotFound):
			appendToResult(feed)

		case err == nil:
			t, err := timeutil.Parse(string(lastTryStored))
			if err != nil {
				log.Error(ctx, err, "parse last try stored time, fallback to continue writing")
				appendToResult(feed)
				continue
			}

			exists, err := s.Dependencies().FeedStorage.Exists(ctx, feed.ID, t)
			if err != nil {
				log.Error(ctx, err, "check feed exists, fallback to continue writing")
				appendToResult(feed)
				continue
			}
			if !exists {
				appendToResult(feed)
			}
		}
	}

	return filtered
}

func (s *scraper) addAdditionalMetaLabels(feeds []*model.Feed) []*model.Feed {
	for _, feed := range feeds {
		feed.Labels = append(
			feed.Labels,
			append(s.Config().Labels, model.Label{Key: model.LabelSource, Value: s.Config().Name})...,
		)
		feed.Labels.EnsureSorted()
	}

	return feeds
}

type mockScraper struct {
	component.Mock
}

func (s *mockScraper) Config() *Config {
	args := s.Called()

	return args.Get(0).(*Config)
}

func (s *mockScraper) Status() Status { return Status{} }

func (s *mockScraper) Trigger() error { return nil }

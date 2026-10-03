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

package scrape

import (
	"context"
	stderrors "errors"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/scrape/scraper"
	"github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/storage/kv"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/lifecycle"
)

// --- Interface code block ---
type Manager interface {
	component.Component
	config.Watcher
	Statuses(context.Context) []scraper.Status
	Refresh(string) error
}

type Config struct {
	Scrapers []scraper.Config
	Sources  []sourceConfig
}

type sourceConfig struct {
	Enabled bool
	Scraper scraper.Config
}

func (c *Config) Validate() error {
	nameUnique := make(map[string]struct{})
	for i := range c.Scrapers {
		scraperCfg := &c.Scrapers[i]
		if _, exists := nameUnique[scraperCfg.Name]; exists {
			return errors.New("scraper name must be unique")
		}
		nameUnique[scraperCfg.Name] = struct{}{}
	}

	for i := range c.Scrapers {
		scraperCfg := &c.Scrapers[i]
		if err := scraperCfg.Validate(); err != nil {
			return errors.Wrapf(err, "invalid scraper %s", scraperCfg.Name)
		}
	}

	return nil
}

func (c *Config) From(app *config.App) {
	c.Scrapers = make([]scraper.Config, 0, len(app.Scrape.Sources))
	c.Sources = make([]sourceConfig, 0, len(app.Scrape.Sources))
	for i := range app.Scrape.Sources {
		source := &app.Scrape.Sources[i]
		scraperConfig := scraper.Config{
			Past:     time.Duration(app.Scrape.Past),
			Name:     source.Name,
			Interval: time.Duration(source.Interval),
			Labels:   model.Labels{},
		}
		scraperConfig.Labels.FromMap(source.Labels)
		if scraperConfig.Interval <= 0 {
			scraperConfig.Interval = time.Duration(app.Scrape.Interval)
		}
		if source.RSS != nil {
			scraperConfig.RSS = &scraper.ScrapeSourceRSS{
				URL:             source.RSS.URL,
				RSSHubEndpoint:  app.Scrape.RSSHubEndpoint,
				RSSHubRoutePath: source.RSS.RSSHubRoutePath,
				RSSHubAccessKey: app.Scrape.RSSHubAccessKey,
			}
		}
		_ = scraperConfig.Validate()
		enabled := source.IsEnabled()
		c.Sources = append(c.Sources, sourceConfig{Enabled: enabled, Scraper: scraperConfig})
		if enabled {
			c.Scrapers = append(c.Scrapers, scraperConfig)
		}
	}
}

type Dependencies struct {
	ScraperFactory scraper.Factory
	FeedStorage    feed.Storage
	KVStorage      kv.Storage
}

// --- Factory code block ---
type Factory component.Factory[Manager, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Manager, config.App, Dependencies](
			func(instance string, app *config.App, dependencies Dependencies) (Manager, error) {
				m := &mockManager{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Manager, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (Manager, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	m := &manager{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "ScrapeManager",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		scrapers:   make(map[string]scraper.Scraper, len(config.Scrapers)),
		childExits: make(chan scraperExit, 1),
	}

	for i := range config.Scrapers {
		c := &config.Scrapers[i]
		s, err := m.newScraper(c)
		if err != nil {
			return nil, stderrors.Join(
				errors.Wrapf(err, "creating scraper %s", c.Name),
				m.Close(),
			)
		}
		m.scrapers[c.Name] = s
	}

	return m, nil
}

// --- Implementation code block ---
type manager struct {
	*component.Base[Config, Dependencies]

	scrapers      map[string]scraper.Scraper
	ownedScrapers map[string]*ownedScraper
	lifecycleMu   sync.Mutex
	closed        bool
	starting      bool
	running       bool
	close         lifecycle.Once
	childExits    chan scraperExit
}

type scraperExit struct {
	child *ownedScraper
	err   error
}

var (
	ErrSourceNotFound  = errors.New("source not found")
	ErrSourceDisabled  = errors.New("source is disabled")
	ErrManagerNotReady = errors.New("scrape manager is not ready")
)

type ownedScraper struct {
	scraper.Scraper
	owner     *lifecycle.Owned
	watchOnce sync.Once
}

func newOwnedScraper(child scraper.Scraper) *ownedScraper {
	return &ownedScraper{Scraper: child, owner: lifecycle.NewOwned(child)}
}

func (s *ownedScraper) Run() error { return s.owner.Run() }

func (s *ownedScraper) Close() error {
	return s.owner.Close()
}

func (s *ownedScraper) watch(ctx context.Context, exits chan<- scraperExit) {
	s.watchOnce.Do(func() {
		go func() {
			<-s.owner.Done()
			if err := s.owner.UnexpectedExit(); err != nil {
				select {
				case exits <- scraperExit{child: s, err: err}:
				case <-ctx.Done():
				}
			}
		}()
	})
}

func (m *manager) Run() (err error) {
	ctx := telemetry.StartWith(m.Context(), append(m.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	m.lifecycleMu.Lock()
	if m.closed {
		m.lifecycleMu.Unlock()

		return errors.New("manager is closed")
	}
	if m.childExits == nil {
		m.childExits = make(chan scraperExit, 1)
	}
	m.starting = true
	scrapers := m.ownedScrapersLocked()
	m.lifecycleMu.Unlock()

	defer func() {
		err = stderrors.Join(err, m.Close())
		telemetry.End(ctx, err)
	}()

	for _, s := range scrapers {
		if err := component.RunUntilReady(ctx, s, 10*time.Second); err != nil {
			return errors.Wrapf(err, "running scraper %s", s.Config().Name)
		}
		s.watch(m.Context(), m.childExits)
	}
	if err := stagedScraperExit(scrapers); err != nil {
		return errors.Wrap(err, "scraper exited during startup")
	}

	m.lifecycleMu.Lock()
	m.starting = false
	m.running = true
	m.lifecycleMu.Unlock()
	m.MarkReady()

	for {
		select {
		case <-ctx.Done():
			return nil
		case exit := <-m.childExits:
			if m.isCurrentScraper(exit.child) {
				return errors.Wrapf(exit.err, "scraper %s exited unexpectedly", exit.child.Instance())
			}
		}
	}
}

func (m *manager) Reload(app *config.App) error {
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "invalid configuration")
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if m.closed {
		return errors.New("manager is closed")
	}
	if m.starting {
		return errors.New("manager is starting")
	}
	if reflect.DeepEqual(m.Config(), newConfig) {
		log.Debug(m.Context(), "no changes in scrape config")

		return nil
	}

	return m.reload(newConfig)
}

func (m *manager) Statuses(ctx context.Context) []scraper.Status {
	m.lifecycleMu.Lock()
	config := m.Config()
	active := make(map[string]scraper.Scraper, len(m.scrapers))
	for name, child := range m.scrapers {
		active[name] = child
	}
	m.lifecycleMu.Unlock()

	statuses := make([]scraper.Status, 0, len(config.Sources))
	for i := range config.Sources {
		source := &config.Sources[i]
		if child, ok := active[source.Scraper.Name]; ok {
			statuses = append(statuses, child.Status())
			continue
		}
		status := scraper.LoadPersistedStatus(ctx, m.Dependencies().KVStorage, &source.Scraper)
		status.State = scraper.StatusDisabled
		status.NextRunAt = nil
		statuses = append(statuses, status)
	}

	return statuses
}

func (m *manager) Refresh(name string) error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	if !m.running {
		return ErrManagerNotReady
	}
	child, ok := m.scrapers[name]
	if !ok {
		for _, source := range m.Config().Sources {
			if source.Scraper.Name == name && !source.Enabled {
				return ErrSourceDisabled
			}
		}
		return ErrSourceNotFound
	}

	return child.Trigger()
}

func (m *manager) Close() error {
	return m.close.Do(func() error {
		baseErr := m.Base.Close()

		m.lifecycleMu.Lock()
		m.closed = true
		m.starting = false
		m.running = false
		scrapers := m.ownedScrapersLocked()
		m.lifecycleMu.Unlock()

		return stderrors.Join(baseErr, lifecycle.CloseAll(scrapers...))
	})
}

func (m *manager) ownedScrapersLocked() []*ownedScraper {
	if m.ownedScrapers == nil {
		m.ownedScrapers = make(map[string]*ownedScraper, len(m.scrapers))
	}
	owned := make([]*ownedScraper, 0, len(m.scrapers))
	for name, child := range m.scrapers {
		wrapper, ok := m.ownedScrapers[name]
		if !ok {
			wrapper = newOwnedScraper(child)
			m.ownedScrapers[name] = wrapper
		}
		owned = append(owned, wrapper)
	}

	return owned
}

func (m *manager) isCurrentScraper(child *ownedScraper) bool {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	for _, current := range m.ownedScrapers {
		if current == child {
			return true
		}
	}

	return false
}

func (m *manager) newScraper(c *scraper.Config) (scraper.Scraper, error) {
	return m.Dependencies().ScraperFactory.New(
		c.Name,
		c,
		scraper.Dependencies{
			FeedStorage: m.Dependencies().FeedStorage,
			KVStorage:   m.Dependencies().KVStorage,
		},
	)
}

func (m *manager) reload(config *Config) (err error) {
	ctx := telemetry.StartWith(m.Context(), append(m.TelemetryLabels(), telemetrymodel.KeyOperation, "reload")...)
	defer func() { telemetry.End(ctx, err) }()

	newScrapers := make(map[string]scraper.Scraper, len(m.scrapers))
	newOwnedScrapers := make(map[string]*ownedScraper, len(m.scrapers))
	created := make([]*ownedScraper, 0, len(config.Scrapers))
	if err := m.runOrRestartScrapers(config, newScrapers, newOwnedScrapers, &created, m.running); err != nil {
		return stderrors.Join(
			errors.Wrap(err, "run or restart RSS scrapers"),
			errors.Wrap(lifecycle.CloseAll(created...), "close newly created scrapers"),
		)
	}
	if m.running {
		if err := stagedScraperExit(created); err != nil {
			return stderrors.Join(
				err,
				errors.Wrap(lifecycle.CloseAll(created...), "close newly created scrapers"),
			)
		}
	}
	replaced := m.replacedScrapers(newOwnedScrapers)

	m.scrapers = newScrapers
	m.ownedScrapers = newOwnedScrapers
	m.SetConfig(config)
	if m.running {
		for _, child := range created {
			child.watch(m.Context(), m.childExits)
		}
	}
	if err := lifecycle.CloseAll(replaced...); err != nil {
		log.Error(ctx, errors.Wrap(err, "stop replaced scrapers"))
	}

	return nil
}

func stagedScraperExit(scrapers []*ownedScraper) error {
	for _, child := range scrapers {
		select {
		case <-child.owner.Done():
			return errors.Wrapf(child.owner.UnexpectedExit(), "new scraper %s exited", child.Instance())
		default:
		}
	}

	return nil
}

func (m *manager) runOrRestartScrapers(
	config *Config,
	newScrapers map[string]scraper.Scraper,
	newOwnedScrapers map[string]*ownedScraper,
	created *[]*ownedScraper,
	runNew bool,
) error {
	for i := range config.Scrapers {
		c := &config.Scrapers[i]
		if err := c.Validate(); err != nil {
			return errors.Wrapf(err, "validate scraper %s", c.Name)
		}

		if err := m.runOrRestartScraper(c, newScrapers, newOwnedScrapers, created, runNew); err != nil {
			return errors.Wrapf(err, "run or restart scraper %s", c.Name)
		}
	}

	return nil
}

func (m *manager) runOrRestartScraper(
	c *scraper.Config,
	newScrapers map[string]scraper.Scraper,
	newOwnedScrapers map[string]*ownedScraper,
	created *[]*ownedScraper,
	runNew bool,
) error {
	if existing, exists := m.scrapers[c.Name]; exists {
		if reflect.DeepEqual(existing.Config(), c) {
			newScrapers[c.Name] = existing
			newOwnedScrapers[c.Name] = m.ownedScraperLocked(c.Name, existing)

			// No changed.
			return nil
		}

	}

	// Recreate & Run.
	if _, exists := newScrapers[c.Name]; !exists {
		s, err := m.newScraper(c)
		if err != nil {
			return errors.Wrap(err, "creating")
		}
		newScrapers[c.Name] = s
		owned := newOwnedScraper(s)
		newOwnedScrapers[c.Name] = owned
		*created = append(*created, owned)
		if runNew {
			if err := component.RunUntilReady(m.Context(), owned, 10*time.Second); err != nil {
				return errors.Wrap(err, "running")
			}
			select {
			case <-owned.owner.Done():
				return errors.Wrap(owned.owner.UnexpectedExit(), "new scraper exited")
			default:
			}
		}
	}

	return nil
}

func (m *manager) replacedScrapers(newScrapers map[string]*ownedScraper) []*ownedScraper {
	oldScrapers := m.ownedScrapersLocked()
	replaced := make([]*ownedScraper, 0, len(oldScrapers))
	for _, old := range oldScrapers {
		if current, exists := newScrapers[old.Instance()]; !exists || current != old {
			replaced = append(replaced, old)
		}
	}

	return replaced
}

func (m *manager) ownedScraperLocked(name string, child scraper.Scraper) *ownedScraper {
	if m.ownedScrapers == nil {
		m.ownedScrapers = make(map[string]*ownedScraper, len(m.scrapers))
	}
	owned, ok := m.ownedScrapers[name]
	if !ok {
		owned = newOwnedScraper(child)
		m.ownedScrapers[name] = owned
	}

	return owned
}

type mockManager struct {
	component.Mock
}

func (m *mockManager) Statuses(ctx context.Context) []scraper.Status {
	args := m.Called(ctx)

	return args.Get(0).([]scraper.Status)
}

func (m *mockManager) Refresh(name string) error {
	return m.Called(name).Error(0)
}

func (m *mockManager) Reload(config *config.App) error {
	args := m.Called(config)

	return args.Error(0)
}

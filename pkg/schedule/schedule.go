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

package schedule

import (
	"context"
	stderrors "errors"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/schedule/rule"
	"github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
)

// --- Interface code block ---
type Scheduler interface {
	component.Component
	config.Watcher
}

type Config struct {
	Rules []rule.Config
}

func (c *Config) Validate() error {
	names := make(map[string]struct{}, len(c.Rules))
	for _, rule := range c.Rules {
		if err := (&rule).Validate(); err != nil {
			return errors.Wrap(err, "validate rule")
		}
		if _, exists := names[rule.Name]; exists {
			return errors.Errorf("rule name %q must be unique", rule.Name)
		}
		names[rule.Name] = struct{}{}
	}

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.Rules = make([]rule.Config, len(app.Scheduls.Rules))
	for i, r := range app.Scheduls.Rules {
		c.Rules[i] = rule.Config{
			Name:          r.Name,
			Query:         r.Query,
			Threshold:     r.Threshold,
			LabelFilters:  r.LabelFilters,
			Labels:        r.Labels,
			EveryDay:      r.EveryDay,
			WatchInterval: time.Duration(r.WatchInterval),
		}
	}

	return c
}

type Dependencies struct {
	RuleFactory rule.Factory
	FeedStorage feed.Storage
	Out         chan<- *rule.Result
}

// --- Factory code block ---
type Factory component.Factory[Scheduler, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Scheduler, config.App, Dependencies](
			func(instance string, app *config.App, dependencies Dependencies) (Scheduler, error) {
				m := &mockScheduler{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Scheduler, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (Scheduler, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	s := &scheduler{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         instance,
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		rules:     make(map[string]*runningRule, len(config.Rules)),
		ruleExits: make(chan ruleExit),
	}

	for i := range config.Rules {
		r := &config.Rules[i]
		rule, err := s.newRule(r)
		if err != nil {
			return nil, stderrors.Join(
				errors.Wrapf(err, "create rule %s", r.Name),
				stopRules(runningRuleValues(s.rules)),
			)
		}
		s.rules[r.Name] = newRunningRule(rule)
	}

	return s, nil
}

// --- Implementation code block ---
type scheduler struct {
	*component.Base[Config, Dependencies]

	rules      map[string]*runningRule
	mu         sync.RWMutex
	reloadMu   sync.Mutex
	closeOnce  sync.Once
	closeErr   error
	closed     bool
	generation uint64
	ruleExits  chan ruleExit
}

func (s *scheduler) Run() (err error) {
	ctx := telemetry.StartWith(s.Context(), append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()

	s.reloadMu.Lock()
	s.mu.RLock()
	if s.closed || s.Context().Err() != nil {
		s.mu.RUnlock()
		s.reloadMu.Unlock()

		return nil
	}
	rules := cloneRunningRules(s.rules)
	s.mu.RUnlock()
	for _, r := range rules {
		if err := r.runUntilReady(ctx, 10*time.Second); err != nil {
			ruleName := r.Config().Name
			cleanupErr := stopRules(runningRuleValues(rules))
			s.reloadMu.Unlock()

			if s.Context().Err() != nil {
				return nil
			}

			return stderrors.Join(errors.Wrapf(err, "running rule %s", ruleName), cleanupErr)
		}
	}
	if err := runningRulesError(rules); err != nil {
		s.reloadMu.Unlock()

		return stderrors.Join(err, stopRules(runningRuleValues(rules)))
	}

	s.mu.Lock()
	if s.closed || s.Context().Err() != nil {
		s.mu.Unlock()
		s.reloadMu.Unlock()

		return nil
	}
	s.generation++
	s.watchRules(rules)
	s.MarkReady()
	s.mu.Unlock()
	s.reloadMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil
		case exit := <-s.ruleExits:
			if s.isCurrentRuleExit(exit) {
				return errors.Wrapf(exit.err, "rule %s exited", exit.name)
			}
		}
	}
}

func (s *scheduler) Reload(app *config.App) error {
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "validate config")
	}
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	s.mu.RLock()
	if s.closed || s.Context().Err() != nil {
		s.mu.RUnlock()

		return nil
	}
	if reflect.DeepEqual(s.Config(), newConfig) {
		s.mu.RUnlock()
		log.Debug(s.Context(), "no changes in schedule config")

		return nil
	}

	oldRules := cloneRunningRules(s.rules)
	s.mu.RUnlock()
	newRules := make(map[string]*runningRule, len(newConfig.Rules))

	if err := s.runOrRestartRules(oldRules, newConfig, newRules); err != nil {
		return errors.Wrap(err, "run or restart rules")
	}
	if err := runningRulesError(newRules); err != nil {
		return stderrors.Join(err, stopNewRules(oldRules, newRules))
	}

	s.mu.Lock()
	if s.closed || s.Context().Err() != nil {
		s.mu.Unlock()
		_ = stopNewRules(oldRules, newRules)

		return nil
	}
	s.rules = newRules
	s.SetConfig(newConfig)
	s.generation++
	s.watchRules(newRules)
	s.mu.Unlock()

	if err := stopReplacedRules(oldRules, newRules); err != nil {
		log.Error(s.Context(), errors.Wrap(err, "stop replaced rules after reload commit"))
	}

	return nil
}

func (s *scheduler) Close() error {
	s.closeOnce.Do(func() {
		var closeErrs []error
		if err := s.Base.Close(); err != nil {
			closeErrs = append(closeErrs, errors.Wrap(err, "close base"))
		}

		s.reloadMu.Lock()
		s.mu.Lock()
		s.closed = true
		rules := make([]*runningRule, 0, len(s.rules))
		for _, r := range s.rules {
			rules = append(rules, r)
		}
		s.mu.Unlock()

		if err := stopRules(rules); err != nil {
			closeErrs = append(closeErrs, err)
		}
		s.reloadMu.Unlock()
		s.closeErr = stderrors.Join(closeErrs...)
	})

	return s.closeErr
}

func (s *scheduler) newRule(config *rule.Config) (rule.Rule, error) {
	return s.Dependencies().RuleFactory.New(config.Name, config, rule.Dependencies{
		FeedStorage: s.Dependencies().FeedStorage,
		Out:         s.Dependencies().Out,
	})
}

func (s *scheduler) runOrRestartRules(
	oldRules map[string]*runningRule,
	config *Config,
	newRules map[string]*runningRule,
) error {
	for _, r := range config.Rules {
		// Reuse an unchanged rule. Replaced rules stay alive until all new rules
		// are ready, so a partial reload can be rolled back cleanly.
		if existing, exists := oldRules[r.Name]; exists {
			if reflect.DeepEqual(existing.Config(), &r) {
				newRules[r.Name] = existing

				continue
			}
		}

		// Create & Run new/updated rule.
		newRule, createErr := s.newRule(&r)
		if createErr != nil {
			return stderrors.Join(
				errors.Wrap(createErr, "create rule"),
				stopNewRules(oldRules, newRules),
			)
		}
		running := newRunningRule(newRule)
		newRules[r.Name] = running
		if runErr := running.runUntilReady(s.Context(), 10*time.Second); runErr != nil {
			delete(newRules, r.Name)
			closeErr := running.stop()

			return stderrors.Join(
				errors.Wrapf(runErr, "running rule %s", r.Name),
				errors.Wrap(closeErr, "close failed rule"),
				stopNewRules(oldRules, newRules),
			)
		}
	}

	return nil
}

func stopReplacedRules(oldRules, newRules map[string]*runningRule) error {
	rules := make([]*runningRule, 0, len(oldRules))
	for name, r := range oldRules {
		if replacement, exists := newRules[name]; !exists || replacement != r {
			rules = append(rules, r)
		}
	}

	return stopRules(rules)
}

func stopNewRules(oldRules, newRules map[string]*runningRule) error {
	rules := make([]*runningRule, 0, len(newRules))
	for name, r := range newRules {
		if existing, ok := oldRules[name]; ok && existing == r {
			continue
		}
		rules = append(rules, r)
	}

	return stopRules(rules)
}

func stopRules(rules []*runningRule) error {
	errs := make([]error, len(rules))
	var wg sync.WaitGroup
	for i, r := range rules {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = errors.Wrapf(r.stop(), "close rule %s", r.Config().Name)
		}()
	}
	wg.Wait()

	return stderrors.Join(errs...)
}

func cloneRunningRules(rules map[string]*runningRule) map[string]*runningRule {
	cloned := make(map[string]*runningRule, len(rules))
	for name, r := range rules {
		cloned[name] = r
	}

	return cloned
}

func runningRuleValues(rules map[string]*runningRule) []*runningRule {
	values := make([]*runningRule, 0, len(rules))
	for _, r := range rules {
		values = append(values, r)
	}

	return values
}

func runningRulesError(rules map[string]*runningRule) error {
	for name, r := range rules {
		select {
		case <-r.done:
			return errors.Wrapf(r.unexpectedRunError(), "rule %s exited", name)
		default:
		}
	}

	return nil
}

type ruleExit struct {
	name string
	rule *runningRule
	err  error
}

func (s *scheduler) watchRules(rules map[string]*runningRule) {
	for name, r := range rules {
		r.watchOnce.Do(func() {
			go func() {
				select {
				case <-r.done:
					exit := ruleExit{name: name, rule: r, err: r.unexpectedRunError()}
					select {
					case s.ruleExits <- exit:
					case <-s.Context().Done():
					}
				case <-s.Context().Done():
				}
			}()
		})
	}
}

func (s *scheduler) isCurrentRuleExit(exit ruleExit) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return !s.closed && s.Context().Err() == nil && s.rules[exit.name] == exit.rule
}

type runningRule struct {
	rule.Rule
	done      chan struct{}
	mu        sync.Mutex
	started   bool
	stopping  bool
	runErr    error
	exitErr   error
	watchOnce sync.Once
	closeOnce sync.Once
	closeErr  error
}

func newRunningRule(r rule.Rule) *runningRule {
	return &runningRule{Rule: r, done: make(chan struct{})}
}

func (r *runningRule) runUntilReady(ctx context.Context, timeout time.Duration) error {
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()

		return errors.New("rule lifecycle is stopping")
	}
	if !r.started {
		r.started = true
		go func() {
			err := r.Run()
			r.mu.Lock()
			r.runErr = err
			if !r.stopping {
				r.exitErr = err
			}
			r.mu.Unlock()
			close(r.done)
		}()
	}
	r.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-r.Ready():
		select {
		case <-r.done:
			return stderrors.Join(r.unexpectedRunError(), r.stop())
		default:
			return nil
		}
	case <-r.done:
		return stderrors.Join(r.unexpectedRunError(), r.stop())
	case <-timer.C:
		return stderrors.Join(errors.New("rule not ready after timeout"), r.stop())
	case <-ctx.Done():
		return stderrors.Join(ctx.Err(), r.stop())
	}
}

func (r *runningRule) stop() error {
	if r == nil {
		return nil
	}

	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.stopping = true
		started := r.started
		r.mu.Unlock()

		closeErr := r.Close()
		if started {
			<-r.done
		}
		r.closeErr = stderrors.Join(closeErr, r.exitError())
	})

	return r.closeErr
}

func (r *runningRule) unexpectedRunError() error {
	if runErr := r.exitError(); runErr != nil {
		return runErr
	}

	return errors.New("rule exited before shutdown")
}

func (r *runningRule) exitError() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.exitErr
}

type mockScheduler struct {
	component.Mock
}

func (m *mockScheduler) Reload(app *config.App) error {
	args := m.Called(app)

	return args.Error(0)
}

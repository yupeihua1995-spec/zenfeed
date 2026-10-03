package schedule

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/schedule/rule"
)

func TestRunningRuleStopJoinsRunAndIsIdempotent(t *testing.T) {
	ruleImpl := newJoinRule()
	running := newRunningRule(ruleImpl)
	require.NoError(t, running.runUntilReady(context.Background(), time.Second))

	stopDone := make(chan error, 1)
	go func() { stopDone <- running.stop() }()
	select {
	case <-ruleImpl.closed:
	case <-time.After(time.Second):
		t.Fatal("rule was not closed")
	}
	select {
	case err := <-stopDone:
		t.Fatalf("stop returned before Run exited: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(ruleImpl.releaseRun)
	require.NoError(t, receiveScheduleError(t, stopDone))
	require.NoError(t, running.stop())
	require.Equal(t, int32(1), ruleImpl.closeCalls.Load())
}

func TestConfigValidateRejectsDuplicateRuleNames(t *testing.T) {
	config := &Config{Rules: []rule.Config{{Name: "duplicate"}, {Name: "duplicate"}}}

	require.EqualError(t, config.Validate(), `rule name "duplicate" must be unique`)
}

func TestNewClosesCreatedRulesWhenFactoryFails(t *testing.T) {
	factoryErr := errors.New("create second rule")
	created := newExitingRule("first")
	call := 0
	factory := lifecycleRuleFactory{newRuleWithError: func(config *rule.Config) (rule.Rule, error) {
		call++
		if call == 2 {
			return nil, factoryErr
		}

		return created, nil
	}}
	app := &config.App{}
	app.Scheduls.Rules = []config.SchedulsRule{{Name: "first"}, {Name: "second"}}

	scheduler, err := new("test", app, Dependencies{RuleFactory: factory})
	require.Nil(t, scheduler)
	require.ErrorIs(t, err, factoryErr)
	require.Equal(t, int32(1), created.closeCalls.Load())
}

func TestRunningRuleStopSuppressesShutdownInducedRunError(t *testing.T) {
	shutdownErr := errors.New("shutdown interrupted rule")
	ruleImpl := newExitingRule("shutdown")
	ruleImpl.shutdownErr = shutdownErr
	running := newRunningRule(ruleImpl)
	require.NoError(t, running.runUntilReady(context.Background(), time.Second))

	require.NoError(t, running.stop())
	require.NoError(t, running.stop())
	require.Equal(t, int32(1), ruleImpl.closeCalls.Load())
}

func TestSchedulerReloadRejectsRuleThatExitsWhileNextRuleStarts(t *testing.T) {
	stagedErr := errors.New("staged rule failed")
	first := newExitingRule("first")
	first.runReturned = make(chan struct{})
	second := newExitingRule("second")
	second.runStarted = make(chan struct{})
	second.readyGate = make(chan struct{})
	factory := lifecycleRuleFactory{newRule: func(config *rule.Config) rule.Rule {
		if config.Name == "first" {
			return first
		}

		return second
	}}
	s := newLifecycleScheduler(map[string]*runningRule{}, factory)
	oldConfig := s.Config()
	app := &config.App{}
	app.Scheduls.Rules = []config.SchedulsRule{{Name: "first"}, {Name: "second"}}

	reloadDone := make(chan error, 1)
	go func() { reloadDone <- s.Reload(app) }()
	requireScheduleSignal(t, second.runStarted, "second staged rule did not start")
	first.exit <- stagedErr
	requireScheduleSignal(t, first.runReturned, "first staged rule did not exit")
	close(second.readyGate)

	err := receiveScheduleError(t, reloadDone)
	require.ErrorIs(t, err, stagedErr)
	require.Empty(t, s.rules)
	require.Same(t, oldConfig, s.Config())
	require.Equal(t, uint64(0), s.generation)
}

func TestSchedulerRunReportsCurrentRuleExitAndIgnoresOldGeneration(t *testing.T) {
	oldRule := newExitingRule("old")
	oldRunning := newRunningRule(oldRule)
	s := newLifecycleScheduler(map[string]*runningRule{"old": oldRunning})

	runDone := make(chan error, 1)
	go func() { runDone <- s.Run() }()
	requireScheduleSignal(t, s.Ready(), "scheduler did not become ready")

	newRule := newExitingRule("new")
	newRunning := newRunningRule(newRule)
	require.NoError(t, newRunning.runUntilReady(s.Context(), time.Second))
	s.mu.Lock()
	s.rules = map[string]*runningRule{"new": newRunning}
	s.generation++
	s.watchRules(s.rules)
	s.mu.Unlock()
	require.NoError(t, oldRunning.stop())

	select {
	case err := <-runDone:
		t.Fatalf("old generation exit stopped scheduler: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	currentErr := errors.New("current rule failed")
	newRule.exit <- currentErr
	err := receiveScheduleError(t, runDone)
	require.ErrorIs(t, err, currentErr)
	require.ErrorIs(t, s.Close(), currentErr)
}

func TestSchedulerReloadReturnsSuccessAfterCommittedCleanupError(t *testing.T) {
	cleanupErr := errors.New("old rule cleanup failed")
	oldRule := newExitingRule("old")
	oldRule.closeErr = cleanupErr
	oldRunning := newRunningRule(oldRule)
	require.NoError(t, oldRunning.runUntilReady(context.Background(), time.Second))

	factory := lifecycleRuleFactory{newRule: func(config *rule.Config) rule.Rule {
		return newExitingRule(config.Name)
	}}
	s := newLifecycleScheduler(map[string]*runningRule{"old": oldRunning}, factory)
	s.SetConfig(&Config{Rules: []rule.Config{{Name: "old"}}})
	app := &config.App{}
	app.Scheduls.Rules = []config.SchedulsRule{{Name: "new"}}

	require.NoError(t, s.Reload(app), "a committed reload must not report old-generation cleanup failure")
	require.Contains(t, s.rules, "new")
	require.NotContains(t, s.rules, "old")
	require.Equal(t, "new", s.Config().Rules[0].Name)
	require.NoError(t, s.Close())
}

func TestSchedulerWatchRulesStartsOneWatcherPerRunningRule(t *testing.T) {
	ruleImpl := newExitingRule("reused")
	running := newRunningRule(ruleImpl)
	require.NoError(t, running.runUntilReady(context.Background(), time.Second))
	s := newLifecycleScheduler(map[string]*runningRule{"reused": running})
	s.ruleExits = make(chan ruleExit, 10)

	for range 10 {
		s.watchRules(s.rules)
	}
	ruleImpl.exit <- errors.New("rule failed")
	select {
	case <-s.ruleExits:
	case <-time.After(time.Second):
		t.Fatal("rule watcher did not report the exit")
	}
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, s.ruleExits, "reused rule accumulated duplicate watchers")
	_ = running.stop()
}

func newLifecycleScheduler(rules map[string]*runningRule, factories ...rule.Factory) *scheduler {
	dependencies := Dependencies{}
	if len(factories) > 0 {
		dependencies.RuleFactory = factories[0]
	}
	return &scheduler{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "Scheduler", Config: &Config{}, Dependencies: dependencies,
		}),
		rules:     rules,
		ruleExits: make(chan ruleExit),
	}
}

type exitingRule struct {
	*component.Base[rule.Config, rule.Dependencies]
	exit        chan error
	closeErr    error
	shutdownErr error
	closeCalls  atomic.Int32
	runStarted  chan struct{}
	runReturned chan struct{}
	readyGate   chan struct{}
}

func newExitingRule(name string) *exitingRule {
	return &exitingRule{
		Base: component.New(&component.BaseConfig[rule.Config, rule.Dependencies]{
			Name: "ExitingRule", Config: &rule.Config{Name: name},
		}),
		exit: make(chan error, 1),
	}
}

func (r *exitingRule) Run() error {
	if r.runStarted != nil {
		close(r.runStarted)
	}
	defer func() {
		if r.runReturned != nil {
			close(r.runReturned)
		}
	}()
	if r.readyGate != nil {
		select {
		case <-r.readyGate:
		case <-r.Context().Done():
			return r.shutdownErr
		}
	}
	r.MarkReady()
	select {
	case err := <-r.exit:
		return err
	case <-r.Context().Done():
		return r.shutdownErr
	}
}

func (r *exitingRule) Close() error {
	r.closeCalls.Add(1)
	_ = r.Base.Close()

	return r.closeErr
}

type lifecycleRuleFactory struct {
	newRule          func(*rule.Config) rule.Rule
	newRuleWithError func(*rule.Config) (rule.Rule, error)
}

func (f lifecycleRuleFactory) New(
	_ string,
	config *rule.Config,
	_ rule.Dependencies,
) (rule.Rule, error) {
	if f.newRuleWithError != nil {
		return f.newRuleWithError(config)
	}

	return f.newRule(config), nil
}

type joinRule struct {
	*component.Base[rule.Config, rule.Dependencies]
	closed     chan struct{}
	releaseRun chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newJoinRule() *joinRule {
	return &joinRule{
		Base: component.New(&component.BaseConfig[rule.Config, rule.Dependencies]{
			Name: "JoinRule", Config: &rule.Config{Name: "join-rule"},
		}),
		closed:     make(chan struct{}),
		releaseRun: make(chan struct{}),
	}
}

func (r *joinRule) Run() error {
	r.MarkReady()
	<-r.Context().Done()
	<-r.releaseRun

	return nil
}

func (r *joinRule) Close() error {
	r.closeCalls.Add(1)
	r.closeOnce.Do(func() {
		_ = r.Base.Close()
		close(r.closed)
	})

	return nil
}

func receiveScheduleError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rule shutdown")
		return nil
	}
}

func requireScheduleSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

var _ rule.Rule = (*joinRule)(nil)
var _ rule.Rule = (*exitingRule)(nil)

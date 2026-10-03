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

package component

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"

	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
)

// Global is the instance name for the global component.
const Global = "Global"

// Component is the interface for a component.
// It is used to start, stop and monitor a component.
// A component means it is runnable, has some async work to do.
// ALL exported biz structs MUST implement this interface.
type Component interface {
	// Name returns the name of the component. e.g. "KVStorage".
	// It SHOULD be unique between different components.
	// It will used as the telemetry info. e.g. log, metrics, etc.
	Name() string
	// Instance returns the instance name of the component. e.g. "kvstorage-1".
	// It SHOULD be unique between different instances of the same component.
	// It will used as the telemetry info. e.g. log, metrics, etc.
	Instance() string
	// Run starts the component.
	// It blocks until the component is closed.
	// It MUST be called only once.
	// After Close returns, Run MUST return promptly.
	Run() (err error)
	// Ready returns a channel that is closed when the component is ready.
	// Returns a chan to notify the component is ready when Run is called.
	Ready() (notify <-chan struct{})
	// Close closes the component and unblocks Run.
	// It MUST be safe before, during, or after Run, MUST be safe to call more
	// than once, and SHOULD return promptly.
	Close() (err error)
}

// Base is the base implementation of a component.
// It provides partial, default implementations of the Component interface.
// It SHOULD BE used as an embedded field in the actual component implementation.
type Base[Config any, Dependencies any] struct {
	baseConfig      *BaseConfig[Config, Dependencies]
	telemetryLabels telemetry.Labels
	mu              sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	ch     chan struct{}

	readyOnce sync.Once
	closeOnce sync.Once
}

type BaseConfig[Config any, Dependencies any] struct {
	Name                      string
	Instance                  string
	AdditionalTelemetryLabels telemetry.Labels
	Config                    *Config
	Dependencies              Dependencies
}

func New[Config any, Dependencies any](config *BaseConfig[Config, Dependencies]) *Base[Config, Dependencies] {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	telemetryLabels := telemetry.Labels{
		telemetrymodel.KeyComponent, config.Name,
		telemetrymodel.KeyComponentInstance, config.Instance,
	}
	telemetryLabels = append(telemetryLabels, config.AdditionalTelemetryLabels...)

	return &Base[Config, Dependencies]{
		telemetryLabels: telemetryLabels,
		baseConfig:      config,
		ctx:             ctx,
		cancel:          cancel,
		ch:              ch,
	}
}

func (c *Base[Config, Dependencies]) Name() string {
	return c.baseConfig.Name
}

func (c *Base[Config, Dependencies]) Instance() string {
	return c.baseConfig.Instance
}

func (c *Base[Config, Dependencies]) TelemetryLabels() telemetry.Labels {
	return c.telemetryLabels
}

func (c *Base[Config, Dependencies]) TelemetryLabelsID() prometheus.Labels {
	return prometheus.Labels{
		telemetrymodel.KeyComponent:         c.telemetryLabels.Get(telemetrymodel.KeyComponent).(string),
		telemetrymodel.KeyComponentInstance: c.telemetryLabels.Get(telemetrymodel.KeyComponentInstance).(string),
	}
}

func (c *Base[Config, Dependencies]) TelemetryLabelsIDFields() []string {
	return []string{
		c.telemetryLabels.Get(telemetrymodel.KeyComponent).(string),
		c.telemetryLabels.Get(telemetrymodel.KeyComponentInstance).(string),
	}
}

func (c *Base[Config, Dependencies]) Config() *Config {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.baseConfig.Config
}

func (c *Base[Config, Dependencies]) SetConfig(config *Config) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baseConfig.Config = config
}

func (c *Base[Config, Dependencies]) Dependencies() Dependencies {
	return c.baseConfig.Dependencies
}

func (c *Base[Config, Dependencies]) Context() context.Context {
	return c.ctx
}

func (c *Base[Config, Dependencies]) Run() error {
	c.MarkReady()
	<-c.ctx.Done()

	return nil
}

func (c *Base[Config, Dependencies]) MarkReady() {
	c.readyOnce.Do(func() {
		close(c.ch)
	})
}

func (c *Base[Config, Dependencies]) Ready() <-chan struct{} {
	return c.ch
}

func (c *Base[Config, Dependencies]) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		telemetry.CloseMetrics(c.TelemetryLabelsID())
		log.Info(c.Context(), "component closed", c.TelemetryLabels()...)
	})

	return nil
}

type Factory[ComponentImpl Component, Config any, Dependencies any] interface {
	New(instance string, config *Config, dependencies Dependencies) (ComponentImpl, error)
}

type FactoryFunc[ComponentImpl Component, Config any, Dependencies any] func(
	instance string,
	config *Config,
	dependencies Dependencies,
) (ComponentImpl, error)

func (f FactoryFunc[ComponentImpl, Config, Dependencies]) New(
	instance string,
	config *Config,
	dependencies Dependencies,
) (ComponentImpl, error) {
	return f(instance, config, dependencies)
}

type Mock struct {
	mock.Mock
}

func (m *Mock) Name() string {
	return m.Called().String(0)
}
func (m *Mock) Instance() string {
	return m.Called().String(0)
}
func (m *Mock) Run() error {
	return m.Called().Error(0)
}
func (m *Mock) Ready() <-chan struct{} {
	return m.Called().Get(0).(<-chan struct{})
}
func (m *Mock) Close() error {
	return m.Called().Error(0)
}

type MockOption func(m *mock.Mock)

type MockOptions []MockOption

func (m MockOptions) Apply(mock *Mock) {
	for _, opt := range m {
		opt(&mock.Mock)
	}
}

func RunUntilReady(waitCtx context.Context, component Component, timeout time.Duration) error {
	if err := waitCtx.Err(); err != nil {
		return err
	}

	runResultCh := make(chan error, 1)
	go func() {
		runResultCh <- component.Run()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-component.Ready():
		// Prefer an already completed Run over readiness. A component that exits
		// before this helper returns is not usable even if it briefly signaled ready.
		select {
		case runErr := <-runResultCh:
			return stopExitedComponent(component, runBeforeReadyError(component, runErr), timeout)
		default:
		}

		log.Info(waitCtx, "component run and ready",
			telemetrymodel.KeyComponent, component.Name(),
			telemetrymodel.KeyComponentInstance, component.Instance(),
		)

		return nil
	case runErr := <-runResultCh:
		return stopExitedComponent(component, runBeforeReadyError(component, runErr), timeout)
	case <-timer.C:
		return stopComponentAndWait(
			component,
			runResultCh,
			errors.New("component not ready after timeout"),
			timeout,
		)
	case <-waitCtx.Done():
		return stopComponentAndWait(component, runResultCh, waitCtx.Err(), timeout)
	}
}

// ErrComponentShutdownTimeout means Close or Run did not finish within the
// shutdown grace period used by RunUntilReady. The original timeout or context
// error is joined with this error so callers can still inspect both causes.
var ErrComponentShutdownTimeout = errors.New("component shutdown timed out")

const defaultComponentShutdownTimeout = 30 * time.Second

func runBeforeReadyError(component Component, runErr error) error {
	if runErr != nil {
		return runErr
	}

	return fmt.Errorf(
		"component %s/%s exited before becoming ready",
		component.Name(),
		component.Instance(),
	)
}

func stopExitedComponent(component Component, cause error, timeout time.Duration) error {
	runResultCh := make(chan error, 1)
	runResultCh <- nil

	return stopComponentAndWait(component, runResultCh, cause, timeout)
}

func stopComponentAndWait(
	component Component,
	runResultCh <-chan error,
	cause error,
	timeout time.Duration,
) error {
	closeResultCh := make(chan error, 1)
	go func() {
		closeResultCh <- component.Close()
	}()

	shutdownTimeout := timeout
	if shutdownTimeout <= 0 || shutdownTimeout > defaultComponentShutdownTimeout {
		shutdownTimeout = defaultComponentShutdownTimeout
	}
	timer := time.NewTimer(shutdownTimeout)
	defer timer.Stop()

	var runErr, closeErr error
	runWait := runResultCh
	closeWait := (<-chan error)(closeResultCh)
waitForShutdown:
	for runWait != nil || closeWait != nil {
		select {
		case runErr = <-runWait:
			runWait = nil
		case closeErr = <-closeWait:
			closeWait = nil
		case <-timer.C:
			// A result may become available at the deadline. Drain both channels
			// before deciding which operations are still outstanding.
			if runWait != nil {
				select {
				case runErr = <-runWait:
					runWait = nil
				default:
				}
			}
			if closeWait != nil {
				select {
				case closeErr = <-closeWait:
					closeWait = nil
				default:
				}
			}
			if runWait == nil && closeWait == nil {
				break waitForShutdown
			}

			pending := "Run"
			if runWait == nil {
				pending = "Close"
			} else if closeWait != nil {
				pending = "Close and Run"
			}

			return errors.Join(
				cause,
				closeError(closeErr),
				runShutdownError(runErr),
				fmt.Errorf(
					"%w after %s waiting for component %s/%s %s",
					ErrComponentShutdownTimeout,
					shutdownTimeout,
					component.Name(),
					component.Instance(),
					pending,
				),
			)
		}
	}

	var shutdownErrors []error
	shutdownErrors = append(shutdownErrors, cause)
	if closeErr != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("close component: %w", closeErr))
	}
	if runErr != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("component exited while shutting down: %w", runErr))
	}
	if len(shutdownErrors) == 1 {
		return cause
	}

	return errors.Join(shutdownErrors...)
}

func closeError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("close component: %w", err)
}

func runShutdownError(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("component exited while shutting down: %w", err)
}

type Group []Component

type componentRunResult struct {
	component Component
	name      string
	instance  string
	err       error
}

type preparedComponent struct {
	component Component
	name      string
	instance  string
	ready     <-chan struct{}
}

type startCoordinator struct {
	mu             sync.Mutex
	stopping       bool
	firstResult    *componentRunResult
	firstPublished chan struct{}
	runResultCh    chan componentRunResult
}

func newStartCoordinator(resultBuffer int) *startCoordinator {
	return &startCoordinator{
		firstPublished: make(chan struct{}),
		runResultCh:    make(chan componentRunResult, resultBuffer),
	}
}

func (c *startCoordinator) authorizeGroup() (componentRunResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.firstResult != nil {
		if c.firstResult != nil {
			return *c.firstResult, false
		}

		return componentRunResult{}, false
	}

	return componentRunResult{}, true
}

func (c *startCoordinator) publish(result componentRunResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if result.err == nil && !c.stopping {
		result.err = fmt.Errorf(
			"component %s/%s exited unexpectedly",
			result.name,
			result.instance,
		)
	}
	if c.firstResult == nil {
		first := result
		c.firstResult = &first
		close(c.firstPublished)
	}
	c.runResultCh <- result
}

func (c *startCoordinator) beginStopping() {
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
}

func Run(ctx context.Context, groups ...Group) error {
	return runGroups(ctx, newStartCoordinator(componentCount(groups)), groups...)
}

func runGroups(ctx context.Context, coordinator *startCoordinator, groups ...Group) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := ctx.Err(); err != nil {
		return err
	}

	// Start groups in order.
	runWGs := make([]sync.WaitGroup, len(groups))
	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			coordinator.beginStopping()
			closeErr := stopGroupsAndWait(groups, runWGs, i-1)
			if runErr := firstRunError(coordinator.runResultCh, err, context.Canceled); runErr != nil {
				return joinErrors(runErr, closeErr)
			}

			return joinErrors(err, closeErr)
		}
		prepared := prepareGroup(ctx, group)
		if err := ctx.Err(); err != nil {
			coordinator.beginStopping()
			closeErr := stopGroupsAndWait(groups, runWGs, i-1)
			if runErr := firstRunError(coordinator.runResultCh, err, context.Canceled); runErr != nil {
				return joinErrors(runErr, closeErr)
			}

			return joinErrors(err, closeErr)
		}
		if result, authorized := coordinator.authorizeGroup(); !authorized {
			coordinator.beginStopping()
			closeErr := stopGroupsAndWait(groups, runWGs, i-1)

			return joinErrors(componentRunError(result), closeErr)
		}
		startComponents(ctx, prepared, coordinator, &runWGs[i])
		if err := waitForGroupReady(ctx, prepared, coordinator.runResultCh); err != nil {
			coordinator.beginStopping()
			closeErr := stopGroupsAndWait(groups, runWGs, i)
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				if runErr := firstRunError(coordinator.runResultCh, ctxErr, context.Canceled); runErr != nil {
					return joinErrors(runErr, closeErr)
				}

				return joinErrors(err, closeErr)
			}

			return joinErrors(err, closeErr)
		}
	}

	// All groups started successfully, wait for any component to fail or context to be canceled.
	select {
	case result := <-coordinator.runResultCh:
		coordinator.beginStopping()
		runErr := result.err
		closeErr := stopGroupsAndWait(groups, runWGs, len(groups)-1)

		return joinErrors(runErr, closeErr)

	case <-ctx.Done():
		coordinator.beginStopping()
		closeErr := stopGroupsAndWait(groups, runWGs, len(groups)-1)
		// All Run goroutines have now published their results. Prefer a concrete
		// runtime failure over cancellation, but ignore the cancellation itself.
		runErr := firstRunError(coordinator.runResultCh, ctx.Err(), context.Canceled)

		return joinErrors(runErr, closeErr)
	}
}

func componentCount(groups []Group) int {
	count := 0
	for _, group := range groups {
		count += len(group)
	}
	if count == 0 {
		return 1
	}

	return count
}

func prepareGroup(ctx context.Context, group Group) []preparedComponent {
	gCtx := log.With(ctx, telemetrymodel.KeyComponent, "group")
	log.Info(gCtx, "starting group", "components", len(group))
	prepared := make([]preparedComponent, 0, len(group))
	for _, component := range group {
		item := preparedComponent{
			component: component,
			name:      component.Name(),
			instance:  component.Instance(),
			ready:     component.Ready(),
		}
		log.Info(gCtx, "preparing component",
			telemetrymodel.KeyComponent, item.name,
			telemetrymodel.KeyComponentInstance, item.instance,
		)
		prepared = append(prepared, item)
	}

	return prepared
}

func startComponents(
	ctx context.Context,
	group []preparedComponent,
	coordinator *startCoordinator,
	runWG *sync.WaitGroup,
) {
	for _, item := range group {
		runWG.Add(1)
		go func(c preparedComponent) {
			defer runWG.Done()
			err := c.component.Run()
			coordinator.publish(componentRunResult{
				component: c.component,
				name:      c.name,
				instance:  c.instance,
				err:       err,
			})
			log.Info(ctx, "component exited",
				telemetrymodel.KeyComponent, c.name,
				telemetrymodel.KeyComponentInstance, c.instance,
			)
		}(item)
	}
}

func waitForGroupReady(ctx context.Context, group []preparedComponent, runResultCh chan componentRunResult) error {
	for _, comp := range group {
		timer := time.NewTimer(30 * time.Second)
		select {
		case <-comp.ready:
			timer.Stop()
			select {
			case result := <-runResultCh:
				return componentRunError(result)
			default:
			}
			log.Info(ctx, "component run and ready",
				telemetrymodel.KeyComponent, comp.name,
				telemetrymodel.KeyComponentInstance, comp.instance,
			)
		case result := <-runResultCh:
			timer.Stop()
			return componentRunError(result)
		case <-timer.C:
			select {
			case result := <-runResultCh:
				return componentRunError(result)
			default:
			}
			select {
			case <-comp.ready:
				continue
			default:
			}

			return errors.New("not ready after 30 seconds")
		case <-ctx.Done():
			timer.Stop()
			select {
			case result := <-runResultCh:
				return componentRunError(result)
			default:
			}
			select {
			case <-comp.ready:
				continue
			default:
			}

			return ctx.Err()
		}
	}

	return nil
}

func componentRunError(result componentRunResult) error {
	return result.err
}

func firstRunError(runResultCh <-chan componentRunResult, ignored ...error) error {
	for {
		select {
		case result := <-runResultCh:
			if result.err != nil && !matchesAnyError(result.err, ignored) {
				return result.err
			}
		default:
			return nil
		}
	}
}

func matchesAnyError(err error, targets []error) bool {
	for _, target := range targets {
		if target != nil && errors.Is(err, target) {
			return true
		}
	}

	return false
}

func joinErrors(errs ...error) error {
	joined := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			joined = append(joined, err)
		}
	}

	switch len(joined) {
	case 0:
		return nil
	case 1:
		return joined[0]
	default:
		return errors.Join(joined...)
	}
}

// stopGroupsAndWait shuts groups down in reverse dependency order. Each group
// is fully closed and joined before the next group is touched. The waits are
// intentionally unbounded: Run must never return while a started Component.Run
// goroutine is still alive, so Component.Close must promptly unblock Run.
func stopGroupsAndWait(groups []Group, runWGs []sync.WaitGroup, runAt int) error {
	var errs []error
	for i := runAt; i >= 0; i-- {
		if err := stopGroup(groups[i]); err != nil {
			errs = append(errs, fmt.Errorf("close group %d: %w", i, err))
		}
		runWGs[i].Wait()
	}

	return joinErrors(errs...)
}

func stopGroup(group Group) error {
	errs := make([]error, len(group))
	var wg sync.WaitGroup
	for i, comp := range group {
		wg.Add(1)
		go func(index int, c Component) {
			defer wg.Done()
			if err := c.Close(); err != nil {
				errs[index] = fmt.Errorf(
					"component %s/%s: %w",
					c.Name(),
					c.Instance(),
					err,
				)
			}
		}(i, comp)
	}
	wg.Wait()

	return joinErrors(errs...)
}

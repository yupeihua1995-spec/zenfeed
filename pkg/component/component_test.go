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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const lifecycleTestDeadline = time.Second

type lifecycleTestComponent struct {
	name      string
	nameFunc  func() string
	ready     chan struct{}
	run       func() error
	close     func() error
	closeOnce sync.Once
	closed    atomic.Bool
}

func (c *lifecycleTestComponent) Name() string {
	if c.nameFunc != nil {
		return c.nameFunc()
	}

	return c.name
}

func (c *lifecycleTestComponent) Instance() string {
	return "test"
}

func (c *lifecycleTestComponent) Run() error {
	return c.run()
}

func (c *lifecycleTestComponent) Ready() <-chan struct{} {
	return c.ready
}

func (c *lifecycleTestComponent) Close() (err error) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if c.close != nil {
			err = c.close()
		}
	})

	return err
}

func TestRunUntilReadyReadyDoesNotCloseComponent(t *testing.T) {
	ready := make(chan struct{})
	stop := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "ready",
		ready: ready,
		run: func() error {
			defer close(runExited)
			close(ready)
			<-stop

			return nil
		},
		close: func() error {
			close(stop)

			return nil
		},
	}
	t.Cleanup(func() {
		_ = component.Close()
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	err := RunUntilReady(context.Background(), component, lifecycleTestDeadline)

	require.NoError(t, err)
	require.False(t, component.closed.Load(), "a ready component must remain running")
}

func TestRunUntilReadyDoesNotStartWithCanceledContext(t *testing.T) {
	var runCalled atomic.Bool
	component := &lifecycleTestComponent{
		name:  "not-started",
		ready: make(chan struct{}),
		run: func() error {
			runCalled.Store(true)

			return nil
		},
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := RunUntilReady(waitCtx, component, lifecycleTestDeadline)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, runCalled.Load())
	require.False(t, component.closed.Load(), "a component that never started must not be closed")
}

func TestRunUntilReadyTimeoutClosesAndJoins(t *testing.T) {
	stop := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "timeout",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)
			<-stop

			return nil
		},
		close: func() error {
			close(stop)

			return nil
		},
	}
	t.Cleanup(func() {
		_ = component.Close()
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	err := RunUntilReady(context.Background(), component, 10*time.Millisecond)
	runJoined := channelClosed(runExited)

	require.EqualError(t, err, "component not ready after timeout")
	require.True(t, component.closed.Load(), "timeout must close the component")
	require.True(t, runJoined, "timeout must join the component Run goroutine")
}

func TestRunUntilReadyCancellationClosesAndJoins(t *testing.T) {
	runStarted := make(chan struct{})
	stop := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "canceled",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)
			close(runStarted)
			<-stop

			return nil
		},
		close: func() error {
			close(stop)

			return nil
		},
	}
	t.Cleanup(func() {
		_ = component.Close()
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	waitCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunUntilReady(waitCtx, component, lifecycleTestDeadline)
	}()
	requireChannelClosed(t, runStarted, "component Run to start")
	cancel()

	err := receiveError(t, errCh, "RunUntilReady to return after cancellation")
	runJoined := channelClosed(runExited)

	require.ErrorIs(t, err, context.Canceled)
	require.True(t, component.closed.Load(), "cancellation must close the component")
	require.True(t, runJoined, "cancellation must join the component Run goroutine")
}

func TestRunUntilReadyReportsBoundedJoinTimeout(t *testing.T) {
	waitCtx, cancel := context.WithCancel(context.Background())
	runStarted := make(chan struct{})
	allowRunExit := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "stuck-run",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)
			close(runStarted)
			<-allowRunExit

			return nil
		},
	}
	t.Cleanup(func() {
		close(allowRunExit)
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunUntilReady(waitCtx, component, 10*time.Millisecond)
	}()
	requireChannelClosed(t, runStarted, "component Run to start")
	cancel()

	err := receiveError(t, errCh, "RunUntilReady to report a bounded join timeout")

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrComponentShutdownTimeout)
	require.True(t, component.closed.Load(), "cancellation must still call Close")
}

func TestRunUntilReadyReportsBoundedCloseTimeout(t *testing.T) {
	waitCtx, cancel := context.WithCancel(context.Background())
	runStarted := make(chan struct{})
	closeStarted := make(chan struct{})
	allowClose := make(chan struct{})
	closeExited := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "stuck-close",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)
			close(runStarted)
			<-closeStarted

			return nil
		},
		close: func() error {
			defer close(closeExited)
			close(closeStarted)
			<-allowClose

			return nil
		},
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(allowClose)
		})
	}
	t.Cleanup(func() {
		release()
		requireChannelClosed(t, closeExited, "component Close to exit during cleanup")
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunUntilReady(waitCtx, component, 10*time.Millisecond)
	}()
	requireChannelClosed(t, runStarted, "component Run to start")
	cancel()
	requireChannelClosed(t, closeStarted, "component Close to start")

	err := receiveError(t, errCh, "RunUntilReady to report a bounded Close timeout")

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrComponentShutdownTimeout)
	require.ErrorContains(t, err, "Close")
	release()
}

func TestRunUntilReadyReturnsRunError(t *testing.T) {
	runErr := errors.New("run failed")
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "run-error",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)

			return runErr
		},
	}

	err := RunUntilReady(context.Background(), component, lifecycleTestDeadline)

	require.ErrorIs(t, err, runErr)
	require.True(t, channelClosed(runExited), "observing the run error must join Run")
	require.True(t, component.closed.Load(), "a failed startup must be closed for partial cleanup")
}

func TestRunUntilReadyRejectsExitBeforeReady(t *testing.T) {
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "early-exit",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)

			return nil
		},
	}

	err := RunUntilReady(context.Background(), component, lifecycleTestDeadline)

	require.ErrorContains(t, err, "exited before becoming ready")
	require.True(t, channelClosed(runExited), "observing the early exit must join Run")
	require.True(t, component.closed.Load(), "an early exit must be closed for partial cleanup")
}

func TestRunContextShutdownJoinsStartedComponents(t *testing.T) {
	ready := make(chan struct{})
	stop := make(chan struct{})
	closeCalled := make(chan struct{})
	allowRunExit := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "shutdown",
		ready: ready,
		run: func() error {
			defer close(runExited)
			close(ready)
			<-stop
			<-allowRunExit

			return nil
		},
		close: func() error {
			close(stop)
			close(closeCalled)

			return nil
		},
	}
	var releaseOnce sync.Once
	releaseRun := func() {
		releaseOnce.Do(func() { close(allowRunExit) })
	}
	t.Cleanup(func() {
		releaseRun()
		_ = component.Close()
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(ctx, Group{component})
	}()
	requireChannelClosed(t, ready, "component to become ready")
	cancel()
	requireChannelClosed(t, closeCalled, "component Close to be called")

	requireNoValue(t, runResult, "Run returned before the component Run goroutine exited")
	releaseRun()

	require.NoError(t, receiveError(t, runResult, "Run to return after joining the component"))
	require.True(t, channelClosed(runExited))
}

func TestRunDoesNotStartWithCanceledContext(t *testing.T) {
	var runCalled atomic.Bool
	component := &lifecycleTestComponent{
		name:  "not-started",
		ready: make(chan struct{}),
		run: func() error {
			runCalled.Store(true)

			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Run(ctx, Group{component})

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, runCalled.Load())
	require.False(t, component.closed.Load(), "a component that never started must not be closed")
}

func TestRunPrefersRuntimeErrorOverConcurrentCancellation(t *testing.T) {
	runErr := errors.New("runtime failed during cancellation")
	ready := make(chan struct{})
	stop := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "concurrent-error",
		ready: ready,
		run: func() error {
			defer close(runExited)
			close(ready)
			<-stop

			return runErr
		},
		close: func() error {
			close(stop)

			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(ctx, Group{component})
	}()
	requireChannelClosed(t, ready, "component to become ready")
	cancel()

	err := receiveError(t, runResult, "Run to finish concurrent cancellation")

	require.ErrorIs(t, err, runErr)
	require.True(t, channelClosed(runExited))
}

func TestRunReportsUnexpectedNilExit(t *testing.T) {
	ready := make(chan struct{})
	exitNow := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "concurrent-nil-exit",
		ready: ready,
		run: func() error {
			close(ready)
			<-exitNow

			return nil
		},
	}

	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(context.Background(), Group{component})
	}()
	requireChannelClosed(t, ready, "component to become ready")
	close(exitNow)

	err := receiveError(t, runResult, "Run to report an unexpected nil exit")

	require.ErrorContains(t, err, "component concurrent-nil-exit/test exited unexpectedly")
	require.True(t, component.closed.Load(), "unexpected exit must trigger component cleanup")
}

func TestRunReturnsCloseErrorAfterJoining(t *testing.T) {
	closeErr := errors.New("close failed")
	ready := make(chan struct{})
	stop := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "close-error",
		ready: ready,
		run: func() error {
			defer close(runExited)
			close(ready)
			<-stop

			return nil
		},
		close: func() error {
			close(stop)

			return closeErr
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(ctx, Group{component})
	}()
	requireChannelClosed(t, ready, "component to become ready")
	cancel()

	err := receiveError(t, runResult, "Run to return the close error")

	require.ErrorIs(t, err, closeErr)
	require.True(t, channelClosed(runExited), "Run must join before returning a close error")
}

func TestRunStartupCancellationJoinsStartedComponents(t *testing.T) {
	runStarted := make(chan struct{})
	stop := make(chan struct{})
	closeCalled := make(chan struct{})
	allowRunExit := make(chan struct{})
	runExited := make(chan struct{})
	component := &lifecycleTestComponent{
		name:  "startup-cancellation",
		ready: make(chan struct{}),
		run: func() error {
			defer close(runExited)
			close(runStarted)
			<-stop
			<-allowRunExit

			return nil
		},
		close: func() error {
			close(stop)
			close(closeCalled)

			return nil
		},
	}
	var releaseOnce sync.Once
	releaseRun := func() {
		releaseOnce.Do(func() { close(allowRunExit) })
	}
	t.Cleanup(func() {
		releaseRun()
		_ = component.Close()
		requireChannelClosed(t, runExited, "component Run to exit during cleanup")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(ctx, Group{component})
	}()
	requireChannelClosed(t, runStarted, "component Run to start")
	cancel()
	requireChannelClosed(t, closeCalled, "component Close to be called during startup")

	requireNoValue(t, runResult, "Run returned before joining the starting component")
	releaseRun()

	require.ErrorIs(t, receiveError(t, runResult, "Run to return after joining the starting component"), context.Canceled)
	require.True(t, channelClosed(runExited))
}

func TestRunStartupFailureJoinsStartedComponents(t *testing.T) {
	startupErr := errors.New("startup failed")
	peerStarted := make(chan struct{})
	peerStop := make(chan struct{})
	peerCloseCalled := make(chan struct{})
	allowPeerExit := make(chan struct{})
	peerRunExited := make(chan struct{})
	peer := &lifecycleTestComponent{
		name:  "startup-peer",
		ready: make(chan struct{}),
		run: func() error {
			defer close(peerRunExited)
			close(peerStarted)
			<-peerStop
			<-allowPeerExit

			return nil
		},
		close: func() error {
			close(peerStop)
			close(peerCloseCalled)

			return nil
		},
	}
	failing := &lifecycleTestComponent{
		name:  "startup-failure",
		ready: make(chan struct{}),
		run: func() error {
			<-peerStarted

			return startupErr
		},
	}
	var releaseOnce sync.Once
	releasePeer := func() {
		releaseOnce.Do(func() { close(allowPeerExit) })
	}
	t.Cleanup(func() {
		releasePeer()
		_ = peer.Close()
		requireChannelClosed(t, peerRunExited, "peer Run to exit during cleanup")
	})

	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(context.Background(), Group{failing, peer})
	}()
	requireChannelClosed(t, peerCloseCalled, "peer Close to be called after startup failure")

	requireNoValue(t, runResult, "Run returned before joining the startup peer")
	releasePeer()

	require.ErrorIs(t, receiveError(t, runResult, "Run to return after joining startup components"), startupErr)
	require.True(t, channelClosed(peerRunExited))
}

func TestRunRuntimeErrorJoinsOtherComponents(t *testing.T) {
	runErr := errors.New("runtime failed")
	failingReady := make(chan struct{})
	peerReady := make(chan struct{})
	failNow := make(chan struct{})
	peerStop := make(chan struct{})
	peerCloseCalled := make(chan struct{})
	allowPeerExit := make(chan struct{})
	peerRunExited := make(chan struct{})
	failing := &lifecycleTestComponent{
		name:  "runtime-failure",
		ready: failingReady,
		run: func() error {
			close(failingReady)
			<-failNow

			return runErr
		},
	}
	peer := &lifecycleTestComponent{
		name:  "runtime-peer",
		ready: peerReady,
		run: func() error {
			defer close(peerRunExited)
			close(peerReady)
			<-peerStop
			<-allowPeerExit

			return nil
		},
		close: func() error {
			close(peerStop)
			close(peerCloseCalled)

			return nil
		},
	}
	var releaseOnce sync.Once
	releasePeer := func() {
		releaseOnce.Do(func() { close(allowPeerExit) })
	}
	t.Cleanup(func() {
		releasePeer()
		_ = peer.Close()
		requireChannelClosed(t, peerRunExited, "peer Run to exit during cleanup")
	})

	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(context.Background(), Group{failing, peer})
	}()
	requireChannelClosed(t, failingReady, "failing component to become ready")
	requireChannelClosed(t, peerReady, "peer component to become ready")
	close(failNow)
	requireChannelClosed(t, peerCloseCalled, "peer Close to be called after runtime error")

	requireNoValue(t, runResult, "Run returned before joining the runtime peer")
	releasePeer()

	require.ErrorIs(t, receiveError(t, runResult, "Run to return after joining runtime components"), runErr)
	require.True(t, channelClosed(peerRunExited))
}

func TestRunJoinsDependentGroupBeforeClosingDependency(t *testing.T) {
	dependentRunExited := make(chan struct{})
	dependencyReady := make(chan struct{})
	dependencyStop := make(chan struct{})
	dependencyCloseCalled := make(chan struct{})
	dependencyRunExited := make(chan struct{})
	var dependencyClosedTooEarly atomic.Bool
	dependency := &lifecycleTestComponent{
		name:  "dependency",
		ready: dependencyReady,
		run: func() error {
			defer close(dependencyRunExited)
			close(dependencyReady)
			<-dependencyStop

			return nil
		},
		close: func() error {
			if !channelClosed(dependentRunExited) {
				dependencyClosedTooEarly.Store(true)
			}
			close(dependencyCloseCalled)
			close(dependencyStop)

			return nil
		},
	}

	dependentReady := make(chan struct{})
	dependentStop := make(chan struct{})
	dependentCloseReturned := make(chan struct{})
	allowDependentRunExit := make(chan struct{})
	dependent := &lifecycleTestComponent{
		name:  "dependent",
		ready: dependentReady,
		run: func() error {
			defer close(dependentRunExited)
			close(dependentReady)
			<-dependentStop
			<-allowDependentRunExit

			return nil
		},
		close: func() error {
			defer close(dependentCloseReturned)
			close(dependentStop)

			return nil
		},
	}
	var releaseOnce sync.Once
	releaseDependent := func() {
		releaseOnce.Do(func() { close(allowDependentRunExit) })
	}
	t.Cleanup(func() {
		releaseDependent()
		_ = dependent.Close()
		_ = dependency.Close()
		requireChannelClosed(t, dependentRunExited, "dependent Run to exit during cleanup")
		requireChannelClosed(t, dependencyRunExited, "dependency Run to exit during cleanup")
	})

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() {
		runResult <- Run(ctx, Group{dependency}, Group{dependent})
	}()
	requireChannelClosed(t, dependentReady, "dependent component to become ready")
	cancel()
	requireChannelClosed(t, dependentCloseReturned, "dependent Close to return")

	releaseDependent()
	requireChannelClosed(t, dependencyCloseCalled, "dependency Close after dependent Run exited")

	require.NoError(t, receiveError(t, runResult, "Run to return after reverse-order shutdown"))
	require.False(t, dependencyClosedTooEarly.Load(), "dependency closed before dependent Run exited")
	require.True(t, channelClosed(dependentRunExited))
	require.True(t, channelClosed(dependencyRunExited))
}

func TestRunDoesNotStartNextGroupAfterPriorGroupExited(t *testing.T) {
	runErr := errors.New("prior group failed")
	peerCloseErr := errors.New("peer close failed")

	dependencyReady := make(chan struct{})
	dependencyStop := make(chan struct{})
	dependencyClosed := make(chan struct{})
	dependencyRunExited := make(chan struct{})
	peerRunExited := make(chan struct{})
	var dependencyClosedBeforePeerJoin atomic.Bool
	dependency := &lifecycleTestComponent{
		name:  "dependency",
		ready: dependencyReady,
		run: func() error {
			defer close(dependencyRunExited)
			close(dependencyReady)
			<-dependencyStop

			return nil
		},
		close: func() error {
			if !channelClosed(peerRunExited) {
				dependencyClosedBeforePeerJoin.Store(true)
			}
			close(dependencyClosed)
			close(dependencyStop)

			return nil
		},
	}

	peerReady := make(chan struct{})
	peerStop := make(chan struct{})
	allowPeerExit := make(chan struct{})
	peerCloseCalled := make(chan struct{})
	peer := &lifecycleTestComponent{
		name:  "peer",
		ready: peerReady,
		run: func() error {
			defer close(peerRunExited)
			close(peerReady)
			<-peerStop
			<-allowPeerExit

			return nil
		},
		close: func() error {
			close(peerCloseCalled)
			close(peerStop)

			return peerCloseErr
		},
	}

	boundaryReady := make(chan struct{})
	exitBoundary := make(chan struct{})
	boundary := &lifecycleTestComponent{
		name:  "boundary-failure",
		ready: boundaryReady,
		run: func() error {
			close(boundaryReady)
			<-exitBoundary

			return runErr
		},
	}
	nextReady := make(chan struct{})
	nextStop := make(chan struct{})
	var nextRunCalls atomic.Int32
	var triggerExit sync.Once
	next := &lifecycleTestComponent{
		name:  "must-not-start",
		ready: nextReady,
		run: func() error {
			nextRunCalls.Add(1)
			close(nextReady)
			<-nextStop

			return nil
		},
		close: func() error {
			close(nextStop)

			return nil
		},
	}
	coordinator := newStartCoordinator(3)
	next.nameFunc = func() string {
		triggerExit.Do(func() {
			close(exitBoundary)
			<-coordinator.firstPublished
		})

		return next.name
	}

	var releaseOnce sync.Once
	releasePeer := func() {
		releaseOnce.Do(func() { close(allowPeerExit) })
	}
	t.Cleanup(func() {
		releasePeer()
		_ = next.Close()
		_ = peer.Close()
		_ = boundary.Close()
		_ = dependency.Close()
	})

	runResult := make(chan error, 1)
	go func() {
		runResult <- runGroups(
			context.Background(),
			coordinator,
			Group{dependency},
			Group{peer, boundary},
			Group{next},
		)
	}()
	requireChannelClosed(t, peerCloseCalled, "peer Close after prior-group failure")
	require.False(t, channelClosed(dependencyClosed), "dependency closed before peer Run joined")
	releasePeer()

	err := receiveError(t, runResult, "Run to return after prior-group failure")
	require.ErrorIs(t, err, runErr)
	require.ErrorIs(t, err, peerCloseErr)
	require.Zero(t, nextRunCalls.Load(), "a later group must not start after an earlier group exited")
	require.False(t, next.closed.Load(), "a never-started later group must not be closed")
	require.False(t, dependencyClosedBeforePeerJoin.Load())
	require.True(t, channelClosed(peerRunExited))
	require.True(t, channelClosed(dependencyRunExited))
}

func TestStartCoordinatorRejectsGroupAfterPublishedExit(t *testing.T) {
	runErr := errors.New("published first")
	coordinator := newStartCoordinator(1)
	coordinator.publish(componentRunResult{name: "prior", instance: "test", err: runErr})

	result, authorized := coordinator.authorizeGroup()

	require.False(t, authorized)
	require.ErrorIs(t, result.err, runErr)
}

func TestStartCoordinatorKeepsPriorGroupAuthorization(t *testing.T) {
	coordinator := newStartCoordinator(1)
	_, authorized := coordinator.authorizeGroup()
	require.True(t, authorized)

	coordinator.publish(componentRunResult{
		name:     "prior",
		instance: "test",
		err:      errors.New("published after authorization"),
	})

	result := <-coordinator.runResultCh
	require.ErrorContains(t, result.err, "published after authorization")
}

func TestBaseReadyAndCloseAreConcurrentSafeAndIdempotent(t *testing.T) {
	base := New(&BaseConfig[struct{}, struct{}]{
		Name:     "base",
		Instance: "test",
	})

	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers * 2)
	for range callers {
		go func() {
			defer wg.Done()
			base.MarkReady()
		}()
		go func() {
			defer wg.Done()
			_ = base.Close()
		}()
	}
	wg.Wait()

	require.True(t, channelClosed(base.Ready()))
	require.ErrorIs(t, base.Context().Err(), context.Canceled)
	require.NoError(t, base.Close())
	require.NotPanics(t, base.MarkReady)
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func requireChannelClosed(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(lifecycleTestDeadline):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func requireNoValue(t *testing.T, ch <-chan error, description string) {
	t.Helper()

	select {
	case err := <-ch:
		t.Fatalf("%s: %v", description, err)
	case <-time.After(20 * time.Millisecond):
	}
}

func receiveError(t *testing.T, ch <-chan error, description string) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(lifecycleTestDeadline):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

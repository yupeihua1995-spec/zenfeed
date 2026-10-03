// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package lifecycle

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

type runnerFunc struct {
	run   func() error
	close func() error
	ready chan struct{}
}

func (f runnerFunc) Name() string           { return "runner" }
func (f runnerFunc) Instance() string       { return "test" }
func (f runnerFunc) Run() error             { return f.run() }
func (f runnerFunc) Ready() <-chan struct{} { return f.ready }
func (f runnerFunc) Close() error           { return f.close() }

func TestOnceCachesCloseResult(t *testing.T) {
	sentinel := errors.New("close failed")
	var once Once
	var calls atomic.Int32

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- once.Do(func() error {
				calls.Add(1)
				return sentinel
			})
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, sentinel)
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestCloseAllStartsEveryCloserAndJoinsErrors(t *testing.T) {
	errOne := errors.New("one")
	errTwo := errors.New("two")
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	closer := func(err error) closerFunc {
		return closerFunc(func() error {
			started <- struct{}{}
			<-release
			return err
		})
	}
	done := make(chan error, 1)
	go func() { done <- CloseAll(closer(errOne), closer(errTwo)) }()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("not every closer started")
		}
	}
	close(release)
	err := <-done
	require.ErrorIs(t, err, errOne)
	require.ErrorIs(t, err, errTwo)
}

func TestOwnedCloseWaitsForRunAndClosesChildOnce(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var closeCalls atomic.Int32
	owned := NewOwned(runnerFunc{
		run: func() error {
			close(started)
			<-release
			return nil
		},
		close: func() error {
			closeCalls.Add(1)
			close(release)
			return nil
		},
	})

	runDone := make(chan error, 1)
	go func() { runDone <- owned.Run() }()
	<-started
	require.NoError(t, owned.Close())
	require.NoError(t, owned.Close())
	require.NoError(t, <-runDone)
	assert.Equal(t, int32(1), closeCalls.Load())
}

func TestOwnedCloseReturnsRunErrorThatPredatesClose(t *testing.T) {
	runErr := errors.New("run failed")
	owned := NewOwned(runnerFunc{
		run:   func() error { return runErr },
		close: func() error { return nil },
	})

	require.ErrorIs(t, owned.Run(), runErr)
	select {
	case err := <-owned.Failure():
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		t.Fatal("unexpected exit was not published")
	}
	require.ErrorIs(t, owned.UnexpectedExit(), runErr)
	require.ErrorIs(t, owned.Wait(), runErr)
	require.ErrorIs(t, owned.Close(), runErr)
}

func TestOwnedCloseSuppressesShutdownInducedRunError(t *testing.T) {
	shutdownErr := errors.New("run stopped during shutdown")
	started := make(chan struct{})
	release := make(chan struct{})
	owned := NewOwned(runnerFunc{
		run: func() error {
			close(started)
			<-release
			return shutdownErr
		},
		close: func() error {
			close(release)
			return nil
		},
	})

	runDone := make(chan error, 1)
	go func() { runDone <- owned.Run() }()
	<-started
	require.NoError(t, owned.Close())
	require.ErrorIs(t, <-runDone, shutdownErr)
	_, ok := <-owned.Failure()
	assert.False(t, ok)
	require.NoError(t, owned.UnexpectedExit())
}

func TestOwnedNormalRunExitIsUnexpected(t *testing.T) {
	owned := NewOwned(runnerFunc{
		run:   func() error { return nil },
		close: func() error { return nil },
	})

	require.NoError(t, owned.Run())
	require.ErrorIs(t, owned.UnexpectedExit(), ErrUnexpectedExit)
	require.ErrorIs(t, owned.Close(), ErrUnexpectedExit)
}

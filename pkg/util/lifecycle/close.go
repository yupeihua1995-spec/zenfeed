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

package lifecycle

import (
	"errors"
	"sync"
)

// Once runs a close operation at most once and returns the same result to all
// callers. Its zero value is ready to use.
type Once struct {
	once sync.Once
	err  error
}

func (o *Once) Do(closeFn func() error) error {
	o.once.Do(func() {
		o.err = closeFn()
	})

	return o.err
}

type Closer interface {
	Close() error
}

type Runner interface {
	Name() string
	Instance() string
	Run() error
	Ready() <-chan struct{}
	Close() error
}

var (
	ErrClosed         = errors.New("owned component is closed")
	ErrAlreadyStarted = errors.New("owned component already started")
	ErrUnexpectedExit = errors.New("owned component exited unexpectedly")
)

// Owned gives an owner a stable handle for a child's Run and Close lifecycle.
// Close is safe before Run, closes the child at most once, and waits for a
// started Run to return.
type Owned struct {
	child Runner

	mu       sync.Mutex
	started  bool
	closed   bool
	done     chan struct{}
	failure  chan error
	runErr   error
	exitErr  error
	doneOnce sync.Once
	close    Once
}

func NewOwned(child Runner) *Owned {
	return &Owned{
		child:   child,
		done:    make(chan struct{}),
		failure: make(chan error, 1),
	}
}

func (o *Owned) Name() string { return o.child.Name() }

func (o *Owned) Instance() string { return o.child.Instance() }

func (o *Owned) Ready() <-chan struct{} { return o.child.Ready() }

func (o *Owned) Run() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()

		return ErrClosed
	}
	if o.started {
		o.mu.Unlock()

		return ErrAlreadyStarted
	}
	o.started = true
	o.mu.Unlock()

	err := o.child.Run()
	o.mu.Lock()
	o.runErr = err
	if !o.closed {
		o.exitErr = err
		if o.exitErr == nil {
			o.exitErr = ErrUnexpectedExit
		}
		o.failure <- o.exitErr
	}
	close(o.failure)
	o.doneOnce.Do(func() { close(o.done) })
	o.mu.Unlock()

	return err
}

func (o *Owned) Close() error {
	closeErr := o.close.Do(func() error {
		o.mu.Lock()
		o.closed = true
		started := o.started
		if !started {
			close(o.failure)
			o.doneOnce.Do(func() { close(o.done) })
		}
		o.mu.Unlock()

		err := o.child.Close()
		if started {
			<-o.done
		}
		o.mu.Lock()
		exitErr := o.exitErr
		o.mu.Unlock()

		return errors.Join(err, exitErr)
	})

	return closeErr
}

// Done is closed when Run returns, or when Close wins before Run starts.
func (o *Owned) Done() <-chan struct{} {
	return o.done
}

// Wait waits for Run to finish and returns its unclassified result. If Close
// happened before Run started, Wait returns nil.
func (o *Owned) Wait() error {
	<-o.done
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.runErr
}

// UnexpectedExit waits for Run to finish and reports an error only when Run
// returned before Close requested shutdown. A nil Run result is normalized.
func (o *Owned) UnexpectedExit() error {
	<-o.done
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.exitErr
}

// Failure publishes at most one unexpected exit error, then closes. Deliberate
// shutdown closes the channel without publishing a value.
func (o *Owned) Failure() <-chan error {
	return o.failure
}

// CloseAll starts every close operation before waiting for them to finish.
// This prevents one slow child from delaying the shutdown signal to siblings.
func CloseAll[T Closer](closers ...T) error {
	errs := make([]error, len(closers))
	var wg sync.WaitGroup
	for i, closer := range closers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = closer.Close()
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

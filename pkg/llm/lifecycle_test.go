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

package llm

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/model"
)

type lifecycleTestLLM struct {
	*component.Base[Config, struct{}]
	closeCalls  atomic.Int32
	closeErr    error
	runFn       func() error
	runResult   chan error
	shutdownErr error
}

func (l *lifecycleTestLLM) Run() error {
	if l.runFn != nil {
		return l.runFn()
	}
	l.MarkReady()
	select {
	case err := <-l.runResult:
		return err
	case <-l.Context().Done():
		return l.shutdownErr
	}
}

func newLifecycleTestLLM(name string, closeErr error) *lifecycleTestLLM {
	config := &Config{Name: name}

	return &lifecycleTestLLM{
		Base: component.New(&component.BaseConfig[Config, struct{}]{
			Name:     "LifecycleTestLLM",
			Instance: name,
			Config:   config,
		}),
		closeErr: closeErr,
	}
}

func (l *lifecycleTestLLM) Close() error {
	l.closeCalls.Add(1)
	_ = l.Base.Close()

	return l.closeErr
}

func (l *lifecycleTestLLM) String(context.Context, []string) (string, error) {
	return "", nil
}

func (l *lifecycleTestLLM) EmbeddingLabels(context.Context, model.Labels) ([][]float32, error) {
	return nil, nil
}

func (l *lifecycleTestLLM) Embedding(context.Context, string) ([]float32, error) {
	return nil, nil
}

func (l *lifecycleTestLLM) WAV(context.Context, string, []Speaker) (io.ReadCloser, error) {
	return nil, nil
}

func newLifecycleTestFactory(children ...*lifecycleTestLLM) *factory {
	f := &factory{
		Base: component.New(&component.BaseConfig[FactoryConfig, FactoryDependencies]{
			Name:     "LLMFactory",
			Instance: "lifecycle-test",
			Config:   &FactoryConfig{},
		}),
		llms: make(map[string]LLM, len(children)),
	}
	for _, child := range children {
		f.llms[child.Instance()] = child
	}

	return f
}

func TestFactoryCloseCancelsRunAndClosesChildrenOnce(t *testing.T) {
	child := newLifecycleTestLLM("child", nil)
	f := newLifecycleTestFactory(child)
	defer f.Base.Close()

	runDone := make(chan error, 1)
	go func() { runDone <- f.Run() }()
	require.Eventually(t, func() bool {
		select {
		case <-f.Ready():
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- f.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("factory Run did not exit after Close")
	}
	assert.Equal(t, int32(1), child.closeCalls.Load())
}

func TestFactoryCloseAttemptsEveryChildAndJoinsErrors(t *testing.T) {
	errOne := errors.New("close one")
	errTwo := errors.New("close two")
	childOne := newLifecycleTestLLM("one", errOne)
	childTwo := newLifecycleTestLLM("two", errTwo)
	f := newLifecycleTestFactory(childOne, childTwo)

	err := f.Close()
	require.ErrorIs(t, err, errOne)
	require.ErrorIs(t, err, errTwo)
	assert.Equal(t, int32(1), childOne.closeCalls.Load())
	assert.Equal(t, int32(1), childTwo.closeCalls.Load())
}

func TestFactoryRunReturnsChildErrorAfterReady(t *testing.T) {
	runErr := errors.New("LLM stopped unexpectedly")
	child := newLifecycleTestLLM("child", nil)
	child.runResult = make(chan error, 1)
	f := newLifecycleTestFactory(child)

	runDone := make(chan error, 1)
	go func() { runDone <- f.Run() }()
	select {
	case <-f.Ready():
	case <-time.After(time.Second):
		t.Fatal("factory did not become ready")
	}
	child.runResult <- runErr

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		_ = f.Close()
		t.Fatal("factory did not report the child Run error")
	}
}

func TestFactoryRunRejectsChildExitDuringInitialStartup(t *testing.T) {
	runErr := errors.New("LLM exited during factory startup")
	started := make(chan *lifecycleTestLLM, 2)
	allowSecondReady := make(chan struct{})
	var startOrder atomic.Int32

	children := []*lifecycleTestLLM{
		newLifecycleTestLLM("one", nil),
		newLifecycleTestLLM("two", nil),
	}
	for _, child := range children {
		child := child
		child.runResult = make(chan error, 1)
		child.runFn = func() error {
			position := startOrder.Add(1)
			started <- child
			if position == 1 {
				child.MarkReady()

				return <-child.runResult
			}

			select {
			case <-allowSecondReady:
				child.MarkReady()
			case <-child.Context().Done():
				return nil
			}
			select {
			case err := <-child.runResult:
				return err
			case <-child.Context().Done():
				return nil
			}
		}
	}
	f := newLifecycleTestFactory(children...)
	runDone := make(chan error, 1)
	go func() { runDone <- f.Run() }()

	first := <-started
	<-started
	f.mu.Lock()
	firstOwned := f.ownedLLMs[first.Instance()]
	f.mu.Unlock()
	first.runResult <- runErr
	<-firstOwned.owner.Done()
	close(allowSecondReady)

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		t.Fatal("factory did not reject the failed startup generation")
	}
	select {
	case <-f.Ready():
		t.Fatal("factory became ready after a child exited during startup")
	default:
	}
}

func TestFactoryReloadDoesNotReportDeliberateOldGenerationExit(t *testing.T) {
	shutdownErr := errors.New("old LLM stopped during reload")
	child := newLifecycleTestLLM("old", nil)
	child.runResult = make(chan error)
	child.shutdownErr = shutdownErr
	f := newLifecycleTestFactory(child)
	f.SetConfig(&FactoryConfig{LLMs: []Config{{Name: "old"}}, defaultLLM: "old"})

	runDone := make(chan error, 1)
	go func() { runDone <- f.Run() }()
	select {
	case <-f.Ready():
	case <-time.After(time.Second):
		t.Fatal("factory did not become ready")
	}

	app := &config.App{LLMs: []config.LLM{{
		Name:     "replacement",
		Provider: string(ProviderTypeOpenAI),
		APIKey:   "test-key",
		Model:    "test-model",
	}}}
	require.NoError(t, f.Reload(app))
	select {
	case err := <-runDone:
		t.Fatalf("factory exited during deliberate reload: %v", err)
	default:
	}

	require.NoError(t, f.Close())
	require.NoError(t, <-runDone)
}

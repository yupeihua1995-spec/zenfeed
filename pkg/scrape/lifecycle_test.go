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

package scrape

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/scrape/scraper"
)

type lifecycleTestScraper struct {
	*component.Base[scraper.Config, struct{}]
	closeCalls  atomic.Int32
	closeErr    error
	runFn       func() error
	runResult   chan error
	shutdownErr error
}

func (s *lifecycleTestScraper) Run() error {
	if s.runFn != nil {
		return s.runFn()
	}
	s.MarkReady()
	select {
	case err := <-s.runResult:
		return err
	case <-s.Context().Done():
		return s.shutdownErr
	}
}

func newLifecycleTestScraper(name string, closeErr error) *lifecycleTestScraper {
	config := &scraper.Config{Name: name}

	return &lifecycleTestScraper{
		Base: component.New(&component.BaseConfig[scraper.Config, struct{}]{
			Name:     "LifecycleTestScraper",
			Instance: name,
			Config:   config,
		}),
		closeErr: closeErr,
	}
}

func (s *lifecycleTestScraper) Close() error {
	s.closeCalls.Add(1)
	_ = s.Base.Close()

	return s.closeErr
}

func (s *lifecycleTestScraper) Status() scraper.Status {
	return scraper.Status{Name: s.Instance(), State: scraper.StatusIdle}
}

func (s *lifecycleTestScraper) Trigger() error { return nil }

func newLifecycleTestManager(children ...*lifecycleTestScraper) *manager {
	m := &manager{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:     "ScrapeManager",
			Instance: "lifecycle-test",
			Config:   &Config{},
		}),
		scrapers: make(map[string]scraper.Scraper, len(children)),
	}
	for _, child := range children {
		m.scrapers[child.Instance()] = child
	}

	return m
}

func TestManagerCloseClosesChildrenOnce(t *testing.T) {
	children := []*lifecycleTestScraper{
		newLifecycleTestScraper("one", nil),
		newLifecycleTestScraper("two", nil),
	}
	m := newLifecycleTestManager(children...)

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- m.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	for _, child := range children {
		assert.Equal(t, int32(1), child.closeCalls.Load())
	}
}

func TestManagerCloseAttemptsEveryChildAndJoinsErrors(t *testing.T) {
	errOne := errors.New("close one")
	errTwo := errors.New("close two")
	childOne := newLifecycleTestScraper("one", errOne)
	childTwo := newLifecycleTestScraper("two", errTwo)
	m := newLifecycleTestManager(childOne, childTwo)

	err := m.Close()
	require.ErrorIs(t, err, errOne)
	require.ErrorIs(t, err, errTwo)
	assert.Equal(t, int32(1), childOne.closeCalls.Load())
	assert.Equal(t, int32(1), childTwo.closeCalls.Load())
}

func TestManagerRunReturnsChildErrorAfterReady(t *testing.T) {
	runErr := errors.New("scraper stopped unexpectedly")
	child := newLifecycleTestScraper("child", nil)
	child.runResult = make(chan error, 1)
	m := newLifecycleTestManager(child)

	runDone := make(chan error, 1)
	go func() { runDone <- m.Run() }()
	select {
	case <-m.Ready():
	case <-time.After(time.Second):
		t.Fatal("manager did not become ready")
	}
	child.runResult <- runErr

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		_ = m.Close()
		t.Fatal("manager did not report the child Run error")
	}
}

func TestManagerRunRejectsChildExitDuringInitialStartup(t *testing.T) {
	runErr := errors.New("scraper exited during manager startup")
	started := make(chan *lifecycleTestScraper, 2)
	allowSecondReady := make(chan struct{})
	var startOrder atomic.Int32

	children := []*lifecycleTestScraper{
		newLifecycleTestScraper("one", nil),
		newLifecycleTestScraper("two", nil),
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
	m := newLifecycleTestManager(children...)
	runDone := make(chan error, 1)
	go func() { runDone <- m.Run() }()

	first := <-started
	<-started
	m.lifecycleMu.Lock()
	firstOwned := m.ownedScrapers[first.Instance()]
	m.lifecycleMu.Unlock()
	first.runResult <- runErr
	<-firstOwned.owner.Done()
	close(allowSecondReady)

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		t.Fatal("manager did not reject the failed startup generation")
	}
	select {
	case <-m.Ready():
		t.Fatal("manager became ready after a child exited during startup")
	default:
	}
}

func TestManagerReloadDoesNotReportDeliberateOldGenerationExit(t *testing.T) {
	shutdownErr := errors.New("old scraper stopped during reload")
	child := newLifecycleTestScraper("old", nil)
	child.runResult = make(chan error)
	child.shutdownErr = shutdownErr
	m := newLifecycleTestManager(child)
	m.SetConfig(&Config{Scrapers: []scraper.Config{{Name: "old"}}})

	runDone := make(chan error, 1)
	go func() { runDone <- m.Run() }()
	select {
	case <-m.Ready():
	case <-time.After(time.Second):
		t.Fatal("manager did not become ready")
	}

	require.NoError(t, m.Reload(&config.App{}))
	select {
	case err := <-runDone:
		t.Fatalf("manager exited during deliberate reload: %v", err)
	default:
	}

	require.NoError(t, m.Close())
	require.NoError(t, <-runDone)
}

func TestManagerRejectsStagedGenerationThatExitedBeforeCommit(t *testing.T) {
	firstRunErr := errors.New("first staged scraper exited")
	child := newLifecycleTestScraper("first", nil)
	child.runResult = make(chan error, 1)
	owned := newOwnedScraper(child)
	runDone := make(chan error, 1)
	go func() { runDone <- owned.Run() }()
	<-owned.Ready()
	child.runResult <- firstRunErr
	<-owned.owner.Done()

	err := stagedScraperExit([]*ownedScraper{owned})
	require.ErrorIs(t, err, firstRunErr)
	require.ErrorIs(t, <-runDone, firstRunErr)
	require.ErrorIs(t, owned.Close(), firstRunErr)
}

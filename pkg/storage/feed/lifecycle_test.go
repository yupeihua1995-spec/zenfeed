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

package feed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
)

type lifecycleTestBlock struct {
	*component.Base[block.Config, struct{}]
	start       time.Time
	closeCalls  atomic.Int32
	closeErr    error
	runResult   chan error
	shutdownErr error
	runHook     func()
}

func (b *lifecycleTestBlock) Run() error {
	if b.runHook != nil {
		b.runHook()
	}
	b.MarkReady()
	select {
	case err := <-b.runResult:
		return err
	case <-b.Context().Done():
		return b.shutdownErr
	}
}

func newLifecycleTestBlock(name string, start time.Time, closeErr error) *lifecycleTestBlock {
	return &lifecycleTestBlock{
		Base: component.New(&component.BaseConfig[block.Config, struct{}]{
			Name:     "LifecycleTestBlock",
			Instance: name,
			Config:   &block.Config{},
		}),
		start:    start,
		closeErr: closeErr,
	}
}

func (b *lifecycleTestBlock) Close() error {
	b.closeCalls.Add(1)
	_ = b.Base.Close()

	return b.closeErr
}

func (b *lifecycleTestBlock) Reload(*block.Config) error { return nil }
func (b *lifecycleTestBlock) Start() time.Time           { return b.start }
func (b *lifecycleTestBlock) End() time.Time             { return b.start.Add(time.Hour) }
func (b *lifecycleTestBlock) State() block.State         { return block.StateHot }
func (b *lifecycleTestBlock) TransformToCold() error     { return nil }
func (b *lifecycleTestBlock) ClearOnDisk() error         { return nil }
func (b *lifecycleTestBlock) Append(context.Context, ...*model.Feed) error {
	return nil
}
func (b *lifecycleTestBlock) Query(context.Context, block.QueryOptions) ([]*block.FeedVO, error) {
	return nil, nil
}
func (b *lifecycleTestBlock) Exists(context.Context, uint64) (bool, error) {
	return false, nil
}

func newLifecycleTestStorage(children ...*lifecycleTestBlock) *storage {
	s := &storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:     "FeedStorage",
			Instance: "lifecycle-test",
			Config:   &Config{},
		}),
		blocks: &blockChain{blocks: make(map[string]block.Block, len(children))},
	}
	for _, child := range children {
		s.blocks.add(child)
	}

	return s
}

func TestStorageCloseClosesBlocksOnce(t *testing.T) {
	children := []*lifecycleTestBlock{
		newLifecycleTestBlock("one", time.Unix(1, 0), nil),
		newLifecycleTestBlock("two", time.Unix(2, 0), nil),
	}
	s := newLifecycleTestStorage(children...)

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Close()
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

func TestStorageCloseAttemptsEveryBlockAndJoinsErrors(t *testing.T) {
	errOne := errors.New("close one")
	errTwo := errors.New("close two")
	childOne := newLifecycleTestBlock("one", time.Unix(1, 0), errOne)
	childTwo := newLifecycleTestBlock("two", time.Unix(2, 0), errTwo)
	s := newLifecycleTestStorage(childOne, childTwo)

	err := s.Close()
	require.ErrorIs(t, err, errOne)
	require.ErrorIs(t, err, errTwo)
	assert.Equal(t, int32(1), childOne.closeCalls.Load())
	assert.Equal(t, int32(1), childTwo.closeCalls.Load())
}

func TestStorageRunReturnsChildErrorAfterReady(t *testing.T) {
	runErr := errors.New("block stopped unexpectedly")
	child := newLifecycleTestBlock("child", time.Now().Add(time.Hour), nil)
	child.runResult = make(chan error, 1)
	s := newLifecycleTestStorage(child)

	runDone := make(chan error, 1)
	go func() { runDone <- s.Run() }()
	select {
	case <-s.Ready():
	case <-time.After(time.Second):
		t.Fatal("storage did not become ready")
	}
	child.runResult <- runErr

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		_ = s.Close()
		t.Fatal("storage did not report the child Run error")
	}
}

func TestStorageRunDoesNotBecomeReadyIfEarlierBlockExitsDuringStartup(t *testing.T) {
	runErr := errors.New("first block exited")
	first := newLifecycleTestBlock("first", time.Now().Add(time.Hour), nil)
	first.runResult = make(chan error, 1)
	second := newLifecycleTestBlock("second", time.Now().Add(2*time.Hour), nil)
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	second.runHook = func() {
		close(secondStarted)
		<-releaseSecond
	}
	s := newLifecycleTestStorage(first, second)

	runDone := make(chan error, 1)
	go func() { runDone <- s.Run() }()
	<-secondStarted
	first.runResult <- runErr
	firstOwned := s.ownedBlocks[blockName(first.Start())]
	select {
	case <-firstOwned.owner.Done():
	case <-time.After(time.Second):
		t.Fatal("first block exit was not observed")
	}
	close(releaseSecond)

	select {
	case <-s.Ready():
		t.Fatal("storage became ready after an earlier block exited")
	case err := <-runDone:
		require.ErrorIs(t, err, runErr)
	case <-time.After(time.Second):
		_ = s.Close()
		t.Fatal("storage did not return the earlier block failure")
	}
}

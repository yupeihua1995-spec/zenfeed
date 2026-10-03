package block

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/chunk"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/inverted"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/primary"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/vector"
)

func TestBlockColdLoadFailureKeepsOldResourcesAndCleansStaging_BitsUT(t *testing.T) {
	decodeErr := errors.New("decode inverted index")
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, chunkDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chunkDirname, chunkFilename(0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, indexDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{indexPrimaryFilename, indexInvertedFilename, indexVectorFilename} {
		if err := os.WriteFile(filepath.Join(dir, indexDirname, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	oldPrimary := newTrackedPrimaryIndex(t, nil)
	oldVector := newTrackedVectorIndex(t, nil)
	oldInverted := newTrackedInvertedIndex(t, nil)
	oldChunk := newLoadTestChunk("old")
	stagedChunk := newLoadTestChunk("staged")
	stagedPrimary := newTrackedPrimaryIndex(t, nil)
	stagedVector := newTrackedVectorIndex(t, nil)
	stagedInverted := newTrackedInvertedIndex(t, decodeErr)

	b := &block{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "FeedBlock", Instance: "cold-load-test", Config: &Config{Dir: dir},
			Dependencies: Dependencies{
				ChunkFactory: component.FactoryFunc[chunk.File, chunk.Config, chunk.Dependencies](
					func(string, *chunk.Config, chunk.Dependencies) (chunk.File, error) { return stagedChunk, nil },
				),
				PrimaryFactory: component.FactoryFunc[primary.Index, primary.Config, primary.Dependencies](
					func(string, *primary.Config, primary.Dependencies) (primary.Index, error) { return stagedPrimary, nil },
				),
				VectorFactory: component.FactoryFunc[vector.Index, vector.Config, vector.Dependencies](
					func(string, *vector.Config, vector.Dependencies) (vector.Index, error) { return stagedVector, nil },
				),
				InvertedFactory: component.FactoryFunc[inverted.Index, inverted.Config, inverted.Dependencies](
					func(string, *inverted.Config, inverted.Dependencies) (inverted.Index, error) {
						return stagedInverted, nil
					},
				),
			},
		}),
		primaryIndex: oldPrimary, vectorIndex: oldVector, invertedIndex: oldInverted,
		chunks: chunkChain{oldChunk},
	}
	b.state.Store(StateCold)
	b.lastDataAccess.Store(clk.Now())
	t.Cleanup(func() {
		_ = b.Close()
		_ = oldPrimary.Close()
		_ = oldVector.Close()
		_ = oldInverted.Close()
		_ = oldChunk.Close()
		_ = stagedPrimary.Close()
		_ = stagedVector.Close()
		_ = stagedInverted.Close()
		_ = stagedChunk.Close()
	})

	err := b.ensureLoaded(context.Background())

	if !errors.Is(err, decodeErr) {
		t.Fatalf("ensureLoaded() error = %v, want wrapped %v", err, decodeErr)
	}
	if b.coldLoaded {
		t.Fatal("coldLoaded = true after failed load")
	}
	if b.primaryIndex != oldPrimary || b.vectorIndex != oldVector || b.invertedIndex != oldInverted {
		t.Fatal("failed load replaced live indexes")
	}
	if len(b.chunks) != 1 || b.chunks[0] != oldChunk {
		t.Fatal("failed load replaced live chunks")
	}
	for name, calls := range map[string]int32{
		"old primary":  oldPrimary.closeCalls.Load(),
		"old vector":   oldVector.closeCalls.Load(),
		"old inverted": oldInverted.closeCalls.Load(),
		"old chunk":    oldChunk.closeCalls.Load(),
	} {
		if calls != 0 {
			t.Fatalf("%s Close calls = %d, want 0", name, calls)
		}
	}
	for name, calls := range map[string]int32{
		"staged primary":  stagedPrimary.closeCalls.Load(),
		"staged vector":   stagedVector.closeCalls.Load(),
		"staged inverted": stagedInverted.closeCalls.Load(),
		"staged chunk":    stagedChunk.closeCalls.Load(),
	} {
		if calls != 1 {
			t.Fatalf("%s Close calls = %d, want 1", name, calls)
		}
	}
}

func TestBlockColdLoadCanceledBeforeChunkStartClosesStagedChunk_BitsUT(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, chunkDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chunkDirname, chunkFilename(0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stagedChunk := newLoadTestChunk("staged")
	b := coldLoadTestBlock(dir, stagedChunk)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.ensureLoaded(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureLoaded() error = %v, want context canceled", err)
	}
	if got := stagedChunk.closeCalls.Load(); got != 1 {
		t.Fatalf("staged chunk Close calls = %d, want 1", got)
	}
}

func TestBlockColdLoadCanceledAfterChunkStartClosesChunkOnce_BitsUT(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, chunkDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chunkDirname, chunkFilename(0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stagedChunk := newLoadTestChunk("staged")
	started := make(chan struct{})
	release := make(chan struct{})
	stagedChunk.runFn = func() error {
		close(started)
		<-release

		return context.Canceled
	}
	stagedChunk.closeFn = func() { close(release) }
	b := coldLoadTestBlock(dir, stagedChunk)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.ensureLoaded(ctx) }()
	<-started
	cancel()

	err := <-done

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureLoaded() error = %v, want context canceled", err)
	}
	if got := stagedChunk.closeCalls.Load(); got != 1 {
		t.Fatalf("staged chunk Close calls = %d, want 1", got)
	}
}

func TestBlockColdLoadChunkEarlyExitClosesChunkOnce_BitsUT(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, chunkDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chunkDirname, chunkFilename(0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	runErr := errors.New("chunk exited before ready")
	stagedChunk := newLoadTestChunk("staged")
	stagedChunk.runFn = func() error { return runErr }
	b := coldLoadTestBlock(dir, stagedChunk)

	err := b.ensureLoaded(context.Background())

	if !errors.Is(err, runErr) {
		t.Fatalf("ensureLoaded() error = %v, want wrapped %v", err, runErr)
	}
	if got := stagedChunk.closeCalls.Load(); got != 1 {
		t.Fatalf("staged chunk Close calls = %d, want 1", got)
	}
}

func TestBlockColdLoadChunkStartTimeoutClosesChunkOnce_BitsUT(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, chunkDirname), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, chunkDirname, chunkFilename(0)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stagedChunk := newLoadTestChunk("staged")
	stagedChunk.runFn = func() error {
		<-stagedChunk.base.Context().Done()

		return nil
	}
	b := coldLoadTestBlock(dir, stagedChunk)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := b.ensureLoaded(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ensureLoaded() error = %v, want deadline exceeded", err)
	}
	if got := stagedChunk.closeCalls.Load(); got != 1 {
		t.Fatalf("staged chunk Close calls = %d, want 1", got)
	}
}

func coldLoadTestBlock(dir string, stagedChunk chunk.File) *block {
	b := &block{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "FeedBlock", Instance: "cold-load-canceled-test", Config: &Config{Dir: dir},
			Dependencies: Dependencies{
				ChunkFactory: component.FactoryFunc[chunk.File, chunk.Config, chunk.Dependencies](
					func(string, *chunk.Config, chunk.Dependencies) (chunk.File, error) { return stagedChunk, nil },
				),
			},
		}),
	}
	b.state.Store(StateCold)
	b.lastDataAccess.Store(clk.Now())

	return b
}

func TestBlockNewCleansInitialIndexesWhenInitFails_BitsUT(t *testing.T) {
	root := t.TempDir()
	badDir := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(badDir, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	primaryIndex := newTrackedPrimaryIndex(t, nil)
	vectorIndex := newTrackedVectorIndex(t, nil)
	invertedIndex := newTrackedInvertedIndex(t, nil)
	deps := trackedIndexDependencies(primaryIndex, vectorIndex, invertedIndex)

	created, err := new("init-failure", &Config{
		Dir: badDir,
		ForCreate: &ForCreateConfig{
			Start: time.Now(), Duration: 24 * time.Hour, EmbeddingLLM: "test",
		},
	}, deps)

	if err == nil || created != nil {
		t.Fatalf("new() = (%v, %v), want nil and error", created, err)
	}
	assertTrackedIndexesClosedOnce(t, primaryIndex, vectorIndex, invertedIndex)
}

func TestBlockNewCleansInitialIndexesWhenHotLoadFails_BitsUT(t *testing.T) {
	dir := t.TempDir()
	metadataBytes, err := json.Marshal(metadata{
		Start: time.Now(), Duration: 24 * time.Hour, EmbeddingLLM: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, metadataFilename), metadataBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	primaryIndex := newTrackedPrimaryIndex(t, nil)
	vectorIndex := newTrackedVectorIndex(t, nil)
	invertedIndex := newTrackedInvertedIndex(t, nil)
	deps := trackedIndexDependencies(primaryIndex, vectorIndex, invertedIndex)

	created, err := new("load-failure", &Config{Dir: dir}, deps)

	if err == nil || created != nil {
		t.Fatalf("new() = (%v, %v), want nil and error", created, err)
	}
	assertTrackedIndexesClosedOnce(t, primaryIndex, vectorIndex, invertedIndex)
}

func trackedIndexDependencies(
	primaryIndex primary.Index,
	vectorIndex vector.Index,
	invertedIndex inverted.Index,
) Dependencies {
	return Dependencies{
		PrimaryFactory: component.FactoryFunc[primary.Index, primary.Config, primary.Dependencies](
			func(string, *primary.Config, primary.Dependencies) (primary.Index, error) { return primaryIndex, nil },
		),
		VectorFactory: component.FactoryFunc[vector.Index, vector.Config, vector.Dependencies](
			func(string, *vector.Config, vector.Dependencies) (vector.Index, error) { return vectorIndex, nil },
		),
		InvertedFactory: component.FactoryFunc[inverted.Index, inverted.Config, inverted.Dependencies](
			func(string, *inverted.Config, inverted.Dependencies) (inverted.Index, error) {
				return invertedIndex, nil
			},
		),
	}
}

func assertTrackedIndexesClosedOnce(
	t *testing.T,
	primaryIndex *trackedPrimaryIndex,
	vectorIndex *trackedVectorIndex,
	invertedIndex *trackedInvertedIndex,
) {
	t.Helper()
	for name, calls := range map[string]int32{
		"primary":  primaryIndex.closeCalls.Load(),
		"vector":   vectorIndex.closeCalls.Load(),
		"inverted": invertedIndex.closeCalls.Load(),
	} {
		if calls != 1 {
			t.Fatalf("%s Close calls = %d, want 1", name, calls)
		}
	}
}

type loadTestLifecycle struct {
	name       string
	base       *component.Base[struct{}, struct{}]
	closeOnce  sync.Once
	closeCalls atomic.Int32
	decodeErr  error
	runFn      func() error
	closeFn    func()
}

func newLoadTestLifecycle(name string, decodeErr error) *loadTestLifecycle {
	return &loadTestLifecycle{
		name: name, decodeErr: decodeErr,
		base: component.New(&component.BaseConfig[struct{}, struct{}]{Name: name, Instance: name}),
	}
}
func (l *loadTestLifecycle) Name() string     { return l.name }
func (l *loadTestLifecycle) Instance() string { return l.name }
func (l *loadTestLifecycle) Run() error {
	if l.runFn != nil {
		return l.runFn()
	}

	return l.base.Run()
}
func (l *loadTestLifecycle) Ready() <-chan struct{} { return l.base.Ready() }
func (l *loadTestLifecycle) Close() error {
	l.closeOnce.Do(func() {
		l.closeCalls.Add(1)
		if l.closeFn != nil {
			l.closeFn()
		}
		_ = l.base.Close()
	})

	return nil
}
func (l *loadTestLifecycle) decode(io.Reader) error { return l.decodeErr }

type trackedPrimaryIndex struct {
	primary.Index
	*loadTestLifecycle
}

func newTrackedPrimaryIndex(t *testing.T, decodeErr error) *trackedPrimaryIndex {
	t.Helper()
	idx, err := primary.NewFactory().New("test", &primary.Config{}, primary.Dependencies{})
	if err != nil {
		t.Fatal(err)
	}

	return &trackedPrimaryIndex{Index: idx, loadTestLifecycle: newLoadTestLifecycle("primary", decodeErr)}
}
func (i *trackedPrimaryIndex) DecodeFrom(context.Context, io.Reader) error { return i.decodeErr }
func (i *trackedPrimaryIndex) Name() string                                { return i.loadTestLifecycle.Name() }
func (i *trackedPrimaryIndex) Instance() string                            { return i.loadTestLifecycle.Instance() }
func (i *trackedPrimaryIndex) Run() error                                  { return i.loadTestLifecycle.Run() }
func (i *trackedPrimaryIndex) Ready() <-chan struct{}                      { return i.loadTestLifecycle.Ready() }
func (i *trackedPrimaryIndex) Close() error                                { return i.loadTestLifecycle.Close() }
func (i *trackedPrimaryIndex) EncodeTo(ctx context.Context, w io.Writer) error {
	return i.Index.EncodeTo(ctx, w)
}

type trackedVectorIndex struct {
	vector.Index
	*loadTestLifecycle
}

func newTrackedVectorIndex(t *testing.T, decodeErr error) *trackedVectorIndex {
	t.Helper()
	idx, err := vector.NewFactory().New("test", &vector.Config{}, vector.Dependencies{})
	if err != nil {
		t.Fatal(err)
	}

	return &trackedVectorIndex{Index: idx, loadTestLifecycle: newLoadTestLifecycle("vector", decodeErr)}
}
func (i *trackedVectorIndex) DecodeFrom(context.Context, io.Reader) error { return i.decodeErr }
func (i *trackedVectorIndex) Name() string                                { return i.loadTestLifecycle.Name() }
func (i *trackedVectorIndex) Instance() string                            { return i.loadTestLifecycle.Instance() }
func (i *trackedVectorIndex) Run() error                                  { return i.loadTestLifecycle.Run() }
func (i *trackedVectorIndex) Ready() <-chan struct{}                      { return i.loadTestLifecycle.Ready() }
func (i *trackedVectorIndex) Close() error                                { return i.loadTestLifecycle.Close() }
func (i *trackedVectorIndex) EncodeTo(ctx context.Context, w io.Writer) error {
	return i.Index.EncodeTo(ctx, w)
}

type trackedInvertedIndex struct {
	inverted.Index
	*loadTestLifecycle
}

func newTrackedInvertedIndex(t *testing.T, decodeErr error) *trackedInvertedIndex {
	t.Helper()
	idx, err := inverted.NewFactory().New("test", &inverted.Config{}, inverted.Dependencies{})
	if err != nil {
		t.Fatal(err)
	}

	return &trackedInvertedIndex{Index: idx, loadTestLifecycle: newLoadTestLifecycle("inverted", decodeErr)}
}
func (i *trackedInvertedIndex) DecodeFrom(context.Context, io.Reader) error { return i.decodeErr }
func (i *trackedInvertedIndex) Name() string                                { return i.loadTestLifecycle.Name() }
func (i *trackedInvertedIndex) Instance() string                            { return i.loadTestLifecycle.Instance() }
func (i *trackedInvertedIndex) Run() error                                  { return i.loadTestLifecycle.Run() }
func (i *trackedInvertedIndex) Ready() <-chan struct{}                      { return i.loadTestLifecycle.Ready() }
func (i *trackedInvertedIndex) Close() error                                { return i.loadTestLifecycle.Close() }
func (i *trackedInvertedIndex) EncodeTo(ctx context.Context, w io.Writer) error {
	return i.Index.EncodeTo(ctx, w)
}

type loadTestChunk struct {
	*loadTestLifecycle
}

func newLoadTestChunk(name string) *loadTestChunk {
	return &loadTestChunk{loadTestLifecycle: newLoadTestLifecycle(name, nil)}
}
func (c *loadTestChunk) EnsureReadonly(context.Context) error { return nil }
func (c *loadTestChunk) Count(context.Context) uint32         { return 0 }
func (c *loadTestChunk) Append(context.Context, []*chunk.Feed, func(*chunk.Feed, uint64) error) error {
	return nil
}
func (c *loadTestChunk) Read(context.Context, uint64) (*chunk.Feed, error) { return nil, nil }
func (c *loadTestChunk) Range(context.Context, func(*chunk.Feed, uint64) error) error {
	return nil
}

var _ primary.Index = (*trackedPrimaryIndex)(nil)
var _ vector.Index = (*trackedVectorIndex)(nil)
var _ inverted.Index = (*trackedInvertedIndex)(nil)
var _ chunk.File = (*loadTestChunk)(nil)

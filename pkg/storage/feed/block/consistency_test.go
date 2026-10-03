package block

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/llm"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/chunk"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/inverted"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/primary"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/vector"
)

func TestBlockAppendReturnsChunkError_BitsUT(t *testing.T) {
	writeErr := errors.New("write failed")
	ck := &controlledChunk{
		appendFn: func(context.Context, []*chunk.Feed, func(*chunk.Feed, uint64) error) error {
			return writeErr
		},
	}
	b := newAppendTestBlock(t, ck)
	t.Cleanup(func() { _ = b.Close() })

	err := b.Append(context.Background(), testModelFeed(1, "first"))

	if !errors.Is(err, writeErr) {
		t.Fatalf("Append() error = %v, want wrapped %v", err, writeErr)
	}
}

func TestBlockAppendDuplicateIDFirstWriteWins_BitsUT(t *testing.T) {
	b := newPersistentTestBlock(t)

	err := b.Append(
		context.Background(),
		testModelFeed(7, "first"),
		testModelFeed(7, "second"),
	)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	assertSinglePersistedFeed(t, b, 7, "first")
}

func TestBlockAppendConcurrentDuplicateIDFirstWriteWins_BitsUT(t *testing.T) {
	b := newPersistentTestBlock(t)

	const writers = 16
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- b.Append(context.Background(), testModelFeed(42, "same-id"))
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	assertSinglePersistedFeed(t, b, 42, "same-id")
}

func TestBlockAppendVectorDimensionMismatchDoesNotPartiallyCommit_BitsUT(t *testing.T) {
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "first"}}).
			Return([][]float32{{1, 0, 0}}, nil)
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "mismatched"}}).
			Return([][]float32{{1, 0}}, nil)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)
	ctx := context.Background()
	if err := b.Append(ctx, testModelFeed(1, "first")); err != nil {
		t.Fatalf("first Append() error = %v", err)
	}

	err = b.Append(ctx, testModelFeed(2, "mismatched"))

	if err == nil || !strings.Contains(err.Error(), "vector dimension mismatch") {
		t.Fatalf("Append() error = %v, want vector dimension mismatch", err)
	}
	if got := b.chunks[0].Count(ctx); got != 1 {
		t.Fatalf("chunk count = %d, want 1", got)
	}
	if got := b.primaryIndex.Count(ctx); got != 1 {
		t.Fatalf("primary count = %d, want 1", got)
	}
	matching := b.invertedIndex.Search(ctx, model.LabelFilter{
		Label: "value", Value: "mismatched", Equal: true,
	})
	if _, ok := matching[2]; ok {
		t.Fatal("inverted index contains rejected ID 2")
	}
	vectorMatches, searchErr := b.vectorIndex.Search(ctx, []float32{1, 0, 0}, 0, 10)
	if searchErr != nil {
		t.Fatalf("vector Search() error = %v", searchErr)
	}
	if _, ok := vectorMatches[2]; ok {
		t.Fatal("vector index contains rejected ID 2")
	}
}

func TestBlockAppendEmbeddingFailureDoesNotPartiallyCommit_BitsUT(t *testing.T) {
	embedErr := errors.New("embedding failed")
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "ok"}}).
			Return([][]float32{{1, 0}}, nil)
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "bad"}}).
			Return(nil, embedErr)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)

	err = b.Append(
		context.Background(),
		testModelFeed(1, "ok"),
		testModelFeed(2, "bad"),
	)

	if !errors.Is(err, embedErr) {
		t.Fatalf("Append() error = %v, want wrapped %v", err, embedErr)
	}
	if got := b.chunks[0].Count(context.Background()); got != 0 {
		t.Fatalf("chunk count = %d, want 0", got)
	}
	if got := b.primaryIndex.Count(context.Background()); got != 0 {
		t.Fatalf("primary count = %d, want 0", got)
	}
}

func TestBlockCloseCancelsBlockedEmbedding_BitsUT(t *testing.T) {
	embeddingStarted := make(chan struct{})
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				close(embeddingStarted)
				<-args.Get(0).(context.Context).Done()
			}).
			Return([][]float32(nil), context.Canceled)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)
	cleanedUp := false
	t.Cleanup(func() {
		if !cleanedUp {
			_ = b.Close()
		}
	})

	appendDone := make(chan error, 1)
	go func() {
		appendDone <- b.Append(context.Background(), testModelFeed(1, "blocked"))
	}()
	select {
	case <-embeddingStarted:
	case <-time.After(time.Second):
		t.Fatal("embedding did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- b.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		cleanedUp = true
	case <-time.After(time.Second):
		t.Fatal("Close blocked on embedding")
	}
	select {
	case err := <-appendDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Append() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Append did not return after Close")
	}
}

func TestBlockAppendExistingDuplicateSkipsEmbedding_BitsUT(t *testing.T) {
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, mock.Anything).
			Once().
			Return([][]float32{{1, 0}}, nil)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)

	if err := b.Append(context.Background(), testModelFeed(1, "first")); err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	if err := b.Append(context.Background(), testModelFeed(1, "ignored")); err != nil {
		t.Fatalf("duplicate Append() error = %v", err)
	}

	assertSinglePersistedFeed(t, b, 1, "first")
}

func TestBlockAppendBatchVectorDimensionMismatchDoesNotPartiallyCommit_BitsUT(t *testing.T) {
	var embeddingCalls atomic.Int32
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "three"}}).
			Run(func(mock.Arguments) { embeddingCalls.Add(1) }).
			Return([][]float32{{1, 0, 0}}, nil)
		m.On("EmbeddingLabels", mock.Anything, model.Labels{{Key: "value", Value: "two"}}).
			Run(func(mock.Arguments) { embeddingCalls.Add(1) }).
			Return([][]float32{{1, 0}}, nil)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)

	err = b.Append(
		context.Background(),
		testModelFeed(1, "three"),
		testModelFeed(2, "two"),
	)

	if err == nil || !strings.Contains(err.Error(), "vector dimension mismatch") {
		t.Fatalf("Append() error = %v, want vector dimension mismatch", err)
	}
	if got := b.chunks[0].Count(context.Background()); got != 0 {
		t.Fatalf("chunk count = %d, want 0", got)
	}
	if got := b.primaryIndex.Count(context.Background()); got != 0 {
		t.Fatalf("primary count = %d, want 0", got)
	}
	if got := embeddingCalls.Load(); got != 2 {
		t.Fatalf("EmbeddingLabels calls = %d, want 2", got)
	}
}

func TestBlockAppendRejectsInvalidVectorBeforeCommit_BitsUT(t *testing.T) {
	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, mock.Anything).
			Return([][]float32{{1, float32(math.NaN())}}, nil)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	b := newPersistentTestBlockWithLLM(t, llmFactory)

	err = b.Append(context.Background(), testModelFeed(1, "invalid"))

	if err == nil || !strings.Contains(err.Error(), "finite") {
		t.Fatalf("Append() error = %v, want finite-vector error", err)
	}
	if got := b.chunks[0].Count(context.Background()); got != 0 {
		t.Fatalf("chunk count = %d, want 0", got)
	}
	if got := b.primaryIndex.Count(context.Background()); got != 0 {
		t.Fatalf("primary count = %d, want 0", got)
	}
}

func TestBlockCloseWaitsForAppendAndRejectsLaterWrites_BitsUT(t *testing.T) {
	appendStarted := make(chan struct{})
	releaseAppend := make(chan struct{})
	closeCalled := make(chan struct{})
	var appendCalls atomic.Int32
	ck := &controlledChunk{
		appendFn: func(_ context.Context, feeds []*chunk.Feed, onSuccess func(*chunk.Feed, uint64) error) error {
			appendCalls.Add(1)
			close(appendStarted)
			<-releaseAppend
			for i, feed := range feeds {
				if err := onSuccess(feed, uint64(64+i)); err != nil {
					return err
				}
			}

			return nil
		},
		closeFn: func() error {
			close(closeCalled)

			return nil
		},
	}
	b := newAppendTestBlock(t, ck)

	appendErr := make(chan error, 1)
	go func() { appendErr <- b.Append(context.Background(), testModelFeed(1, "first")) }()

	select {
	case <-appendStarted:
	case <-time.After(time.Second):
		t.Fatal("Append returned before starting the chunk write")
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- b.Close() }()
	select {
	case <-closeCalled:
		t.Fatal("Close closed the chunk while Append was in progress")
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseAppend)
	if err := <-appendErr; err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := b.Append(context.Background(), testModelFeed(2, "after-close")); err == nil {
		t.Fatal("Append() after Close returned nil error")
	}
	if got := appendCalls.Load(); got != 1 {
		t.Fatalf("chunk Append calls = %d, want 1", got)
	}
}

func TestBlockCloseIsIdempotent_BitsUT(t *testing.T) {
	var closeCalls atomic.Int32
	ck := &controlledChunk{
		closeFn: func() error {
			closeCalls.Add(1)

			return nil
		},
	}
	b := newAppendTestBlock(t, ck)

	if err := b.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("chunk Close calls = %d, want 1", got)
	}
}

func TestBlockCloseAttemptsEveryChildAndJoinsErrors_BitsUT(t *testing.T) {
	chunkErr := errors.New("chunk close failed")
	ck := &controlledChunk{closeFn: func() error { return chunkErr }}
	b := newAppendTestBlock(t, ck)
	vectorErr := errors.New("vector close failed")
	b.vectorIndex = &controlledVectorIndex{Index: b.vectorIndex, closeErr: vectorErr}

	err := b.Close()

	if !errors.Is(err, vectorErr) {
		t.Fatalf("Close() error = %v, want wrapped %v", err, vectorErr)
	}
	if !errors.Is(err, chunkErr) {
		t.Fatalf("Close() error = %v, want wrapped %v", err, chunkErr)
	}
}

func newPersistentTestBlock(t *testing.T) *block {
	t.Helper()

	deps := testBlockDependencies(t, nil)

	return newPersistentTestBlockWithDependencies(t, deps)
}

func newPersistentTestBlockWithLLM(t *testing.T, llmFactory llm.Factory) *block {
	t.Helper()

	deps := testBlockDependencies(t, nil)
	deps.LLMFactory = llmFactory

	return newPersistentTestBlockWithDependencies(t, deps)
}

func newPersistentTestBlockWithDependencies(t *testing.T, deps Dependencies) *block {
	t.Helper()

	created, err := new("test", &Config{
		Dir: t.TempDir(),
		ForCreate: &ForCreateConfig{
			Start:        time.Now().Add(-time.Hour),
			Duration:     24 * time.Hour,
			EmbeddingLLM: "test",
		},
	}, deps)
	if err != nil {
		t.Fatalf("new() error = %v", err)
	}
	t.Cleanup(func() {
		if err := created.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return created.(*block)
}

func newAppendTestBlock(t *testing.T, ck chunk.File) *block {
	t.Helper()

	deps := testBlockDependencies(t, ck)
	primaryIndex, err := deps.PrimaryFactory.New("test", &primary.Config{}, primary.Dependencies{})
	if err != nil {
		t.Fatalf("create primary index: %v", err)
	}
	vectorIndex, err := deps.VectorFactory.New("test", &vector.Config{}, vector.Dependencies{})
	if err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	invertedIndex, err := deps.InvertedFactory.New("test", &inverted.Config{}, inverted.Dependencies{})
	if err != nil {
		t.Fatalf("create inverted index: %v", err)
	}
	b := &block{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "FeedBlock",
			Instance:     "test",
			Config:       &Config{ForCreate: &ForCreateConfig{EmbeddingLLM: "test"}, embeddingLLM: "test"},
			Dependencies: deps,
		}),
		primaryIndex:  primaryIndex,
		vectorIndex:   vectorIndex,
		invertedIndex: invertedIndex,
		chunks:        chunkChain{ck},
	}
	b.state.Store(StateHot)
	b.lastDataAccess.Store(time.Now())

	return b
}

func testBlockDependencies(t *testing.T, ck chunk.File) Dependencies {
	t.Helper()

	llmFactory, err := llm.NewFactory("test", nil, llm.FactoryDependencies{}, func(m *mock.Mock) {
		m.On("EmbeddingLabels", mock.Anything, mock.Anything).Return([][]float32{{1, 0}}, nil)
	})
	if err != nil {
		t.Fatalf("create LLM factory: %v", err)
	}
	chunkFactory := chunk.NewFactory()
	if ck != nil {
		chunkFactory = component.FactoryFunc[chunk.File, chunk.Config, chunk.Dependencies](
			func(string, *chunk.Config, chunk.Dependencies) (chunk.File, error) { return ck, nil },
		)
	}

	return Dependencies{
		ChunkFactory:    chunkFactory,
		PrimaryFactory:  primary.NewFactory(),
		InvertedFactory: inverted.NewFactory(),
		VectorFactory:   vector.NewFactory(),
		LLMFactory:      llmFactory,
	}
}

func testModelFeed(id uint64, value string) *model.Feed {
	return &model.Feed{
		ID:     id,
		Labels: model.Labels{{Key: "value", Value: value}},
		Time:   time.Unix(int64(id+1), 0),
	}
}

func assertSinglePersistedFeed(t *testing.T, b *block, id uint64, value string) {
	t.Helper()

	if got := b.primaryIndex.Count(context.Background()); got != 1 {
		t.Fatalf("primary count = %d, want 1", got)
	}
	ref, ok := b.primaryIndex.Search(context.Background(), id)
	if !ok {
		t.Fatalf("primary index does not contain ID %d", id)
	}
	if got := b.chunks[ref.Chunk].Count(context.Background()); got != 1 {
		t.Fatalf("chunk count = %d, want 1", got)
	}
	persisted, err := b.chunks[ref.Chunk].Read(context.Background(), ref.Offset)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := persisted.Labels[0].Value; got != value {
		t.Fatalf("persisted value = %q, want %q", got, value)
	}
	matching := b.invertedIndex.Search(context.Background(), model.LabelFilter{
		Label: "value", Value: value, Equal: true,
	})
	if _, ok := matching[id]; !ok {
		t.Fatalf("inverted index does not contain ID %d", id)
	}
	vectorMatches, err := b.vectorIndex.Search(context.Background(), []float32{1, 0}, 0, 10)
	if err != nil {
		t.Fatalf("vector Search() error = %v", err)
	}
	if _, ok := vectorMatches[id]; !ok {
		t.Fatalf("vector index does not contain ID %d", id)
	}
}

type controlledChunk struct {
	appendFn func(context.Context, []*chunk.Feed, func(*chunk.Feed, uint64) error) error
	closeFn  func() error
	count    atomic.Uint32
}

type controlledVectorIndex struct {
	vector.Index
	closeErr error
}

func (i *controlledVectorIndex) Close() error { return i.closeErr }

func (c *controlledChunk) Name() string                         { return "controlled" }
func (c *controlledChunk) Instance() string                     { return "controlled" }
func (c *controlledChunk) Run() error                           { return nil }
func (c *controlledChunk) Ready() <-chan struct{}               { return closedTestChannel() }
func (c *controlledChunk) EnsureReadonly(context.Context) error { return nil }
func (c *controlledChunk) Count(context.Context) uint32         { return c.count.Load() }
func (c *controlledChunk) Read(context.Context, uint64) (*chunk.Feed, error) {
	return nil, errors.New("not implemented")
}
func (c *controlledChunk) Range(context.Context, func(*chunk.Feed, uint64) error) error {
	return nil
}
func (c *controlledChunk) Append(
	ctx context.Context,
	feeds []*chunk.Feed,
	onSuccess func(*chunk.Feed, uint64) error,
) error {
	if c.appendFn != nil {
		err := c.appendFn(ctx, feeds, onSuccess)
		if err == nil {
			c.count.Add(uint32(len(feeds)))
		}

		return err
	}

	return nil
}
func (c *controlledChunk) Close() error {
	if c.closeFn != nil {
		return c.closeFn()
	}

	return nil
}

func closedTestChannel() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

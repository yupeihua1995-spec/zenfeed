package feed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
)

func TestEnsureHeadBlockPublishesOnlyAfterSuccessfulStart_BitsUT(t *testing.T) {
	runErr := errors.New("start failed")
	dir := t.TempDir()
	now := time.Date(2025, time.January, 2, 12, 0, 0, 0, time.UTC)
	var factoryCalls atomic.Int32
	var candidates []*headPublishTestBlock
	firstRunStarted := make(chan struct{})
	releaseFirstRun := make(chan struct{})
	factory := component.FactoryFunc[block.Block, block.Config, block.Dependencies](
		func(instance string, config *block.Config, _ block.Dependencies) (block.Block, error) {
			if err := os.MkdirAll(config.Dir, 0o700); err != nil {
				return nil, err
			}
			if err := os.WriteFile(filepath.Join(config.Dir, "candidate"), []byte(instance), 0o600); err != nil {
				return nil, err
			}
			candidate := newHeadPublishTestBlock(instance, config.Dir, config.ForCreate.Start, config.ForCreate.Duration)
			if factoryCalls.Add(1) == 1 {
				candidate.runErr = runErr
				candidate.runStarted = firstRunStarted
				candidate.releaseRun = releaseFirstRun
			}
			candidates = append(candidates, candidate)

			return candidate, nil
		},
	)
	s := &storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:     "FeedStorage",
			Instance: "head-publish-test",
			Config: &Config{
				Dir: dir, BlockDuration: 24 * time.Hour, EmbeddingLLM: "test",
			},
			Dependencies: Dependencies{BlockFactory: factory},
		}),
		blocks:     &blockChain{blocks: make(map[string]block.Block)},
		childExits: make(chan blockExit, 1),
		running:    true,
	}
	t.Cleanup(func() { _ = s.Close() })

	errCh := make(chan error, 1)
	go func() { errCh <- s.ensureHeadBlock(context.Background(), now) }()
	select {
	case <-firstRunStarted:
	case <-time.After(time.Second):
		t.Fatal("candidate did not start")
	}
	publishedBeforeReady := len(s.blocks.list(nil))
	close(releaseFirstRun)
	err := <-errCh
	if publishedBeforeReady != 0 {
		t.Fatalf("candidate was published before becoming ready: blocks = %d", publishedBeforeReady)
	}
	if !errors.Is(err, runErr) {
		t.Fatalf("first ensureHeadBlock() error = %v, want wrapped %v", err, runErr)
	}
	if got := len(s.blocks.list(nil)); got != 0 {
		t.Fatalf("published blocks after failed start = %d, want 0", got)
	}
	if got := s.blocks.endTime(); !got.IsZero() {
		t.Fatalf("endTime after failed start = %v, want zero", got)
	}
	if got := len(s.ownedBlocks); got != 0 {
		t.Fatalf("owned blocks after failed start = %d, want 0", got)
	}
	if got := candidates[0].closeCalls.Load(); got != 1 {
		t.Fatalf("failed candidate Close calls = %d, want 1", got)
	}
	if got := candidates[0].clearCalls.Load(); got != 1 {
		t.Fatalf("failed candidate ClearOnDisk calls = %d, want 1", got)
	}
	if _, statErr := os.Stat(candidates[0].dir); !os.IsNotExist(statErr) {
		t.Fatalf("failed candidate directory still exists: %v", statErr)
	}

	if err := s.ensureHeadBlock(context.Background(), now); err != nil {
		t.Fatalf("second ensureHeadBlock() error = %v", err)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("block factory calls = %d, want 2", got)
	}
	if got := len(s.blocks.list(nil)); got != 1 {
		t.Fatalf("published blocks after retry = %d, want 1", got)
	}
	published, ok := s.blocks.get(now.Add(time.Minute))
	if !ok {
		t.Fatal("successfully started candidate was not published")
	}
	if published != candidates[1] {
		t.Fatal("successfully started candidate was not published as head")
	}
}

type headPublishTestBlock struct {
	*component.Base[block.Config, struct{}]
	dir        string
	start      time.Time
	duration   time.Duration
	runErr     error
	runStarted chan struct{}
	releaseRun chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
	clearCalls atomic.Int32
}

func newHeadPublishTestBlock(name, dir string, start time.Time, duration time.Duration) *headPublishTestBlock {
	return &headPublishTestBlock{
		Base: component.New(&component.BaseConfig[block.Config, struct{}]{
			Name: "HeadPublishTestBlock", Instance: name, Config: &block.Config{},
		}),
		dir: dir, start: start, duration: duration,
	}
}

func (b *headPublishTestBlock) Run() error {
	if b.runStarted != nil {
		close(b.runStarted)
		<-b.releaseRun
	}
	if b.runErr != nil {
		return b.runErr
	}

	return b.Base.Run()
}
func (b *headPublishTestBlock) Close() error {
	b.closeOnce.Do(func() {
		b.closeCalls.Add(1)
		_ = b.Base.Close()
	})

	return nil
}
func (b *headPublishTestBlock) Reload(*block.Config) error { return nil }
func (b *headPublishTestBlock) Start() time.Time           { return b.start }
func (b *headPublishTestBlock) End() time.Time             { return b.start.Add(b.duration) }
func (b *headPublishTestBlock) State() block.State         { return block.StateHot }
func (b *headPublishTestBlock) TransformToCold() error     { return nil }
func (b *headPublishTestBlock) ClearOnDisk() error {
	b.clearCalls.Add(1)

	return os.RemoveAll(b.dir)
}
func (b *headPublishTestBlock) Append(context.Context, ...*model.Feed) error { return nil }
func (b *headPublishTestBlock) Query(context.Context, block.QueryOptions) ([]*block.FeedVO, error) {
	return nil, nil
}
func (b *headPublishTestBlock) Exists(context.Context, uint64) (bool, error) {
	return false, nil
}

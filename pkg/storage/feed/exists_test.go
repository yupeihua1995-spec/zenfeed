package feed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/mock"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/rewrite"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
)

func TestStorageExistsHintFallback_BitsUT(t *testing.T) {
	now := time.Date(2025, time.January, 2, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMock()
	mockClock.Set(now)
	previousClock := clk
	clk = mockClock
	t.Cleanup(func() { clk = previousClock })

	tests := []struct {
		name         string
		hintedExists bool
		hintedErr    error
		headExists   bool
		wantExists   bool
		wantErr      error
		wantHead     int32
	}{
		{
			name:         "hint miss checks current head",
			hintedExists: false,
			headExists:   true,
			wantExists:   true,
			wantHead:     1,
		},
		{
			name:         "hint hit returns immediately",
			hintedExists: true,
			headExists:   false,
			wantExists:   true,
			wantHead:     0,
		},
		{
			name:       "hint error is propagated",
			hintedErr:  errors.New("hint lookup failed"),
			headExists: true,
			wantErr:    errors.New("hint lookup failed"),
			wantHead:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintErr := tt.hintedErr
			if tt.wantErr != nil {
				hintErr = tt.wantErr
			}
			hinted := &existsTestBlock{
				start:  now.Add(-48 * time.Hour),
				end:    now.Add(-24 * time.Hour),
				exists: tt.hintedExists,
				err:    hintErr,
			}
			head := &existsTestBlock{
				start:  now.Add(-time.Hour),
				end:    now.Add(time.Hour),
				exists: tt.headExists,
			}
			s := storage{blocks: &blockChain{blocks: map[string]block.Block{
				"hinted": hinted,
				"head":   head,
			}}}

			got, err := s.Exists(context.Background(), 99, now.Add(-36*time.Hour))
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Exists() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Exists() error = %v", err)
			}
			if got != tt.wantExists {
				t.Fatalf("Exists() = %v, want %v", got, tt.wantExists)
			}
			if calls := hinted.calls.Load(); calls != 1 {
				t.Fatalf("hinted block calls = %d, want 1", calls)
			}
			if calls := head.calls.Load(); calls != tt.wantHead {
				t.Fatalf("head block calls = %d, want %d", calls, tt.wantHead)
			}
		})
	}
}

func TestStorageExistsHintIsHeadOnlyChecksOnce_BitsUT(t *testing.T) {
	now := time.Date(2025, time.January, 2, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMock()
	mockClock.Set(now)
	previousClock := clk
	clk = mockClock
	t.Cleanup(func() { clk = previousClock })

	head := &existsTestBlock{
		start: now.Add(-time.Hour),
		end:   now.Add(time.Hour),
	}
	s := storage{blocks: &blockChain{blocks: map[string]block.Block{"head": head}}}

	exists, err := s.Exists(context.Background(), 99, now)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Fatal("Exists() = true, want false")
	}
	if calls := head.calls.Load(); calls != 1 {
		t.Fatalf("head block calls = %d, want 1", calls)
	}
}

func TestStorageExistsFallsBackToOtherRetainedBlocks_BitsUT(t *testing.T) {
	now := time.Date(2025, time.January, 4, 12, 0, 0, 0, time.UTC)
	mockClock := clock.NewMock()
	mockClock.Set(now)
	previousClock := clk
	clk = mockClock
	t.Cleanup(func() { clk = previousClock })

	hinted := &existsTestBlock{start: now.Add(-72 * time.Hour), end: now.Add(-48 * time.Hour)}
	formerHead := &existsTestBlock{start: now.Add(-48 * time.Hour), end: now.Add(-24 * time.Hour), exists: true}
	head := &existsTestBlock{start: now.Add(-time.Hour), end: now.Add(time.Hour)}
	s := storage{blocks: &blockChain{blocks: map[string]block.Block{
		"hinted": hinted, "former-head": formerHead, "head": head,
	}}}

	exists, err := s.Exists(context.Background(), 99, now.Add(-60*time.Hour))

	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Fatal("Exists() = false, want true from former ingestion head")
	}
	if hinted.calls.Load() != 1 || head.calls.Load() != 1 || formerHead.calls.Load() != 1 {
		t.Fatalf("lookup calls hint/head/former = %d/%d/%d, want 1/1/1", hinted.calls.Load(), head.calls.Load(), formerHead.calls.Load())
	}
}

func TestStorageRewritePreservesInputOrder_BitsUT(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	rewriterFactory := rewrite.NewFactory(func(m *mock.Mock) {
		m.On("Labels", mock.Anything, model.Labels{{Key: "value", Value: "first"}}).
			Run(func(mock.Arguments) {
				close(firstStarted)
				<-releaseFirst
			}).
			Return(model.Labels{{Key: "value", Value: "first"}}, nil)
		m.On("Labels", mock.Anything, model.Labels{{Key: "value", Value: "second"}}).
			Run(func(mock.Arguments) {
				<-firstStarted
				close(releaseFirst)
			}).
			Return(model.Labels{{Key: "value", Value: "second"}}, nil)
	})
	rewriter, err := rewriterFactory.New("test", nil, rewrite.Dependencies{})
	if err != nil {
		t.Fatalf("create rewriter: %v", err)
	}
	s := storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "FeedStorage", Dependencies: Dependencies{Rewriter: rewriter},
		}),
	}
	first := &model.Feed{ID: 7, Labels: model.Labels{{Key: "value", Value: "first"}}}
	second := &model.Feed{ID: 7, Labels: model.Labels{{Key: "value", Value: "second"}}}

	rewritten, err := s.rewrite(context.Background(), []*model.Feed{first, second})
	if err != nil {
		t.Fatalf("rewrite() error = %v", err)
	}
	if len(rewritten) != 2 {
		t.Fatalf("rewrite() length = %d, want 2", len(rewritten))
	}
	if rewritten[0] != first || rewritten[1] != second {
		t.Fatalf("rewrite() order = %p, %p; want %p, %p", rewritten[0], rewritten[1], first, second)
	}
}

type existsTestBlock struct {
	start, end time.Time
	exists     bool
	err        error
	calls      atomic.Int32
}

func (b *existsTestBlock) Name() string                                 { return "test" }
func (b *existsTestBlock) Instance() string                             { return "test" }
func (b *existsTestBlock) Run() error                                   { return nil }
func (b *existsTestBlock) Ready() <-chan struct{}                       { return closedExistsTestChannel() }
func (b *existsTestBlock) Close() error                                 { return nil }
func (b *existsTestBlock) Reload(*block.Config) error                   { return nil }
func (b *existsTestBlock) Start() time.Time                             { return b.start }
func (b *existsTestBlock) End() time.Time                               { return b.end }
func (b *existsTestBlock) State() block.State                           { return block.StateHot }
func (b *existsTestBlock) TransformToCold() error                       { return nil }
func (b *existsTestBlock) ClearOnDisk() error                           { return nil }
func (b *existsTestBlock) Append(context.Context, ...*model.Feed) error { return nil }
func (b *existsTestBlock) Query(context.Context, block.QueryOptions) ([]*block.FeedVO, error) {
	return nil, nil
}
func (b *existsTestBlock) Exists(context.Context, uint64) (bool, error) {
	b.calls.Add(1)

	return b.exists, b.err
}

func closedExistsTestChannel() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)

	return ch
}

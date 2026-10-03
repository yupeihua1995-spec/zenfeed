package feed

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/rewrite"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
)

func TestStorageAppendWithoutHeadReturnsError_BitsUT(t *testing.T) {
	rewriterFactory := rewrite.NewFactory(func(m *mock.Mock) {
		m.On("Labels", mock.Anything, mock.Anything).Return(model.Labels{{Key: "value", Value: "x"}}, nil)
	})
	rewriter, err := rewriterFactory.New("test", nil, rewrite.Dependencies{})
	if err != nil {
		t.Fatal(err)
	}
	s := storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "FeedStorage", Dependencies: Dependencies{Rewriter: rewriter},
		}),
		blocks: &blockChain{blocks: make(map[string]block.Block)},
	}

	err = s.Append(context.Background(), &model.Feed{
		ID: 1, Labels: model.Labels{{Key: "value", Value: "x"}}, Time: time.Now(),
	})

	if err == nil || !strings.Contains(err.Error(), "head block") {
		t.Fatalf("Append() error = %v, want head block error", err)
	}
}

func TestReloadWaitsForHeadPublication_BitsUT(t *testing.T) {
	start := time.Now()
	created := make(chan struct{})
	release := make(chan struct{})
	fake := newHeadPublishTestBlock("candidate", t.TempDir(), start, 24*time.Hour)
	factory := component.FactoryFunc[block.Block, block.Config, block.Dependencies](
		func(string, *block.Config, block.Dependencies) (block.Block, error) {
			close(created)
			<-release

			return fake, nil
		},
	)
	s := &storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name: "FeedStorage", Config: &Config{Dir: t.TempDir(), Retention: 48 * time.Hour, BlockDuration: 24 * time.Hour, EmbeddingLLM: "old"},
			Dependencies: Dependencies{BlockFactory: factory},
		}),
		blocks: &blockChain{blocks: make(map[string]block.Block)}, childExits: make(chan blockExit, 1), running: true,
	}
	t.Cleanup(func() { _ = s.Close() })

	headDone := make(chan error, 1)
	go func() { headDone <- s.ensureHeadBlock(context.Background(), start) }()
	<-created
	reloadDone := make(chan error, 1)
	go func() {
		app := &config.App{}
		app.Storage.Dir = s.Config().Dir
		app.Storage.Feed.Retention = timeutil.Duration(48 * time.Hour)
		app.Storage.Feed.BlockDuration = timeutil.Duration(24 * time.Hour)
		app.Storage.Feed.EmbeddingLLM = "new"
		reloadDone <- s.Reload(app)
	}()

	select {
	case err := <-reloadDone:
		t.Fatalf("Reload returned during head construction: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-headDone; err != nil {
		t.Fatalf("ensureHeadBlock() error = %v", err)
	}
	if err := <-reloadDone; err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
}

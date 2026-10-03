package rule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
)

func TestWatchExecuteBlockedOutputStopsOnClose(t *testing.T) {
	queryCalled := make(chan struct{})
	feedStorage, err := feed.NewFactory(component.MockOption(func(m *mock.Mock) {
		m.On("Query", mock.Anything, mock.AnythingOfType("block.QueryOptions")).
			Run(func(mock.Arguments) { close(queryCalled) }).
			Return([]*block.FeedVO{{Feed: &model.Feed{Time: time.Now()}}}, nil)
	})).New(component.Global, nil, feed.Dependencies{})
	require.NoError(t, err)

	config := &Config{Name: "watch", WatchInterval: 10 * time.Minute}
	r := &watch{Base: component.New(&component.BaseConfig[Config, Dependencies]{
		Name:     "WatchRuler",
		Instance: "test",
		Config:   config,
		Dependencies: Dependencies{
			FeedStorage: feedStorage,
			Out:         make(chan *Result),
		},
	})}

	executeDone := make(chan error, 1)
	go func() {
		executeDone <- r.execute(r.Context(), time.Now().Add(-time.Hour), time.Now())
	}()
	select {
	case <-queryCalled:
	case <-time.After(time.Second):
		t.Fatal("rule did not query feeds")
	}

	require.NoError(t, r.Close())
	select {
	case err := <-executeDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("blocked output send did not stop after Close")
	}
}

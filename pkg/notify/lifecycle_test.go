package notify

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/notify/channel"
	"github.com/glidea/zenfeed/pkg/notify/route"
	"github.com/glidea/zenfeed/pkg/schedule/rule"
	"github.com/glidea/zenfeed/pkg/storage/kv"
)

func TestNotifierCloseUnblocksFullQueueAndJoinsRun(t *testing.T) {
	withSendConcurrency(t, 0)

	in := make(chan *rule.Result)
	routeCalled := make(chan struct{})
	router := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		close(routeCalled)

		return []*route.Group{{
			FeedGroup: route.FeedGroup{Name: "group", Time: time.Now()},
			Receivers: []string{"receiver"},
		}}, nil
	})
	n := newLifecycleNotifier(in, router, newLifecycleChannel(), newLifecycleKV())
	for range cap(n.channelSendWork) {
		n.channelSendWork <- sendWork{}
	}

	runDone := make(chan error, 1)
	go func() { runDone <- n.Run() }()
	requireClosed(t, n.Ready(), "notifier did not become ready")

	in <- &rule.Result{}
	requireClosed(t, routeCalled, "notifier did not route the input")

	closeDone := make(chan error, 1)
	go func() { closeDone <- n.Close() }()
	require.NoError(t, receiveError(t, closeDone, "notifier Close blocked on a full work queue"))
	require.NoError(t, receiveError(t, runDone, "notifier Run did not exit after Close"))
	require.NoError(t, n.Close(), "a second Close must be safe")
}

func TestNotifierRunDoesNotRouteClosedInput(t *testing.T) {
	withSendConcurrency(t, 0)

	in := make(chan *rule.Result)
	close(in)
	routeCalled := make(chan struct{}, 1)
	router := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		routeCalled <- struct{}{}

		return nil, nil
	})
	n := newLifecycleNotifier(in, router, newLifecycleChannel(), newLifecycleKV())

	runDone := make(chan error, 1)
	go func() { runDone <- n.Run() }()
	requireClosed(t, n.Ready(), "notifier did not become ready")

	select {
	case <-routeCalled:
		t.Fatal("closed input was routed as a nil result")
	case err := <-runDone:
		t.Fatalf("Run returned before Close: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, n.Close())
	require.NoError(t, receiveError(t, runDone, "notifier Run did not exit after Close"))
}

func TestNotifierCloseWaitsForActiveSendWorker(t *testing.T) {
	withSendConcurrency(t, 1)

	in := make(chan *rule.Result)
	router := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	notifyChannel := newLifecycleChannel()
	notifyChannel.blockSend = true
	n := newLifecycleNotifier(in, router, notifyChannel, newLifecycleKV())

	runDone := make(chan error, 1)
	go func() { runDone <- n.Run() }()
	requireClosed(t, n.Ready(), "notifier did not become ready")
	n.channelSendWork <- sendWork{
		group:    &route.FeedGroup{Name: "group", Time: time.Now()},
		receiver: Receiver{Name: "receiver"},
	}
	requireClosed(t, notifyChannel.sendStarted, "send worker did not start")

	closeDone := make(chan error, 1)
	go func() { closeDone <- n.Close() }()
	requireClosed(t, notifyChannel.sendReturned, "active send did not observe cancellation")
	require.NoError(t, receiveError(t, closeDone, "Close did not join the active send worker"))
	require.True(t, notifyChannel.sendReturnedBeforeClose(), "channel closed before active Send returned")
	require.NoError(t, receiveError(t, runDone, "notifier Run did not exit after Close"))
}

func TestNotifierRunReportsCurrentChildExitAndIgnoresOldGeneration(t *testing.T) {
	withSendConcurrency(t, 0)

	oldRouter := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	oldRouter.runExit = make(chan error, 1)
	oldChannel := newLifecycleChannel()
	n := newLifecycleNotifier(make(chan *rule.Result), oldRouter, oldChannel, newLifecycleKV())
	runDone := make(chan error, 1)
	go func() { runDone <- n.Run() }()
	requireClosed(t, n.Ready(), "notifier did not become ready")

	currentErr := errors.New("current router failed")
	newRouter := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	newRouter.runExit = make(chan error, 1)
	newChannel := newLifecycleChannel()
	newRouterRun := newComponentLifecycle(newRouter)
	newChannelRun := newComponentLifecycle(newChannel)
	require.NoError(t, newRouterRun.runUntilReady(n.Context(), time.Second))
	require.NoError(t, newChannelRun.runUntilReady(n.Context(), time.Second))

	n.mu.Lock()
	oldRouterRun := n.routerRun
	oldChannelRun := n.channelRun
	n.router = newRouter
	n.channel = newChannel
	n.routerRun = newRouterRun
	n.channelRun = newChannelRun
	n.generation++
	n.mu.Unlock()
	require.NoError(t, closeNotifierChildren(oldRouterRun, oldChannelRun))

	select {
	case err := <-runDone:
		t.Fatalf("old generation exit stopped notifier: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	newRouter.runExit <- currentErr
	err := receiveError(t, runDone, "current child failure was not reported")
	require.ErrorIs(t, err, currentErr)
	require.ErrorIs(t, n.Close(), currentErr)
}

func TestNotifierRunReportsCurrentChannelExit(t *testing.T) {
	withSendConcurrency(t, 0)

	router := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	notifyChannel := newLifecycleChannel()
	notifyChannel.runExit = make(chan error, 1)
	n := newLifecycleNotifier(make(chan *rule.Result), router, notifyChannel, newLifecycleKV())
	runDone := make(chan error, 1)
	go func() { runDone <- n.Run() }()
	requireClosed(t, n.Ready(), "notifier did not become ready")

	currentErr := errors.New("current channel failed")
	notifyChannel.runExit <- currentErr
	err := receiveError(t, runDone, "current channel failure was not reported")
	require.ErrorIs(t, err, currentErr)
	require.ErrorIs(t, n.Close(), currentErr)
}

func TestComponentLifecycleStopSuppressesShutdownInducedRunError(t *testing.T) {
	shutdownErr := errors.New("shutdown interrupted run")
	component := newShutdownErrorComponent(shutdownErr)
	lifecycle := newComponentLifecycle(component)
	require.NoError(t, lifecycle.runUntilReady(context.Background(), time.Second))

	require.NoError(t, lifecycle.stop())
	require.NoError(t, lifecycle.stop())
	require.Equal(t, int32(1), component.closeCalls.Load())
}

func TestNotifierReloadRejectsRouterThatExitsWhileChannelStarts(t *testing.T) {
	withSendConcurrency(t, 0)

	oldRouter := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	oldChannel := newLifecycleChannel()
	n := newLifecycleNotifier(make(chan *rule.Result), oldRouter, oldChannel, newLifecycleKV())
	n.generation = 1
	oldConfig := n.Config()

	stagedErr := errors.New("staged router failed")
	stagedRouter := newLifecycleRouter(func(context.Context, *rule.Result) ([]*route.Group, error) {
		return nil, nil
	})
	stagedRouter.runExit = make(chan error, 1)
	stagedRouter.runReturned = make(chan struct{})
	stagedChannel := newLifecycleChannel()
	stagedChannel.runStarted = make(chan struct{})
	stagedChannel.readyGate = make(chan struct{})
	n.Base = component.New(&component.BaseConfig[Config, Dependencies]{
		Name: "Notifier", Instance: "test", Config: oldConfig,
		Dependencies: Dependencies{
			In:             make(chan *rule.Result),
			KVStorage:      newLifecycleKV(),
			RouterFactory:  lifecycleRouterFactory{router: stagedRouter},
			ChannelFactory: lifecycleChannelFactory{channel: stagedChannel},
		},
	})

	app := &config.App{}
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- n.Reload(app) }()
	requireClosed(t, stagedChannel.runStarted, "staged channel did not start")
	stagedRouter.runExit <- stagedErr
	requireClosed(t, stagedRouter.runReturned, "staged router did not exit")
	close(stagedChannel.readyGate)

	err := receiveError(t, reloadDone, "reload did not reject a dead staged router")
	require.ErrorIs(t, err, stagedErr)
	require.Same(t, oldRouter, n.router)
	require.Same(t, oldChannel, n.channel)
	require.Same(t, oldConfig, n.Config())
	require.Equal(t, uint64(1), n.generation)
}

func newLifecycleNotifier(
	in <-chan *rule.Result,
	router route.Router,
	notifyChannel channel.Channel,
	storage kv.Storage,
) *notifier {
	config := &Config{Receivers: Receivers{{Name: "receiver"}}}
	n := &notifier{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:     "Notifier",
			Instance: "test",
			Config:   config,
			Dependencies: Dependencies{
				In:        in,
				KVStorage: storage,
			},
		}),
		router:          router,
		channel:         notifyChannel,
		channelSendWork: make(chan sendWork, 100),
	}
	n.routerRun = newComponentLifecycle(router)
	n.channelRun = newComponentLifecycle(notifyChannel)

	return n
}

type lifecycleRouter struct {
	*component.Base[struct{}, struct{}]
	route       func(context.Context, *rule.Result) ([]*route.Group, error)
	runExit     chan error
	runReturned chan struct{}
}

func (r *lifecycleRouter) Run() error {
	r.MarkReady()
	defer func() {
		if r.runReturned != nil {
			close(r.runReturned)
		}
	}()
	if r.runExit == nil {
		<-r.Context().Done()

		return nil
	}
	select {
	case err := <-r.runExit:
		return err
	case <-r.Context().Done():
		return nil
	}
}

func newLifecycleRouter(routeFn func(context.Context, *rule.Result) ([]*route.Group, error)) *lifecycleRouter {
	return &lifecycleRouter{
		Base: component.New(&component.BaseConfig[struct{}, struct{}]{
			Name: "LifecycleRouter", Config: &struct{}{},
		}),
		route: routeFn,
	}
}

func (r *lifecycleRouter) Route(ctx context.Context, result *rule.Result) ([]*route.Group, error) {
	return r.route(ctx, result)
}

type lifecycleChannel struct {
	*component.Base[struct{}, struct{}]
	blockSend    bool
	runExit      chan error
	sendStarted  chan struct{}
	sendReturned chan struct{}
	mu           sync.Mutex
	sendDone     bool
	closedEarly  bool
	runStarted   chan struct{}
	readyGate    chan struct{}
}

func (c *lifecycleChannel) Run() error {
	if c.runStarted != nil {
		close(c.runStarted)
	}
	if c.readyGate != nil {
		select {
		case <-c.readyGate:
		case <-c.Context().Done():
			return nil
		}
	}
	c.MarkReady()
	if c.runExit == nil {
		<-c.Context().Done()

		return nil
	}
	select {
	case err := <-c.runExit:
		return err
	case <-c.Context().Done():
		return nil
	}
}

func newLifecycleChannel() *lifecycleChannel {
	return &lifecycleChannel{Base: component.New(&component.BaseConfig[struct{}, struct{}]{
		Name: "LifecycleChannel", Config: &struct{}{},
	}), sendStarted: make(chan struct{}), sendReturned: make(chan struct{})}
}

func (c *lifecycleChannel) Send(ctx context.Context, _ channel.Receiver, _ *route.FeedGroup) error {
	if !c.blockSend {
		return nil
	}
	close(c.sendStarted)
	<-ctx.Done()
	c.mu.Lock()
	c.sendDone = true
	c.mu.Unlock()
	close(c.sendReturned)

	return ctx.Err()
}

func (c *lifecycleChannel) Close() error {
	c.mu.Lock()
	if c.blockSend && !c.sendDone {
		c.closedEarly = true
	}
	c.mu.Unlock()

	return c.Base.Close()
}

func (c *lifecycleChannel) sendReturnedBeforeClose() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return !c.closedEarly
}

type lifecycleKV struct {
	*component.Base[struct{}, struct{}]
}

func newLifecycleKV() *lifecycleKV {
	return &lifecycleKV{Base: component.New(&component.BaseConfig[struct{}, struct{}]{
		Name: "LifecycleKV", Config: &struct{}{},
	})}
}

func (s *lifecycleKV) Get(context.Context, []byte) ([]byte, error) {
	return nil, kv.ErrNotFound
}

func (s *lifecycleKV) Set(context.Context, []byte, []byte, time.Duration) error {
	return nil
}

func withSendConcurrency(t *testing.T, concurrency int) {
	t.Helper()
	previous := sendConcurrency
	sendConcurrency = concurrency
	t.Cleanup(func() { sendConcurrency = previous })
}

func requireClosed(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func receiveError(t *testing.T, ch <-chan error, message string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal(message)
		return nil
	}
}

var _ route.Router = (*lifecycleRouter)(nil)
var _ channel.Channel = (*lifecycleChannel)(nil)
var _ kv.Storage = (*lifecycleKV)(nil)

type lifecycleRouterFactory struct {
	router route.Router
}

func (f lifecycleRouterFactory) New(
	_ string,
	_ *route.Config,
	_ route.Dependencies,
) (route.Router, error) {
	return f.router, nil
}

type lifecycleChannelFactory struct {
	channel channel.Channel
}

func (f lifecycleChannelFactory) New(
	_ string,
	_ *channel.Config,
	_ channel.Dependencies,
) (channel.Channel, error) {
	return f.channel, nil
}

type shutdownErrorComponent struct {
	*component.Base[struct{}, struct{}]
	err        error
	closeCalls atomic.Int32
}

func newShutdownErrorComponent(err error) *shutdownErrorComponent {
	return &shutdownErrorComponent{
		Base: component.New(&component.BaseConfig[struct{}, struct{}]{
			Name: "ShutdownErrorComponent", Config: &struct{}{},
		}),
		err: err,
	}
}

func (c *shutdownErrorComponent) Run() error {
	c.MarkReady()
	<-c.Context().Done()

	return c.err
}

func (c *shutdownErrorComponent) Close() error {
	c.closeCalls.Add(1)

	return c.Base.Close()
}

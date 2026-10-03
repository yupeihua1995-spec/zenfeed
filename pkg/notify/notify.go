// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package notify

import (
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/llm"
	"github.com/glidea/zenfeed/pkg/notify/channel"
	"github.com/glidea/zenfeed/pkg/notify/route"
	"github.com/glidea/zenfeed/pkg/schedule/rule"
	"github.com/glidea/zenfeed/pkg/storage/kv"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
)

// --- Interface code block ---
type Notifier interface {
	component.Component
	config.Watcher
}

type Config struct {
	Route     route.Config
	Receivers Receivers
	Channels  channel.Config
}

func (c *Config) Validate() error {
	if err := (&c.Route).Validate(); err != nil {
		return errors.Wrap(err, "invalid route")
	}
	if err := (&c.Receivers).Validate(); err != nil {
		return errors.Wrap(err, "invalid receivers")
	}
	if err := (&c.Channels).Validate(); err != nil {
		return errors.Wrap(err, "invalid channels")
	}

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.Route = route.Config{
		Route: route.Route{
			GroupBy:                    app.Notify.Route.GroupBy,
			SourceLabel:                app.Notify.Route.SourceLabel,
			SummaryPrompt:              app.Notify.Route.SummaryPrompt,
			LLM:                        app.Notify.Route.LLM,
			CompressByRelatedThreshold: app.Notify.Route.CompressByRelatedThreshold,
			Receivers:                  app.Notify.Route.Receivers,
		},
	}
	for i := range app.Notify.Route.SubRoutes {
		c.Route.SubRoutes = append(c.Route.SubRoutes, convertSubRoute(&app.Notify.Route.SubRoutes[i]))
	}
	c.Receivers = make(Receivers, len(app.Notify.Receivers))
	for i := range app.Notify.Receivers {
		c.Receivers[i] = Receiver{
			Name: app.Notify.Receivers[i].Name,
		}
		if app.Notify.Receivers[i].Email != "" {
			c.Receivers[i].Email = app.Notify.Receivers[i].Email
		}
		if app.Notify.Receivers[i].Webhook != nil {
			c.Receivers[i].Webhook = &channel.WebhookReceiver{URL: app.Notify.Receivers[i].Webhook.URL}
		}
	}

	c.Channels = channel.Config{}
	if app.Notify.Channels.Email != nil {
		c.Channels.Email = &channel.Email{
			SmtpEndpoint:            app.Notify.Channels.Email.SmtpEndpoint,
			From:                    app.Notify.Channels.Email.From,
			Password:                app.Notify.Channels.Email.Password,
			FeedMarkdownTemplate:    app.Notify.Channels.Email.FeedMarkdownTemplate,
			FeedHTMLSnippetTemplate: app.Notify.Channels.Email.FeedHTMLSnippetTemplate,
		}
	}

	return c
}

func convertSubRoute(from *config.NotifySubRoute) *route.SubRoute {
	to := &route.SubRoute{
		Route: route.Route{
			GroupBy:                    from.GroupBy,
			SourceLabel:                from.SourceLabel,
			SummaryPrompt:              from.SummaryPrompt,
			LLM:                        from.LLM,
			CompressByRelatedThreshold: from.CompressByRelatedThreshold,
			Receivers:                  from.Receivers,
		},
	}

	to.Matchers = from.Matchers
	to.Receivers = from.Receivers
	for i := range from.SubRoutes {
		to.SubRoutes = append(to.SubRoutes, convertSubRoute(&from.SubRoutes[i]))
	}

	return to
}

type Receivers []Receiver

func (rs Receivers) Validate() error {
	names := make(map[string]bool)
	for i := range rs {
		r := &rs[i]
		if err := r.Validate(); err != nil {
			return errors.Wrap(err, "invalid receiver")
		}
		if _, ok := names[r.Name]; ok {
			return errors.New("receiver name must be unique")
		}
		names[r.Name] = true
	}

	return nil
}

func (r Receivers) get(name string) *Receiver {
	for _, receiver := range r {
		if receiver.Name == name {
			return &receiver
		}
	}

	return nil
}

type Receiver struct {
	channel.Receiver
	Name string
}

func (r *Receiver) Validate() error {
	if r.Name == "" {
		return errors.New("name is required")
	}
	if err := (&r.Receiver).Validate(); err != nil {
		return errors.Wrap(err, "invalid receiver")
	}

	return nil
}

type Dependencies struct {
	In             <-chan *rule.Result
	RelatedScore   func(a, b [][]float32) (float32, error)
	RouterFactory  route.Factory
	ChannelFactory channel.Factory
	KVStorage      kv.Storage
	LLMFactory     llm.Factory
}

// --- Factory code block ---
type Factory component.Factory[Notifier, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Notifier, config.App, Dependencies](
			func(instance string, app *config.App, dependencies Dependencies) (Notifier, error) {
				m := &mockNotifier{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Notifier, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (Notifier, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	n := &notifier{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "Notifier",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		channelSendWork: make(chan sendWork, 100),
	}

	router, err := n.newRouter(&config.Route)
	if err != nil {
		return nil, errors.Wrap(err, "create router")
	}
	n.router = router
	n.routerRun = newComponentLifecycle(router)
	channel, err := n.newChannel(&config.Channels)
	if err != nil {
		closeErr := router.Close()

		return nil, stderrors.Join(
			errors.Wrap(err, "create channel"),
			errors.Wrap(closeErr, "close router after channel creation failure"),
		)
	}
	n.channel = channel
	n.channelRun = newComponentLifecycle(channel)

	return n, nil
}

// --- Implementation code block ---
type notifier struct {
	*component.Base[Config, Dependencies]

	router          route.Router
	channel         channel.Channel
	routerRun       *componentLifecycle
	channelRun      *componentLifecycle
	channelSendWork chan sendWork
	mu              sync.RWMutex
	reloadMu        sync.Mutex
	workerWG        sync.WaitGroup
	closeOnce       sync.Once
	closeErr        error
	closed          bool
	generation      uint64
}

var sendConcurrency = runtime.NumCPU() * 2

func (n *notifier) Run() (err error) {
	ctx := telemetry.StartWith(n.Context(), append(n.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()

	n.reloadMu.Lock()
	if n.closed || n.Context().Err() != nil {
		n.reloadMu.Unlock()

		return nil
	}

	if err := n.routerRun.runUntilReady(n.Context(), 10*time.Second); err != nil {
		closeErr := closeNotifierChildren(n.routerRun, n.channelRun)
		n.reloadMu.Unlock()

		if n.Context().Err() != nil {
			return nil
		}

		return stderrors.Join(errors.Wrap(err, "router not ready"), closeErr)
	}
	if err := n.channelRun.runUntilReady(n.Context(), 10*time.Second); err != nil {
		closeErr := closeNotifierChildren(n.routerRun, n.channelRun)
		n.reloadMu.Unlock()

		if n.Context().Err() != nil {
			return nil
		}

		return stderrors.Join(errors.Wrap(err, "channel not ready"), closeErr)
	}
	if err := notifierChildrenRunError(n.routerRun, n.channelRun); err != nil {
		closeErr := closeNotifierChildren(n.routerRun, n.channelRun)
		n.reloadMu.Unlock()

		if n.Context().Err() != nil {
			return nil
		}

		return stderrors.Join(err, closeErr)
	}

	if n.closed || n.Context().Err() != nil {
		closeErr := closeNotifierChildren(n.routerRun, n.channelRun)
		n.reloadMu.Unlock()

		return closeErr
	}
	n.mu.Lock()
	if n.generation == 0 {
		n.generation = 1
	}
	n.mu.Unlock()
	n.workerWG.Add(sendConcurrency)
	for i := range sendConcurrency {
		go func() {
			defer n.workerWG.Done()
			n.sendWorker(i)
		}()
	}

	n.MarkReady()
	n.reloadMu.Unlock()

	in := n.Dependencies().In
	for {
		generation, routerRun, channelRun := n.childGeneration()
		select {
		case <-ctx.Done():
			return nil
		case <-routerRun.done:
			if childErr, current := n.currentChildError(generation, routerRun, true); current {
				return childErr
			}
		case <-channelRun.done:
			if childErr, current := n.currentChildError(generation, channelRun, false); current {
				return childErr
			}
		case result, ok := <-in:
			if !ok {
				in = nil

				continue
			}
			n.handle(ctx, result)
		}
	}
}

func (n *notifier) childGeneration() (uint64, *componentLifecycle, *componentLifecycle) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.generation, n.routerRun, n.channelRun
}

func (n *notifier) currentChildError(
	generation uint64,
	child *componentLifecycle,
	routerChild bool,
) (error, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.closed || n.Context().Err() != nil || n.generation != generation {
		return nil, false
	}
	childName := "channel"
	if routerChild {
		if n.routerRun != child {
			return nil, false
		}
		childName = "router"
	} else if n.channelRun != child {
		return nil, false
	}

	return errors.Wrapf(child.unexpectedRunError(), "%s exited", childName), true
}

func (n *notifier) Close() error {
	n.closeOnce.Do(func() {
		var closeErrs []error
		if err := n.Base.Close(); err != nil {
			closeErrs = append(closeErrs, errors.Wrap(err, "close base"))
		}

		n.reloadMu.Lock()
		n.mu.Lock()
		n.closed = true
		n.mu.Unlock()
		n.workerWG.Wait()
		if err := closeNotifierChildren(n.routerRun, n.channelRun); err != nil {
			closeErrs = append(closeErrs, err)
		}
		n.reloadMu.Unlock()

		n.closeErr = stderrors.Join(closeErrs...)
	})

	return n.closeErr
}

func (n *notifier) Reload(app *config.App) error {
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "invalid config")
	}

	n.reloadMu.Lock()
	defer n.reloadMu.Unlock()

	if n.closed || n.Context().Err() != nil {
		return nil
	}
	if reflect.DeepEqual(n.Config(), newConfig) {
		log.Debug(n.Context(), "no changes in notify config")

		return nil
	}

	router, err := n.newRouter(&route.Config{Route: newConfig.Route.Route})
	if err != nil {
		return errors.Wrap(err, "create router")
	}
	routerRun := newComponentLifecycle(router)
	if err := routerRun.runUntilReady(n.Context(), 10*time.Second); err != nil {
		closeErr := routerRun.stop()

		return stderrors.Join(
			errors.Wrap(err, "router not ready"),
			errors.Wrap(closeErr, "close new router"),
		)
	}

	channel, err := n.newChannel(&channel.Config{Email: newConfig.Channels.Email})
	if err != nil {
		closeErr := routerRun.stop()

		return stderrors.Join(
			errors.Wrap(err, "create channel"),
			errors.Wrap(closeErr, "close new router"),
		)
	}
	channelRun := newComponentLifecycle(channel)
	if err := channelRun.runUntilReady(n.Context(), 10*time.Second); err != nil {
		closeErr := closeNotifierChildren(routerRun, channelRun)

		return stderrors.Join(
			errors.Wrap(err, "channel not ready"),
			closeErr,
		)
	}
	if err := notifierChildrenRunError(routerRun, channelRun); err != nil {
		return stderrors.Join(err, closeNotifierChildren(routerRun, channelRun))
	}

	n.mu.Lock()
	if n.closed || n.Context().Err() != nil {
		n.mu.Unlock()
		if err := closeNotifierChildren(routerRun, channelRun); err != nil {
			log.Error(n.Context(), errors.Wrap(err, "close staged notifier children"))
		}

		return nil
	}
	oldRouterRun := n.routerRun
	oldChannelRun := n.channelRun
	n.SetConfig(newConfig)
	n.router = router
	n.channel = channel
	n.routerRun = routerRun
	n.channelRun = channelRun
	n.generation++
	n.mu.Unlock()

	if err := closeNotifierChildren(oldRouterRun, oldChannelRun); err != nil {
		log.Error(n.Context(), err)
	}

	return nil
}

func closeNotifierChildren(routerRun, channelRun *componentLifecycle) error {
	closeErrs := make([]error, 2)
	var wg sync.WaitGroup
	if routerRun != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			closeErrs[0] = errors.Wrap(routerRun.stop(), "close router")
		}()
	}
	if channelRun != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			closeErrs[1] = errors.Wrap(channelRun.stop(), "close channel")
		}()
	}
	wg.Wait()

	return stderrors.Join(closeErrs...)
}

func notifierChildrenRunError(routerRun, channelRun *componentLifecycle) error {
	if err := notifierChildRunError("router", routerRun); err != nil {
		return err
	}

	return notifierChildRunError("channel", channelRun)
}

func notifierChildRunError(name string, child *componentLifecycle) error {
	if child == nil {
		return nil
	}
	select {
	case <-child.done:
		return errors.Wrapf(child.unexpectedRunError(), "%s exited", name)
	default:
		return nil
	}
}

type componentLifecycle struct {
	component component.Component
	done      chan struct{}
	mu        sync.Mutex
	started   bool
	stopping  bool
	runErr    error
	exitErr   error
	closeOnce sync.Once
	closeErr  error
}

func newComponentLifecycle(c component.Component) *componentLifecycle {
	return &componentLifecycle{component: c, done: make(chan struct{})}
}

func (r *componentLifecycle) runUntilReady(ctx context.Context, timeout time.Duration) error {
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()

		return errors.New("component lifecycle is stopping")
	}
	if !r.started {
		r.started = true
		go func() {
			err := r.component.Run()
			r.mu.Lock()
			r.runErr = err
			if !r.stopping {
				r.exitErr = err
			}
			r.mu.Unlock()
			close(r.done)
		}()
	}
	r.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-r.component.Ready():
		select {
		case <-r.done:
			return stderrors.Join(r.unexpectedRunError(), r.stop())
		default:
			return nil
		}
	case <-r.done:
		return stderrors.Join(r.unexpectedRunError(), r.stop())
	case <-timer.C:
		return stderrors.Join(errors.New("component not ready after timeout"), r.stop())
	case <-ctx.Done():
		return stderrors.Join(ctx.Err(), r.stop())
	}
}

func (r *componentLifecycle) stop() error {
	if r == nil {
		return nil
	}

	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.stopping = true
		started := r.started
		r.mu.Unlock()

		closeErr := r.component.Close()
		if started {
			<-r.done
		}
		r.closeErr = stderrors.Join(closeErr, r.exitError())
	})

	return r.closeErr
}

func (r *componentLifecycle) unexpectedRunError() error {
	if runErr := r.exitError(); runErr != nil {
		return runErr
	}

	return errors.New("component exited before shutdown")
}

func (r *componentLifecycle) exitError() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.exitErr
}

func (n *notifier) newRouter(config *route.Config) (route.Router, error) {
	return n.Dependencies().RouterFactory.New(
		n.Instance(),
		config,
		route.Dependencies{
			RelatedScore: n.Dependencies().RelatedScore,
			LLMFactory:   n.Dependencies().LLMFactory,
		},
	)
}

func (n *notifier) newChannel(config *channel.Config) (channel.Channel, error) {
	return n.Dependencies().ChannelFactory.New(
		n.Instance(),
		config,
		channel.Dependencies{},
	)
}

func (n *notifier) handle(ctx context.Context, result *rule.Result) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return
	}
	router := n.router

	groups, err := router.Route(ctx, result)
	if err != nil {
		// We don't retry in notifier, retry should be upstream.
		log.Error(ctx, errors.Wrap(err, "route"))

		return
	}

	for _, group := range groups {
		for i := range group.Receivers {
			n.trySummitSendWork(ctx, group, group.Receivers[i])
		}
	}
}

func (n *notifier) trySummitSendWork(ctx context.Context, group *route.Group, receiverName string) {
	config := n.Config()
	receiver := config.Receivers.get(receiverName)
	if receiver == nil {
		log.Error(ctx, errors.New("receiver not found"), "receiver", receiverName)

		return
	}
	if n.isSent(ctx, &group.FeedGroup, *receiver) {
		log.Debug(ctx, "already sent")

		return
	}
	select {
	case n.channelSendWork <- sendWork{
		group:    &group.FeedGroup,
		receiver: *receiver,
	}:
	case <-ctx.Done():
	}
}

func (n *notifier) sendWorker(i int) {
	for {
		select {
		case <-n.Context().Done():
			return
		case work := <-n.channelSendWork:
			if n.Context().Err() != nil {
				return
			}
			n.processSendWork(i, work)
		}
	}
}

func (n *notifier) processSendWork(i int, work sendWork) {
	spanCtx := telemetry.StartWith(n.Context(),
		append(n.TelemetryLabels(),
			telemetrymodel.KeyOperation, "Run",
			"worker", i,
			"group", work.group.Name,
			"time", timeutil.Format(work.group.Time),
			"receiver", work.receiver.Name,
		)...,
	)
	defer func() { telemetry.End(spanCtx, nil) }()

	workCtx, cancel := context.WithTimeout(spanCtx, 30*time.Second)
	defer cancel()

	if err := n.duplicateSend(workCtx, work); err != nil {
		log.Error(workCtx, err, "duplicate send")

		return
	}
	log.Info(workCtx, "send success")
}

func (n *notifier) duplicateSend(ctx context.Context, work sendWork) error {
	if n.isSent(ctx, work.group, work.receiver) { // Double check.
		return nil
	}

	if err := n.send(ctx, work); err != nil {
		return errors.Wrap(err, "send")
	}

	if err := n.markSent(ctx, work.group, work.receiver); err != nil {
		log.Error(ctx, errors.Wrap(err, "set nlog, may duplicate sending in next time"))
	}

	return nil
}

func (n *notifier) send(ctx context.Context, work sendWork) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return context.Canceled
	}
	channel := n.channel

	return channel.Send(ctx, work.receiver.Receiver, work.group)
}

var nlogKey = func(group *route.FeedGroup, receiver Receiver) []byte {
	return fmt.Appendf(nil, "notifier.group.%s.receiver.%s.%d", group.Name, receiver.Name, group.Time.Unix())
}

func (n *notifier) isSent(ctx context.Context, group *route.FeedGroup, receiver Receiver) bool {
	_, err := n.Dependencies().KVStorage.Get(ctx, nlogKey(group, receiver))
	switch {
	case err == nil:
		return true // Already sent.
	case errors.Is(err, kv.ErrNotFound):
		return false
	default:
		log.Warn(ctx, errors.Wrap(err, "get nlog, continue sending"))

		return false
	}
}

func (n *notifier) markSent(ctx context.Context, group *route.FeedGroup, receiver Receiver) error {
	return n.Dependencies().KVStorage.Set(ctx, nlogKey(group, receiver), []byte(timeutil.Format(time.Now())), timeutil.Day)
}

type sendWork struct {
	group    *route.FeedGroup
	receiver Receiver
}

type mockNotifier struct {
	component.Mock
}

func (m *mockNotifier) Reload(app *config.App) error {
	return m.Called(app).Error(0)
}

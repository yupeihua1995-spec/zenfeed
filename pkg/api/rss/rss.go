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

package rss

import (
	"context"
	"net"
	"net/http"
	"text/template"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/gorilla/feeds"
	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/api"
	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/model"
	telemetry "github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/buffer"
)

var clk = clock.New()

// --- Interface code block ---
type Server interface {
	component.Component
	config.Watcher
}

type Config struct {
	Address             string
	ContentHTMLTemplate string
	contentHTMLTemplate *template.Template
}

const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 2 * time.Minute
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 10 * time.Second
	maxHeaderBytes    = 1 << 20
)

func (c *Config) Validate() error {
	if c.Address == "" {
		c.Address = "127.0.0.1:1302"
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return errors.Wrap(err, "invalid address")
	}

	if c.ContentHTMLTemplate == "" {
		c.ContentHTMLTemplate = "{{ .summary_html_snippet }}"
	}
	t, err := template.New("").Parse(c.ContentHTMLTemplate)
	if err != nil {
		return errors.Wrap(err, "parse rss content template")
	}
	c.contentHTMLTemplate = t

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.Address = app.API.RSS.Address
	c.ContentHTMLTemplate = app.API.RSS.ContentHTMLTemplate

	return c
}

type Dependencies struct {
	API api.API
}

// --- Factory code block ---
type Factory component.Factory[Server, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Server, config.App, Dependencies](
			func(instance string, config *config.App, dependencies Dependencies) (Server, error) {
				m := &mockServer{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Server, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (Server, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	s := &server{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "RSSServer",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
	}

	router := http.NewServeMux()
	router.Handle("/", http.HandlerFunc(s.rss))

	s.http = &http.Server{
		Addr:              config.Address,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}

	return s, nil
}

// --- Implementation code block ---
type server struct {
	*component.Base[Config, Dependencies]
	http *http.Server
}

func (s *server) Run() (err error) {
	ctx := telemetry.StartWith(s.Context(), append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()

	listener, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- s.http.Serve(listener)
	}()

	s.MarkReady()
	select {
	case <-ctx.Done():
		log.Info(ctx, "shutting down")

		return shutdownHTTPServer(s.http)
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.Wrap(err, "listen and serve")
	}
}

func shutdownHTTPServer(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		if closeErr := server.Close(); closeErr != nil {
			return errors.Wrapf(err, "shutdown server; force close: %v", closeErr)
		}

		return errors.Wrap(err, "shutdown server; connections force-closed")
	}

	return nil
}

func (s *server) Reload(app *config.App) error {
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "validate config")
	}
	if s.Config().Address != newConfig.Address {
		return errors.New("address cannot be reloaded")
	}

	s.SetConfig(newConfig)

	return nil
}

func (s *server) rss(w http.ResponseWriter, r *http.Request) {
	var err error
	ctx := telemetry.StartWith(r.Context(), append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "rss")...)
	defer telemetry.End(ctx, err)

	// Extract parameters.
	ps := r.URL.Query()
	labelFilters := ps["label_filter"]
	query := ps.Get("query")

	// Forward query request to API.
	now := clk.Now()
	queryResult, err := s.Dependencies().API.Query(ctx, &api.QueryRequest{
		Query:        query,
		LabelFilters: labelFilters,
		Start:        now.Add(-24 * time.Hour),
		End:          now,
		Limit:        100,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // TODO: standardize error handling.

		return
	}

	// Render and convert to RSS.
	rssObj := &feeds.Feed{
		Title:       "Zenfeed RSS - " + ps.Encode(),
		Description: "Powered by Github Zenfeed - https://github.com/glidea/zenfeed. If you use Folo, please enable 'Appearance - Content - Render inline styles'",
		Items:       make([]*feeds.Item, 0, len(queryResult.Feeds)),
	}

	buf := buffer.Get()
	defer buffer.Put(buf)

	for _, feed := range queryResult.Feeds {
		buf.Reset()

		if err = s.Config().contentHTMLTemplate.Execute(buf, feed.Labels.Map()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		item := &feeds.Item{
			Title:   feed.Labels.Get(model.LabelTitle),
			Link:    &feeds.Link{Href: feed.Labels.Get(model.LabelLink)},
			Created: feed.Time, // NOTE: scrape time, not pub time.
			Content: buf.String(),
		}

		rssObj.Items = append(rssObj.Items, item)
	}

	if err = rssObj.WriteRss(w); err != nil {
		log.Error(ctx, errors.Wrap(err, "write rss response"))

		return
	}
}

type mockServer struct {
	component.Mock
}

func (m *mockServer) Reload(app *config.App) error {
	return m.Called(app).Error(0)
}

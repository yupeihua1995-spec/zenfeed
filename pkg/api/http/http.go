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

package http

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/api"
	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	telemetry "github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/jsonrpc"
)

// --- Interface code block ---
type Server interface {
	component.Component
	config.Watcher
}

type Config struct {
	Address        string
	AllowedOrigins []string
}

const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 2 * time.Minute
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 10 * time.Second
	maxHeaderBytes    = 1 << 20
)

var defaultAllowedOrigins = []string{"http://localhost:1400", "http://127.0.0.1:1400"}

func (c *Config) Validate() error {
	if c.Address == "" {
		c.Address = "127.0.0.1:1300"
	}
	if c.AllowedOrigins == nil {
		c.AllowedOrigins = append([]string(nil), defaultAllowedOrigins...)
	}
	for _, origin := range c.AllowedOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" ||
			parsed.Fragment != "" || strings.Contains(origin, "*") {
			return errors.Errorf("invalid allowed origin %q", origin)
		}
	}
	if _, _, err := net.SplitHostPort(c.Address); err != nil {
		return errors.Wrap(err, "invalid address")
	}

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.Address = app.API.HTTP.Address
	if app.API.HTTP.AllowedOrigins == nil {
		c.AllowedOrigins = nil
	} else {
		c.AllowedOrigins = make([]string, len(*app.API.HTTP.AllowedOrigins))
		copy(c.AllowedOrigins, *app.API.HTTP.AllowedOrigins)
	}

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
			Name:         "HTTPServer",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
	}
	router := http.NewServeMux()
	api := dependencies.API
	router.Handle("/write", jsonrpc.APIWithLimit(api.Write, jsonrpc.WriteMaxRequestBodyBytes))
	router.Handle("/update_feed_labels", jsonrpc.API(api.UpdateFeedLabels))
	router.Handle("/query_config", jsonrpc.API(api.QueryAppConfig))
	router.Handle("/apply_config", jsonrpc.API(api.ApplyAppConfig))
	router.Handle("/query_config_schema", jsonrpc.API(api.QueryAppConfigSchema))
	router.Handle("/query_rsshub_categories", jsonrpc.API(api.QueryRSSHubCategories))
	router.Handle("/query_rsshub_websites", jsonrpc.API(api.QueryRSSHubWebsites))
	router.Handle("/query_rsshub_routes", jsonrpc.API(api.QueryRSSHubRoutes))
	router.Handle("/query_source_statuses", jsonrpc.API(api.QuerySourceStatuses))
	router.Handle("/refresh_source", jsonrpc.API(api.RefreshSource))
	router.Handle("/query", jsonrpc.API(api.Query))
	httpServer := &http.Server{
		Addr:              config.Address,
		Handler:           s.cors(router),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	s.http = httpServer

	return s, nil
}

// --- Implementation code block ---
type server struct {
	*component.Base[Config, Dependencies]
	http *http.Server
}

func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Add("Vary", "Origin")
			if !slices.Contains(s.Config().AllowedOrigins, origin) {
				http.Error(w, "origin not allowed", http.StatusForbidden)

				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")
		}

		next.ServeHTTP(w, r)
	})
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

type mockServer struct {
	component.Mock
}

func (m *mockServer) Reload(app *config.App) error {
	return m.Called(app).Error(0)
}

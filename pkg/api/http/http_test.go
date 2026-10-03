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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/api"
	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
)

func TestConfigRevisionRoutes_BitsUT(t *testing.T) {
	backend := &stubAPI{
		queryResponse: &api.QueryAppConfigResponse{
			Revision: "revision-1",
			App:      config.App{Timezone: "UTC"},
		},
		applyErr: api.ErrConflict(errors.New("config revision conflict")),
	}
	handler := newTestHTTPHandler(t, backend)

	t.Run("query config returns the top-level revision", func(t *testing.T) {
		req := httptest.NewRequest(stdhttp.MethodPost, "/query_config", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		require.Equal(t, stdhttp.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "revision-1", body["_revision"])
		assert.Equal(t, "UTC", body["timezone"])
	})

	t.Run("apply config returns HTTP 409 for a stale revision", func(t *testing.T) {
		req := httptest.NewRequest(stdhttp.MethodPost, "/apply_config", bytes.NewBufferString(
			`{"_revision":"stale","timezone":"UTC"}`,
		))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		require.Equal(t, stdhttp.StatusConflict, rec.Code)
		var body api.Error
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, stdhttp.StatusConflict, body.Code)
		require.NotNil(t, backend.applyRequest)
		require.NotNil(t, backend.applyRequest.Revision)
		assert.Equal(t, "stale", *backend.applyRequest.Revision)
	})

	t.Run("apply config without a revision returns HTTP 428", func(t *testing.T) {
		mgr := &httpStubConfigManager{app: &config.App{Timezone: "UTC"}, revision: "revision-1"}
		realAPI, err := api.NewFactory().New("test-http-api", &config.App{}, api.Dependencies{ConfigManager: mgr})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, realAPI.Close()) })
		realHandler := newTestHTTPHandler(t, realAPI)
		req := httptest.NewRequest(stdhttp.MethodPost, "/apply_config", bytes.NewBufferString(`{"timezone":"UTC"}`))
		rec := httptest.NewRecorder()

		realHandler.ServeHTTP(rec, req)

		require.Equal(t, stdhttp.StatusPreconditionRequired, rec.Code)
		assert.Nil(t, mgr.savedApp)
	})

	t.Run("apply config with an empty revision returns HTTP 428", func(t *testing.T) {
		mgr := &httpStubConfigManager{app: &config.App{Timezone: "UTC"}, revision: "revision-1"}
		realAPI, err := api.NewFactory().New("test-empty-revision-api", &config.App{}, api.Dependencies{ConfigManager: mgr})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, realAPI.Close()) })
		realHandler := newTestHTTPHandler(t, realAPI)
		req := httptest.NewRequest(stdhttp.MethodPost, "/apply_config", bytes.NewBufferString(
			`{"_revision":"","timezone":"UTC"}`,
		))
		rec := httptest.NewRecorder()

		realHandler.ServeHTTP(rec, req)

		require.Equal(t, stdhttp.StatusPreconditionRequired, rec.Code)
		assert.Nil(t, mgr.savedApp)
	})
}

func TestServerSafetyDefaults_BitsUT(t *testing.T) {
	backend := &stubAPI{}
	got, err := new("test", &config.App{}, Dependencies{API: backend})
	require.NoError(t, err)
	s := got.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	assert.Equal(t, "127.0.0.1:1300", s.http.Addr)
	assert.Equal(t, 10*time.Second, s.http.ReadHeaderTimeout)
	assert.Equal(t, 30*time.Second, s.http.ReadTimeout)
	assert.Equal(t, 2*time.Minute, s.http.WriteTimeout)
	assert.Equal(t, 2*time.Minute, s.http.IdleTimeout)
	assert.Equal(t, 1<<20, s.http.MaxHeaderBytes)
	assert.Equal(t, []string{"http://localhost:1400", "http://127.0.0.1:1400"}, s.Config().AllowedOrigins)
}

func TestCORSAllowlistAndReload_BitsUT(t *testing.T) {
	backend := &stubAPI{queryResponse: &api.QueryAppConfigResponse{}}
	got, err := new("test", &config.App{}, Dependencies{API: backend})
	require.NoError(t, err)
	s := got.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	t.Run("allows an exact configured origin", func(t *testing.T) {
		req := httptest.NewRequest(stdhttp.MethodOptions, "/query_config", nil)
		req.Header.Set("Origin", "http://localhost:1400")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		assert.Equal(t, stdhttp.StatusOK, rec.Code)
		assert.Equal(t, "http://localhost:1400", rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "POST, OPTIONS", rec.Header().Get("Access-Control-Allow-Methods"))
		assert.Equal(t, "Content-Type, Accept", rec.Header().Get("Access-Control-Allow-Headers"))
		assert.Contains(t, rec.Header().Values("Vary"), "Origin")
	})

	t.Run("denies an untrusted origin", func(t *testing.T) {
		req := httptest.NewRequest(stdhttp.MethodPost, "/query_config", nil)
		req.Header.Set("Origin", "https://evil.example")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		assert.Equal(t, stdhttp.StatusForbidden, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("reloads the allowlist", func(t *testing.T) {
		app := &config.App{}
		origins := []string{"https://trusted.example"}
		app.API.HTTP.AllowedOrigins = &origins
		require.NoError(t, s.Reload(app))
		req := httptest.NewRequest(stdhttp.MethodOptions, "/query_config", nil)
		req.Header.Set("Origin", "https://trusted.example")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		assert.Equal(t, stdhttp.StatusOK, rec.Code)
		assert.Equal(t, "https://trusted.example", rec.Header().Get("Access-Control-Allow-Origin"))
	})
}

func TestConfigRejectsWildcardOrigin_BitsUT(t *testing.T) {
	c := &Config{Address: "127.0.0.1:1300", AllowedOrigins: []string{"*"}}
	require.ErrorContains(t, c.Validate(), "invalid allowed origin")
}

func TestConfigExplicitEmptyOriginsDisablesCORS_BitsUT(t *testing.T) {
	app := &config.App{}
	empty := []string{}
	app.API.HTTP.AllowedOrigins = &empty
	c := (&Config{}).From(app)
	require.NoError(t, c.Validate())
	assert.NotNil(t, c.AllowedOrigins)
	assert.Empty(t, c.AllowedOrigins)
}

func TestServerRunExitsAfterClose_BitsUT(t *testing.T) {
	app := &config.App{}
	app.API.HTTP.Address = "127.0.0.1:0"
	got, err := new("test", app, Dependencies{API: &stubAPI{}})
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- got.Run() }()
	select {
	case <-got.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not become ready")
	}
	require.NoError(t, got.Close())
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server Run did not exit after Close")
	}
}

func newTestHTTPHandler(t *testing.T, backend api.API) stdhttp.Handler {
	t.Helper()
	got, err := new("test", &config.App{}, Dependencies{API: backend})
	require.NoError(t, err)
	server, ok := got.(*server)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	return server.http.Handler
}

type stubAPI struct {
	component.Mock
	queryResponse *api.QueryAppConfigResponse
	applyRequest  *api.ApplyAppConfigRequest
	applyErr      error
}

func (s *stubAPI) QueryAppConfig(
	ctx context.Context,
	req *api.QueryAppConfigRequest,
) (*api.QueryAppConfigResponse, error) {
	return s.queryResponse, nil
}

func (s *stubAPI) ApplyAppConfig(
	ctx context.Context,
	req *api.ApplyAppConfigRequest,
) (*api.ApplyAppConfigResponse, error) {
	s.applyRequest = req

	return &api.ApplyAppConfigResponse{}, s.applyErr
}

func (s *stubAPI) Reload(app *config.App) error {
	return nil
}

func (s *stubAPI) QueryAppConfigSchema(
	ctx context.Context,
	req *api.QueryAppConfigSchemaRequest,
) (*api.QueryAppConfigSchemaResponse, error) {
	return nil, nil
}

func (s *stubAPI) QueryRSSHubCategories(
	ctx context.Context,
	req *api.QueryRSSHubCategoriesRequest,
) (*api.QueryRSSHubCategoriesResponse, error) {
	return nil, nil
}

func (s *stubAPI) QueryRSSHubWebsites(
	ctx context.Context,
	req *api.QueryRSSHubWebsitesRequest,
) (*api.QueryRSSHubWebsitesResponse, error) {
	return nil, nil
}

func (s *stubAPI) QueryRSSHubRoutes(
	ctx context.Context,
	req *api.QueryRSSHubRoutesRequest,
) (*api.QueryRSSHubRoutesResponse, error) {
	return nil, nil
}

func (s *stubAPI) QuerySourceStatuses(
	ctx context.Context,
	req *api.QuerySourceStatusesRequest,
) (*api.QuerySourceStatusesResponse, error) {
	return &api.QuerySourceStatusesResponse{}, nil
}

func (s *stubAPI) RefreshSource(
	ctx context.Context,
	req *api.RefreshSourceRequest,
) (*api.RefreshSourceResponse, error) {
	return &api.RefreshSourceResponse{Accepted: true}, nil
}

func (s *stubAPI) Write(ctx context.Context, req *api.WriteRequest) (*api.WriteResponse, error) {
	return nil, nil
}

func (s *stubAPI) UpdateFeedLabels(
	ctx context.Context,
	req *api.UpdateFeedLabelsRequest,
) (*api.UpdateFeedLabelsResponse, error) {
	return &api.UpdateFeedLabelsResponse{}, nil
}

func (s *stubAPI) Query(ctx context.Context, req *api.QueryRequest) (*api.QueryResponse, error) {
	return nil, nil
}

type httpStubConfigManager struct {
	component.Mock
	app      *config.App
	revision string
	savedApp *config.App
}

func (m *httpStubConfigManager) AppConfig() *config.App { return m.app }
func (m *httpStubConfigManager) AppConfigSnapshot() (*config.App, string) {
	return m.app, m.revision
}
func (m *httpStubConfigManager) SaveAppConfig(app *config.App, revision *string) error {
	m.savedApp = app

	return nil
}
func (m *httpStubConfigManager) Subscribe(config.Watcher) {}

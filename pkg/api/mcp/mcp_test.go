// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package mcp

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mcptypes "github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/api"
	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
)

func TestApplyAppConfigPreservesRevision_BitsUT(t *testing.T) {
	backend := &revisionCapturingAPI{}
	serverComponent, err := new("test", &config.App{}, Dependencies{API: backend})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverComponent.Close()) })

	req := mcptypes.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"yaml": "_revision: revision-1\ntimezone: UTC\n",
	}
	result, err := serverComponent.(*server).applyAppConfig(context.Background(), req)

	require.NoError(t, err)
	require.False(t, result.IsError)
	require.NotNil(t, backend.applyRequest)
	require.NotNil(t, backend.applyRequest.Revision)
	require.Equal(t, "revision-1", *backend.applyRequest.Revision)
	require.Equal(t, "UTC", backend.applyRequest.Timezone)
}

func TestApplyAppConfigRequiresRevision_BitsUT(t *testing.T) {
	mgr := &mcpStubConfigManager{}
	backend, err := api.NewFactory().New("test-mcp-api", &config.App{}, api.Dependencies{ConfigManager: mgr})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	serverComponent, err := new("test", &config.App{}, Dependencies{API: backend})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverComponent.Close()) })

	req := mcptypes.CallToolRequest{}
	req.Params.Arguments = map[string]any{"yaml": "timezone: UTC\n"}
	result, err := serverComponent.(*server).applyAppConfig(context.Background(), req)

	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Contains(t, result.Content[0].(mcptypes.TextContent).Text, "_revision")
	require.Nil(t, mgr.savedApp)
}

func TestServerSafetyDefaults_BitsUT(t *testing.T) {
	serverComponent, err := new("test", &config.App{}, Dependencies{API: &revisionCapturingAPI{}})
	require.NoError(t, err)
	s := serverComponent.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.Equal(t, "127.0.0.1:1301", s.http.Addr)
	require.Equal(t, 10*time.Second, s.http.ReadHeaderTimeout)
	require.Equal(t, 30*time.Second, s.http.ReadTimeout)
	require.Equal(t, 2*time.Minute, s.http.IdleTimeout)
	require.Equal(t, 1<<20, s.http.MaxHeaderBytes)
}

func TestServerRunExitsAfterClose_BitsUT(t *testing.T) {
	app := &config.App{}
	app.API.MCP.Address = "127.0.0.1:0"
	serverComponent, err := new("test", app, Dependencies{API: &revisionCapturingAPI{}})
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- serverComponent.Run() }()
	select {
	case <-serverComponent.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not become ready")
	}
	require.NoError(t, serverComponent.Close())
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server Run did not exit after Close")
	}
}

func TestServerShutdownWithActiveSSE_BitsUT(t *testing.T) {
	app := &config.App{}
	app.API.MCP.Address = "127.0.0.1:0"
	serverComponent, err := new("test", app, Dependencies{API: &revisionCapturingAPI{}})
	require.NoError(t, err)
	s := serverComponent.(*server)
	s.shutdownTimeout = 100 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.listen = func(_, _ string) (net.Listener, error) { return listener, nil }

	runErr := make(chan error, 1)
	go func() { runErr <- s.Run() }()
	select {
	case <-s.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("server did not become ready")
	}

	responseCh := make(chan *http.Response, 1)
	requestErr := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + listener.Addr().String() + "/sse") //nolint:gosec,noctx
		if err != nil {
			requestErr <- err
			return
		}
		responseCh <- resp
	}()

	var response *http.Response
	select {
	case response = <-responseCh:
		require.Equal(t, http.StatusOK, response.StatusCode)
	case err := <-requestErr:
		t.Fatalf("open SSE stream: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("SSE stream did not connect")
	}
	defer func() { _ = response.Body.Close() }()

	require.NoError(t, s.Close())
	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server Run did not exit with an active SSE stream")
	}
}

func TestServerHTTPBoundary_BitsUT(t *testing.T) {
	serverComponent, err := new("test", &config.App{}, Dependencies{API: &revisionCapturingAPI{}})
	require.NoError(t, err)
	s := serverComponent.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	t.Run("rejects untrusted browser origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/sse", nil)
		req.Header.Set("Origin", "https://evil.example")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code)
		require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("trusted browser origin is reflected instead of wildcard", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/sse", nil)
		req.Header.Set("Origin", "http://localhost:1400")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "http://localhost:1400", rec.Header().Get("Access-Control-Allow-Origin"))
		require.NotEqual(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("allows non-browser clients without Origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/message", bytes.NewBufferString(`{}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "Missing sessionId")
	})

	t.Run("rejects oversized message before MCP dispatch", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/message?sessionId=missing",
			bytes.NewReader(make([]byte, maxMessageBytes+1)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		s.http.Handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})
}

type revisionCapturingAPI struct {
	component.Mock
	applyRequest *api.ApplyAppConfigRequest
}

func (a *revisionCapturingAPI) Reload(*config.App) error { return nil }

func (a *revisionCapturingAPI) QueryAppConfigSchema(context.Context, *api.QueryAppConfigSchemaRequest) (*api.QueryAppConfigSchemaResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) QueryAppConfig(context.Context, *api.QueryAppConfigRequest) (*api.QueryAppConfigResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) ApplyAppConfig(_ context.Context, req *api.ApplyAppConfigRequest) (*api.ApplyAppConfigResponse, error) {
	a.applyRequest = req

	return &api.ApplyAppConfigResponse{}, nil
}

func (a *revisionCapturingAPI) QueryRSSHubCategories(context.Context, *api.QueryRSSHubCategoriesRequest) (*api.QueryRSSHubCategoriesResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) QueryRSSHubWebsites(context.Context, *api.QueryRSSHubWebsitesRequest) (*api.QueryRSSHubWebsitesResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) QueryRSSHubRoutes(context.Context, *api.QueryRSSHubRoutesRequest) (*api.QueryRSSHubRoutesResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) QuerySourceStatuses(context.Context, *api.QuerySourceStatusesRequest) (*api.QuerySourceStatusesResponse, error) {
	return &api.QuerySourceStatusesResponse{}, nil
}

func (a *revisionCapturingAPI) RefreshSource(context.Context, *api.RefreshSourceRequest) (*api.RefreshSourceResponse, error) {
	return &api.RefreshSourceResponse{Accepted: true}, nil
}

func (a *revisionCapturingAPI) Write(context.Context, *api.WriteRequest) (*api.WriteResponse, error) {
	return nil, nil
}

func (a *revisionCapturingAPI) UpdateFeedLabels(
	context.Context,
	*api.UpdateFeedLabelsRequest,
) (*api.UpdateFeedLabelsResponse, error) {
	return &api.UpdateFeedLabelsResponse{}, nil
}

func (a *revisionCapturingAPI) Query(context.Context, *api.QueryRequest) (*api.QueryResponse, error) {
	return nil, nil
}

var _ api.API = (*revisionCapturingAPI)(nil)

type mcpStubConfigManager struct {
	component.Mock
	savedApp *config.App
}

func (m *mcpStubConfigManager) AppConfig() *config.App { return &config.App{} }
func (m *mcpStubConfigManager) AppConfigSnapshot() (*config.App, string) {
	return &config.App{}, "revision-1"
}
func (m *mcpStubConfigManager) SaveAppConfig(app *config.App, revision *string) error {
	m.savedApp = app

	return nil
}
func (m *mcpStubConfigManager) Subscribe(config.Watcher) {}

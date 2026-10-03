package http

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/config"
)

func TestServerSafetyDefaults_BitsUT(t *testing.T) {
	got, err := new("test", &config.App{}, Dependencies{})
	require.NoError(t, err)
	s := got.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	assert.Equal(t, "127.0.0.1:9090", s.http.Addr)
	assert.Equal(t, 10*time.Second, s.http.ReadHeaderTimeout)
	assert.Equal(t, 30*time.Second, s.http.ReadTimeout)
	assert.Equal(t, 2*time.Minute, s.http.WriteTimeout)
	assert.Equal(t, 2*time.Minute, s.http.IdleTimeout)
	assert.Equal(t, 1<<20, s.http.MaxHeaderBytes)
}

func TestServerRunExitsAfterClose_BitsUT(t *testing.T) {
	app := &config.App{}
	app.Telemetry.Address = "127.0.0.1:0"
	got, err := new("test", app, Dependencies{})
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

func TestServerReload_BitsUT(t *testing.T) {
	app := &config.App{}
	app.Telemetry.Address = "127.0.0.1:9090"
	got, err := new("test", app, Dependencies{})
	require.NoError(t, err)
	s := got.(*server)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	sameAddress := &config.App{}
	sameAddress.Telemetry.Address = "127.0.0.1:9090"
	sameAddress.Telemetry.Log.Level = "debug"
	require.NoError(t, s.Reload(sameAddress))

	changedAddress := &config.App{}
	changedAddress.Telemetry.Address = "127.0.0.1:9191"
	require.ErrorContains(t, s.Reload(changedAddress), "address cannot be reloaded")
}

func TestConfigManagerRollsBackRejectedTelemetryAddress_BitsUT(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := []byte("telemetry:\n  address: 127.0.0.1:9090\n")
	require.NoError(t, os.WriteFile(path, initial, 0o600))

	manager, err := config.NewFactory().New("test-manager", &config.Config{Path: path}, config.Dependencies{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	app, revision := manager.AppConfigSnapshot()
	serverComponent, err := new("test-server", app, Dependencies{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverComponent.Close()) })
	manager.Subscribe(serverComponent)

	next := *app
	next.Telemetry.Address = "127.0.0.1:9191"
	err = manager.SaveAppConfig(&next, &revision)

	require.ErrorContains(t, err, "address cannot be reloaded")
	onDisk, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	assert.Equal(t, initial, onDisk)
	current, currentRevision := manager.AppConfigSnapshot()
	assert.Equal(t, "127.0.0.1:9090", current.Telemetry.Address)
	assert.Equal(t, revision, currentRevision)
}

var _ config.Watcher = (*server)(nil)

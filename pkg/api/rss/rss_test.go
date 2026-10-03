package rss

import (
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

	assert.Equal(t, "127.0.0.1:1302", s.http.Addr)
	assert.Equal(t, 10*time.Second, s.http.ReadHeaderTimeout)
	assert.Equal(t, 30*time.Second, s.http.ReadTimeout)
	assert.Equal(t, 2*time.Minute, s.http.WriteTimeout)
	assert.Equal(t, 2*time.Minute, s.http.IdleTimeout)
	assert.Equal(t, 1<<20, s.http.MaxHeaderBytes)
}

func TestServerRunExitsAfterClose_BitsUT(t *testing.T) {
	app := &config.App{}
	app.API.RSS.Address = "127.0.0.1:0"
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

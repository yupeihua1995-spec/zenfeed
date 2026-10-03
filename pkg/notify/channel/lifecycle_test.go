package channel

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/notify/route"
)

func TestEmailSendStopsWhenContextCanceled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan struct{})
	releaseServer := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		close(accepted)
		<-releaseServer
	}()
	t.Cleanup(func() { close(releaseServer) })

	config := &Email{SmtpEndpoint: listener.Addr().String(), From: "sender@example.com"}
	require.NoError(t, config.Validate())
	sender, err := newEmail(config, Dependencies{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sender.Send(ctx, Receiver{Email: "recipient@example.com"}, &route.FeedGroup{Name: "test"})
	}()
	requireSignal(t, accepted, "SMTP connection was not accepted")
	cancel()

	select {
	case err := <-sendDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("email send did not stop after context cancellation")
	}
}

func TestWebhookSendStopsWhenContextCanceled(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		select {
		case <-r.Context().Done():
		case <-releaseHandler:
		}
	}))
	t.Cleanup(func() {
		close(releaseHandler)
		server.Close()
	})

	sender := newWebhook()
	ctx, cancel := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sender.Send(ctx, Receiver{Webhook: &WebhookReceiver{URL: server.URL}}, &route.FeedGroup{})
	}()
	requireSignal(t, requestStarted, "webhook request did not start")
	cancel()

	select {
	case err := <-sendDone:
		require.True(t, errors.Is(err, context.Canceled), "unexpected webhook error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("webhook send did not stop after context cancellation")
	}
}

func TestLoginAuthRejectsCredentialsOnPlaintextConnection(t *testing.T) {
	auth := &loginAuth{username: "user", password: "secret", host: "smtp.example.com"}

	mechanism, initialResponse, err := auth.Start(&smtp.ServerInfo{
		Name: "smtp.example.com",
		TLS:  false,
		Auth: []string{"LOGIN"},
	})

	require.ErrorContains(t, err, "unencrypted connection")
	require.Empty(t, mechanism)
	require.Nil(t, initialResponse)
}

func requireSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

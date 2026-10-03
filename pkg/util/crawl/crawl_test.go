package crawl

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

func TestHTTPClientSafetyDefaults_BitsUT(t *testing.T) {
	localCrawler := NewLocal().(*local)
	jinaCrawler := NewJina("secret").(*jina)

	assert.Equal(t, 45*time.Second, localCrawler.hc.Timeout)
	assert.Equal(t, 45*time.Second, jinaCrawler.hc.Timeout)
	require.NotNil(t, localCrawler.hc.CheckRedirect)

	transport, ok := localCrawler.hc.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, transport.Proxy)
	assert.NotNil(t, transport.DialContext)

	jinaTransport, ok := jinaCrawler.hc.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Nil(t, jinaTransport.Proxy)
	require.NotNil(t, jinaCrawler.hc.CheckRedirect)
}

func TestLocalRejectsUnsafeURLsBeforeNetwork_BitsUT(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{name: "relative URL", url: "/relative", wantErr: "absolute HTTP or HTTPS"},
		{name: "unsupported scheme", url: "ftp://example.com/file", wantErr: "absolute HTTP or HTTPS"},
		{name: "credentials", url: "http://user:secret@example.com/", wantErr: "credentials"},
		{name: "localhost", url: "http://localhost/", wantErr: "localhost"},
		{name: "localhost subdomain", url: "http://api.localhost./", wantErr: "localhost"},
		{name: "IPv4 loopback", url: "http://127.0.0.1/", wantErr: "not public"},
		{name: "IPv4 private", url: "http://10.0.0.1/", wantErr: "not public"},
		{name: "IPv4 link-local", url: "http://169.254.169.254/latest/meta-data/", wantErr: "not public"},
		{name: "IPv4 unspecified", url: "http://0.0.0.0/", wantErr: "not public"},
		{name: "IPv4 multicast", url: "http://224.0.0.1/", wantErr: "not public"},
		{name: "IPv6 loopback", url: "http://[::1]/", wantErr: "not public"},
		{name: "IPv6 private", url: "http://[fc00::1]/", wantErr: "not public"},
		{name: "IPv6 link-local", url: "http://[fe80::1]/", wantErr: "not public"},
		{name: "IPv6 multicast", url: "http://[ff02::1]/", wantErr: "not public"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			networkCalls := 0
			crawler := &local{hc: &http.Client{
				Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					networkCalls++
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       io.NopCloser(strings.NewReader("")),
					}, nil
				}),
			}}

			_, err := crawler.Markdown(context.Background(), tt.url)

			require.ErrorContains(t, err, tt.wantErr)
			assert.Zero(t, networkCalls)
		})
	}
}

func TestLocalRejectsRedirectToPrivateAddress_BitsUT(t *testing.T) {
	crawler := NewLocal().(*local)
	require.NotNil(t, crawler.hc.CheckRedirect)

	fromURL, err := url.Parse("https://93.184.216.34/start")
	require.NoError(t, err)
	toURL, err := url.Parse("http://127.0.0.1/admin")
	require.NoError(t, err)

	err = crawler.hc.CheckRedirect(
		&http.Request{URL: toURL},
		[]*http.Request{{URL: fromURL}},
	)
	require.ErrorContains(t, err, "not public")
}

func TestSafeDialContextRejectsPrivateResolution_BitsUT(t *testing.T) {
	resolver := resolverFunc(func(_ context.Context, host string) ([]net.IPAddr, error) {
		assert.Equal(t, "example.com", host)
		return []net.IPAddr{{IP: net.ParseIP("10.1.2.3")}}, nil
	})
	dialed := false
	dial := newSafeDialContext(resolver, func(context.Context, string, string) (net.Conn, error) {
		dialed = true
		return nil, errors.New("must not dial")
	})

	conn, err := dial(context.Background(), "tcp", "example.com:8080")

	assert.Nil(t, conn)
	require.ErrorContains(t, err, "not public")
	assert.False(t, dialed)
}

func TestSafeDialContextDialsValidatedIPv6WithOriginalPort_BitsUT(t *testing.T) {
	resolver := resolverFunc(func(_ context.Context, host string) ([]net.IPAddr, error) {
		assert.Equal(t, "example.com", host)
		return []net.IPAddr{{IP: net.ParseIP("2606:4700:4700::1111")}}, nil
	})
	wantErr := errors.New("dial stopped by test")
	var dialedAddress string
	dial := newSafeDialContext(resolver, func(_ context.Context, network, address string) (net.Conn, error) {
		assert.Equal(t, "tcp", network)
		dialedAddress = address
		return nil, wantErr
	})

	conn, err := dial(context.Background(), "tcp", "example.com:8443")

	assert.Nil(t, conn)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, "[2606:4700:4700::1111]:8443", dialedAddress)
}

func TestLocalRobotsUsesOriginalOrigin_BitsUT(t *testing.T) {
	var requestedURLs []string
	crawler := &local{hc: &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requestedURLs = append(requestedURLs, req.URL.String())
			status := http.StatusOK
			body := "<p>page</p>"
			if req.URL.Path == "/robots.txt" {
				status = http.StatusNotFound
				body = ""
			}
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	}}

	_, err := crawler.Markdown(context.Background(), "http://example.com:8080/article")
	require.NoError(t, err)
	assert.Equal(t, []string{
		"http://example.com:8080/robots.txt",
		"http://example.com:8080/article",
	}, requestedURLs)
}

func TestLocalRedirectChecksTargetRobots_BitsUT(t *testing.T) {
	var requestedURLs []string
	crawler := &local{hc: &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requestedURLs = append(requestedURLs, req.URL.String())
			status := http.StatusNotFound
			body := ""
			header := make(http.Header)
			switch req.URL.String() {
			case "https://93.184.216.34/start":
				status = http.StatusFound
				header.Set("Location", "https://1.1.1.1/private")
			case "https://1.1.1.1/robots.txt":
				status = http.StatusOK
				body = "User-agent: ZenFeed\nDisallow: /private\n"
			}
			return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
		}),
	}}

	_, err := crawler.Markdown(context.Background(), "https://93.184.216.34/start")

	require.ErrorContains(t, err, "disallowed by robots.txt")
	assert.NotContains(t, requestedURLs, "https://1.1.1.1/private")
}

func TestJinaRejectsMalformedTargetBeforeNetwork_BitsUT(t *testing.T) {
	networkCalls := 0
	crawler := NewJina("").(*jina)
	crawler.hc.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls++
		return nil, errors.New("unexpected network call")
	})

	for _, target := range []string{"/relative", "ftp://example.com/file", "https://user:secret@example.com/"} {
		_, err := crawler.Markdown(context.Background(), target)
		require.Error(t, err)
	}
	assert.Zero(t, networkCalls)
}

func TestJinaDoesNotValidateEmbeddedTarget_BitsUT(t *testing.T) {
	var requestedURL string
	crawler := NewJina("").(*jina)
	crawler.hc = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requestedURL = req.URL.String()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("markdown")),
			}, nil
		}),
	}

	got, err := crawler.Markdown(context.Background(), "http://127.0.0.1/private")

	require.NoError(t, err)
	assert.Equal(t, []byte("markdown"), got)
	assert.Equal(t, "https://r.jina.ai/http://127.0.0.1/private", requestedURL)
}

func TestReadLimited_BitsUT(t *testing.T) {
	got, err := readLimited(strings.NewReader("1234"), 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("1234"), got)

	_, err = readLimited(strings.NewReader("12345"), 4)
	require.ErrorContains(t, err, "exceeds")
}

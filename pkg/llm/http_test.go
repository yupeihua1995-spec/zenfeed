package llm

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOutboundHTTPClientTimeouts_BitsUT(t *testing.T) {
	config := &Config{Name: "test", Provider: ProviderTypeGemini, Endpoint: "https://example.test", APIKey: "secret", Model: "model"}
	geminiLLM := newGemini(config).(*gemini)
	require.Equal(t, 2*time.Minute, geminiLLM.hc.Timeout)
	require.NoError(t, geminiLLM.Close())

	openaiLLM := newOpenAI(config).(*openai)
	require.NoError(t, openaiLLM.Close())
}

func TestGeminiNullContentReturnsError_BitsUT(t *testing.T) {
	config := &Config{Name: "test", Endpoint: "https://example.test", APIKey: "secret", TTSModel: "tts"}
	g := newGemini(config).(*gemini)
	g.hc.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"candidates":[{"content":null}]}`)),
			Header:     make(http.Header),
		}, nil
	})

	_, err := g.doWAVRequest(context.Background(), &geminiRequest{})
	require.ErrorContains(t, err, "no audio data")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

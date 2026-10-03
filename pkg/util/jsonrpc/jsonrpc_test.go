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

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/glidea/zenfeed/pkg/api"
	"github.com/glidea/zenfeed/pkg/test"
)

func TestAPI(t *testing.T) {
	RegisterTestingT(t)

	type TestRequest struct {
		Name string `json:"name"`
	}

	type TestResponse struct {
		Greeting string `json:"greeting"`
	}

	type givenDetail struct {
		handler Handler[TestRequest, TestResponse]
	}
	type whenDetail struct {
		method      string
		requestBody string
	}
	type thenExpected struct {
		statusCode   int
		responseBody string
	}

	successHandler := func(ctx context.Context, req *TestRequest) (*TestResponse, error) {
		return &TestResponse{Greeting: "Hello, " + req.Name}, nil
	}

	badRequestHandler := func(ctx context.Context, req *TestRequest) (*TestResponse, error) {
		return nil, api.ErrBadRequest(errors.New("invalid request"))
	}

	notFoundHandler := func(ctx context.Context, req *TestRequest) (*TestResponse, error) {
		return nil, api.ErrNotFound(errors.New("resource not found"))
	}

	internalErrorHandler := func(ctx context.Context, req *TestRequest) (*TestResponse, error) {
		return nil, api.ErrInternal(errors.New("server error"))
	}

	genericErrorHandler := func(ctx context.Context, req *TestRequest) (*TestResponse, error) {
		return nil, errors.New("generic error")
	}

	tests := []test.Case[givenDetail, whenDetail, thenExpected]{
		{
			Scenario: "Successful request",
			Given:    "a handler that returns a successful response",
			When:     "making a valid request",
			Then:     "should return 200 OK with the expected response",
			GivenDetail: givenDetail{
				handler: successHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":"World"}`,
			},
			ThenExpected: thenExpected{
				statusCode:   http.StatusOK,
				responseBody: `{"greeting":"Hello, World"}`,
			},
		},
		{
			Scenario: "Empty request body",
			Given:    "a handler that returns a successful response",
			When:     "making a request with empty body",
			Then:     "should return 200 OK with default values",
			GivenDetail: givenDetail{
				handler: successHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: "",
			},
			ThenExpected: thenExpected{
				statusCode:   http.StatusOK,
				responseBody: `{"greeting":"Hello, "}`,
			},
		},
		{
			Scenario: "Invalid JSON request",
			Given:    "a handler that processes JSON",
			When:     "making a request with invalid JSON",
			Then:     "should return 400 Bad Request",
			GivenDetail: givenDetail{
				handler: successHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":`,
			},
			ThenExpected: thenExpected{
				statusCode: http.StatusBadRequest,
			},
		},
		{
			Scenario: "Bad request error",
			Given:    "a handler that returns a bad request error",
			When:     "making a request that triggers a bad request error",
			Then:     "should return 400 Bad Request with error details",
			GivenDetail: givenDetail{
				handler: badRequestHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":"World"}`,
			},
			ThenExpected: thenExpected{
				statusCode:   http.StatusBadRequest,
				responseBody: `{"code":400,"message":"invalid request"}`,
			},
		},
		{
			Scenario: "Not found error",
			Given:    "a handler that returns a not found error",
			When:     "making a request that triggers a not found error",
			Then:     "should return 404 Not Found with error details",
			GivenDetail: givenDetail{
				handler: notFoundHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":"World"}`,
			},
			ThenExpected: thenExpected{
				statusCode:   http.StatusNotFound,
				responseBody: `{"code":404,"message":"resource not found"}`,
			},
		},
		{
			Scenario: "Internal server error",
			Given:    "a handler that returns an internal server error",
			When:     "making a request that triggers an internal server error",
			Then:     "should return 500 Internal Server Error with error details",
			GivenDetail: givenDetail{
				handler: internalErrorHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":"World"}`,
			},
			ThenExpected: thenExpected{
				statusCode:   http.StatusInternalServerError,
				responseBody: `{"code":500,"message":"server error"}`,
			},
		},
		{
			Scenario: "Generic error",
			Given:    "a handler that returns a generic error",
			When:     "making a request that triggers a generic error",
			Then:     "should return 500 Internal Server Error",
			GivenDetail: givenDetail{
				handler: genericErrorHandler,
			},
			WhenDetail: whenDetail{
				method:      http.MethodPost,
				requestBody: `{"name":"World"}`,
			},
			ThenExpected: thenExpected{
				statusCode: http.StatusInternalServerError,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Scenario, func(t *testing.T) {
			// Given.
			handler := API(tt.GivenDetail.handler)

			// When.
			var req *http.Request
			if tt.WhenDetail.requestBody == "" {
				req = httptest.NewRequest(tt.WhenDetail.method, "/test", nil)
			} else {
				req = httptest.NewRequest(tt.WhenDetail.method, "/test", bytes.NewBufferString(tt.WhenDetail.requestBody))
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			// Then.
			Expect(rec.Code).To(Equal(tt.ThenExpected.statusCode))

			if tt.ThenExpected.responseBody != "" {
				var expected, actual interface{}
				err := json.Unmarshal([]byte(tt.ThenExpected.responseBody), &expected)
				Expect(err).NotTo(HaveOccurred())

				body, err := io.ReadAll(rec.Body)
				Expect(err).NotTo(HaveOccurred())

				err = json.Unmarshal(body, &actual)
				Expect(err).NotTo(HaveOccurred())

				Expect(actual).To(Equal(expected))
			}
		})
	}
}

func TestAPIRequestHardening_BitsUT(t *testing.T) {
	type request struct {
		Name string `json:"name"`
	}
	type response struct{}

	called := 0
	handler := API(func(ctx context.Context, req *request) (*response, error) {
		called++

		return &response{}, nil
	})

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		t.Run("rejects "+method, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(method, "/test", nil))

			NewWithT(t).Expect(rec.Code).To(Equal(http.StatusMethodNotAllowed))
			NewWithT(t).Expect(rec.Header().Get("Allow")).To(Equal("POST, OPTIONS"))
		})
	}

	t.Run("allows preflight without invoking handler", func(t *testing.T) {
		g := NewWithT(t)
		before := called
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodOptions, "/test", nil))

		g.Expect(rec.Code).To(Equal(http.StatusOK))
		g.Expect(rec.Header().Get("Allow")).To(Equal("POST, OPTIONS"))
		g.Expect(rec.Header().Get("Access-Control-Allow-Origin")).To(BeEmpty())
		g.Expect(called).To(Equal(before))
	})

	for _, body := range []string{
		`{"name":"first"}{"name":"second"}`,
		`{"name":"first"} trailing`,
	} {
		t.Run("rejects trailing JSON data", func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body)))

			NewWithT(t).Expect(rec.Code).To(Equal(http.StatusBadRequest))
		})
	}

	t.Run("rejects request bodies larger than one MiB", func(t *testing.T) {
		g := NewWithT(t)
		body := `{"name":"` + string(bytes.Repeat([]byte("x"), (1<<20)+1)) + `"}`
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body)))

		g.Expect(rec.Code).To(Equal(http.StatusRequestEntityTooLarge))
	})

	t.Run("allows a larger route-specific body limit", func(t *testing.T) {
		g := NewWithT(t)
		largeHandler := APIWithLimit(func(ctx context.Context, req *request) (*response, error) {
			return &response{}, nil
		}, WriteMaxRequestBodyBytes)
		body := `{"name":"` + string(bytes.Repeat([]byte("x"), (1<<20)+1)) + `"}`
		rec := httptest.NewRecorder()
		largeHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body)))

		g.Expect(rec.Code).To(Equal(http.StatusOK))
	})

	t.Run("accepts a body without content type for compatibility", func(t *testing.T) {
		g := NewWithT(t)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(
			http.MethodPost, "/test", bytes.NewBufferString(`{"name":"ok"}`),
		))

		g.Expect(rec.Code).To(Equal(http.StatusOK))
	})

	t.Run("rejects an explicitly unsupported content type", func(t *testing.T) {
		g := NewWithT(t)
		req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"name":"ok"}`))
		req.Header.Set("Content-Type", "text/plain")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		g.Expect(rec.Code).To(Equal(http.StatusUnsupportedMediaType))
	})
}

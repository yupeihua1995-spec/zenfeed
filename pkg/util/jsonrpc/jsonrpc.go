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
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/glidea/zenfeed/pkg/api"
)

type Handler[Request any, Response any] func(ctx context.Context, req *Request) (*Response, error)

const (
	DefaultMaxRequestBodyBytes int64 = 1 << 20
	WriteMaxRequestBodyBytes   int64 = 8 << 20
)

func API[Request any, Response any](handler Handler[Request, Response]) http.Handler {
	return APIWithLimit(handler, DefaultMaxRequestBodyBytes)
}

func APIWithLimit[Request any, Response any](
	handler Handler[Request, Response],
	maxRequestBodyBytes int64,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "POST, OPTIONS")

		switch r.Method {
		case http.MethodOptions:
			return
		case http.MethodPost:
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		var req Request
		if r.Body != http.NoBody {
			if contentType := r.Header.Get("Content-Type"); contentType != "" {
				mediaType, _, err := mime.ParseMediaType(contentType)
				if err != nil || mediaType != "application/json" {
					http.Error(w, "request body must use application/json", http.StatusUnsupportedMediaType)

					return
				}
			}

			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
			if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
				writeDecodeError(w, err)

				return
			}
			if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
				if err == nil {
					http.Error(w, "request body must contain exactly one JSON value", http.StatusBadRequest)
				} else {
					writeDecodeError(w, err)
				}

				return
			}
		}

		resp, err := handler(r.Context(), &req)
		if err != nil {
			var apiErr api.Error
			if errors.As(err, &apiErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(apiErr.Code)
				_ = json.NewEncoder(w).Encode(apiErr)

				return
			}

			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	})
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)

		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

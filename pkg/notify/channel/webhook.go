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

package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/notify/route"
	runtimeutil "github.com/glidea/zenfeed/pkg/util/runtime"
)

type WebhookReceiver struct {
	URL string `json:"url"`
}

func (r *WebhookReceiver) Validate() error {
	if r.URL == "" {
		return errors.New("webhook.url is required")
	}

	return nil
}

type webhookBody struct {
	Group   string        `json:"group"`
	Labels  model.Labels  `json:"labels"`
	Summary string        `json:"summary"`
	Feeds   []*route.Feed `json:"feeds"`
}

func newWebhook() sender {
	return &webhook{
		httpClient: &http.Client{Timeout: sendTimeout},
	}
}

type webhook struct {
	httpClient *http.Client
}

func (w *webhook) Send(ctx context.Context, receiver Receiver, group *route.FeedGroup) error {
	// Prepare request.
	body := &webhookBody{
		Group:   group.Name,
		Labels:  group.Labels,
		Summary: group.Summary,
		Feeds:   group.Feeds,
	}
	b := runtimeutil.Must1(json.Marshal(body))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, receiver.Webhook.URL, bytes.NewReader(b))
	if err != nil {
		return errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/json")

	// Send request.
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "send request")
	}
	defer func() { _ = resp.Body.Close() }()

	// Handle response.
	if resp.StatusCode != http.StatusOK {
		return errors.New("send request")
	}

	return nil
}

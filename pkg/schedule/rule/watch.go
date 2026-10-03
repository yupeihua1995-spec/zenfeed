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

package rule

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
)

func newWatch(instance string, config *Config, dependencies Dependencies) (Rule, error) {
	return &watch{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "WatchRuler",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
	}, nil
}

type watch struct {
	*component.Base[Config, Dependencies]
}

func (r *watch) Run() (err error) {
	ctx := telemetry.StartWith(r.Context(), append(r.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()
	r.MarkReady()

	iter := func(now time.Time) {
		config := r.Config()
		end := time.Unix(now.Unix(), 0).Truncate(config.WatchInterval)
		// Interval 0, 1 are retry, to ensure success.
		// That means, one execution result at least send 3 times.
		// So the customer need to deduplicate the result by themselves.
		start := end.Add(-3 * config.WatchInterval)

		if err := r.execute(ctx, start, end); err != nil {
			log.Warn(ctx, errors.Wrap(err, "execute, retry in next time"))
		}
		log.Debug(ctx, "watch rule executed", "start", start, "end", end)
	}

	offset := timeutil.Random(time.Minute)
	log.Debug(ctx, "computed watch offset", "offset", offset)

	tick := time.NewTimer(offset)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return nil
		case now := <-tick.C:
			iter(now)
			tick.Reset(r.Config().WatchInterval)
		}
	}
}

func (r *watch) execute(ctx context.Context, start, end time.Time) error {
	ctx = log.With(ctx, "start", start, "end", end)

	// Query.
	config := r.Config()
	feeds, err := r.Dependencies().FeedStorage.Query(ctx, block.QueryOptions{
		Query:        config.Query,
		Threshold:    config.Threshold,
		LabelFilters: config.LabelFilters,
		Start:        start,
		End:          end,
		Limit:        500,
	})
	if err != nil {
		return errors.Wrap(err, "query")
	}
	if len(feeds) == 0 {
		log.Debug(ctx, "no feeds found")

		return nil
	}

	// Attach labels to feeds.
	for _, feed := range feeds {
		feed.Labels = append(feed.Labels, config.labels...)
		feed.Labels.EnsureSorted()
	}

	// Split feeds by start time.
	feedsByStart := make(map[time.Time][]*block.FeedVO) // Start time -> feeds.
	for _, feed := range feeds {
		interval := time.Unix(feed.Time.Unix(), 0).Truncate(config.WatchInterval)
		feedsByStart[interval] = append(feedsByStart[interval], feed)
	}

	// Notify.
	for start, feeds := range feedsByStart {
		result := &Result{
			Rule:  config.Name,
			Time:  start,
			Feeds: feeds,
		}
		select {
		case r.Dependencies().Out <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	log.Debug(ctx, "rule notified", "feeds", len(feedsByStart))

	return nil
}

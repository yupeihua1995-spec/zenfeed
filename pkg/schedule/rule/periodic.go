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

func newPeriodic(instance string, config *Config, dependencies Dependencies) (Rule, error) {
	return &periodic{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "PeriodicRuler",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
	}, nil
}

type periodic struct {
	*component.Base[Config, Dependencies]
}

func (r *periodic) Run() (err error) {
	ctx := telemetry.StartWith(r.Context(), append(r.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	defer func() { telemetry.End(ctx, err) }()
	r.MarkReady()

	iter := func(now time.Time) {
		config := r.Config()
		today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		end := time.Date(today.Year(), today.Month(), today.Day(),
			config.end.Hour(), config.end.Minute(), 0, 0, today.Location())

		buffer := 30 * time.Minute
		endPlusBuffer := end.Add(buffer)
		if now.Before(end) || now.After(endPlusBuffer) {
			return
		}
		if err := r.execute(ctx, now); err != nil {
			log.Warn(ctx, errors.Wrap(err, "execute, retry in next time"))
		}
		log.Debug(ctx, "rule executed", "now", now, "end", end)
	}

	offset := timeutil.Random(time.Minute)
	log.Debug(ctx, "computed watch offset", "offset", offset)

	tick := time.NewTimer(offset)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			iter(now)
			tick.Reset(5 * time.Minute)
		}
	}
}

func (r *periodic) execute(ctx context.Context, now time.Time) error {
	// Determine the query interval based on now and config's start, end and crossDay.
	config := r.Config()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	var start, end time.Time
	if config.crossDay {
		yesterday := today.AddDate(0, 0, -1)
		start = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(),
			config.start.Hour(), config.start.Minute(), 0, 0, yesterday.Location())
		end = time.Date(today.Year(), today.Month(), today.Day(),
			config.end.Hour(), config.end.Minute(), 0, 0, today.Location())
	} else {
		start = time.Date(today.Year(), today.Month(), today.Day(),
			config.start.Hour(), config.start.Minute(), 0, 0, today.Location())
		end = time.Date(today.Year(), today.Month(), today.Day(),
			config.end.Hour(), config.end.Minute(), 0, 0, today.Location())
	}

	// Query.
	ctx = log.With(ctx, "start", start, "end", end)
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

	// Notify.
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
	log.Debug(ctx, "rule notified", "feeds", len(feeds))

	return nil
}

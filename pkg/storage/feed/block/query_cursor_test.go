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

package block

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/glidea/zenfeed/pkg/model"
)

func TestFeedCursorOrdering(t *testing.T) {
	baseTime := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	cursor := &QueryCursor{Score: 0.8, Time: baseTime, ID: 20}

	tests := []struct {
		name  string
		feed  *FeedVO
		after bool
	}{
		{name: "lower score", feed: cursorFeed(1, baseTime.Add(time.Hour), 0.7), after: true},
		{name: "higher score", feed: cursorFeed(1, baseTime.Add(-time.Hour), 0.9), after: false},
		{name: "older at same score", feed: cursorFeed(1, baseTime.Add(-time.Second), 0.8), after: true},
		{name: "newer at same score", feed: cursorFeed(1, baseTime.Add(time.Second), 0.8), after: false},
		{name: "lower ID at same score and time", feed: cursorFeed(19, baseTime, 0.8), after: true},
		{name: "higher ID at same score and time", feed: cursorFeed(21, baseTime, 0.8), after: false},
		{name: "cursor item", feed: cursorFeed(20, baseTime, 0.8), after: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.after, feedIsAfterCursor(tt.feed, cursor))
		})
	}
}

func TestFeedHeapUsesStableIDTieBreaker(t *testing.T) {
	when := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	heap := NewFeedVOHeap(make(FeedVOs, 0, 3))
	heap.TryEvictPush(cursorFeed(2, when, 0.5))
	heap.TryEvictPush(cursorFeed(1, when, 0.5))
	heap.TryEvictPush(cursorFeed(3, when, 0.5))
	heap.DESCSort()

	assert.Equal(t, uint64(3), heap.Slice()[0].ID)
	assert.Equal(t, uint64(2), heap.Slice()[1].ID)
	assert.Equal(t, uint64(1), heap.Slice()[2].ID)
}

func TestQueryOptionsExcludedSources(t *testing.T) {
	query := QueryOptions{ExcludedSources: []string{"disabled", "paused"}}

	assert.NoError(t, query.Validate())
	assert.True(t, query.excludesSource("disabled"))
	assert.True(t, query.excludesSource("paused"))
	assert.False(t, query.excludesSource("enabled"))
}

func cursorFeed(id uint64, when time.Time, score float32) *FeedVO {
	return &FeedVO{
		Feed:  &model.Feed{ID: id, Time: when},
		Score: score,
	}
}

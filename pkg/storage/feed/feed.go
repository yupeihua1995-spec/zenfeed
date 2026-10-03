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

package feed

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/llm"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/rewrite"
	"github.com/glidea/zenfeed/pkg/storage/feed/block"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/chunk"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/inverted"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/primary"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index/vector"
	"github.com/glidea/zenfeed/pkg/telemetry"
	"github.com/glidea/zenfeed/pkg/telemetry/log"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/lifecycle"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
)

var clk = clock.New()

// --- Interface code block ---
type Storage interface {
	component.Component
	config.Watcher

	// Append stores some feeds.
	Append(ctx context.Context, feeds ...*model.Feed) error

	// Query retrieves feeds by query options.
	// Results are sorted by score (if vector query) and time.
	Query(ctx context.Context, query block.QueryOptions) ([]*block.FeedVO, error)

	// Exists checks if a feed exists in the storage.
	// If hintTime is zero, it only checks the head block.
	Exists(ctx context.Context, id uint64, hintTime time.Time) (bool, error)

	// UpdateLabels persists label overrides for an existing feed.
	UpdateLabels(ctx context.Context, id uint64, hintTime time.Time, labels map[string]string) error
}

type Config struct {
	Dir           string
	Retention     time.Duration
	BlockDuration time.Duration
	EmbeddingLLM  string
	FlushInterval time.Duration
}

const subDir = "feed"
const labelOverridesFilename = "label-overrides.json"

func (c *Config) Validate() error {
	if c.Dir == "" {
		c.Dir = "./data/" + subDir
	}
	if c.Retention <= 0 {
		c.Retention = 8 * timeutil.Day
	}
	if c.Retention < timeutil.Day || c.Retention > 15*timeutil.Day {
		return errors.New("retention must be between 1 day and 15 days")
	}
	if c.BlockDuration <= 0 {
		c.BlockDuration = 25 * time.Hour
	}
	if c.Retention < c.BlockDuration {
		return errors.Errorf("retention must be greater than %s", c.BlockDuration)
	}
	if c.EmbeddingLLM == "" {
		return errors.New("embedding LLM is required")
	}

	return nil
}

func (c *Config) From(app *config.App) {
	*c = Config{
		Dir:           app.Storage.Dir,
		Retention:     time.Duration(app.Storage.Feed.Retention),
		BlockDuration: time.Duration(app.Storage.Feed.BlockDuration),
		FlushInterval: time.Duration(app.Storage.Feed.FlushInterval),
		EmbeddingLLM:  app.Storage.Feed.EmbeddingLLM,
	}
}

type Dependencies struct {
	BlockFactory    block.Factory
	LLMFactory      llm.Factory
	ChunkFactory    chunk.Factory
	PrimaryFactory  primary.Factory
	InvertedFactory inverted.Factory
	VectorFactory   vector.Factory
	Rewriter        rewrite.Rewriter
}

// --- Factory code block ---
type Factory component.Factory[Storage, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Storage, config.App, Dependencies](
			func(instance string, app *config.App, dependencies Dependencies) (Storage, error) {
				m := &mockStorage{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Storage, config.App, Dependencies](new)
}

func new(instance string, app *config.App, dependencies Dependencies) (Storage, error) {
	config := &Config{}
	config.From(app)
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	if err := os.MkdirAll(config.Dir, 0700); err != nil {
		return nil, errors.Wrap(err, "ensure data dir")
	}
	labelOverrides, err := loadLabelOverrides(config.Dir)
	if err != nil {
		return nil, errors.Wrap(err, "load label overrides")
	}
	s := &storage{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "FeedStorage",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
		blocks:         &blockChain{blocks: make(map[string]block.Block)},
		labelOverrides: labelOverrides,
		childExits:     make(chan blockExit, 1),
	}
	if err := loadBlocks(config.Dir, s); err != nil {
		return nil, stderrors.Join(errors.Wrap(err, "load blocks"), s.Close())
	}

	// Ensure head block.
	if len(s.blocks.list(nil)) == 0 {
		b, err := s.createBlock(clk.Now())
		if err != nil {
			return nil, stderrors.Join(errors.Wrap(err, "create head block"), s.Close())
		}
		if err := s.publishBlock(b, newOwnedBlock(b)); err != nil {
			return nil, joinErrors(
				errors.Wrap(err, "publish head block"),
				errors.Wrap(b.Close(), "close unpublished head block"),
				errors.Wrap(b.ClearOnDisk(), "clear unpublished head block"),
				s.Close(),
			)
		}
	}

	return s, nil
}

func loadBlocks(path string, s *storage) error {
	// Scan path.
	ls, err := os.ReadDir(path)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}

	// Load blocks.
	for _, info := range ls {
		if !info.IsDir() {
			continue
		}
		if _, err := s.loadBlock(info.Name()); err != nil {
			return errors.Wrapf(err, "load block %s", info.Name())
		}
	}

	return nil
}

type blockChain struct {
	blocks map[string]block.Block
	mu     sync.RWMutex
}

func (c *blockChain) isHead(b block.Block) bool {
	return timeutil.InRange(clk.Now(), b.Start(), b.End())
}
func (c *blockChain) head() block.Block {
	b, ok := c.get(clk.Now())
	if !ok {
		return nil
	}

	return b
}
func (c *blockChain) list(filter func(block block.Block) bool) []block.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	blocks := make([]block.Block, 0, len(c.blocks))
	for _, b := range c.blocks {
		if filter != nil && !filter(b) {
			continue
		}
		blocks = append(blocks, b)
	}

	return blocks
}
func (c *blockChain) endTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.blocks) == 0 {
		return time.Time{}
	}
	var maxEnd time.Time
	for _, b := range c.blocks {
		if !b.End().After(maxEnd) {
			continue
		}
		maxEnd = b.End()
	}

	return maxEnd
}
func (c *blockChain) get(time time.Time) (block.Block, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, b := range c.blocks {
		if timeutil.InRange(time, b.Start(), b.End()) {
			return b, true
		}
	}

	return nil, false
}
func (c *blockChain) add(block block.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[blockName(block.Start())] = block
}
func (c *blockChain) delete(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.blocks, name)
}
func (c *blockChain) remove(before time.Time, callback func(block block.Block)) {
	c.mu.Lock()

	removed := make([]block.Block, 0)
	for key, b := range c.blocks {
		if b.End().After(before) {
			continue
		}
		delete(c.blocks, key)
		removed = append(removed, b)
	}
	c.mu.Unlock()

	for _, b := range removed {
		callback(b)
	}
}

// --- Implementation code block ---

type storage struct {
	*component.Base[Config, Dependencies]
	blocks           *blockChain
	labelOverridesMu sync.RWMutex
	labelOverrides   map[string]map[string]string

	lifecycleMu sync.Mutex
	headMu      sync.Mutex
	ownedBlocks map[string]*ownedBlock
	closed      bool
	starting    bool
	running     bool
	close       lifecycle.Once
	childExits  chan blockExit
}

func loadLabelOverrides(dir string) (map[string]map[string]string, error) {
	path := filepath.Join(dir, labelOverridesFilename)
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return make(map[string]map[string]string), nil
	case err != nil:
		return nil, errors.Wrap(err, "read label overrides")
	}

	overrides := make(map[string]map[string]string)
	if err := json.Unmarshal(b, &overrides); err != nil {
		return nil, errors.Wrap(err, "decode label overrides")
	}

	return overrides, nil
}

type blockExit struct {
	child *ownedBlock
	err   error
}

type ownedBlock struct {
	block.Block
	owner     *lifecycle.Owned
	watchOnce sync.Once
}

func newOwnedBlock(child block.Block) *ownedBlock {
	return &ownedBlock{Block: child, owner: lifecycle.NewOwned(child)}
}

func (b *ownedBlock) Run() error { return b.owner.Run() }

func (b *ownedBlock) Close() error {
	return b.owner.Close()
}

func (b *ownedBlock) watch(ctx context.Context, exits chan<- blockExit) {
	b.watchOnce.Do(func() {
		go func() {
			<-b.owner.Done()
			if err := b.owner.UnexpectedExit(); err != nil {
				select {
				case exits <- blockExit{child: b, err: err}:
				case <-ctx.Done():
				}
			}
		}()
	})
}

func (s *storage) Run() (err error) {
	ctx := telemetry.StartWith(s.Context(), append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Run")...)
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()

		return errors.New("storage is closed")
	}
	if s.childExits == nil {
		s.childExits = make(chan blockExit, 1)
	}
	s.starting = true
	blocks := s.ownedBlocksLocked()
	s.lifecycleMu.Unlock()

	defer func() {
		err = stderrors.Join(err, s.Close())
		telemetry.End(ctx, err)
	}()

	// Run blocks.
	for _, b := range blocks {
		if err := component.RunUntilReady(ctx, b, 10*time.Second); err != nil {
			return errors.Wrap(err, "run block")
		}
		b.watch(s.Context(), s.childExits)
	}
	for _, b := range blocks {
		select {
		case <-b.owner.Done():
			if err := b.owner.UnexpectedExit(); err != nil {
				return errors.Wrapf(err, "block %s exited during startup", b.Instance())
			}
		default:
		}
	}

	// Maintain blocks.
	s.lifecycleMu.Lock()
	s.starting = false
	s.running = true
	s.lifecycleMu.Unlock()
	s.MarkReady()

	ticker := clk.Timer(0)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			if err := s.reconcileBlocks(ctx, now); err != nil {
				log.Error(ctx, errors.Wrap(err, "reconcile blocks"))

				continue
			}

			log.Debug(ctx, "reconcile blocks success")
			ticker.Reset(30 * time.Second)

		case <-ctx.Done():
			return nil
		case exit := <-s.childExits:
			if s.isCurrentBlock(exit.child) {
				return errors.Wrapf(exit.err, "block %s exited unexpectedly", exit.child.Instance())
			}
		}
	}
}

func (s *storage) Close() error {
	return s.close.Do(func() error {
		baseErr := s.Base.Close()

		s.lifecycleMu.Lock()
		s.closed = true
		s.starting = false
		s.running = false
		blocks := s.ownedBlocksLocked()
		s.lifecycleMu.Unlock()

		return stderrors.Join(baseErr, lifecycle.CloseAll(blocks...))
	})
}

func (s *storage) ownedBlocksLocked() []*ownedBlock {
	if s.ownedBlocks == nil {
		s.ownedBlocks = make(map[string]*ownedBlock)
	}
	for _, child := range s.blocks.list(nil) {
		name := blockName(child.Start())
		if _, ok := s.ownedBlocks[name]; !ok {
			s.ownedBlocks[name] = newOwnedBlock(child)
		}
	}
	blocks := make([]*ownedBlock, 0, len(s.ownedBlocks))
	for _, child := range s.ownedBlocks {
		blocks = append(blocks, child)
	}
	slices.SortFunc(blocks, func(a, b *ownedBlock) int {
		return a.Start().Compare(b.Start())
	})

	return blocks
}

func (s *storage) isCurrentBlock(child *ownedBlock) bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	for _, current := range s.ownedBlocks {
		if current == child {
			return true
		}
	}

	return false
}

func (s *storage) ownedBlock(child block.Block) *ownedBlock {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	return s.ownedBlockLocked(blockName(child.Start()), child)
}

func (s *storage) ownedBlockLocked(name string, child block.Block) *ownedBlock {
	if s.ownedBlocks == nil {
		s.ownedBlocks = make(map[string]*ownedBlock)
	}
	owned, ok := s.ownedBlocks[name]
	if !ok {
		owned = newOwnedBlock(child)
		s.ownedBlocks[name] = owned
	}

	return owned
}

func (s *storage) Reload(app *config.App) error {
	s.headMu.Lock()
	defer s.headMu.Unlock()
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return errors.New("storage is closed")
	}
	if s.starting {
		return errors.New("storage is starting")
	}

	// Validate new config.
	newConfig := &Config{}
	newConfig.From(app)
	if err := newConfig.Validate(); err != nil {
		return errors.Wrap(err, "validate config")
	}
	if reflect.DeepEqual(s.Config(), newConfig) {
		log.Debug(s.Context(), "no changes in feed storage config")

		return nil
	}

	// Check immutable fields.
	curConfig := s.Config()
	if newConfig.Dir != curConfig.Dir {
		return errors.New("cannot reload the dir, MUST pass the same dir, or set it to empty for unchange")
	}

	// Reload blocks.
	for _, b := range s.blocks.list(nil) {
		if err := b.Reload(&block.Config{
			FlushInterval: newConfig.FlushInterval,
		}); err != nil {
			return errors.Wrapf(err, "reload block %s", blockName(b.Start()))
		}
	}

	// Set config.
	s.SetConfig(newConfig)

	return nil
}

func (s *storage) Append(ctx context.Context, feeds ...*model.Feed) (err error) {
	ctx = telemetry.StartWith(ctx, append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Append")...)
	defer func() { telemetry.End(ctx, err) }()
	for _, f := range feeds {
		if err := f.Validate(); err != nil {
			return errors.Wrap(err, "validate feed")
		}
	}

	// Rewrite feeds.
	rewritten, err := s.rewrite(ctx, feeds)
	if err != nil {
		return errors.Wrap(err, "rewrite feeds")
	}
	if len(rewritten) == 0 {
		log.Debug(ctx, "no feeds to write after rewrites")

		return nil
	}

	// Append feeds to head block.
	log.Debug(ctx, "append feeds", "count", len(rewritten))
	head := s.blocks.head()
	if head == nil {
		return errors.New("head block not found")
	}
	if err := head.Append(ctx, rewritten...); err != nil {
		return errors.Wrap(err, "append feeds")
	}

	return nil
}

func (s *storage) Query(ctx context.Context, query block.QueryOptions) (feeds []*block.FeedVO, err error) {
	ctx = telemetry.StartWith(ctx, append(s.TelemetryLabels(), telemetrymodel.KeyOperation, "Query")...)
	defer func() { telemetry.End(ctx, err) }()
	if err := (&query).Validate(); err != nil {
		return nil, errors.Wrap(err, "validate query")
	}
	originalOnMatch := query.OnMatch
	query.OnMatch = func(candidate *block.FeedVO) {
		s.applyLabelOverrides(candidate)
		if originalOnMatch != nil {
			originalOnMatch(candidate)
		}
	}

	// Parallel read.
	blocks := s.blocks.list(nil)
	feedHeap := block.NewFeedVOHeap(make(block.FeedVOs, 0, query.Limit))
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs []error
	)

	for _, b := range blocks {
		if !query.HitTimeRangeCondition(b) {
			continue
		}

		wg.Add(1)
		go func(b block.Block) {
			defer wg.Done()
			fs, err := b.Query(ctx, query)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()

				return
			}

			mu.Lock()
			for _, f := range fs {
				feedHeap.TryEvictPush(f)
			}
			mu.Unlock()
		}(b)
	}
	wg.Wait()
	if len(errs) > 0 {
		return nil, errs[0]
	}

	feedHeap.DESCSort()

	return feedHeap.Slice(), nil
}

func (s *storage) UpdateLabels(
	ctx context.Context,
	id uint64,
	hintTime time.Time,
	labels map[string]string,
) error {
	if id == 0 {
		return errors.New("feed id is required")
	}
	if len(labels) == 0 {
		return errors.New("labels are required")
	}
	for key, value := range labels {
		if key == "" {
			return errors.New("label key is required")
		}
		if len(value) > 4096 {
			return errors.Errorf("label %q exceeds 4096 bytes", key)
		}
	}

	exists, err := s.Exists(ctx, id, hintTime)
	if err != nil {
		return errors.Wrap(err, "check feed exists")
	}
	if !exists {
		return errors.New("feed not found")
	}

	s.labelOverridesMu.Lock()
	defer s.labelOverridesMu.Unlock()
	next := make(map[string]map[string]string, len(s.labelOverrides)+1)
	for feedID, current := range s.labelOverrides {
		next[feedID] = make(map[string]string, len(current))
		for key, value := range current {
			next[feedID][key] = value
		}
	}
	feedID := strconv.FormatUint(id, 10)
	if next[feedID] == nil {
		next[feedID] = make(map[string]string)
	}
	for key, value := range labels {
		next[feedID][key] = value
	}

	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return errors.Wrap(err, "encode label overrides")
	}
	path := filepath.Join(s.Config().Dir, labelOverridesFilename)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, b, 0600); err != nil {
		return errors.Wrap(err, "write label overrides")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return errors.Wrap(err, "replace label overrides")
	}
	s.labelOverrides = next

	return nil
}

func (s *storage) applyLabelOverrides(feed *block.FeedVO) {
	if feed == nil || feed.Feed == nil {
		return
	}
	feedID := strconv.FormatUint(feed.ID, 10)
	s.labelOverridesMu.RLock()
	current := s.labelOverrides[feedID]
	if len(current) == 0 {
		s.labelOverridesMu.RUnlock()
		return
	}
	overrides := make(map[string]string, len(current))
	for key, value := range current {
		overrides[key] = value
	}
	s.labelOverridesMu.RUnlock()

	clonedFeed := *feed.Feed
	clonedFeed.Labels = append(model.Labels(nil), feed.Labels...)
	for key, value := range overrides {
		clonedFeed.Labels.Put(key, value, false)
	}
	feed.Feed = &clonedFeed
}

func (s *storage) Exists(ctx context.Context, id uint64, hintTime time.Time) (bool, error) {
	// A hint narrows the first lookup, but a miss is not authoritative: the
	// feed may have been persisted to the current head after the hint was saved.
	var hinted block.Block
	if !hintTime.IsZero() {
		b, ok := s.blocks.get(hintTime)
		if ok {
			exists, err := b.Exists(ctx, id)
			if err != nil || exists {
				return exists, err
			}
			hinted = b
		}
	}

	checked := make(map[string]struct{}, 2)
	if hinted != nil {
		checked[blockName(hinted.Start())] = struct{}{}
	}

	// Fallback to head block.
	head := s.blocks.head()
	if head == nil {
		return false, errors.New("head block not found")
	}
	if hintTime.IsZero() {
		return head.Exists(ctx, id)
	}
	headName := blockName(head.Start())
	if _, alreadyChecked := checked[headName]; !alreadyChecked {
		exists, err := head.Exists(ctx, id)
		if err != nil || exists {
			return exists, err
		}
		checked[headName] = struct{}{}
	}

	// The hint is based on event time while writes always go to the ingestion
	// head, so after head rotation the authoritative copy can be in any retained
	// block. Check every remaining block once before reporting a miss.
	for _, candidate := range s.blocks.list(nil) {
		name := blockName(candidate.Start())
		if _, alreadyChecked := checked[name]; alreadyChecked {
			continue
		}
		exists, err := candidate.Exists(ctx, id)
		if err != nil || exists {
			return exists, err
		}
		checked[name] = struct{}{}
	}

	return false, nil
}

const headBlockCreateBuffer = 30 * time.Minute

func (s *storage) reconcileBlocks(ctx context.Context, now time.Time) error {
	// Create new head block if needed.
	if err := s.ensureHeadBlock(ctx, now); err != nil {
		return errors.Wrap(err, "ensure head block")
	}

	// Transform non-head hot blocks to cold.
	if err := s.ensureColdBlocks(ctx); err != nil {
		return errors.Wrap(err, "ensure cold blocks")
	}

	// Remove expired blocks.
	s.ensureRemovedExpiredBlocks(ctx, now)

	return nil
}

func (s *storage) ensureHeadBlock(ctx context.Context, now time.Time) error {
	s.headMu.Lock()
	defer s.headMu.Unlock()

	if maxEnd := s.blocks.endTime(); now.After(maxEnd.Add(-headBlockCreateBuffer)) {
		nextStart := maxEnd
		if now.After(maxEnd) {
			nextStart = now
		}
		b, err := s.createBlock(nextStart)
		if err != nil {
			return errors.Wrap(err, "create new hot block")
		}
		owned := newOwnedBlock(b)
		if err := component.RunUntilReady(ctx, owned, 10*time.Second); err != nil {
			return joinErrors(
				errors.Wrap(err, "run new hot block"),
				errors.Wrap(owned.Close(), "close failed head block"),
				errors.Wrap(b.ClearOnDisk(), "clear failed head block"),
			)
		}
		if err := s.publishBlock(b, owned); err != nil {
			return joinErrors(
				errors.Wrap(err, "publish new hot block"),
				errors.Wrap(owned.Close(), "close unpublished head block"),
				errors.Wrap(b.ClearOnDisk(), "clear unpublished head block"),
			)
		}
		owned.watch(s.Context(), s.childExits)
		log.Info(ctx, "block created", "name", blockName(b.Start()))
	}

	return nil
}

func (s *storage) ensureColdBlocks(ctx context.Context) error {
	for _, b := range s.blocks.list(func(b block.Block) bool {
		return b.State() == block.StateHot &&
			!s.blocks.isHead(b) &&
			clk.Now().After(b.End().Add(s.Config().BlockDuration)) // For recent queries.
	}) {
		if err := b.TransformToCold(); err != nil {
			return errors.Wrap(err, "transform to cold")
		}
		log.Info(ctx, "block transformed to cold", "name", blockName(b.Start()))
	}

	return nil
}

func (s *storage) ensureRemovedExpiredBlocks(ctx context.Context, now time.Time) {
	s.blocks.remove(now.Add(-s.Config().Retention), func(b block.Block) {
		var err error
		if err = s.retireBlock(b); err != nil {
			log.Error(ctx, errors.Wrap(err, "close block"))
		}
		if err = b.ClearOnDisk(); err != nil {
			log.Error(ctx, errors.Wrap(err, "clear on disk"))
		}
		if err == nil {
			log.Info(ctx, "block deleted", "name", blockName(b.Start()))
		}
	})
}

func (s *storage) retireBlock(child block.Block) error {
	name := blockName(child.Start())
	s.lifecycleMu.Lock()
	s.blocks.delete(name)
	owned := s.ownedBlockLocked(name, child)
	if s.ownedBlocks[name] == owned {
		delete(s.ownedBlocks, name)
	}
	s.lifecycleMu.Unlock()

	return owned.Close()
}

var blockName = func(start time.Time) string {
	return strconv.FormatInt(start.Unix(), 10)
}

func (s *storage) createBlock(start time.Time) (block.Block, error) {
	s.lifecycleMu.Lock()
	if s.closed {
		s.lifecycleMu.Unlock()

		return nil, errors.New("storage is closed")
	}
	config := s.Config()
	s.lifecycleMu.Unlock()

	name := blockName(start)
	dir := filepath.Join(config.Dir, name)
	if _, err := os.Stat(dir); err == nil {
		return nil, errors.Errorf("block directory already exists: %s", dir)
	} else if !os.IsNotExist(err) {
		return nil, errors.Wrap(err, "check block directory")
	}

	b, err := s.Dependencies().BlockFactory.New(
		name,
		&block.Config{
			Dir:           dir,
			FlushInterval: config.FlushInterval,
			ForCreate: &block.ForCreateConfig{
				Start:        start,
				Duration:     config.BlockDuration,
				EmbeddingLLM: config.EmbeddingLLM,
			},
		},
		s.blockDependencies(),
	)
	if err != nil {
		return nil, joinErrors(
			errors.Wrap(err, "create block"),
			errors.Wrap(os.RemoveAll(dir), "clear failed block directory"),
		)
	}

	return b, nil
}

func joinErrors(errs ...error) error {
	nonNil := errs[:0]
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}

	return stderrors.Join(nonNil...)
}

func (s *storage) publishBlock(child block.Block, owned *ownedBlock) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return errors.New("storage is closed")
	}
	name := blockName(child.Start())
	if s.ownedBlocks == nil {
		s.ownedBlocks = make(map[string]*ownedBlock)
	}
	if _, exists := s.ownedBlocks[name]; exists {
		return errors.Errorf("block %s already exists", name)
	}

	s.ownedBlocks[name] = owned
	// Publish to readers only after ownership is established.
	s.blocks.add(child)

	return nil
}

func (s *storage) loadBlock(name string) (block.Block, error) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.closed {
		return nil, errors.New("storage is closed")
	}

	dir := filepath.Join(s.Config().Dir, name)

	b, err := s.Dependencies().BlockFactory.New(
		name,
		&block.Config{Dir: dir},
		s.blockDependencies(),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create block")
	}

	s.blocks.add(b)
	s.ownedBlockLocked(name, b)

	return b, nil
}

func (s *storage) blockDependencies() block.Dependencies {
	deps := s.Dependencies()

	return block.Dependencies{
		ChunkFactory:    deps.ChunkFactory,
		PrimaryFactory:  deps.PrimaryFactory,
		InvertedFactory: deps.InvertedFactory,
		VectorFactory:   deps.VectorFactory,
		LLMFactory:      deps.LLMFactory,
	}
}

func (s *storage) rewrite(ctx context.Context, feeds []*model.Feed) ([]*model.Feed, error) {
	var (
		rewritten = make([]*model.Feed, len(feeds))
		wg        sync.WaitGroup
		mu        sync.Mutex
		errs      []error
		dropped   atomic.Int32
	)

	for i, item := range feeds { // TODO: Limit the concurrency & goroutine number.
		wg.Add(1)
		go func(i int, item *model.Feed) {
			defer wg.Done()
			labels, err := s.Dependencies().Rewriter.Labels(ctx, item.Labels)
			if err != nil {
				mu.Lock()
				errs = append(errs, errors.Wrap(err, "rewrite item"))
				mu.Unlock()

				return
			}
			if len(labels) == 0 {
				log.Debug(ctx, "drop feed", "id", item.ID)
				dropped.Add(1)

				return // Drop empty labels.
			}

			item.Labels = labels
			rewritten[i] = item
		}(i, item)
	}
	wg.Wait()

	switch len(errs) {
	case 0:
	case len(feeds) - int(dropped.Load()):
		return nil, errs[0] // All failed.
	default:
		log.Error(ctx, errors.Wrap(errs[0], "rewrite feeds"), "error_count", len(errs))
	}

	result := rewritten[:0]
	for _, item := range rewritten {
		if item != nil {
			result = append(result, item)
		}
	}

	return result, nil
}

type mockStorage struct {
	component.Mock
}

func (m *mockStorage) Reload(app *config.App) error {
	args := m.Called(app)

	return args.Error(0)
}

func (m *mockStorage) Append(ctx context.Context, feeds ...*model.Feed) error {
	args := m.Called(ctx, feeds)

	return args.Error(0)
}

func (m *mockStorage) Query(ctx context.Context, query block.QueryOptions) ([]*block.FeedVO, error) {
	args := m.Called(ctx, query)

	return args.Get(0).([]*block.FeedVO), args.Error(1)
}

func (m *mockStorage) Exists(ctx context.Context, id uint64, hintTime time.Time) (bool, error) {
	args := m.Called(ctx, id, hintTime)

	return args.Get(0).(bool), args.Error(1)
}

func (m *mockStorage) UpdateLabels(
	ctx context.Context,
	id uint64,
	hintTime time.Time,
	labels map[string]string,
) error {
	args := m.Called(ctx, id, hintTime, labels)

	return args.Error(0)
}

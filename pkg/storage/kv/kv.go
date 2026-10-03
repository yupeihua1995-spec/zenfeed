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

package kv

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"
	"time"

	"github.com/nutsdb/nutsdb"
	"github.com/pkg/errors"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/config"
	"github.com/glidea/zenfeed/pkg/telemetry"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/lifecycle"
)

// --- Interface code block ---
type Storage interface {
	component.Component
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, ttl time.Duration) error
}

var ErrNotFound = errors.New("not found")

type Config struct {
	Dir string
}

const subDir = "kv"

func (c *Config) Validate() error {
	if c.Dir == "" {
		c.Dir = "./data/" + subDir
	}

	return nil
}

func (c *Config) From(app *config.App) *Config {
	c.Dir = app.Storage.Dir

	return c
}

type Dependencies struct{}

// --- Factory code block ---
type Factory component.Factory[Storage, config.App, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Storage, config.App, Dependencies](
			func(instance string, config *config.App, dependencies Dependencies) (Storage, error) {
				m := &mockKV{}
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

	return &kv{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:         "KVStorage",
			Instance:     instance,
			Config:       config,
			Dependencies: dependencies,
		}),
	}, nil
}

// --- Implementation code block ---
type kv struct {
	*component.Base[Config, Dependencies]
	db              *nutsdb.DB
	lifecycleMu     sync.RWMutex
	closed          bool
	close           lifecycle.Once
	beforeMarkReady func()
}

func (k *kv) Run() error {
	k.lifecycleMu.Lock()
	if k.closed {
		k.lifecycleMu.Unlock()

		return errors.New("KV storage is closed")
	}
	k.lifecycleMu.Unlock()

	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(k.Config().Dir),
		nutsdb.WithSyncEnable(false),
	)
	if err != nil {
		return errors.Wrap(err, "open db")
	}
	if err := db.Update(func(tx *nutsdb.Tx) error {
		if !tx.ExistBucket(nutsdb.DataStructureBTree, bucket) {
			return tx.NewBucket(nutsdb.DataStructureBTree, bucket)
		}

		return nil
	}); err != nil {
		return stderrors.Join(
			errors.Wrap(err, "create bucket"),
			errors.Wrap(db.Close(), "close db after bucket creation failure"),
		)
	}

	k.lifecycleMu.Lock()
	if k.closed {
		k.lifecycleMu.Unlock()

		return stderrors.Join(
			errors.New("KV storage closed during startup"),
			errors.Wrap(db.Close(), "close db after canceled startup"),
		)
	}
	k.db = db
	if k.beforeMarkReady != nil {
		k.beforeMarkReady()
	}
	k.MarkReady()
	k.lifecycleMu.Unlock()
	<-k.Context().Done()

	return nil
}

func (k *kv) Close() error {
	return k.close.Do(func() error {
		baseErr := k.Base.Close()

		k.lifecycleMu.Lock()
		k.closed = true
		db := k.db
		if db == nil {
			k.lifecycleMu.Unlock()
			return baseErr
		}
		dbErr := db.Close()
		k.db = nil
		k.lifecycleMu.Unlock()

		return stderrors.Join(baseErr, dbErr)
	})
}

const bucket = "0"

func (k *kv) Get(ctx context.Context, key []byte) (value []byte, err error) {
	ctx = telemetry.StartWith(ctx, append(k.TelemetryLabels(), telemetrymodel.KeyOperation, "Get")...)
	defer func() {
		telemetry.End(ctx, func() error {
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}

			return nil
		}())
	}()
	k.lifecycleMu.RLock()
	defer k.lifecycleMu.RUnlock()
	if k.db == nil {
		return nil, errors.New("KV storage is not ready")
	}

	var b []byte
	err = k.db.View(func(tx *nutsdb.Tx) error {
		b, err = tx.Get(bucket, []byte(key))

		return err
	})
	switch {
	case err == nil:
		return b, nil
	case errors.Is(err, nutsdb.ErrNotFoundKey):
		return nil, ErrNotFound
	case strings.Contains(err.Error(), "key not found"):
		return nil, ErrNotFound
	default:
		return nil, err
	}
}

func (k *kv) Set(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err error) {
	ctx = telemetry.StartWith(ctx, append(k.TelemetryLabels(), telemetrymodel.KeyOperation, "Set")...)
	defer func() { telemetry.End(ctx, err) }()
	k.lifecycleMu.RLock()
	defer k.lifecycleMu.RUnlock()
	if k.db == nil {
		return errors.New("KV storage is not ready")
	}

	return k.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(bucket, key, value, uint32(ttl.Seconds()))
	})
}

type mockKV struct {
	component.Mock
}

func (m *mockKV) Get(ctx context.Context, key []byte) ([]byte, error) {
	args := m.Called(ctx, key)

	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockKV) Set(ctx context.Context, key []byte, value []byte, ttl time.Duration) error {
	args := m.Called(ctx, key, value, ttl)

	return args.Error(0)
}

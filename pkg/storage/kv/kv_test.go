// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package kv

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/glidea/zenfeed/pkg/component"
)

func newLifecycleTestKV(t *testing.T) *kv {
	t.Helper()

	return &kv{
		Base: component.New(&component.BaseConfig[Config, Dependencies]{
			Name:     "KVStorage",
			Instance: "lifecycle-test",
			Config:   &Config{Dir: t.TempDir()},
		}),
	}
}

func TestKVCloseBeforeRunIsSafeAndIdempotent(t *testing.T) {
	k := newLifecycleTestKV(t)

	require.NotPanics(t, func() {
		require.NoError(t, k.Close())
		require.NoError(t, k.Close())
	})
}

func TestKVCloseAfterRunIsConcurrentSafe(t *testing.T) {
	k := newLifecycleTestKV(t)
	runDone := make(chan error, 1)
	go func() { runDone <- k.Run() }()

	select {
	case <-k.Ready():
	case err := <-runDone:
		require.NoError(t, err)
		t.Fatal("KV Run exited before ready")
	case <-time.After(5 * time.Second):
		t.Fatal("KV did not become ready")
	}

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- k.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("KV Run did not exit after Close")
	}
}

func TestKVRunPublishesDatabaseAndReadyUnderLifecycleLock(t *testing.T) {
	k := newLifecycleTestKV(t)
	lockAvailable := make(chan bool, 1)
	k.beforeMarkReady = func() {
		acquired := k.lifecycleMu.TryLock()
		if acquired {
			k.lifecycleMu.Unlock()
		}
		lockAvailable <- acquired
	}

	runDone := make(chan error, 1)
	go func() { runDone <- k.Run() }()

	var acquired bool
	select {
	case acquired = <-lockAvailable:
	case err := <-runDone:
		require.NoError(t, err)
		t.Fatal("KV Run exited before publishing readiness")
	case <-time.After(5 * time.Second):
		t.Fatal("KV did not reach the readiness publication point")
	}

	select {
	case <-k.Ready():
	case <-time.After(time.Second):
		t.Fatal("KV did not become ready")
	}
	require.NoError(t, k.Close())

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("KV Run did not exit after Close")
	}
	require.False(t, acquired, "lifecycle lock was available between database publication and readiness")
}

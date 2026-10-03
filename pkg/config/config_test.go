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

package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerAppConfigSnapshot_BitsUT(t *testing.T) {
	initial := []byte("# keep file-level changes in the revision\ntimezone: UTC\nllms:\n  - name: original\n")
	m := newTestManager(t, initial)

	app, revision := m.AppConfigSnapshot()

	sum := sha256.Sum256(initial)
	assert.Equal(t, hex.EncodeToString(sum[:]), revision)
	assert.Equal(t, "UTC", app.Timezone)
	require.Len(t, app.LLMs, 1)

	app.Timezone = "mutated by caller"
	app.LLMs[0].Name = "mutated by caller"
	current, currentRevision := m.AppConfigSnapshot()
	assert.Equal(t, "UTC", current.Timezone)
	assert.Equal(t, "original", current.LLMs[0].Name)
	assert.Equal(t, revision, currentRevision)

	legacy := m.AppConfig()
	legacy.LLMs[0].Name = "mutated through AppConfig"
	assert.Equal(t, "original", m.AppConfig().LLMs[0].Name)
}

func TestManagerSaveAppConfig_BitsUT(t *testing.T) {
	t.Run("matching revision saves and advances the revision", func(t *testing.T) {
		m := newTestManager(t, []byte("timezone: UTC\n"))
		_, revision := m.AppConfigSnapshot()

		want := &App{Timezone: "Asia/Shanghai"}
		require.NoError(t, m.SaveAppConfig(want, &revision))

		got, nextRevision := m.AppConfigSnapshot()
		assert.Equal(t, want, got)
		assert.NotEqual(t, revision, nextRevision)

		onDisk, err := os.ReadFile(m.Config().Path)
		require.NoError(t, err)
		sum := sha256.Sum256(onDisk)
		assert.Equal(t, hex.EncodeToString(sum[:]), nextRevision)
	})

	t.Run("external edit conflicts before the polling reload observes it", func(t *testing.T) {
		m := newTestManager(t, []byte("timezone: UTC\n"))
		_, revision := m.AppConfigSnapshot()
		external := []byte("timezone: Europe/London\n")
		require.NoError(t, os.WriteFile(m.Config().Path, external, 0o644))

		err := m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, &revision)

		var conflict *RevisionConflictError
		require.ErrorAs(t, err, &conflict)
		assert.Equal(t, revision, conflict.Expected)
		assert.NotEqual(t, revision, conflict.Actual)
		onDisk, readErr := os.ReadFile(m.Config().Path)
		require.NoError(t, readErr)
		assert.Equal(t, external, onDisk)
		got, nextRevision := m.AppConfigSnapshot()
		assert.Equal(t, "Europe/London", got.Timezone)
		assert.Equal(t, conflict.Actual, nextRevision)
	})

	t.Run("omitted revision is rejected", func(t *testing.T) {
		m := newTestManager(t, []byte("timezone: UTC\n"))
		require.NoError(t, os.WriteFile(m.Config().Path, []byte("timezone: Europe/London\n"), 0o644))

		err := m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "revision")
		onDisk, readErr := os.ReadFile(m.Config().Path)
		require.NoError(t, readErr)
		assert.Equal(t, []byte("timezone: Europe/London\n"), onDisk)
	})

	t.Run("redacted placeholders preserve current secrets", func(t *testing.T) {
		initial := []byte(`llms:
  - name: primary
    api_key: llm-secret
jina:
  token: jina-secret
scrape:
  rsshub_access_key: rsshub-secret
storage:
  object:
    access_key_id: storage-id
    secret_access_key: storage-secret
notify:
  receivers:
    - name: hook
      webhook:
        url: https://hooks.example.test/private-token
  channels:
    email:
      password: smtp-secret
`)
		m := newTestManager(t, initial)
		redacted, revision := m.AppConfigSnapshot()
		redacted.Timezone = "Asia/Shanghai"
		redacted.LLMs[0].APIKey = "<redacted>"
		redacted.Jina.Token = "<redacted>"
		redacted.Scrape.RSSHubAccessKey = "<redacted>"
		redacted.Storage.Object.AccessKeyID = "<redacted>"
		redacted.Storage.Object.SecretAccessKey = "<redacted>"
		redacted.Notify.Channels.Email.Password = "<redacted>"
		redacted.Notify.Receivers[0].Webhook.URL = "<redacted>"

		require.NoError(t, m.SaveAppConfig(redacted, &revision))

		got, _ := m.AppConfigSnapshot()
		assert.Equal(t, "llm-secret", got.LLMs[0].APIKey)
		assert.Equal(t, "jina-secret", got.Jina.Token)
		assert.Equal(t, "rsshub-secret", got.Scrape.RSSHubAccessKey)
		assert.Equal(t, "storage-id", got.Storage.Object.AccessKeyID)
		assert.Equal(t, "storage-secret", got.Storage.Object.SecretAccessKey)
		assert.Equal(t, "smtp-secret", got.Notify.Channels.Email.Password)
		assert.Equal(t, "https://hooks.example.test/private-token", got.Notify.Receivers[0].Webhook.URL)
	})
}

func TestManagerSaveAppConfigConcurrent_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	_, revision := m.AppConfigSnapshot()

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, timezone := range []string{"Asia/Shanghai", "Europe/London"} {
		timezone := timezone
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- m.SaveAppConfig(&App{Timezone: timezone}, &revision)
		}()
	}
	close(start)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent saves did not complete")
	}
	close(results)

	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		var conflict *RevisionConflictError
		if errors.As(err, &conflict) {
			conflicts++
			continue
		}
		t.Fatalf("unexpected save error: %v", err)
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, conflicts)
}

func TestManagerReloadFailureRollsBackFileAndWatchers_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	var firstSeen []string
	m.Subscribe(WatcherFunc(func(app *App) error {
		firstSeen = append(firstSeen, app.Timezone)

		return nil
	}))
	var secondSeen []string
	m.Subscribe(WatcherFunc(func(app *App) error {
		secondSeen = append(secondSeen, app.Timezone)
		if app.Timezone == "Asia/Shanghai" {
			return errors.New("reject reload")
		}

		return nil
	}))
	_, revision := m.AppConfigSnapshot()

	err := m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, &revision)

	require.ErrorContains(t, err, "reject reload")
	onDisk, readErr := os.ReadFile(m.Config().Path)
	require.NoError(t, readErr)
	assert.Equal(t, []byte("timezone: UTC\n"), onDisk)
	got, nextRevision := m.AppConfigSnapshot()
	assert.Equal(t, "UTC", got.Timezone)
	assert.Equal(t, revision, nextRevision)
	assert.Equal(t, []string{"Asia/Shanghai", "UTC"}, firstSeen)
	assert.Equal(t, []string{"Asia/Shanghai", "UTC"}, secondSeen)
}

func TestManagerSaveAppConfigPreservesFileMode_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	require.NoError(t, os.Chmod(m.Config().Path, 0o600))
	_, revision := m.AppConfigSnapshot()

	require.NoError(t, m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, &revision))

	info, err := os.Stat(m.Config().Path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestManagerSaveRejectsDuplicateSecretKeys_BitsUT(t *testing.T) {
	t.Run("duplicate current LLM names cannot select the wrong secret", func(t *testing.T) {
		initial := []byte(`llms:
  - name: duplicate
    api_key: first-secret
  - name: duplicate
    api_key: second-secret
`)
		m := newTestManager(t, initial)
		next, revision := m.AppConfigSnapshot()
		for i := range next.LLMs {
			next.LLMs[i].APIKey = SecretPlaceholder
		}

		err := m.SaveAppConfig(next, &revision)

		require.ErrorContains(t, err, `duplicate LLM name "duplicate" in current config`)
		onDisk, readErr := os.ReadFile(m.Config().Path)
		require.NoError(t, readErr)
		assert.Equal(t, initial, onDisk)
	})

	t.Run("duplicate next LLM names are rejected", func(t *testing.T) {
		current := &App{LLMs: []LLM{{Name: "one", APIKey: "one-secret"}}}
		next := &App{LLMs: []LLM{
			{Name: "one", APIKey: SecretPlaceholder},
			{Name: "one", APIKey: SecretPlaceholder},
		}}

		require.ErrorContains(t, restoreRedactedSecrets(next, current), `duplicate LLM name "one" in new config`)
	})

	t.Run("duplicate webhook receiver names are rejected", func(t *testing.T) {
		current := &App{}
		current.Notify.Receivers = []NotifyReceiver{
			{Name: "hook", Webhook: &NotifyReceiverWebhook{URL: "https://one.example"}},
			{Name: "hook", Webhook: &NotifyReceiverWebhook{URL: "https://two.example"}},
		}

		require.ErrorContains(t, restoreRedactedSecrets(&App{}, current),
			`duplicate webhook receiver name "hook" in current config`)
	})

	t.Run("duplicate new webhook receiver names are rejected", func(t *testing.T) {
		next := &App{}
		next.Notify.Receivers = []NotifyReceiver{
			{Name: "hook", Webhook: &NotifyReceiverWebhook{URL: SecretPlaceholder}},
			{Name: "hook", Webhook: &NotifyReceiverWebhook{URL: SecretPlaceholder}},
		}

		require.ErrorContains(t, restoreRedactedSecrets(next, &App{}),
			`duplicate webhook receiver name "hook" in new config`)
	})
}

func TestManagerRollbackFailurePublishesDiskSnapshot_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	_, oldRevision := m.AppConfigSnapshot()
	m.Subscribe(WatcherFunc(func(app *App) error {
		if app.Timezone == "Asia/Shanghai" {
			return errors.New("reject reload")
		}

		return nil
	}))

	rollbackFailure := errors.New("injected rollback failure")
	m.atomicWrite = func(string, []byte, os.FileMode) error { return rollbackFailure }

	err := m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, &oldRevision)

	require.ErrorContains(t, err, rollbackFailure.Error())
	onDisk, readErr := os.ReadFile(m.Config().Path)
	require.NoError(t, readErr)
	got, revision := m.AppConfigSnapshot()
	assert.Equal(t, "Asia/Shanghai", got.Timezone)
	assert.Equal(t, appConfigRevision(onDisk), revision)
	assert.NotEqual(t, oldRevision, revision)
	assert.True(t, m.reloadPending)
	require.Error(t, m.degradedError)
}

func TestManagerRollbackFailureWithInvalidDiskClearsRevision_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	_, oldRevision := m.AppConfigSnapshot()
	m.Subscribe(WatcherFunc(func(app *App) error {
		if app.Timezone == "Asia/Shanghai" {
			return errors.New("reject reload")
		}

		return nil
	}))
	m.atomicWrite = func(path string, _ []byte, mode os.FileMode) error {
		if err := os.WriteFile(path, []byte("[invalid"), mode); err != nil {
			return err
		}

		return errors.New("injected rollback failure")
	}

	err := m.SaveAppConfig(&App{Timezone: "Asia/Shanghai"}, &oldRevision)

	require.ErrorContains(t, err, "injected rollback failure")
	_, revision := m.AppConfigSnapshot()
	assert.Empty(t, revision)
	assert.True(t, m.reloadPending)
	require.Error(t, m.degradedError)
}

func TestManagerRepeatedExternalReloadFailureUsesLastAppliedBaseline_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	var firstSeen []string
	m.Subscribe(WatcherFunc(func(app *App) error {
		firstSeen = append(firstSeen, app.Timezone)

		return nil
	}))
	m.Subscribe(WatcherFunc(func(app *App) error {
		if app.Timezone == "Asia/Shanghai" {
			return errors.New("reject reload")
		}

		return nil
	}))
	external := []byte("timezone: Asia/Shanghai\n")
	require.NoError(t, os.WriteFile(m.Config().Path, external, 0o644))

	require.ErrorContains(t, m.tryReloadAppConfig(context.Background()), "reject reload")
	require.ErrorContains(t, m.tryReloadAppConfig(context.Background()), "reject reload")

	assert.Equal(t, []string{"Asia/Shanghai", "UTC", "Asia/Shanghai", "UTC"}, firstSeen)
	assert.Equal(t, "UTC", m.AppConfig().Timezone, "runtime config remains the last fully applied state")
	diskSnapshot, diskRevision := m.AppConfigSnapshot()
	assert.Equal(t, "Asia/Shanghai", diskSnapshot.Timezone, "API snapshot represents current disk state")
	assert.Equal(t, appConfigRevision(external), diskRevision)
}

func TestManagerCanRecoverFromRejectedExternalConfig_BitsUT(t *testing.T) {
	m := newTestManager(t, []byte("timezone: UTC\n"))
	m.Subscribe(WatcherFunc(func(app *App) error {
		if app.Timezone == "Asia/Shanghai" {
			return errors.New("reject external config")
		}

		return nil
	}))
	require.NoError(t, os.WriteFile(m.Config().Path, []byte("timezone: Asia/Shanghai\n"), 0o644))
	require.ErrorContains(t, m.tryReloadAppConfig(context.Background()), "reject external config")
	diskConfig, diskRevision := m.AppConfigSnapshot()
	assert.Equal(t, "Asia/Shanghai", diskConfig.Timezone)

	diskConfig.Timezone = "Europe/London"
	require.NoError(t, m.SaveAppConfig(diskConfig, &diskRevision))

	assert.Equal(t, "Europe/London", m.AppConfig().Timezone)
	snapshot, revision := m.AppConfigSnapshot()
	assert.Equal(t, "Europe/London", snapshot.Timezone)
	assert.NotEqual(t, diskRevision, revision)
	assert.False(t, m.reloadPending)
	assert.NoError(t, m.degradedError)
	onDisk, err := os.ReadFile(m.Config().Path)
	require.NoError(t, err)
	assert.Equal(t, revision, appConfigRevision(onDisk))
}

func newTestManager(t *testing.T, initial []byte) *manager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, initial, 0o644))

	got, err := new("test", &Config{Path: path}, Dependencies{})
	require.NoError(t, err)
	m, ok := got.(*manager)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, m.Close()) })

	return m
}

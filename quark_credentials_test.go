package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quarkCredentialFixture(t *testing.T) (*quarkManager, *quarkClient) {
	t.Helper()
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	m := newQuarkManager(g)
	g.quark = m
	c := quarkCredentials{UID: "credential-account", Cookies: map[string]string{"__pus": "old-cookie"}, AccessToken: "old-access", RefreshToken: "old-refresh", OpenVerified: true}
	if err := m.commitCredential(context.Background(), c, ""); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Close(ctx)
	})
	q, err := m.client(quarkAccountKey(c.UID))
	if err != nil {
		t.Fatal(err)
	}
	return m, q
}

func quarkSavedFixture(t *testing.T, m *quarkManager) quarkCredentials {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(m.app.data, "quark-accounts.bin"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := unprotectLocal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var saved quarkSavedAccounts
	if err := json.Unmarshal(plain, &saved); err != nil || len(saved.Accounts) != 1 {
		t.Fatalf("invalid saved account: %v, count %d", err, len(saved.Accounts))
	}
	return saved.Accounts[0]
}

func quarkCredentialWait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("credential operation did not finish")
		return nil
	}
}

func quarkCredentialSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("credential operation did not reach its synchronization point")
	}
}

func quarkCredentialReply(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestQuarkCredentialCommitPreservesConcurrentChannels(t *testing.T) {
	for _, loginChannel := range []quarkCredentialChannel{quarkCookieChannel, quarkOAuthChannel} {
		t.Run(map[quarkCredentialChannel]string{quarkCookieChannel: "cookie-login", quarkOAuthChannel: "oauth-login"}[loginChannel], func(t *testing.T) {
			m, q := quarkCredentialFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var writes atomic.Int64
			m.writeVault = func(path string, data []byte) error {
				if writes.Add(1) == 1 {
					close(entered)
					<-release
				}
				return writePrivateBytes(path, data)
			}
			login := quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "new-cookie"}}
			updateChannel := quarkOAuthChannel
			if loginChannel == quarkOAuthChannel {
				login.Cookies = nil
				login.AccessToken, login.RefreshToken, login.OpenVerified = "new-access", "new-refresh", true
				updateChannel = quarkCookieChannel
			}
			committed := make(chan error, 1)
			go func() { committed <- m.commitCredential(context.Background(), login, "") }()
			quarkCredentialSignal(t, entered)
			_, version := q.snapshotChannel(updateChannel)
			updated, applied := make(chan error, 1), make(chan struct{})
			go func() {
				updated <- q.updateCredential(context.Background(), updateChannel, version, func(c *quarkCredentials) bool {
					if updateChannel == quarkCookieChannel {
						c.Cookies["__pus"] = "new-cookie"
					} else {
						c.AccessToken, c.RefreshToken = "new-access", "new-refresh"
					}
					close(applied)
					return true
				})
			}()
			quarkCredentialSignal(t, applied)
			// Taking the state lock confirms the update finished publication,
			// even though persistence is queued behind the staged login write.
			if m.Status()["credential_save_pending"] != true {
				t.Fatal("concurrent update was not marked pending")
			}
			once.Do(func() { close(release) })
			for _, done := range []<-chan error{committed, updated} {
				if err := quarkCredentialWait(t, done); err != nil {
					t.Fatal(err)
				}
			}
			for _, c := range []quarkCredentials{q.snapshot(), quarkSavedFixture(t, m)} {
				if c.Cookies["__pus"] != "new-cookie" || c.AccessToken != "new-access" || c.RefreshToken != "new-refresh" {
					t.Fatal("concurrent channel update was lost")
				}
			}
		})
	}
}

func TestQuarkCredentialStaleResponsesCannotReplaceNewLogin(t *testing.T) {
	for _, channel := range []quarkCredentialChannel{quarkCookieChannel, quarkOAuthChannel} {
		t.Run(map[quarkCredentialChannel]string{quarkCookieChannel: "cookie", quarkOAuthChannel: "oauth"}[channel], func(t *testing.T) {
			m, q := quarkCredentialFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			q.metadata = &http.Client{Transport: protocolRoundTripper(func(*http.Request) (*http.Response, error) {
				close(entered)
				<-release
				response := quarkCredentialReply(`{"status":0,"data":{"access_token":"late-access","refresh_token":"late-refresh"}}`)
				if channel == quarkCookieChannel {
					response = quarkCredentialReply(`{"code":0,"data":{}}`)
					response.Header.Set("Set-Cookie", "__pus=late-cookie")
				}
				return response, nil
			})}
			done := make(chan error, 1)
			go func() {
				if channel == quarkOAuthChannel {
					done <- q.rotate(context.Background(), "old-access")
				} else {
					_, err := q.request(context.Background(), quarkWeb, "GET", "/resource", nil, nil, false)
					done <- err
				}
			}()
			quarkCredentialSignal(t, entered)
			login := quarkCredentials{UID: q.snapshot().UID, AccessToken: "login-access", RefreshToken: "login-refresh", OpenVerified: true}
			if channel == quarkCookieChannel {
				login.Cookies = map[string]string{"__pus": "login-cookie"}
			}
			if err := m.commitCredential(context.Background(), login, ""); err != nil {
				t.Fatal(err)
			}
			once.Do(func() { close(release) })
			if err := quarkCredentialWait(t, done); err != nil {
				t.Fatal(err)
			}
			for _, c := range []quarkCredentials{q.snapshot(), quarkSavedFixture(t, m)} {
				if channel == quarkCookieChannel && c.Cookies["__pus"] != "login-cookie" || channel == quarkOAuthChannel && c.RefreshToken != "login-refresh" {
					t.Fatal("late response overwrote a newer login")
				}
			}
		})
	}
}

func TestQuarkCredentialRemovedClientCannotUpdateReplacement(t *testing.T) {
	m, old := quarkCredentialFixture(t)
	key := quarkAccountKey(old.snapshot().UID)
	if err := m.removeAccount(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := m.commitCredential(context.Background(), quarkCredentials{UID: old.snapshot().UID, Cookies: map[string]string{"__pus": "replacement"}}, ""); err != nil {
		t.Fatal(err)
	}
	if err := old.ingest([]*http.Cookie{{Name: "__pus", Value: "late-old-client"}}); !errors.Is(err, context.Canceled) {
		t.Fatal("removed client was allowed to update credentials", err)
	}
	if got := quarkSavedFixture(t, m).Cookies["__pus"]; got != "replacement" || m.Status()["credential_save_pending"] != false {
		t.Fatal("removed incarnation changed the vault or scheduled a save")
	}
}

func TestQuarkCredentialSaveAcknowledgesOnlyCapturedRevision(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var firstOnce, secondOnce sync.Once
	t.Cleanup(func() {
		firstOnce.Do(func() { close(releaseFirst) })
		secondOnce.Do(func() { close(releaseSecond) })
	})
	var writes atomic.Int64
	m.writeVault = func(path string, data []byte) error {
		switch writes.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
		case 2:
			close(secondEntered)
			<-releaseSecond
		}
		return writePrivateBytes(path, data)
	}
	first, second, applied := make(chan error, 1), make(chan error, 1), make(chan struct{})
	go func() { first <- q.ingest([]*http.Cookie{{Name: "__pus", Value: "first"}}) }()
	quarkCredentialSignal(t, firstEntered)
	_, version := q.snapshotChannel(quarkCookieChannel)
	go func() {
		second <- q.updateCredential(context.Background(), quarkCookieChannel, version, func(c *quarkCredentials) bool {
			c.Cookies["__pus"] = "second"
			close(applied)
			return true
		})
	}()
	quarkCredentialSignal(t, applied)
	firstOnce.Do(func() { close(releaseFirst) })
	quarkCredentialSignal(t, secondEntered)
	if m.Status()["credential_save_pending"] != true {
		t.Fatal("older save cleared the newer pending revision")
	}
	secondOnce.Do(func() { close(releaseSecond) })
	for _, done := range []<-chan error{first, second} {
		if err := quarkCredentialWait(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if quarkSavedFixture(t, m).Cookies["__pus"] != "second" || m.Status()["credential_save_pending"] != false {
		t.Fatal("latest revision was not durably saved")
	}
}

func TestQuarkCredentialRefreshFailureIsVisibleAndAutomaticallyRetried(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	m.retryDelay = 10 * time.Millisecond
	storageErr := errors.New("transient vault failure")
	var writable atomic.Bool
	retried := make(chan struct{}, 1)
	m.writeVault = func(path string, data []byte) error {
		if !writable.Load() {
			return storageErr
		}
		err := writePrivateBytes(path, data)
		select {
		case retried <- struct{}{}:
		default:
		}
		return err
	}
	q.metadata = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		body := `{"status":0,"data":{"ok":true}}`
		if strings.HasSuffix(r.URL.Path, "/rotate") {
			body = `{"status":0,"data":{"access_token":"new-access","refresh_token":"new-refresh"}}`
		} else if r.URL.Query().Get("access_token") == "old-access" {
			body = `{"status":1,"errno":401,"error_info":"expired access token"}`
		}
		return quarkCredentialReply(body), nil
	})}
	_, err := q.call(context.Background(), quarkOpen, "GET", "/resource", nil, nil)
	if !errors.Is(err, storageErr) || q.snapshot().RefreshToken != "new-refresh" {
		t.Fatal("successful rotation or local persistence error was lost", err)
	}
	status := m.Status()
	if status["credential_save_pending"] != true || valueString(status["credential_save_error"]) == "" {
		t.Fatal("pending credential failure was hidden")
	}
	if _, err := q.call(context.Background(), quarkOpen, "GET", "/resource", nil, nil); err != nil {
		t.Fatal("new in-memory token stopped serving requests", err)
	}
	writable.Store(true)
	quarkCredentialSignal(t, retried)
	// Wait for the retry's acknowledgment, without racing its final state write.
	m.saveMu.Lock()
	m.saveMu.Unlock()
	if quarkSavedFixture(t, m).RefreshToken != "new-refresh" || m.Status()["credential_save_pending"] != false || m.Status()["credential_save_error"] != "" {
		t.Fatal("automatic retry did not durably recover the rotated token")
	}
}

func TestQuarkCredentialCloseFlushesPendingAndRejectsLateCallbacks(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	m.retryDelay = time.Hour
	var writable atomic.Bool
	m.writeVault = func(path string, data []byte) error {
		if !writable.Load() {
			return errors.New("temporarily read-only")
		}
		return writePrivateBytes(path, data)
	}
	if err := q.ingest([]*http.Cookie{{Name: "__pus", Value: "pending"}}); err == nil {
		t.Fatal("fixture did not fail persistence")
	}
	m.Stop()
	if m.Status()["credential_save_pending"] != true {
		t.Fatal("canceling login cleared pending credentials")
	}
	writable.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if quarkSavedFixture(t, m).Cookies["__pus"] != "pending" || m.Status()["credential_save_pending"] != false {
		t.Fatal("close failed to flush pending credentials")
	}
	if err := q.ingest([]*http.Cookie{{Name: "__pus", Value: "late"}}); !errors.Is(err, context.Canceled) {
		t.Fatal("closed manager accepted a late callback", err)
	}
	if err := m.Start("open", ""); !errors.Is(err, context.Canceled) {
		t.Fatal("closed manager restarted login", err)
	}
}

func TestQuarkCredentialCloseDeadlineBoundsBlockedWriter(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var writes atomic.Int64
	m.writeVault = func(path string, data []byte) error {
		if writes.Add(1) == 1 {
			close(entered)
			<-release
		}
		return writePrivateBytes(path, data)
	}
	done := make(chan error, 1)
	go func() { done <- q.ingest([]*http.Cookie{{Name: "__pus", Value: "new-cookie"}}) }()
	quarkCredentialSignal(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- m.Close(ctx) }()
	if err := quarkCredentialWait(t, closed); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked save did not respect shutdown deadline", err)
	}
	once.Do(func() { close(release) })
	if err := quarkCredentialWait(t, done); err != nil {
		t.Fatal(err)
	}
	quarkCredentialSignal(t, m.closeDone)
	if writes.Load() != 1 {
		t.Fatal("expired final flush started a late vault write")
	}
}

func TestQuarkCredentialCanceledSaveWaitKeepsPendingForRetry(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	m.retryDelay = 10 * time.Millisecond
	saved := make(chan struct{}, 1)
	m.writeVault = func(path string, data []byte) error {
		err := writePrivateBytes(path, data)
		select {
		case saved <- struct{}{}:
		default:
		}
		return err
	}
	m.saveMu.Lock()
	var unlock sync.Once
	t.Cleanup(func() { unlock.Do(m.saveMu.Unlock) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, version := q.snapshotChannel(quarkOAuthChannel)
	done, applied := make(chan error, 1), make(chan struct{})
	go func() {
		done <- q.updateCredential(ctx, quarkOAuthChannel, version, func(c *quarkCredentials) bool {
			c.AccessToken, c.RefreshToken = "new-access", "new-refresh"
			close(applied)
			return true
		})
	}()
	quarkCredentialSignal(t, applied)
	if m.Status()["credential_save_pending"] != true {
		t.Fatal("upstream update was not pending before persistence")
	}
	cancel()
	if err := quarkCredentialWait(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal("credential persistence ignored request cancellation", err)
	}
	if q.snapshot().RefreshToken != "new-refresh" || m.Status()["credential_save_pending"] != true {
		t.Fatal("canceling persistence discarded an already rotated token")
	}
	unlock.Do(m.saveMu.Unlock)
	quarkCredentialSignal(t, saved)
	m.saveMu.Lock()
	m.saveMu.Unlock()
	if quarkSavedFixture(t, m).RefreshToken != "new-refresh" || m.Status()["credential_save_pending"] != false {
		t.Fatal("canceling the caller stranded its pending revision")
	}
}

func TestQuarkCredentialCloseWithoutChangesDoesNotCreateVault(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	m := newQuarkManager(g)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(g.data, "quark-accounts.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("closing an untouched manager created a vault", err)
	}
}

func TestQuarkCredentialRefreshNetworkAndLockWaitAreCancelable(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	entered := make(chan struct{})
	q.metadata = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rotating := make(chan error, 1)
	go func() { rotating <- q.rotate(ctx, "old-access") }()
	quarkCredentialSignal(t, entered)
	waiting, stopWaiting := context.WithCancel(context.Background())
	stopWaiting()
	if err := q.rotate(waiting, "old-access"); !errors.Is(err, context.Canceled) {
		t.Fatal("refresh lock ignored cancellation", err)
	}
	// A network refresh must not hold credential state or disk locks.
	commitCtx, stopCommit := context.WithTimeout(context.Background(), time.Second)
	defer stopCommit()
	if err := m.commitCredential(commitCtx, quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "new-cookie"}}, ""); err != nil {
		t.Fatal("refresh network blocked the other channel", err)
	}
	cancel()
	if err := quarkCredentialWait(t, rotating); !errors.Is(err, context.Canceled) {
		t.Fatal("refresh network ignored cancellation", err)
	}
	if q.snapshot().RefreshToken != "old-refresh" || q.snapshot().Cookies["__pus"] != "new-cookie" {
		t.Fatal("canceled network refresh changed credentials")
	}
}

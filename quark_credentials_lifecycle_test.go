package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuarkCredentialStagedLoginSupersedesPreviousSessionUpdates(t *testing.T) {
	for _, channel := range []quarkCredentialChannel{quarkCookieChannel, quarkOAuthChannel} {
		for _, failWrite := range []bool{false, true} {
			name := map[quarkCredentialChannel]string{quarkCookieChannel: "cookie", quarkOAuthChannel: "oauth"}[channel]
			if failWrite {
				name += "-failed-login-save"
			}
			t.Run(name, func(t *testing.T) {
				m, q := quarkCredentialFixture(t)
				m.retryDelay = time.Hour
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				t.Cleanup(func() { once.Do(func() { close(release) }) })
				storageErr := errors.New("first login write failed")
				var writes atomic.Int64
				m.writeVault = func(path string, data []byte) error {
					if writes.Add(1) == 1 {
						close(entered)
						<-release
						if failWrite {
							return storageErr
						}
					}
					return writePrivateBytes(path, data)
				}
				login := quarkCredentials{UID: q.snapshot().UID, AccessToken: "login-access", RefreshToken: "login-refresh", OpenVerified: true}
				if channel == quarkCookieChannel {
					login.Cookies = map[string]string{"__pus": "login-cookie"}
				}
				_, oldVersion := q.snapshotChannel(channel)
				committed, updated := make(chan error, 1), make(chan error, 1)
				go func() { committed <- m.commitCredential(context.Background(), login, "") }()
				quarkCredentialSignal(t, entered)
				applied := make(chan struct{})
				go func() {
					updated <- q.updateCredential(context.Background(), channel, oldVersion, func(c *quarkCredentials) bool {
						if channel == quarkCookieChannel {
							c.Cookies["__pus"] = "previous-session-cookie"
						} else {
							c.AccessToken, c.RefreshToken = "previous-session-access", "previous-session-refresh"
						}
						close(applied)
						return true
					})
				}()
				quarkCredentialSignal(t, applied)
				_, beforePublication := q.snapshotChannel(channel)
				once.Do(func() { close(release) })
				if err := quarkCredentialWait(t, committed); failWrite && !errors.Is(err, storageErr) || !failWrite && err != nil {
					t.Fatal("unexpected login result", err)
				}
				if err := quarkCredentialWait(t, updated); err != nil {
					t.Fatal(err)
				}
				for _, c := range []quarkCredentials{q.snapshot(), quarkSavedFixture(t, m)} {
					expectedCookie, expectedRefresh := "login-cookie", "login-refresh"
					if failWrite {
						expectedCookie, expectedRefresh = "previous-session-cookie", "previous-session-refresh"
					}
					if channel == quarkCookieChannel && c.Cookies["__pus"] != expectedCookie || channel == quarkOAuthChannel && c.RefreshToken != expectedRefresh {
						t.Fatal("staged login published the wrong session")
					}
				}
				if !failWrite {
					changed := false
					if err := q.updateCredential(context.Background(), channel, beforePublication, func(*quarkCredentials) bool {
						changed = true
						return true
					}); err != nil || changed {
						t.Fatal("new login retained the old session's response version", err)
					}
				}
			})
		}
	}
}

func TestQuarkCredentialLifecycleLockWaitsRespectRequestCancellation(t *testing.T) {
	for _, operation := range []string{"commit", "remove"} {
		for _, heldLock := range []string{"commit", "save"} {
			t.Run(operation+"-waiting-for-"+heldLock, func(t *testing.T) {
				m, q := quarkCredentialFixture(t)
				lock := &m.commitMu
				if heldLock == "save" {
					lock = &m.saveMu
				}
				lock.Lock()
				var unlock sync.Once
				t.Cleanup(func() { unlock.Do(lock.Unlock) })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started, done := make(chan struct{}), make(chan error, 1)
				go func() {
					close(started)
					if operation == "commit" {
						done <- m.commitCredential(ctx, quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "canceled"}}, "")
					} else {
						done <- m.removeAccount(ctx, quarkAccountKey(q.snapshot().UID))
					}
				}()
				quarkCredentialSignal(t, started)
				cancel()
				if err := quarkCredentialWait(t, done); !errors.Is(err, context.Canceled) {
					t.Fatal("canceled lifecycle operation stayed in the lock queue", err)
				}
				unlock.Do(lock.Unlock)
				if q.snapshot().Cookies["__pus"] != "old-cookie" || m.Status()["credential_save_pending"] != false {
					t.Fatal("canceled lifecycle operation changed credentials")
				}
			})
		}
	}
}

func TestQuarkCredentialStopInvalidatesLoginDuringStagedWrite(t *testing.T) {
	for _, cancelContext := range []bool{false, true} {
		name := "generation-only"
		if cancelContext {
			name = "cancel-context"
		}
		t.Run(name, func(t *testing.T) {
			m, q := quarkCredentialFixture(t)
			m.retryDelay = time.Hour
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m.mu.Lock()
			m.generation++
			ctx = context.WithValue(ctx, quarkLoginGenerationKey{}, m.generation)
			m.flow = &quarkLoginFlow{stage: "saving"}
			if cancelContext {
				m.cancel = cancel
			}
			m.mu.Unlock()
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
			go func() {
				done <- m.commitCredential(ctx, quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "canceled-login"}}, "")
			}()
			quarkCredentialSignal(t, entered)
			stopped := make(chan struct{})
			go func() {
				m.Stop()
				close(stopped)
			}()
			quarkCredentialSignal(t, stopped)
			if cancelContext && !errors.Is(ctx.Err(), context.Canceled) || m.Status()["stage"] != "登录已取消" {
				t.Fatal("Stop waited for disk before canceling the login")
			}
			once.Do(func() { close(release) })
			if err := quarkCredentialWait(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled generation published its staged login", err)
			}
			if err := m.flushCredentials(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			if q.snapshot().Cookies["__pus"] != "old-cookie" || quarkSavedFixture(t, m).Cookies["__pus"] != "old-cookie" || m.Status()["completed"] != false {
				t.Fatal("canceled login survived in memory or the vault")
			}
		})
	}
}

func TestQuarkCredentialCloseCancelsQueuedLifecycleOperations(t *testing.T) {
	for _, operation := range []string{"commit", "remove"} {
		t.Run(operation, func(t *testing.T) {
			m, q := quarkCredentialFixture(t)
			m.commitMu.Lock()
			var unlock sync.Once
			t.Cleanup(func() { unlock.Do(m.commitMu.Unlock) })
			started, done := make(chan struct{}), make(chan error, 1)
			go func() {
				close(started)
				if operation == "commit" {
					done <- m.commitCredential(context.Background(), quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "late"}}, "")
				} else {
					done <- m.removeAccount(context.Background(), quarkAccountKey(q.snapshot().UID))
				}
			}()
			quarkCredentialSignal(t, started)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := m.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err := quarkCredentialWait(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal("close left a lifecycle operation queued", err)
			}
			unlock.Do(m.commitMu.Unlock)
		})
	}
}

func TestQuarkCredentialCompletedLoginStatusRecoversWithItsSavedRevision(t *testing.T) {
	m, q := quarkCredentialFixture(t)
	m.retryDelay = 10 * time.Millisecond
	m.mu.Lock()
	m.generation++
	ctx := context.WithValue(context.Background(), quarkLoginGenerationKey{}, m.generation)
	m.flow = &quarkLoginFlow{stage: "saving"}
	m.mu.Unlock()
	entered, release, retried := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var writes atomic.Int64
	var writable atomic.Bool
	storageErr := errors.New("follow-up vault save failed")
	m.writeVault = func(path string, data []byte) error {
		if writes.Add(1) == 1 {
			close(entered)
			<-release
			return writePrivateBytes(path, data)
		}
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
	committed, updated := make(chan error, 1), make(chan error, 1)
	go func() {
		committed <- m.commitCredential(ctx, quarkCredentials{UID: q.snapshot().UID, Cookies: map[string]string{"__pus": "login-cookie"}}, "")
	}()
	quarkCredentialSignal(t, entered)
	_, version := q.snapshotChannel(quarkOAuthChannel)
	applied := make(chan struct{})
	go func() {
		updated <- q.updateCredential(context.Background(), quarkOAuthChannel, version, func(c *quarkCredentials) bool {
			c.AccessToken, c.RefreshToken = "new-access", "new-refresh"
			close(applied)
			return true
		})
	}()
	quarkCredentialSignal(t, applied)
	if m.Status()["credential_save_pending"] != true {
		t.Fatal("concurrent update did not become pending")
	}
	once.Do(func() { close(release) })
	for _, done := range []<-chan error{committed, updated} {
		if err := quarkCredentialWait(t, done); !errors.Is(err, storageErr) {
			t.Fatal("fixture did not expose follow-up save failure", err)
		}
	}
	status := m.Status()
	if status["completed"] != true || status["stage"] != "夸克登录成功，新凭据等待保存" || valueString(status["error"]) == "" {
		t.Fatal("completed login hid its pending revision", status)
	}
	writable.Store(true)
	quarkCredentialSignal(t, retried)
	m.saveMu.Lock()
	m.saveMu.Unlock()
	status = m.Status()
	if status["stage"] != "夸克登录成功，授权已保存" || status["error"] != "" || status["credential_save_pending"] != false {
		t.Fatal("successful retry retained the completed login's old warning", status)
	}
	// A later request has a different pending revision; it must not turn the
	// already durable login back into a failed or unsaved login.
	writable.Store(false)
	if err := q.ingest([]*http.Cookie{{Name: "__pus", Value: "later-cookie"}}); !errors.Is(err, storageErr) {
		t.Fatal("fixture did not produce a later pending revision", err)
	}
	status = m.Status()
	if status["stage"] != "夸克登录成功，授权已保存" || status["error"] != "" || status["credential_save_pending"] != true || status["credential_save_error"] == "" {
		t.Fatal("later credential error replaced the completed login status", status)
	}
	writable.Store(true)
}

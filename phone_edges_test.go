package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
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

func TestPhoneRepairSlowProgressKeepsUploadAttemptAlive(t *testing.T) {
	_, drive := newQuarkFixture(t)
	drive.uploadIdle = 40 * time.Millisecond
	var calls atomic.Int64
	drive.bodyHTTP = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		for i := 0; i < 12; i++ {
			if err := davWait(r.Context(), 10*time.Millisecond); err != nil {
				return nil, err
			}
			data := make([]byte, 1)
			if _, err := io.ReadFull(r.Body, data); err != nil {
				return nil, err
			}
		}
		resp := phoneRepairResponse(r, io.NopCloser(strings.NewReader("")))
		resp.Header.Set("ETag", `"ok"`)
		return resp, nil
	})}
	upload := &quarkUpload{drive: drive, route: quarkWeb, task: "task", uploadID: "up", bucket: "b", key: "k", base: "http://127.0.0.1/part", mime: "application/octet-stream"}
	var sent, confirmed atomic.Int64
	if _, err := upload.part(context.Background(), strings.NewReader("twelve-bytes"), 1, 1, 0, 12, &sent, &confirmed); err != nil {
		t.Fatal("steady progress was restarted", err)
	}
	if calls.Load() != 1 || confirmed.Load() != 12 {
		t.Fatal("steady progress replayed")
	}
}
func TestPhoneRepairIdleUploadRetriesWithoutWaitingForAbsoluteDeadline(t *testing.T) {
	_, drive := newQuarkFixture(t)
	drive.uploadIdle = 20 * time.Millisecond
	observed := make(chan struct{})
	drive.bodyHTTP = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		close(observed)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	stop := errors.New("retry boundary observed")
	drive.retryWait = func(context.Context, time.Duration) error { return stop }
	upload := &quarkUpload{drive: drive, route: quarkWeb, task: "task", uploadID: "up", bucket: "b", key: "k", base: "http://127.0.0.1/part", mime: "application/octet-stream"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sent, confirmed atomic.Int64
	start := time.Now()
	_, err := upload.part(ctx, strings.NewReader("body"), 1, 1, 0, 4, &sent, &confirmed)
	if !errors.Is(err, stop) || time.Since(start) > 500*time.Millisecond {
		t.Fatal("idle upload did not reach retry boundary", err)
	}
	<-observed
}
func TestPhoneRepairOlderGenerationCannotReplaceCurrentLogin(t *testing.T) {
	app := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	manager := newQuarkManager(app)
	app.quark = manager
	manager.generation = 4
	ctx := context.WithValue(context.Background(), quarkLoginGenerationKey{}, uint64(3))
	err := manager.commitCredential(ctx, quarkCredentials{UID: "stale", Cookies: map[string]string{"__pus": "stale"}}, "")
	if !errors.Is(err, context.Canceled) || manager.loggedClients() {
		t.Fatal("older generation became current", err)
	}
}
func TestPhoneRepairVaultFailureCannotPublishPendingCredentials(t *testing.T) {
	app := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	manager := newQuarkManager(app)
	app.quark = manager
	manager.addClient(quarkCredentials{UID: "existing", Cookies: map[string]string{"__pus": "current"}})
	manager.writeVault = func(string, []byte) error { return errors.New("injected disk failure") }
	if err := manager.commitCredential(context.Background(), quarkCredentials{UID: "existing", Cookies: map[string]string{"__pus": "pending"}}, ""); err == nil {
		t.Fatal("disk failure ignored")
	}
	client, _ := manager.client(quarkAccountKey("existing"))
	if client.snapshot().Cookies["__pus"] != "current" {
		t.Fatal("unpersisted credential became visible")
	}
}
func TestPhoneRepairPhoneCookiesAreScopedAndNotChallenges(t *testing.T) {
	now := time.Now()
	cookies := []quarkBrowserCookie{
		{Name: "__pus", Value: "evil", Domain: "pan.quark.cn.evil"},
		{Name: "__pus", Value: "expired", Domain: ".quark.cn", Expires: float64(now.Add(-time.Minute).Unix())},
		{Name: "__pus", Value: "shared", Domain: ".quark.cn", Expires: -1},
		{Name: "__pus", Value: "host", Domain: "pan.quark.cn", Expires: -1},
		{Name: "challenge", Value: "token", Domain: "uop.quark.cn", Expires: -1},
		{Name: "unrelated", Value: "secret", Domain: "example.org", Expires: -1},
	}
	got := quarkPhoneCookies(cookies, now)
	if got["__pus"] != "shared" || got["unrelated"] != "" {
		t.Fatal("phone window cookie boundary failed")
	}
	if got = quarkPhoneCookies([]quarkBrowserCookie{{Name: "challenge", Value: "token", Domain: ".quark.cn"}}, now); got != nil {
		t.Fatal("challenge-only cookies treated as a login")
	}
}

type phoneFakeWindow struct {
	closed chan struct{}
	once   sync.Once
	poll   func(context.Context) (quarkPhoneSnapshot, error)
}

func (w *phoneFakeWindow) Poll(ctx context.Context) (quarkPhoneSnapshot, error) { return w.poll(ctx) }
func (w *phoneFakeWindow) Close()                                               { w.once.Do(func() { close(w.closed) }) }
func phoneTestManager(t *testing.T) *quarkManager {
	t.Helper()
	app := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	manager := newQuarkManager(app)
	app.quark = manager
	manager.validateClient = func(c quarkCredentials) *quarkClient {
		q := newQuarkClient(c, nil)
		q.metadata = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
			return phoneRepairResponse(r, io.NopCloser(strings.NewReader(`{"success":true,"code":"OK","data":{"uid":"phone-user","nickname":"fixture"}}`))), nil
		})}
		return q
	}
	t.Cleanup(manager.Stop)
	return manager
}
func TestPhoneRepairPhoneWindowImportsOnlyVerifiedSession(t *testing.T) {
	manager := phoneTestManager(t)
	window := &phoneFakeWindow{closed: make(chan struct{}), poll: func(context.Context) (quarkPhoneSnapshot, error) {
		return quarkPhoneSnapshot{Cookies: map[string]string{"__pus": "private-phone-session"}, MobileReady: true}, nil
	}}
	manager.phoneWindow = func(context.Context, string) (quarkPhoneWindow, error) { return window, nil }
	if err := manager.Start("phone", ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-window.closed:
	case <-time.After(time.Second):
		t.Fatal("phone login did not finish")
	}
	if !manager.loggedClients() {
		t.Fatal("verified phone account missing")
	}
	if _, err := os.Stat(filepath.Join(manager.app.data, "quark-accounts.bin")); err != nil {
		t.Fatal("phone session not protected", err)
	}
	status := manager.Status()
	if status["image"] != "" || status["authorize_url"] != "" {
		t.Fatal("completed phone flow retained QR or URL")
	}
}
func TestPhoneRepairCancelClosesOnlyOwnedWindow(t *testing.T) {
	manager := phoneTestManager(t)
	opened := make(chan struct{})
	window := &phoneFakeWindow{closed: make(chan struct{}), poll: func(ctx context.Context) (quarkPhoneSnapshot, error) {
		close(opened)
		<-ctx.Done()
		return quarkPhoneSnapshot{}, ctx.Err()
	}}
	manager.phoneWindow = func(context.Context, string) (quarkPhoneWindow, error) { return window, nil }
	if err := manager.Start("phone", ""); err != nil {
		t.Fatal(err)
	}
	<-opened
	manager.Stop()
	select {
	case <-window.closed:
	case <-time.After(time.Second):
		t.Fatal("cancel left phone window running")
	}
	if manager.loggedClients() {
		t.Fatal("cancelled phone login imported credentials")
	}
}
func TestPhoneRepairUncertainUploadCanVerifyMD5ButCannotIgnoreMismatch(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching MD5", true: "mismatched MD5"}[changed], func(t *testing.T) {
			fixture, drive := newQuarkFixture(t)
			fixture.loseCommit = true
			original := fixture.server.Config.Handler
			fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/file/sort") {
					fixture.mu.Lock()
					rows := []any{}
					for _, file := range fixture.files {
						if file.Parent == r.URL.Query().Get("pdir_fid") {
							row := fixture.row(file, false)
							delete(row, "sha1")
							content := file.Content
							if changed {
								content = bytes.Repeat([]byte("x"), len(content))
							}
							h := md5.Sum(content)
							row["md5"] = hex.EncodeToString(h[:])
							rows = append(rows, row)
						}
					}
					fixture.mu.Unlock()
					respondJSON(w, 200, map[string]any{"code": 0, "status": 200, "data": map[string]any{"list": rows}, "metadata": map[string]any{"_total": len(rows)}})
					return
				}
				original.ServeHTTP(w, r)
			})
			file, err := os.CreateTemp(t.TempDir(), "uncertain-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			file.Write([]byte("actual content"))
			entry, err := drive.Upload(context.Background(), "0", "CloudProbe-upload-md5.txt", file)
			if changed {
				if err == nil {
					t.Fatal("different content accepted after lost completion")
				}
			} else if err != nil || entry.ID == "" {
				t.Fatal("verified MD5 was lost", err)
			}
			if fixture.apiCalls["/web/file/upload/pre"] != 0 {
				t.Fatal("uncertain upload replayed on another route")
			}
		})
	}
}

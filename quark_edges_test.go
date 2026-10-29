package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuarkNativeGUIAndDAVWorkWithoutOriginalEngine(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.add("0", "native.txt", []byte("independent"), false)
	key := quarkAccountKey("account")
	app := &guiApp{data: t.TempDir(), host: "127.0.0.1:12345", nonce: "test-ui", accounts: map[string]guiAccount{}, downloads: map[string]guiDownload{}}
	app.quark = &quarkManager{app: app, clients: map[string]*quarkClient{key: drive.client}, deviceID: "fixture", deviceName: "fixture"}
	app.quark.publishAccounts()
	app.dav = newDAVManager(app)
	defer app.dav.Stop()
	call := func(path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(body)
		r := httptest.NewRequest("POST", "http://"+app.host+path, bytes.NewReader(b))
		r.Header.Set("X-CloudProbe-UI", app.nonce)
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		return w
	}
	response := call("/api/list", map[string]any{"account": key, "directory": "0"})
	if response.Code != 200 || !strings.Contains(response.Body.String(), "native.txt") {
		t.Fatalf("native GUI required engine: %d %s", response.Code, response.Body.String())
	}
	status := app.publicStatus()
	if status["engine_ready"] != false || status["logged_in"] != false || status["has_accounts"] != true {
		t.Fatal("native readiness conflated with original login")
	}
	port, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	number := port.Addr().(*net.TCPAddr).Port
	port.Close()
	settings := map[string]any{"listen": "127.0.0.1", "port": number, "username": "tester", "password": "test-password-123", "upload_storage_mode": "memory", "mounts": []any{map[string]any{"account": key, "alias": "Quark", "root": "0", "writable": true, "upload_storage_mode": "disk"}}}
	response = call("/api/webdav/start", settings)
	if response.Code != 200 {
		t.Fatalf("native DAV required engine: %d %s", response.Code, response.Body.String())
	}
	content := wantDAV(t, "http://127.0.0.1:"+fmtInt(number), "GET", "/Quark/native.txt", "", 200, nil)
	if string(content) != "independent" {
		t.Fatal("native DAV content changed")
	}
	app.dav.Stop()
	restored := newDAVManager(app)
	if restored.config.UploadStorageMode != "memory" || len(restored.config.Mounts) != 1 || restored.config.Mounts[0].UploadStorageMode != "disk" {
		t.Fatal("per-mount cache choice did not survive save/restore")
	}
}
func fmtInt(n int) string { return strconv.Itoa(n) }

func TestQuarkAccountVaultIdentityAndPublicStatus(t *testing.T) {
	uid := "account-a"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{"code": "OK", "success": true, "data": map[string]any{"uid": uid, "nickname": "fixture"}})
	}))
	defer api.Close()
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}, downloads: map[string]guiDownload{}}
	manager := newQuarkManager(g)
	g.quark = manager
	manager.validateClient = func(c quarkCredentials) *quarkClient {
		q := newQuarkClient(c, nil)
		q.pan = api.URL
		q.metadata = api.Client()
		return q
	}
	secret := "private-cookie-not-public"
	if err := manager.accept(context.Background(), quarkCredentials{Cookies: map[string]string{"__pus": secret}}, ""); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(g.data, "quark-accounts.bin"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := unprotectLocal(stored)
	if err != nil || !bytes.Contains(plain, []byte(secret)) {
		t.Fatal("vault did not persist session", err)
	}
	encoded, _ := json.Marshal(g.publicStatus())
	if bytes.Contains(encoded, []byte(secret)) || bytes.Contains(encoded, []byte("access_token")) {
		t.Fatal("public status exposed credentials")
	}
	restored := newQuarkManager(g)
	if !restored.loggedClients() {
		t.Fatal("vault did not restore independent account")
	}
	key := quarkAccountKey(uid)
	uid = "different-account"
	if err := manager.accept(context.Background(), quarkCredentials{Cookies: map[string]string{"__pus": "another-cookie"}}, key); err == nil {
		t.Fatal("credentials merged using the selected account rather than verified identity")
	}
	if len(manager.clients) != 1 {
		t.Fatal("failed identity verification added an account")
	}
}
func TestQuarkMetadataRedirectDoesNotLeakCookiesOrToken(t *testing.T) {
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "" || r.URL.Query().Get("access_token") != "" {
			leaked.Store(true)
		}
		respondJSON(w, 200, map[string]any{"code": 0, "status": 200, "data": map[string]any{}})
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer source.Close()
	q := newQuarkClient(quarkCredentials{Cookies: map[string]string{"__pus": "private-cookie"}, AccessToken: "private-token"}, nil)
	q.endpoints[quarkDesktop] = source.URL
	q.endpoints[quarkOpen] = source.URL
	for _, route := range []quarkRoute{quarkDesktop, quarkOpen} {
		_, err := q.call(context.Background(), route, "GET", "/endpoint", nil, nil)
		if err == nil {
			t.Fatal("API redirect was followed")
		}
		if strings.Contains(err.Error(), "private-token") {
			t.Fatal("transport error exposed access token")
		}
	}
	if leaked.Load() {
		t.Fatal("cross-origin redirect received session")
	}
	if q.validBodyURL("https://untrusted.example/file") == nil {
		t.Fatal("untrusted CDN accepted")
	}
}
func TestQuarkPartialMoveCannotReplayAcrossRoutes(t *testing.T) {
	f, d := newQuarkFixture(t)
	item := f.add("0", "before.txt", []byte("body"), false)
	it := davEntry{Item: Item{ID: item.ID, Name: item.Name}, Parent: "0"}
	original := f.server.Config.Handler
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/file/move") {
			respondJSON(w, 500, map[string]any{"code": 500, "message": "unknown execution state"})
			return
		}
		original.ServeHTTP(w, r)
	})
	err := d.Move(context.Background(), it, "another-folder", "after.txt")
	var partial *davMutationError
	if !errors.As(err, &partial) || !partial.Partial {
		t.Fatal("partial move classified as safely replayable", err)
	}
	if f.apiCalls["/web/file/rename"] != 0 || f.apiCalls["/open/open/v1/file/rename"] != 0 {
		t.Fatal("partial rename was replayed on backup route")
	}
}
func TestQuarkTaskUnknownStatusIsNotSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{"status": 200, "code": 0, "data": map[string]any{"status": 9}})
	}))
	defer s.Close()
	q := newQuarkClient(quarkCredentials{Cookies: map[string]string{"__pus": "fixture"}}, nil)
	q.endpoints[quarkDesktop] = s.URL
	q.metadata = s.Client()
	d := newQuarkDrive(q, nil, 4, "auto")
	d.retryWait = func(context.Context, time.Duration) error { return nil }
	if err := d.task(context.Background(), quarkDesktop, map[string]any{"task_id": "task"}); err == nil {
		t.Fatal("unknown task status interpreted as successful mutation")
	}
}
func TestQuarkMalformedFileHashCannotAuthorizeMixedReads(t *testing.T) {
	it, err := parseQuarkEntry(map[string]any{"fid": "file", "file_name": "text.txt", "size": json.Number("10"), "dir": false, "sha1": "not-a-hash"}, "0")
	if err != nil || it.ContentVersion != "" {
		t.Fatal("unvalidated hash used as content version", err)
	}
}

func TestQuarkPartRetriesHaveNoFixedAttemptLimit(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.partFailureLimit = 7
	payload := bytes.Repeat([]byte("retry-data"), 5000)
	file, err := os.CreateTemp(t.TempDir(), "retry-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	file.Write(payload)
	entry, err := drive.Upload(context.Background(), "0", "retry.txt", file)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	attempts := fixture.partCalls[1]
	saved := fixture.files[entry.ID]
	fixture.mu.Unlock()
	if attempts != 8 || !bytes.Equal(saved.Content, payload) {
		t.Fatal("part retry limit or body corruption", attempts)
	}
}
func TestQuarkCancellationStopsRetriesAndHashing(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.partFailureLimit = 1000000
	file, err := os.CreateTemp(t.TempDir(), "cancel-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	file.Write(bytes.Repeat([]byte("a"), 65536))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = drive.Upload(ctx, "0", "cancel.txt", file)
	if err == nil || ctx.Err() == nil {
		t.Fatal("cancelled retry loop reported success")
	}
	already, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	reader := quarkHashReader{ctx: already, reader: strings.NewReader("body")}
	if _, err = io.Copy(io.Discard, reader); !errors.Is(err, context.Canceled) {
		t.Fatal("hashing ignored cancellation", err)
	}
}
func TestQuarkConcurrentExpiredRequestsRefreshOnce(t *testing.T) {
	var stale, refreshes atomic.Int64
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/rotate") {
			refreshes.Add(1)
			respondJSON(w, 200, map[string]any{"status": 0, "data": map[string]any{"access_token": "fresh-token", "refresh_token": "fresh-refresh"}})
			return
		}
		if r.URL.Query().Get("access_token") == "old-token" {
			if stale.Add(1) == 6 {
				close(gate)
			}
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
			respondJSON(w, 200, map[string]any{"status": 1, "errno": 401, "error_info": "expired"})
			return
		}
		respondJSON(w, 200, map[string]any{"status": 0, "data": map[string]any{"ok": true}})
	}))
	defer server.Close()
	q := newQuarkClient(quarkCredentials{AccessToken: "old-token", RefreshToken: "old-refresh", OpenVerified: true}, nil)
	q.endpoints[quarkOpen] = server.URL
	q.metadata = server.Client()
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 6; i++ {
		wg.Go(func() { _, err := q.call(ctx, quarkOpen, "GET", "/resource", nil, nil); errs <- err })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if refreshes.Load() != 1 {
		t.Fatal("stale concurrent requests rotated refresh token repeatedly", refreshes.Load())
	}
}
func TestQuarkRepeatedReadFailureUsesCooledRouteLast(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.desktopFailure = true
	fixture.add("0", "text.txt", []byte("body"), false)
	for i := 0; i < 4; i++ {
		if _, err := drive.List(context.Background(), "0"); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.apiCalls["/desktop/file/sort"] != 2 || fixture.apiCalls["/web/file/sort"] != 4 {
		t.Fatal("failed route kept delaying reads", fixture.apiCalls)
	}
}

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReleaseOpenListHTTPFallbackPreservesSourceAndDestination(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	root := "/联通云盘-1/"
	wantDAV(t, server.URL, "PUT", root+"source.txt", "new source", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"target.txt", "old target", 201, nil)
	api, writes := newOpenListFixture(t, base)
	primary := &failingBackupPrimary{fakeDAVDrive: base, failList: true}
	fs.mounts[0].drive = &backupDAVDrive{primary: primary, backup: api, root: "0", paths: map[string]string{"0": ""}, entries: map[string]davEntry{}}
	fs.invalidate(fs.mounts[0])
	for _, method := range []string{"PUT", "MOVE", "COPY", "DELETE", "MKCOL", "LOCK"} {
		t.Run(method, func(t *testing.T) {
			target := "source.txt"
			if method == "MKCOL" || method == "LOCK" {
				target = "new-resource"
			}
			code, _, _ := davRequest(t, server.URL, method, root+target, strings.NewReader("replacement"), map[string]string{"Destination": "/dav" + root + "target.txt", "Overwrite": "T"})
			if code != http.StatusServiceUnavailable && code != http.StatusForbidden {
				t.Fatalf("unsafe fallback request returned %d", code)
			}
			for name, want := range map[string]string{"source.txt": "new source", "target.txt": "old target"} {
				got := wantDAV(t, server.URL, "GET", root+name, "", 200, nil)
				if string(got) != want {
					t.Fatalf("%s changed after rejected %s: %q", name, method, got)
				}
			}
		})
	}
	if entries, _ := base.List(context.Background(), "0"); len(entries) != 2 || writes.Load() != 0 || primary.uploads.Load() != 0 {
		t.Fatal("rejected request changed cloud state", entries, writes.Load(), primary.uploads.Load())
	}
}

func TestReleaseOpenListNativeOverwriteKeepsNewTarget(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	api, writes := newOpenListFixture(t, base)
	fs.mounts[0].drive = &backupDAVDrive{primary: base, backup: api, root: "0", paths: map[string]string{"0": ""}, entries: map[string]davEntry{}}
	root := "/联通云盘-1/"
	wantDAV(t, server.URL, "PUT", root+"source.txt", "new source", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"target.txt", "old target", 201, nil)
	wantDAV(t, server.URL, "MOVE", root+"source.txt", "", 204, map[string]string{"Destination": "/dav" + root + "target.txt", "Overwrite": "T"})
	if got := wantDAV(t, server.URL, "GET", root+"target.txt", "", 200, nil); string(got) != "new source" {
		t.Fatal("committed target was lost", string(got))
	}
	entries, _ := base.List(context.Background(), "0")
	if len(entries) != 1 || entries[0].Name != "target.txt" || writes.Load() != 0 {
		t.Fatal("wrong object cleaned up", entries, writes.Load())
	}
	pathEntry := davEntry{Item: Item{ID: "openlist:target.txt"}}
	if sameCloudObject(pathEntry, pathEntry) {
		t.Fatal("a location was accepted as stable object identity")
	}
}

func TestReleaseCredentialCorruptionPreservedAndPendingSaveRetried(t *testing.T) {
	for _, content := range []string{"{broken", "null", "[]"} {
		t.Run(content, func(t *testing.T) {
			a := guiAccount{Key: "a", Provider: "wopan", credentials: map[string]any{"user_id": "account-a", "token": "old-a"}}
			b := guiAccount{Key: "b", Provider: "wopan", credentials: map[string]any{"user_id": "account-b", "token": "old-b"}}
			g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": a, "b": b}}
			t.Cleanup(func() {
				g.mu.Lock()
				if g.credentialRetry != nil {
					g.credentialRetry.Stop()
				}
				g.mu.Unlock()
			})
			file := filepath.Join(g.data, "webdav-credentials.bin")
			original, err := protectLocal([]byte(content))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(file, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err = g.saveDAVCredential(a, "new-a", "new-refresh-a"); err == nil {
				t.Fatal("corrupt store overwritten without an error")
			}
			if err = g.saveDAVWebCredential(a, "new-web-a"); err == nil {
				t.Fatal("website credential overwrote corrupt store")
			}
			got, _ := os.ReadFile(file)
			if !bytes.Equal(got, original) || len(g.pendingDAVCredentials) != 1 || g.credentialSaveError == "" {
				t.Fatal("original or explicit pending-save state lost")
			}
			if g.accounts["a"].credentials["token"] != "new-a" || g.accounts["b"].credentials["token"] != "old-b" {
				t.Fatal("rotated token lost or unrelated account changed")
			}
			refreshed := map[string]guiAccount{"a": cloneGUIAccount(a), "b": cloneGUIAccount(b)}
			g.applyDAVCredentials(refreshed)
			if refreshed["a"].credentials["token"] != "new-a" || refreshed["b"].credentials["token"] != "old-b" {
				t.Fatal("account refresh lost pending in-memory credentials")
			}
			valid := map[string]davCredential{davIdentity(a): {Access: "old-a", WebToken: "existing-web-a"}, davIdentity(b): {Access: "old-b", Refresh: "refresh-b"}}
			plain, _ := json.Marshal(valid)
			protected, _ := protectLocal(plain)
			if err = os.WriteFile(file, protected, 0600); err != nil {
				t.Fatal(err)
			}
			g.mu.Lock()
			err = g.persistDAVCredentialsLocked("", "")
			g.mu.Unlock()
			if err != nil {
				t.Fatal("pending-save retry failed", err)
			}
			saved, err := readDAVCredentials(file)
			if err != nil || saved[davIdentity(a)].Access != "new-a" || saved[davIdentity(a)].WebToken != "existing-web-a" || saved[davIdentity(b)] != valid[davIdentity(b)] || len(g.pendingDAVCredentials) != 0 || g.credentialSaveError != "" {
				t.Fatal("retry lost account fields or pending state", err, saved)
			}
		})
	}
}

func TestReleaseCredentialReadFailureAndDecryptionFailurePreserveStorage(t *testing.T) {
	for _, directory := range []bool{false, true} {
		g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
		a := guiAccount{Provider: "wopan", credentials: map[string]any{"user_id": "stable-a", "token": "old"}}
		file := filepath.Join(g.data, "webdav-credentials.bin")
		if directory {
			if err := os.Mkdir(file, 0700); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(file, []byte("not a protected credential store"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := g.saveDAVCredential(a, "rotated", "refresh"); err == nil {
			t.Fatal("read/decryption error ignored")
		}
		g.mu.Lock()
		g.credentialRetry.Stop()
		g.mu.Unlock()
		fi, err := os.Stat(file)
		if err != nil || fi.IsDir() != directory {
			t.Fatal("failed read replaced storage", err)
		}
		if !directory {
			raw, _ := os.ReadFile(file)
			if string(raw) != "not a protected credential store" {
				t.Fatal("corrupt protected file replaced")
			}
		}
	}
}

func TestReleaseOpenListConfigErrorsDoNotInitializeAnEmptyStore(t *testing.T) {
	for _, malformed := range []string{"null", "{broken", "not an encrypted store"} {
		g := &guiApp{data: t.TempDir()}
		file := filepath.Join(g.data, "openlist-config.bin")
		raw, err := protectLocal([]byte(malformed))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = g.openListConfig("account", nil, "identity"); err == nil {
			t.Fatal("invalid OpenList config was accepted")
		}
		if err = g.saveOpenList("account", openListSaved{}); err == nil {
			t.Fatal("invalid OpenList config was overwritten")
		}
		after, _ := os.ReadFile(file)
		if !bytes.Equal(raw, after) {
			t.Fatal("OpenList config changed after read failure")
		}
	}
	g := &guiApp{data: t.TempDir()}
	file := filepath.Join(g.data, "openlist-config.bin")
	if err := os.Mkdir(file, 0700); err != nil {
		t.Fatal(err)
	}
	if err := g.saveOpenList("account", openListSaved{}); err == nil {
		t.Fatal("OpenList storage read error was ignored")
	}
}

func TestReleaseRefreshedSDKTokenRemainsUsableWhilePersistenceIsPending(t *testing.T) {
	a := guiAccount{Key: "a", Provider: "wopan", credentials: map[string]any{"user_id": "stable-a", "token": "fixture-old-token-123456789", "refresh_token": "old-refresh"}}
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": a}}
	t.Cleanup(func() {
		g.mu.Lock()
		if g.credentialRetry != nil {
			g.credentialRetry.Stop()
		}
		g.mu.Unlock()
	})
	file := filepath.Join(g.data, "webdav-credentials.bin")
	original, err := protectLocal([]byte("{broken"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(file, original, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := defaultConfig("wopan")
	cfg.Transport, cfg.Wopan.AccessToken = "bridge", valueString(a.credentials["token"])
	cl, err := newClient(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	d := newWopanDAVDrive(cl, a.credentials, "0", func(access, refresh string) error { return g.saveDAVCredential(a, access, refresh) })
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":{"access_token":"fixture-new-token-123456789","refresh_token":"new-refresh"}}}`)), Request: r}, nil
	})})
	if err = d.w.RefreshToken(); err != nil {
		t.Fatal("SDK rotation failed", err)
	}
	access, refresh := d.w.GetToken()
	if access != "fixture-new-token-123456789" || refresh != "new-refresh" || d.ready() != nil || len(g.pendingDAVCredentials) != 1 || g.credentialSaveError == "" {
		t.Fatal("persistence failure disabled an already-refreshed token")
	}
	after, _ := os.ReadFile(file)
	if !bytes.Equal(original, after) {
		t.Fatal("SDK refresh overwrote corrupt credential store")
	}
}

type signaledBridgeInput struct {
	io.WriteCloser
	entered chan struct{}
	once    sync.Once
}

func (w *signaledBridgeInput) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	return w.WriteCloser.Write(p)
}

func TestReleaseBridgeBlockedPipeRespectsCancellationAndClosesWriter(t *testing.T) {
	reader, writer := io.Pipe()
	input := &signaledBridgeInput{WriteCloser: writer, entered: make(chan struct{})}
	b := &OriginalBridge{stdin: input, done: make(chan struct{})}
	t.Cleanup(func() { close(b.done); reader.Close(); b.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := b.Call(ctx, map[string]any{"kind": "request", "parameters": strings.Repeat("x", 256<<10)})
		done <- err
	}()
	select {
	case <-input.entered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("bridge never entered pipe write")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked pipe succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("pipe write ignored call cancellation")
	}
	select {
	case <-b.writerDone:
	case <-time.After(time.Second):
		t.Fatal("canceled writer retained blocked pipe")
	}
}

func TestReleaseBridgeCanceledQueuedCommandNeverReachesSDK(t *testing.T) {
	reader, writer := io.Pipe()
	b := &OriginalBridge{stdin: writer, done: make(chan struct{})}
	t.Cleanup(func() { close(b.done); reader.Close(); b.Close() })
	lines := make(chan map[string]any, 3)
	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			var command map[string]any
			if json.Unmarshal(scanner.Bytes(), &command) == nil {
				lines <- command
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := b.Call(ctx, map[string]any{"kind": "identity", "marker": "first"}); first <- err }()
	var sent map[string]any
	select {
	case sent = <-lines:
	case <-time.After(time.Second):
		t.Fatal("first command never dispatched")
	}
	queued, cancelQueued := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelQueued()
	_, err := b.Call(queued, map[string]any{"kind": "identity", "marker": "canceled"})
	var notExecuted *davNotExecuted
	if !errors.As(err, &notExecuted) {
		t.Fatal("queued cancellation not classified as unexecuted", err)
	}
	id := int64(sent["id"].(float64))
	b.mu.Lock()
	ch := b.pending[id]
	b.mu.Unlock()
	ch <- BridgeReply{ID: id, OK: true}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	third := make(chan error, 1)
	go func() { _, err := b.Call(ctx, map[string]any{"kind": "identity", "marker": "third"}); third <- err }()
	select {
	case sent = <-lines:
		if sent["marker"] != "third" || int64(sent["deadline_unix_ms"].(float64)) <= time.Now().UnixMilli() {
			t.Fatal("canceled/expired command reached SDK", sent)
		}
	case <-time.After(time.Second):
		t.Fatal("queue did not resume")
	}
	id = int64(sent["id"].(float64))
	b.mu.Lock()
	ch = b.pending[id]
	b.mu.Unlock()
	ch <- BridgeReply{ID: id, OK: true}
	if err := <-third; err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("Close waited on normal IPC")
	}
}

func TestReleaseDuplicateWritableMountsRejected(t *testing.T) {
	accounts := map[string]guiAccount{
		"a":             {Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "same-account"}},
		"alias-account": {Provider: "wopan", Root: "root", credentials: map[string]any{"user_id": "same-account"}},
		"other":         {Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "different-account"}},
	}
	for _, writable := range []bool{false, true} {
		mounts := []davMountInput{{Account: "a", Alias: "A", Root: "root", Writable: true}, {Account: "alias-account", Alias: "B", Writable: writable}}
		if err := validateDAVMountResources(mounts, accounts); err == nil {
			t.Fatal("same cloud root accepted through writable aliases")
		}
		mounts[0].Writable, mounts[1].Writable = false, false
		if err := validateDAVMountResources(mounts, accounts); err != nil {
			t.Fatal("read-only aliases rejected", err)
		}
		mounts[0].Writable, mounts[1].Account = true, "other"
		if err := validateDAVMountResources(mounts, accounts); err != nil {
			t.Fatal("different accounts rejected", err)
		}
	}
}

func TestReleaseDownloadPrefetchBudgetRetainedUntilConsumptionOrClose(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 6<<20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"stable-version"`)
		http.ServeContent(w, r, "file.bin", time.Unix(10000, 0), bytes.NewReader(data))
	}))
	defer server.Close()
	budget := newByteBudget(2 << 20)
	makeReader := func(ctx context.Context) *transferReader {
		r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("fixture", server.URL, "")})
		r.ctx, r.memory, r.partSize, r.threads, r.parallel = ctx, budget, 1<<20, 2, true
		r.session.budget = newFairLimiter(1)
		return r
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r1 := makeReader(ctx)
	if n, err := r1.Read(make([]byte, 513)); n != 513 || err != nil {
		t.Fatal(n, err)
	}
	if budget.status()["used_bytes"].(int64) != 2<<20 {
		t.Fatal("prefetched bytes released before consumption", budget.status())
	}
	canceled, stop := context.WithCancel(ctx)
	r2 := makeReader(canceled)
	waiting := make(chan error, 1)
	go func() { _, err := r2.Read(make([]byte, 513)); waiting <- err }()
	deadline := time.Now().Add(time.Second)
	for budget.status()["queued"].(int) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stop()
	select {
	case err := <-waiting:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("budget waiter ignored cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("budget waiter stuck")
	}
	r1.Close()
	r3 := makeReader(ctx)
	got, err := io.ReadAll(r3)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("budgeted download changed bytes", err, len(got))
	}
	if status := budget.status(); status["used_bytes"].(int64) != 0 || status["peak_bytes"].(int64) > 2<<20 || status["queued"].(int) != 0 {
		t.Fatal("download memory leaked or exceeded global budget", status)
	}
}

func TestReleaseFullGETsUseIndependentFileLeases(t *testing.T) {
	runtime := newTransferRuntime(transferOptions{ConcurrentDownloads: 1})
	ctx := context.WithValue(context.Background(), davRequestKey{}, &davRequestState{method: "GET"})
	release, err := runtime.file(ctx, "下载", "/same-file")
	if err != nil {
		t.Fatal(err)
	}
	queued, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if second, err := runtime.file(queued, "下载", "/same-file"); err == nil {
		second()
		t.Fatal("same-file full GET bypassed file limit")
	}
	release()
	ranges := context.WithValue(context.Background(), davRequestKey{}, &davRequestState{method: "GET", externalRange: true})
	a, err := runtime.file(ranges, "下载", "/same-file")
	if err != nil {
		t.Fatal(err)
	}
	defer a()
	b, err := runtime.file(ranges, "下载", "/same-file")
	if err != nil {
		t.Fatal(err)
	}
	b()
}

func TestReleaseAuthenticationCacheBudgetAndConfigurationInvalidation(t *testing.T) {
	salt := []byte("fixture-salt")
	hash, _ := pbkdf2.Key(sha256.New, "fixture-password", salt, 100000, 32)
	s := &davService{username: "fixture-user", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash)}
	request := func(password string) *http.Request {
		r := httptest.NewRequest("GET", "http://localhost/dav/file", nil)
		r.SetBasicAuth("fixture-user", password)
		return r
	}
	if code := s.authenticationStatus(request("fixture-password")); code != 0 {
		t.Fatal("valid authentication failed", code)
	}
	for range cap(s.auth.workers) {
		s.auth.workers <- struct{}{}
	}
	if code := s.authenticationStatus(request("fixture-password")); code != 0 {
		t.Fatal("cached credentials repeated expensive derivation", code)
	}
	if code := s.authenticationStatus(request("wrong-password")); code != http.StatusTooManyRequests {
		t.Fatal("authentication computation limit ignored", code)
	}
	for range cap(s.auth.workers) {
		<-s.auth.workers
	}
	next, _ := pbkdf2.Key(sha256.New, "replacement-password", salt, 100000, 32)
	s.passwordHash = hex.EncodeToString(next)
	if code := s.authenticationStatus(request("fixture-password")); code == 0 {
		t.Fatal("old cache survived password change")
	}
	if code := s.authenticationStatus(request("replacement-password")); code != 0 {
		t.Fatal("new password rejected", code)
	}
	limited := false
	for range 15 {
		if s.authenticationStatus(request("wrong-password")) == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("source authentication rate was not limited")
	}
}

func TestReleaseRuntimeLogRotationRetentionAndExport(t *testing.T) {
	profile := t.TempDir()
	l := newRuntimeLog(profile, nil)
	defer l.close()
	l.maxFileBytes, l.maxTotalBytes, l.maxFiles = 800, 2400, 3
	unrelated := filepath.Join(profile, "run-logs", "unrelated.jsonl")
	if err := os.WriteFile(unrelated, []byte("keep me"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 80 {
		l.add(runLogEntry{Operation: "fixture", Result: "ok", Detail: strings.Repeat("a", 80)})
	}
	if l.err != "" {
		t.Fatal(l.err)
	}
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	count := 0
	for _, entry := range entries {
		if !runtimeLogName.MatchString(entry.Name()) {
			continue
		}
		fi, _ := entry.Info()
		if fi.Size() > l.maxFileBytes {
			t.Fatal("segment exceeded size limit", fi.Size())
		}
		total += fi.Size()
		count++
	}
	if count > 3 || total > 2400 || len(l.segments) > 3 {
		t.Fatal("retention exceeded bounds", count, total)
	}
	if _, err = os.Stat(unrelated); err != nil {
		t.Fatal("retention removed unrelated file", err)
	}
	g := &guiApp{runLog: l}
	w := httptest.NewRecorder()
	g.runLogAPI(w, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil))
	if w.Code != 200 {
		t.Fatal("rotated log export failed", w.Code)
	}
	scanner := bufio.NewScanner(w.Body)
	var previous uint64
	for scanner.Scan() {
		var row runLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal("broken JSONL export", err)
		}
		if row.ID <= previous {
			t.Fatal("exported segments out of order")
		}
		previous = row.ID
	}
	if err := scanner.Err(); err != nil || previous != 80 {
		t.Fatal("export omitted latest records", err, previous)
	}
}

type cancelAfterRead struct {
	reader io.Reader
	cancel context.CancelFunc
}

func (r cancelAfterRead) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.cancel()
	return n, err
}

func TestReleaseMobileUploadHashStopsBeforeOrDuringRead(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "mobile-upload-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := &mobileDAVDrive{}
	if _, err := d.Upload(ctx, "0", "file.bin", f); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-canceled upload scanned file or contacted SDK", err)
	}
	if offset, _ := f.Seek(0, io.SeekCurrent); offset != 0 {
		t.Fatal("canceled hash read staged file", offset)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	n, err := copyWithContext(ctx, md5.New(), cancelAfterRead{reader: f, cancel: cancel})
	if !errors.Is(err, context.Canceled) || n > 64<<10 || n == 0 {
		t.Fatal("hash did not stop between bounded reads", n, err)
	}
}

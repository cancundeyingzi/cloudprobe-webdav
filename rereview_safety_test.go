package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fail only the first parent listing. A handler that continues after the failed
// guard sees a healthy drive and can perform the mutation.
type rereviewWriteDrive struct {
	*fakeDAVDrive
	listError error
	lists     atomic.Int64
	writes    atomic.Int64
}

func (d *rereviewWriteDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	if d.lists.Add(1) == 1 {
		return nil, d.listError
	}
	return d.fakeDAVDrive.List(ctx, parent)
}
func (d *rereviewWriteDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	d.writes.Add(1)
	return d.fakeDAVDrive.Mkdir(ctx, parent, name)
}
func (d *rereviewWriteDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	d.writes.Add(1)
	return d.fakeDAVDrive.Upload(ctx, parent, name, f)
}
func (d *rereviewWriteDrive) Remove(ctx context.Context, entry davEntry) error {
	d.writes.Add(1)
	return d.fakeDAVDrive.Remove(ctx, entry)
}
func (d *rereviewWriteDrive) Move(ctx context.Context, entry davEntry, parent, name string) error {
	d.writes.Add(1)
	return d.fakeDAVDrive.Move(ctx, entry, parent, name)
}

func TestReReviewWriteCheckFailureStopsEveryMutation(t *testing.T) {
	for _, method := range []string{"DELETE", "PUT", "MKCOL", "MOVE", "COPY", "LOCK", "PROPPATCH"} {
		t.Run(method, func(t *testing.T) {
			server, base, fs := testDAVService(t, true)
			root := "/联通云盘-1/nested/"
			wantDAV(t, server.URL, "MKCOL", strings.TrimRight(root, "/"), "", 201, nil)
			wantDAV(t, server.URL, "PUT", root+"source.txt", "source", 201, nil)
			wantDAV(t, server.URL, "PUT", root+"target.txt", "target", 201, nil)
			d := &rereviewWriteDrive{fakeDAVDrive: base, listError: errors.New("one-shot parent listing failure")}
			fs.mounts[0].drive = d
			fs.invalidate(fs.mounts[0])
			target, body := root+"source.txt", "replacement"
			switch method {
			case "MKCOL":
				target, body = root+"new-dir", ""
			case "LOCK":
				body = `<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`
			case "PROPPATCH":
				body = `<d:propertyupdate xmlns:d="DAV:"><d:set><d:prop><x:label xmlns:x="test">changed</x:label></d:prop></d:set></d:propertyupdate>`
			}
			code, reply, _ := davRequest(t, server.URL, method, target, strings.NewReader(body), map[string]string{"Destination": "/dav" + root + "target.txt", "Overwrite": "T", "Depth": "0"})
			if code != http.StatusBadGateway || !bytes.Contains(reply, []byte("one-shot parent listing failure")) {
				t.Errorf("guard response = %d %s", code, reply)
			}
			if d.writes.Load() != 0 || d.lists.Load() != 1 {
				t.Errorf("handler continued after failed guard: writes=%d listings=%d", d.writes.Load(), d.lists.Load())
			}
			base.mu.Lock()
			defer base.mu.Unlock()
			if len(base.entries) != 3 {
				t.Errorf("cloud entries changed: %v", base.entries)
			}
			for _, name := range []string{"source.txt", "target.txt"} {
				found := false
				for id, entry := range base.entries {
					if entry.Name == name {
						found = true
						if string(base.contents[id]) != strings.TrimSuffix(name, ".txt") {
							t.Errorf("%s contents changed", name)
						}
					}
				}
				if !found {
					t.Errorf("%s disappeared", name)
				}
			}
		})
	}
}

func TestReReviewWriteCheckErrorStatuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code int
	}{
		{"permission", os.ErrPermission, http.StatusForbidden},
		{"canceled", context.Canceled, http.StatusRequestTimeout},
		{"deadline", context.DeadlineExceeded, http.StatusGatewayTimeout},
		{"upstream", errors.New("upstream unavailable"), http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, base, fs := testDAVService(t, true)
			d := &rereviewWriteDrive{fakeDAVDrive: base, listError: tc.err}
			fs.mounts[0].drive = d
			code, reply, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/nested/file", strings.NewReader("body"), nil)
			if code != tc.code || d.writes.Load() != 0 || d.lists.Load() != 1 {
				t.Fatalf("response=%d %s, writes=%d listings=%d", code, reply, d.writes.Load(), d.lists.Load())
			}
		})
	}
}

func TestReReviewDestinationCheckPreventsOverwrite(t *testing.T) {
	for _, method := range []string{"MOVE", "COPY"} {
		t.Run(method, func(t *testing.T) {
			server, base, fs := testDAVService(t, true)
			root := "/联通云盘-1/"
			wantDAV(t, server.URL, "MKCOL", root+"nested", "", 201, nil)
			wantDAV(t, server.URL, "PUT", root+"source", "original-source", 201, nil)
			wantDAV(t, server.URL, "PUT", root+"nested/target", "original-target", 201, nil)
			d := &rereviewWriteDrive{fakeDAVDrive: base, listError: errors.New("destination parent unavailable")}
			fs.mounts[0].drive = d
			fs.invalidate(fs.mounts[0])
			wantDAV(t, server.URL, method, root+"source", "", 502, map[string]string{"Destination": "/dav" + root + "nested/target", "Overwrite": "T"})
			if d.writes.Load() != 0 || d.lists.Load() != 1 {
				t.Fatalf("destination guard did not stop dispatch: writes=%d lists=%d", d.writes.Load(), d.lists.Load())
			}
			for name, content := range map[string]string{"source": "original-source", "nested/target": "original-target"} {
				if got := wantDAV(t, server.URL, "GET", root+name, "", 200, nil); string(got) != content {
					t.Fatalf("%s changed: %s", name, got)
				}
			}
		})
	}
}

func TestReReviewMissingParentAndMountRootLock(t *testing.T) {
	server, _, _ := testDAVService(t, true)
	root := "/联通云盘-1"
	for _, method := range []string{"PUT", "MKCOL", "DELETE", "PROPPATCH"} {
		code := http.StatusConflict
		if method == "DELETE" || method == "PROPPATCH" {
			code = http.StatusNotFound
		}
		wantDAV(t, server.URL, method, root+"/missing/child", "", code, nil)
	}
	body := `<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`
	code, reply, headers := davRequest(t, server.URL, "LOCK", root, strings.NewReader(body), map[string]string{"Depth": "infinity"})
	if code != http.StatusOK || headers.Get("Lock-Token") == "" {
		t.Fatalf("mount root lock: %d %s", code, reply)
	}
	wantDAV(t, server.URL, "PUT", root+"/child", "blocked", http.StatusLocked, nil)
	wantDAV(t, server.URL, "UNLOCK", root, "", http.StatusNoContent, map[string]string{"Lock-Token": headers.Get("Lock-Token")})
	wantDAV(t, server.URL, "PUT", root+"/child", "allowed", http.StatusCreated, nil)
}

func TestReReviewOverlappingMountsRejectedBeforeStartup(t *testing.T) {
	accounts := map[string]guiAccount{
		"root": {Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "same"}},
		"sub":  {Provider: "wopan", Root: "directory-D", credentials: map[string]any{"user_id": "same"}},
		"else": {Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "other"}},
	}
	for _, flags := range [][2]bool{{true, true}, {true, false}, {false, true}} {
		mounts := []davMountInput{{Account: "root", Alias: "A", Writable: flags[0]}, {Account: "sub", Alias: "B", Writable: flags[1]}}
		if err := validateDAVMountResources(mounts, accounts); err == nil {
			t.Errorf("overlapping roots accepted: writable=%v", flags)
		}
		g := &guiApp{logged: true, data: t.TempDir(), accounts: accounts}
		d := newDAVManager(g)
		_, err := d.Start(context.Background(), davStartInput{Port: 19081, Mounts: mounts})
		if err == nil || !strings.Contains(err.Error(), "A") || !strings.Contains(err.Error(), "B") || !strings.Contains(err.Error(), "同账号") {
			t.Errorf("startup did not reject conflicting aliases: %v", err)
		}
		d.Stop()
		if _, err := os.Stat(filepath.Join(g.data, "webdav.json")); !os.IsNotExist(err) {
			t.Errorf("rejected configuration persisted: %v", err)
		}
	}
	for _, mounts := range [][]davMountInput{
		{{Account: "root", Alias: "A"}, {Account: "sub", Alias: "B"}},
		{{Account: "root", Alias: "A", Writable: true}, {Account: "else", Alias: "B", Writable: true}},
		{{Account: "sub", Alias: "B", Writable: true}},
	} {
		if err := validateDAVMountResources(mounts, accounts); err != nil {
			t.Errorf("safe mounts rejected: %v", err)
		}
	}
}

type rereviewSlowWriter struct {
	*httptest.ResponseRecorder
	started, release chan struct{}
	once             sync.Once
}

func (w *rereviewSlowWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.ResponseRecorder.Write(p)
}

func TestReReviewSlowLogExportAllowsRotation(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	l.maxFileBytes, l.maxTotalBytes, l.maxFiles = 1024, 4096, 4
	for range 9 {
		l.add(runLogEntry{Operation: "before-export", Detail: strings.Repeat("x", 150)})
	}
	var expected []byte
	for _, path := range l.segments {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, data...)
	}
	w := &rereviewSlowWriter{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(w.release) }) }
	defer unblock()
	g := &guiApp{runLog: l}
	exported := make(chan struct{})
	go func() {
		defer close(exported)
		g.runLogAPI(w, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil))
	}()
	select {
	case <-w.started:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not start")
	}
	other := httptest.NewRecorder()
	g.runLogAPI(other, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil))
	if other.Code != http.StatusConflict {
		t.Errorf("concurrent export was not bounded: %d", other.Code)
	}
	assertReReviewSnapshots(t, l, 1)
	written := make(chan struct{})
	go func() {
		defer close(written)
		for range 50 {
			l.add(runLogEntry{Operation: "during-export", Detail: strings.Repeat("y", 150)})
		}
		l.add(runLogEntry{Operation: "last-disk-record"})
	}()
	select {
	case <-written:
	case <-time.After(3 * time.Second):
		t.Fatal("slow export blocked logging")
	}
	if status := l.status(); status["error"] != "" || status["disk_paused"] == true {
		t.Errorf("rotation stopped disk logging: %v", status)
	}
	data, err := os.ReadFile(l.path)
	if err != nil || !bytes.Contains(data, []byte("last-disk-record")) {
		t.Errorf("last record did not reach disk: %s %v", data, err)
	}
	unblock()
	select {
	case <-exported:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not finish")
	}
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), expected) {
		t.Errorf("export snapshot changed during rotation: status=%d got=%d want=%d", w.Code, w.Body.Len(), len(expected))
	}
	assertReReviewSnapshots(t, l, 0)
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var total int64
	for _, entry := range entries {
		if runtimeLogName.MatchString(entry.Name()) {
			info, _ := entry.Info()
			count++
			total += info.Size()
		}
	}
	if count > l.maxFiles || total > l.maxTotalBytes {
		t.Errorf("retention exceeded: count=%d bytes=%d", count, total)
	}
}

func assertReReviewSnapshots(t *testing.T, l *runtimeLog, want int) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if !runtimeLogExportName.MatchString(entry.Name()) {
			continue
		}
		count++
		info, err := entry.Info()
		if err != nil || info.Size() > l.maxTotalBytes {
			t.Fatalf("export snapshot exceeded budget: %v %v", info, err)
		}
	}
	if count != want {
		t.Fatalf("snapshot count=%d, want=%d", count, want)
	}
}

type rereviewCancelWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *rereviewCancelWriter) Write(p []byte) (int, error) {
	w.cancel()
	return 0, context.Canceled
}

func TestReReviewLogExportCancellationAndBudget(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	l.add(runLogEntry{Operation: "export-source"})
	g := &guiApp{runLog: l}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &rereviewCancelWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	g.runLogAPI(w, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil).WithContext(ctx))
	assertReReviewSnapshots(t, l, 0)
	// An aborted transfer must release the concurrency slot as well as the file.
	next := httptest.NewRecorder()
	g.runLogAPI(next, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil))
	if next.Code != 200 || !bytes.Contains(next.Body.Bytes(), []byte("export-source")) {
		t.Fatalf("export did not recover after cancellation: %d %s", next.Code, next.Body.Bytes())
	}
	// Force an oversized snapshot without allocating a large test fixture.
	l.maxTotalBytes = 1
	tooLarge := httptest.NewRecorder()
	g.runLogAPI(tooLarge, httptest.NewRequest("GET", "http://localhost/api/run-log/export", nil))
	if tooLarge.Code < 400 {
		t.Fatal("oversized snapshot exported")
	}
	assertReReviewSnapshots(t, l, 0)
	if status := l.status(); status["error"] != "" {
		t.Fatalf("export failure disabled the logger: %v", status)
	}
}

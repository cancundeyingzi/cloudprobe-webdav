package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

func TestUploadActualHTTPProtocolsAndConnectionCounts(t *testing.T) {
	for _, mode := range []string{"http1", "http2"} {
		t.Run(mode, func(t *testing.T) {
			var active atomic.Int64
			gate := make(chan struct{})
			var once sync.Once
			var mu sync.Mutex
			addresses, protocols := map[string]bool{}, map[string]bool{}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if active.Add(1) == 4 {
					once.Do(func() { close(gate) })
				}
				defer active.Add(-1)
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				case <-time.After(3 * time.Second):
					t.Error("four requests did not reach the server concurrently")
					return
				}
				mu.Lock()
				addresses[r.RemoteAddr], protocols[r.Proto] = true, true
				mu.Unlock()
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Error(err)
					return
				}
				defer r.MultipartForm.RemoveAll()
				io.WriteString(w, `{"code":"0000","data":{"fid":"verified"}}`)
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			f, _ := os.CreateTemp(t.TempDir(), "parts-*")
			defer f.Close()
			f.Write(make([]byte, 4*64<<10))
			log := newRuntimeLog(t.TempDir(), nil)
			defer log.close()
			ctx, cancel := context.WithTimeout(newRunTrace(context.Background(), log, "PUT", "/test"), 8*time.Second)
			defer cancel()
			u := &wopanUploadSession{endpoint: server.URL, name: "file.bin", size: 4 * 64 << 10, partSize: 64 << 10, concurrency: 4, mode: mode, source: "联通网页授权", client: server.Client()}
			if _, err := u.run(ctx, f); err != nil {
				t.Fatal(err)
			}
			want, protocol := 4, "HTTP/1.1"
			if mode == "http2" {
				want, protocol = 1, "HTTP/2.0"
			}
			mu.Lock()
			defer mu.Unlock()
			if len(addresses) != want || !protocols[protocol] || len(protocols) != 1 {
				t.Fatalf("actual connections=%d protocols=%v, want %d %s", len(addresses), protocols, want, protocol)
			}
			logged := map[string]bool{}
			for _, event := range log.entries {
				if event.Operation == "上传连接" {
					if !addresses[event.LocalAddress] || event.RemoteAddress == "" || event.Protocol != protocol {
						t.Fatal("connection log not grounded in real socket", event)
					}
					logged[event.ConnectionID] = true
				}
			}
			if len(logged) != want {
				t.Fatalf("logged connection ids=%d, want %d", len(logged), want)
			}
		})
	}
}

func TestUploadEOFRebuildsConnectionAndKeepsConfirmedParts(t *testing.T) {
	var mu sync.Mutex
	counts := map[int]int{}
	ports := []string{}
	unique := "stable-retry-session"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		index, _ := strconv.Atoi(r.FormValue("partIndex"))
		if r.FormValue("uniqueId") != unique {
			t.Error("retry lost its session")
		}
		mu.Lock()
		counts[index]++
		attempt := counts[index]
		if index == 2 {
			ports = append(ports, r.RemoteAddr)
		}
		mu.Unlock()
		if index == 2 && attempt == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"verified"}}`)
	}))
	defer server.Close()
	f, _ := os.CreateTemp(t.TempDir(), "parts-*")
	defer f.Close()
	f.Write(make([]byte, 6*64<<10))
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	ctx, cancel := context.WithTimeout(newRunTrace(context.Background(), l, "PUT", "/test"), 6*time.Second)
	defer cancel()
	u := &wopanUploadSession{endpoint: server.URL, name: "file.bin", unique: unique, size: 6 * 64 << 10, partSize: 64 << 10, concurrency: 4, source: "联通网页授权", retryDelay: 10 * time.Millisecond}
	if _, err := u.run(ctx, f); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for index := 1; index <= 6; index++ {
		want := 1
		if index == 2 {
			want = 2
		}
		if counts[index] != want {
			t.Fatalf("part %d sent %d times, want %d", index, counts[index], want)
		}
	}
	if len(ports) != 2 || ports[0] == ports[1] {
		t.Fatal("EOF retry reused failed connection", ports)
	}
	tr := ctx.Value(runTraceKey{}).(*runTrace)
	if tr.confirmed.Load() != 6*64<<10 {
		t.Fatal("confirmation counted twice")
	}
	var retried bool
	for _, event := range l.entries {
		if event.Operation == "分片重试" && event.Part == 2 {
			retried = true
		}
	}
	if !retried {
		t.Fatal("missing retry evidence")
	}
}

func TestMetadataMutexCancellationAndReservationBoundaries(t *testing.T) {
	var lock contextMutex
	lock.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lock.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("metadata wait did not cancel", err)
	}
	lock.Unlock()
	if err := lock.LockContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	lock.Unlock()
	var reservations davReservations
	owner := &davRequestState{}
	owned := context.WithValue(context.Background(), davRequestKey{}, owner)
	release, err := reservations.acquire(owned, "/mount/a/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, target := range []string{"/mount/a", "/mount/a/file.bin", "/mount/a/file.bin/child"} {
		if unlock, err := reservations.acquire(context.Background(), target); err == nil {
			unlock()
			t.Fatalf("overlap allowed: %s", target)
		}
	}
	for _, target := range []string{"/mount/a/other.bin", "/mount/ab", "/another/a"} {
		unlock, err := reservations.acquire(context.Background(), target)
		if err != nil {
			t.Fatalf("independent path blocked: %s", target)
		}
		unlock()
	}
	unlock, err := reservations.acquire(owned, "/mount/a/file.bin")
	if err != nil {
		t.Fatal("same-request nested operations blocked", err)
	}
	unlock()
	if _, err = reservations.acquire(context.Background(), "/unoccupied", "/mount/a"); err == nil {
		t.Fatal("multi-path conflict ignored")
	}
	unlock, err = reservations.acquire(context.Background(), "/unoccupied")
	if err != nil {
		t.Fatal("partial reservation leaked", err)
	}
	unlock()
}

type stalledDAVDrive struct {
	*fakeDAVDrive
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *stalledDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	if d.block.Load() {
		d.once.Do(func() { close(d.started) })
		select {
		case <-d.release:
		case <-ctx.Done():
			return davEntry{}, ctx.Err()
		}
	}
	return d.fakeDAVDrive.Upload(ctx, parent, name, f)
}

func TestUnrelatedDAVOperationsProceedDuringUploadAndConflictsAreImmediate(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	stalled := &stalledDAVDrive{fakeDAVDrive: base, started: make(chan struct{}), release: make(chan struct{})}
	fs.mounts[0].drive = stalled
	root := "/联通云盘-1"
	wantDAV(t, server.URL, "MKCOL", root+"/busy", "", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"/remove.txt", "delete this", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"/rename.txt", "rename this", 201, nil)
	stalled.block.Store(true)
	client := &http.Client{Timeout: 6 * time.Second}
	call := func(method, target string, headers map[string]string) (int, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + target
		var body io.Reader
		if method == "PUT" {
			body = strings.NewReader("body")
		}
		r, _ := http.NewRequest(method, u.String(), body)
		r.SetBasicAuth("tester", "test-password-123")
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		resp, err := client.Do(r)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}
	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() { status, err := call("PUT", root+"/busy/live.bin", nil); done <- result{status, err} }()
	defer func() {
		select {
		case <-stalled.release:
		default:
			close(stalled.release)
		}
	}()
	select {
	case <-stalled.started:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start")
	}
	started := time.Now()
	if status, err := call("DELETE", root+"/remove.txt", nil); err != nil || status != 204 {
		t.Fatal("independent delete blocked", status, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("independent delete waited for upload")
	}
	if status, err := call("PROPFIND", root, map[string]string{"Depth": "1"}); err != nil || status != 207 {
		t.Fatal("listing blocked", status, err)
	}
	dest, _ := url.Parse(server.URL)
	dest.Path = "/dav" + root + "/renamed.txt"
	if status, err := call("MOVE", root+"/rename.txt", map[string]string{"Destination": dest.String()}); err != nil || status != 201 {
		t.Fatal("independent rename blocked", status, err)
	}
	for _, target := range []string{root + "/busy", root + "/busy/live.bin"} {
		started = time.Now()
		status, err := call("DELETE", target, nil)
		if err != nil || status != 423 || time.Since(started) > time.Second {
			t.Fatal("conflict queued instead of returning 423", status, err)
		}
	}
	close(stalled.release)
	select {
	case result := <-done:
		if result.err != nil || result.status != 201 {
			t.Fatal("upload failed", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not finish")
	}
}

func TestCanceledUploadReleasesPathAndDoesNotCommit(t *testing.T) {
	_, base, fs := testDAVService(t, true)
	d := &stalledDAVDrive{fakeDAVDrive: base, started: make(chan struct{}), release: make(chan struct{})}
	d.block.Store(true)
	fs.mounts[0].drive = d
	ctx, cancel := context.WithCancel(context.Background())
	f, err := fs.OpenFile(ctx, "/联通云盘-1/cancel.bin", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("not committed"))
	done := make(chan error, 1)
	go func() { done <- f.Close() }()
	<-d.started
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled upload succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop upload")
	}
	unlock, err := fs.reserve(context.Background(), "/联通云盘-1/cancel.bin")
	if err != nil {
		t.Fatal("path reservation leaked", err)
	}
	unlock()
	entries, _ := base.List(context.Background(), "0")
	if len(entries) != 0 {
		t.Fatal("canceled upload committed data")
	}
}

func TestWopanMetadataLockNotHeldByBodyTransfer(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fixture-access-token-0123456789"
	cl, _ := newClient(cfg, "")
	d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
	d.w.ZoneURL = server.URL
	d.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
	f, _ := os.CreateTemp(t.TempDir(), "spool-*")
	defer f.Close()
	f.Write(bytes.Repeat([]byte("data"), 16384))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := d.Upload(ctx, "0", "stage.bin", f); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upload did not reach node")
	}
	probe, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if err := d.directMu.LockContext(probe); err != nil {
		t.Fatal("body transfer retained SDK metadata lock", err)
	}
	d.directMu.Unlock()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled upload reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("transfer did not cancel")
	}
}

func TestUploadRecheckRejectsExternalTargetChange(t *testing.T) {
	_, base, fs := testDAVService(t, true)
	d := &stalledDAVDrive{fakeDAVDrive: base, started: make(chan struct{}), release: make(chan struct{})}
	d.block.Store(true)
	fs.mounts[0].drive = d
	f, err := fs.OpenFile(context.Background(), "/联通云盘-1/target.bin", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("pending"))
	done := make(chan error, 1)
	go func() { done <- f.Close() }()
	<-d.started
	other, _ := os.CreateTemp(t.TempDir(), "external-*")
	defer other.Close()
	other.Write([]byte("external writer"))
	if _, err = base.Upload(context.Background(), "0", "target.bin", other); err != nil {
		t.Fatal(err)
	}
	close(d.release)
	err = <-done
	var conflict *davStatusError
	if !errors.As(err, &conflict) || conflict.Code != 409 {
		t.Fatal("external change overwritten", err)
	}
	entries, _ := base.List(context.Background(), "0")
	for _, entry := range entries {
		if entry.Name == "target.bin" && entry.Size != int64(len("external writer")) {
			t.Fatal("original content changed")
		}
	}
}

type delayedFirstListing struct {
	*fakeDAVDrive
	calls            atomic.Int64
	started, release chan struct{}
}

func (d *delayedFirstListing) List(ctx context.Context, parent string) ([]davEntry, error) {
	n := d.calls.Add(1)
	entries, err := d.fakeDAVDrive.List(ctx, parent)
	if n == 1 {
		close(d.started)
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return entries, err
}
func TestOldDirectoryResponseCannotReplaceCacheAfterMutation(t *testing.T) {
	_, base, fs := testDAVService(t, true)
	d := &delayedFirstListing{fakeDAVDrive: base, started: make(chan struct{}), release: make(chan struct{})}
	m := fs.mounts[0]
	m.drive = d
	done := make(chan error, 1)
	go func() { _, err := fs.listing(context.Background(), m, "0"); done <- err }()
	<-d.started
	if _, err := base.Mkdir(context.Background(), "0", "created-during-listing"); err != nil {
		t.Fatal(err)
	}
	fs.invalidate(m)
	if entries, err := fs.listing(context.Background(), m, "0"); err != nil || len(entries) != 1 {
		t.Fatal("fresh listing missing", err)
	}
	close(d.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if entries, err := fs.listing(context.Background(), m, "0"); err != nil || len(entries) != 1 {
		t.Fatal("old empty response replaced fresh cache", err)
	}
}

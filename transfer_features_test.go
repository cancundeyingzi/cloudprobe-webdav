package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransferDefaultsMatchOpenWoPanApplication(t *testing.T) {
	o := transferOptions{}.defaults()
	if o.UploadPartMiB != 5 || o.DownloadConcurrency != 8 || o.ConcurrentUploads != 3 || o.ConcurrentDownloads != 4 {
		t.Fatal("application defaults diverged", o)
	}
	for _, n := range []int{1, 3, 4, 7, 8, 15, 16} {
		if uploadConcurrency(n) != n {
			t.Fatal("upload thread value clamped", n)
		}
	}
	o.DownloadConcurrency = 16
	o.ConcurrentDownloads = 5
	o.ConcurrentUploads = 5
	o.UploadPartMiB = 16
	o.DownloadPartMiB = 32
	if e := o.validate(); e != nil {
		t.Fatal(e)
	}
	o.DownloadConcurrency = 17
	if o.validate() == nil {
		t.Fatal("invalid download threads accepted")
	}
}

func TestSixteenUploadWorkersAreRealRequests(t *testing.T) {
	var active, peak atomic.Int64
	gate := make(chan struct{})
	var once sync.Once
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if n == 16 {
			once.Do(func() { close(gate) })
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		case <-time.After(3 * time.Second):
			t.Error("16 uploads did not arrive")
			return
		}
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"code":"0000","data":{"fid":"complete"}}`)
	}))
	defer s.Close()
	f, _ := os.CreateTemp(t.TempDir(), "parts-*")
	defer f.Close()
	f.Write(make([]byte, 16*64<<10))
	u := &wopanUploadSession{endpoint: s.URL, name: "sixteen.bin", size: 16 * 64 << 10, partSize: 64 << 10, concurrency: 16, mode: "http1", budget: newFairLimiter(16)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e := u.run(ctx, f); e != nil {
		t.Fatal(e)
	}
	if peak.Load() != 16 {
		t.Fatal("16 upload workers were silently capped", peak.Load())
	}
}

func TestAutomaticUploadProtocolFallbackKeepsSession(t *testing.T) {
	var mu sync.Mutex
	protocols := []string{}
	sessions := []string{}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e := r.ParseMultipartForm(1 << 20); e != nil {
			t.Error(e)
			return
		}
		defer r.MultipartForm.RemoveAll()
		mu.Lock()
		protocols = append(protocols, r.Proto)
		sessions = append(sessions, r.FormValue("uniqueId"))
		first := len(protocols) == 1
		mu.Unlock()
		if first {
			c, _, e := w.(http.Hijacker).Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			c.Close()
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"complete"}}`)
	}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	f, _ := os.CreateTemp(t.TempDir(), "part-*")
	defer f.Close()
	f.Write(make([]byte, 64<<10))
	u := &wopanUploadSession{endpoint: s.URL, name: "auto.bin", unique: "stable-auto", size: 64 << 10, partSize: 64 << 10, concurrency: 1, mode: "auto", client: s.Client(), retryDelay: time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, e := u.run(ctx, f); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(protocols) != 2 || protocols[0] != "HTTP/1.1" || protocols[1] != "HTTP/2.0" || sessions[0] != sessions[1] {
		t.Fatal("automatic mode did not safely change transport", protocols, sessions)
	}
}
func TestFairLimiterFIFOAndCanceledWaiter(t *testing.T) {
	l := newFairLimiter(1)
	release, e := l.acquire(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		r, e := l.acquire(ctx)
		if r != nil {
			r()
		}
		done <- e
	}()
	deadline := time.Now().Add(time.Second)
	for l.status()["queued"] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	order := make(chan int, 2)
	for _, i := range []int{1, 2} {
		go func() {
			r, e := l.acquire(context.Background())
			if e != nil {
				return
			}
			order <- i
			r()
		}()
		for l.status()["queued"] != i && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	release()
	if <-order != 1 || <-order != 2 {
		t.Fatal("queue not FIFO")
	}
	if l.status()["active"] != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if l.status()["active"] != 0 || l.status()["queued"] != 0 {
		t.Fatal("limiter leaked", l.status())
	}
}
func TestFileSchedulerSharesRangesAndSeparatesUploadDownload(t *testing.T) {
	runtime := newTransferRuntime(transferOptions{ConcurrentUploads: 1, ConcurrentDownloads: 1})
	u, _ := runtime.uploads.acquire(context.Background(), "a")
	defer u()
	d, _ := runtime.downloads.acquire(context.Background(), "a")
	rangeLease, e := runtime.downloads.acquire(context.Background(), "a")
	if e != nil {
		t.Fatal(e)
	}
	if runtime.downloads.limiter.status()["active"] != 1 {
		t.Fatal("ranges consumed file slots")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, e = runtime.downloads.acquire(ctx, "b"); e == nil {
		t.Fatal("download limit ignored")
	}
	d()
	if runtime.downloads.limiter.status()["active"] != 1 {
		t.Fatal("shared lease released early")
	}
	rangeLease()
	if runtime.downloads.limiter.status()["active"] != 0 {
		t.Fatal("lease leaked")
	}
}
func TestCancelingQueuedRangeLeaderDoesNotCancelOtherRequests(t *testing.T) {
	s := newFileScheduler(1)
	active, _ := s.acquire(context.Background(), "active")
	ctx, cancel := context.WithCancel(context.Background())
	leader := make(chan error, 1)
	follower := make(chan error, 1)
	go func() {
		release, e := s.acquire(ctx, "shared")
		if release != nil {
			release()
		}
		leader <- e
	}()
	deadline := time.Now().Add(time.Second)
	for s.limiter.status()["queued"] != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go func() {
		release, e := s.acquire(context.Background(), "shared")
		if release != nil {
			release()
		}
		follower <- e
	}()
	for time.Now().Before(deadline) {
		s.mu.Lock()
		refs := 0
		if lease := s.files["shared"]; lease != nil {
			refs = lease.refs
		}
		s.mu.Unlock()
		if refs == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if e := <-leader; !errors.Is(e, context.Canceled) {
		active()
		t.Fatal(e)
	}
	active()
	select {
	case e := <-follower:
		if e != nil {
			t.Fatal("independent Range request inherited cancellation", e)
		}
	case <-time.After(time.Second):
		t.Fatal("follower stayed blocked")
	}
}

type routeFixtureDrive struct {
	davDrive
	routes []readRoute
}

func (d *routeFixtureDrive) ReadRoutes(context.Context, davEntry) []readRoute { return d.routes }
func fixtureReader(t *testing.T, size int64, routes []readRoute) *transferReader {
	t.Helper()
	m := &davMount{drive: &routeFixtureDrive{routes: routes}, health: &routeHealth{}}
	r := newTransferReader(context.Background(), m, davEntry{Item: Item{ID: "f", Name: "test.bin", Size: size}}, newTransferRuntime(transferOptions{}))
	r.partSize = 64 << 10
	t.Cleanup(r.Close)
	return r
}
func fixtureRoute(name, endpoint, version string) readRoute {
	return readRoute{Name: name, Version: version, Resolve: func(context.Context) (readTarget, error) { return readTarget{URL: endpoint}, nil }}
}

func TestParallelDownloadExactBytesBudgetAndEightWorkers(t *testing.T) {
	data := make([]byte, 13*64<<10)
	for i := range data {
		data[i] = byte(i * 17 % 251)
	}
	var active, peak atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("credentials forwarded to body node")
		}
		w.Header().Set("ETag", `"same-version"`)
		time.Sleep(10 * time.Millisecond)
		http.ServeContent(w, r, "test.bin", time.Time{}, bytes.NewReader(data))
	}))
	defer s.Close()
	r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("primary", s.URL, "")})
	r.parallel = true
	r.session.budget = newFairLimiter(4)
	got, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("download bytes differ", e)
	}
	if peak.Load() < 2 || peak.Load() > 4 {
		t.Fatal("parallel budget was not respected", peak.Load())
	}
}
func TestDownloadWorkerCountsEightAndSixteen(t *testing.T) {
	for _, threads := range []int{8, 16} {
		t.Run(fmt.Sprint(threads), func(t *testing.T) {
			data := make([]byte, threads*64<<10)
			var active, peak atomic.Int64
			gate := make(chan struct{})
			var once sync.Once
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") != "bytes=0-0" {
					n := active.Add(1)
					defer active.Add(-1)
					for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
					}
					if n == int64(threads) {
						once.Do(func() { close(gate) })
					}
					select {
					case <-gate:
					case <-r.Context().Done():
						return
					case <-time.After(3 * time.Second):
						t.Error("download tasks not concurrent")
						return
					}
				}
				w.Header().Set("ETag", `"v"`)
				http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(data))
			}))
			defer s.Close()
			r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("parallel", s.URL, "")})
			r.parallel = true
			r.threads = threads
			r.session.budget = newFairLimiter(threads)
			var got bytes.Buffer
			buffer := make([]byte, 32<<10)
			for {
				n, e := r.Read(buffer)
				got.Write(buffer[:n])
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
			}
			if !bytes.Equal(got.Bytes(), data) || peak.Load() != int64(threads) {
				t.Fatal("download tasks silently capped", peak.Load())
			}
		})
	}
}

func TestGUISelectedFileUsesRedundantReaderAndReceipt(t *testing.T) {
	data := []byte("ordinary text contents")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(data) }))
	defer s.Close()
	drive := &routeFixtureDrive{routes: []readRoute{fixtureRoute("download", s.URL, "")}}
	cl, _ := newClient(defaultConfig("mobile"), t.TempDir())
	entry := davEntry{Item: Item{ID: "selected", Name: "readme.txt", Size: int64(len(data))}}
	receipt, e := guiFetchRedundant(context.Background(), drive, newTransferRuntime(transferOptions{}), cl, entry, "saved.txt")
	if e != nil {
		t.Fatal(e)
	}
	if receipt.Bytes != int64(len(data)) || receipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal("GUI receipt mismatch")
	}
	if _, e = guiFetchRedundant(context.Background(), drive, nil, cl, davEntry{}, "unknown.txt"); e == nil {
		t.Fatal("unlisted file was accepted")
	}
}
func TestDownloadBodyErrorTriesNextRouteAndResumesOffset(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 60000)
	var first, second atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.WriteHeader(200)
		w.Write(data[:12345])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		conn, _, e := w.(http.Hijacker).Hijack()
		if e == nil {
			conn.Close()
		}
	}))
	defer bad.Close()
	var offset atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		var start int64
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start)
		offset.Store(start)
		w.Header().Set("ETag", `"v1"`)
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(data))
	}))
	defer good.Close()
	r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("bad", bad.URL, ""), fixtureRoute("good", good.URL, "")})
	r.parallel = false
	got, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("resume failed", e, len(got))
	}
	if first.Load() != 1 || second.Load() != 1 || offset.Load() != 12345 {
		t.Fatal("did not skip bad body route", first.Load(), second.Load(), offset.Load())
	}
}
func TestDownloadHTTPFailuresCooldownAndChangedVersionRejected(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var hits atomic.Int64
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); http.Error(w, "retry elsewhere", status) }))
			defer bad.Close()
			data := []byte("actual contents")
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"same"`)
				http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(data))
			}))
			defer good.Close()
			r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("unavailable", bad.URL, ""), fixtureRoute("available", good.URL, "")})
			got, e := io.ReadAll(r)
			if e != nil || !bytes.Equal(got, data) {
				t.Fatal(e)
			}
			if hits.Load() != 1 {
				t.Fatal("bad candidate was repeatedly selected")
			}
		})
	}
	data := bytes.Repeat([]byte("x"), 2000)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Content-Length", "2000")
		w.Write(data[:100])
		w.(http.Flusher).Flush()
		c, _, _ := w.(http.Hijacker).Hijack()
		c.Close()
	}))
	defer bad.Close()
	changed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"new"`)
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(data))
	}))
	defer changed.Close()
	r := fixtureReader(t, 2000, []readRoute{fixtureRoute("old", bad.URL, ""), fixtureRoute("new", changed.URL, "")})
	r.parallel = false
	got, e := io.ReadAll(r)
	if e == nil || len(got) >= len(data) {
		t.Fatal("mixed versions were reported successful", e, len(got))
	}
}
func TestNoRangeDownloadFallsBackAndExternalRangesDoNotAmplify(t *testing.T) {
	data := bytes.Repeat([]byte("n"), 400000)
	var ranged, full atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			ranged.Add(1)
		} else {
			full.Add(1)
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.Write(data)
	}))
	defer s.Close()
	r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("no-range", s.URL, "")})
	r.parallel = true
	got, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("single stream fallback failed", e)
	}
	if full.Load() != 1 || ranged.Load() != 1 {
		t.Fatal("unbounded range attempts", ranged.Load(), full.Load())
	}
	ctx := context.WithValue(context.Background(), davRequestKey{}, &davRequestState{method: "GET", externalRange: true})
	m := &davMount{drive: &routeFixtureDrive{routes: []readRoute{fixtureRoute("body", s.URL, "")}}}
	re := newTransferReader(ctx, m, davEntry{Item: Item{ID: "f", Size: 10 << 20}}, newTransferRuntime(transferOptions{}))
	defer re.Close()
	if re.parallel {
		t.Fatal("client Range spawned nested multipart downloads")
	}
}
func TestBadContentRangeAndAllCandidatesFailDoNotSucceed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 3-4/99")
		w.WriteHeader(206)
		w.Write([]byte("xx"))
	}))
	defer s.Close()
	r := fixtureReader(t, 10, []readRoute{fixtureRoute("wrong", s.URL, "")})
	r.parallel = false
	r.pos = 2
	if _, e := io.ReadAll(r); e == nil {
		t.Fatal("invalid Content-Range accepted")
	}
}

func TestDownloadCancellationReleasesPrefetchAndBudget(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"stable"`)
		var start, end int64
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, 1<<20))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(206)
		if start == 0 && end == 0 {
			w.Write([]byte{0})
			return
		}
		w.(http.Flusher).Flush()
		once.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer s.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	r := fixtureReader(t, 1<<20, []readRoute{fixtureRoute("stalled", s.URL, "id")})
	r.ctx = ctx
	r.session.ctx = ctx
	r.parallel = true
	r.session.budget = newFairLimiter(4)
	done := make(chan error, 1)
	go func() { _, e := io.Copy(io.Discard, r); r.Close(); done <- e }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}
	cancel()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("cancellation reported success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prefetch did not cancel")
	}
	if q := r.session.budget.status(); q["active"] != 0 || q["queued"] != 0 {
		t.Fatal("download budget leaked", q)
	}
}

func TestWopanPaginationAndSuspiciousEmptyResponseUseDirectListing(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(fmt.Sprint(omitted), func(t *testing.T) {
			cfg := defaultConfig("wopan")
			cfg.Transport = "bridge"
			cfg.Wopan.AccessToken = "fixture-token-012345678901234567"
			cl, _ := newClient(cfg, "")
			cl.bridge = failedProtocolBridge{}
			if omitted {
				cl.bridge = &rootVerifyingBridge{t: t, response: `{"code":0,"data":{"meta":{"code":"200"},"nextPage":false}}`}
			}
			d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
			var pages atomic.Int64
			d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
				var request map[string]any
				json.NewDecoder(r.Body).Decode(&request)
				crypto := wopan.NewCrypto()
				crypto.SetAccessToken(cfg.Wopan.AccessToken)
				raw, e := crypto.WoHomeDecrypt(valueString(object(request["body"])["param"]))
				if e != nil {
					t.Fatal(e)
				}
				var params map[string]any
				json.Unmarshal([]byte(raw), &params)
				page := int(number(params["pageNum"]))
				pages.Add(1)
				items := []any{}
				count := 100
				if page == 1 {
					count = 1
				}
				for i := 0; i < count; i++ {
					idx := page*100 + i
					items = append(items, map[string]any{"id": fmt.Sprint(idx), "fid": "fid" + fmt.Sprint(idx), "name": fmt.Sprintf("file-%03d.txt", idx), "size": idx + 1, "type": "1"})
				}
				data := map[string]any{"files": items, "systemDirs": []any{map[string]any{"id": "folder", "fid": "0", "name": "directory", "type": "0"}, map[string]any{"dirName": "系统入口", "dirType": "1", "icon": "navigation"}}}
				doc, _ := json.Marshal(map[string]any{"STATUS": "200", "RSP": map[string]any{"RSP_CODE": "0000", "DATA": data}})
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(doc)), Request: r}, nil
			})})
			entries, e := d.List(context.Background(), "0")
			if e != nil || len(entries) != 102 || pages.Load() != 2 {
				t.Fatal("pagination or empty-response verification failed", len(entries), pages.Load(), e)
			}
		})
	}
}
func TestSystemNavigationWithoutFilesIsNotConfirmedEmpty(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fixture-token-012345678901234567"
	cl, _ := newClient(cfg, "")
	cl.bridge = failedProtocolBridge{}
	d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":{"systemDirs":[{"dirName":"system","dirType":"1"}]}}}`)), Request: r}, nil
	})})
	if _, e := d.List(context.Background(), "0"); e == nil {
		t.Fatal("navigation metadata was reported as empty files")
	}
}

func TestOpenListCredentialStorageDoesNotExposeTokens(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}, nonce: "session", host: "localhost:123"}
	stored := openListSaved{openListInput: openListInput{Enabled: true, Address: "https://openlist.test", Root: "/cloud", Token: "openlist-private-token", MetaPassword: "directory-private-pass"}, Identity: "account", Verified: true}
	if e := g.saveOpenList("key", stored); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(g.data + string(os.PathSeparator) + "openlist-config.bin")
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(raw, []byte(stored.Token)) || bytes.Contains(raw, []byte(stored.MetaPassword)) {
		t.Fatal("OpenList credential file is plaintext")
	}
	loaded, e := g.openListConfig("key", nil, "account")
	if e != nil || loaded.Token != stored.Token || !loaded.Verified {
		t.Fatal("stored config did not recover", e)
	}
	reused, e := g.openListConfig("key", &openListInput{Enabled: true, Address: stored.Address, Root: stored.Root}, "account")
	if e != nil || reused.Token != stored.Token || !reused.Verified {
		t.Fatal("empty secret did not retain saved value")
	}
	changed, e := g.openListConfig("key", &openListInput{Enabled: true, Address: stored.Address, Root: "/different"}, "account")
	if e != nil || changed.Verified {
		t.Fatal("changed mapping reused verification")
	}
	public, _ := json.Marshal(g.publicStatus())
	if bytes.Contains(public, []byte(stored.Token)) || bytes.Contains(public, []byte(stored.MetaPassword)) {
		t.Fatal("status leaked external credentials")
	}
}

type failingBackupPrimary struct {
	*fakeDAVDrive
	failList                   bool
	uploadError, errorMutation error
	uploads                    atomic.Int64
}

func (d *failingBackupPrimary) List(c context.Context, p string) ([]davEntry, error) {
	if d.failList {
		return nil, errors.New("primary list unavailable")
	}
	return d.fakeDAVDrive.List(c, p)
}
func (d *failingBackupPrimary) Upload(c context.Context, p, n string, f *os.File) (davEntry, error) {
	d.uploads.Add(1)
	if d.uploadError != nil {
		return davEntry{}, d.uploadError
	}
	return d.fakeDAVDrive.Upload(c, p, n, f)
}
func (d *failingBackupPrimary) Remove(c context.Context, it davEntry) error {
	if d.errorMutation != nil {
		return d.errorMutation
	}
	return d.fakeDAVDrive.Remove(c, it)
}
func (d *failingBackupPrimary) Move(c context.Context, it davEntry, p, n string) error {
	if d.errorMutation != nil {
		return d.errorMutation
	}
	return d.fakeDAVDrive.Move(c, it, p, n)
}
func (d *failingBackupPrimary) Mkdir(c context.Context, p, n string) (davEntry, error) {
	if d.errorMutation != nil {
		return davEntry{}, d.errorMutation
	}
	return d.fakeDAVDrive.Mkdir(c, p, n)
}

func newOpenListFixture(t *testing.T, drive *fakeDAVDrive) (*openListClient, *atomic.Int64) {
	t.Helper()
	var writes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "fixture-openlist-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		var req map[string]any
		if r.Method != "PUT" {
			json.NewDecoder(r.Body).Decode(&req)
		}
		respond := func(data any) { respondJSON(w, 200, map[string]any{"code": 200, "data": data}) }
		resolve := func(p string) (davEntry, string, bool) {
			rel := strings.TrimPrefix(path.Clean(p), "/mount")
			if rel == "" || rel == "/" {
				return davEntry{Item: Item{ID: "0", Directory: true}}, "", true
			}
			parts := strings.Split(strings.Trim(rel, "/"), "/")
			parent := "0"
			var found davEntry
			for _, name := range parts {
				items, _ := drive.List(r.Context(), parent)
				ok := false
				for _, it := range items {
					if it.Name == name {
						found = it
						ok = true
						break
					}
				}
				if !ok {
					return davEntry{}, "", false
				}
				if name != parts[len(parts)-1] {
					parent = found.ID
				}
			}
			return found, parent, true
		}
		switch r.URL.Path {
		case "/api/fs/list":
			parent, _, ok := resolve(valueString(req["path"]))
			if !ok {
				respondJSON(w, 200, map[string]any{"code": 404})
				return
			}
			items, _ := drive.List(r.Context(), parent.ID)
			content := []map[string]any{}
			for _, it := range items {
				content = append(content, map[string]any{"name": it.Name, "size": it.Size, "is_dir": it.Directory, "modified": it.Modified.Format(time.RFC3339)})
			}
			respond(map[string]any{"content": content, "total": len(content)})
		case "/api/fs/get":
			it, _, ok := resolve(valueString(req["path"]))
			if !ok {
				respondJSON(w, 200, map[string]any{"code": 404})
				return
			}
			u, _ := drive.URL(r.Context(), it)
			respond(map[string]any{"name": it.Name, "size": it.Size, "is_dir": it.Directory, "raw_url": u})
		case "/api/fs/put":
			writes.Add(1)
			name := r.Header.Get("File-Path")
			parent, _, ok := resolve(path.Dir(name))
			if !ok {
				t.Error("unknown parent")
				return
			}
			f, _ := os.CreateTemp(t.TempDir(), "upload-*")
			defer f.Close()
			io.Copy(f, r.Body)
			if _, e := drive.Upload(r.Context(), parent.ID, path.Base(name), f); e != nil {
				t.Error(e)
			}
			respond(nil)
		case "/api/fs/mkdir":
			writes.Add(1)
			name := valueString(req["path"])
			p, _, _ := resolve(path.Dir(name))
			drive.Mkdir(r.Context(), p.ID, path.Base(name))
			respond(nil)
		case "/api/fs/remove":
			writes.Add(1)
			for _, name := range list(req["names"]) {
				it, _, ok := resolve(path.Join(valueString(req["dir"]), valueString(name)))
				if ok {
					drive.Remove(r.Context(), it)
				}
			}
			respond(nil)
		case "/api/fs/rename":
			writes.Add(1)
			it, _, ok := resolve(valueString(req["path"]))
			if !ok {
				t.Error("unknown rename source")
				return
			}
			drive.Move(r.Context(), it, "", valueString(req["name"]))
			respond(nil)
		case "/api/fs/move":
			writes.Add(1)
			dst, _, _ := resolve(valueString(req["dst_dir"]))
			for _, name := range list(req["names"]) {
				it, _, _ := resolve(path.Join(valueString(req["src_dir"]), valueString(name)))
				drive.Move(r.Context(), it, dst.ID, it.Name)
			}
			respond(nil)
		default:
			if strings.HasPrefix(r.URL.Path, "/p/") {
				it, _, ok := resolve(strings.TrimPrefix(r.URL.Path, "/p"))
				if !ok {
					http.NotFound(w, r)
					return
				}
				drive.mu.Lock()
				body := append([]byte(nil), drive.contents[it.ID]...)
				drive.mu.Unlock()
				w.Header().Set("ETag", `"proxy-version"`)
				http.ServeContent(w, r, it.Name, it.Modified, bytes.NewReader(body))
				return
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, e := newOpenListClient(openListInput{Enabled: true, Address: server.URL, Root: "/mount", Token: "fixture-openlist-token", VerifyWrite: true})
	if e != nil {
		t.Fatal(e)
	}
	return client, &writes
}
func TestOpenListMappingVerificationAndAutomaticReadWriteFallback(t *testing.T) {
	base := newFakeDAV(t)
	api, writes := newOpenListFixture(t, base)
	if e := verifyOpenList(context.Background(), base, "0", api, t.TempDir()); e != nil {
		t.Fatal(e)
	}
	if entries, _ := base.List(context.Background(), "0"); len(entries) != 0 {
		t.Fatal("mapping proof not cleaned up")
	}
	primary := &failingBackupPrimary{fakeDAVDrive: base, failList: true, uploadError: &davNotExecuted{errors.New("500020001 rejected before execution")}, errorMutation: &davNotExecuted{errors.New("500020001 rejected before execution")}}
	b := &backupDAVDrive{primary: primary, backup: api, root: "0", paths: map[string]string{"0": ""}, entries: map[string]davEntry{}}
	f, _ := os.CreateTemp(t.TempDir(), "body-*")
	defer f.Close()
	f.Write([]byte("ordinary non-video contents"))
	entry, e := b.Upload(context.Background(), "0", "readme.txt", f)
	if e != nil {
		t.Fatal(e)
	}
	items, e := b.List(context.Background(), "0")
	if e != nil || len(items) != 1 || items[0].Name != "readme.txt" {
		t.Fatal("list fallback", e)
	}
	r := newTransferReader(context.Background(), &davMount{drive: b}, entry, newTransferRuntime(transferOptions{}))
	defer r.Close()
	raw, e := io.ReadAll(r)
	if e != nil || string(raw) != "ordinary non-video contents" {
		t.Fatal("content fallback", e)
	}
	if e = b.Move(context.Background(), entry, "", "renamed.txt"); e != nil {
		t.Fatal(e)
	}
	if e = b.Remove(context.Background(), entry); e != nil {
		t.Fatal(e)
	}
	if got, _ := base.List(context.Background(), "0"); len(got) != 0 {
		t.Fatal("rename mapping removed wrong entry")
	}
	if writes.Load() != 3 {
		t.Fatal("unexpected API writes", writes.Load())
	}
}
func TestOpenListUnknownWriteOutcomeIsNotReplayed(t *testing.T) {
	base := newFakeDAV(t)
	api, writes := newOpenListFixture(t, base)
	primary := &failingBackupPrimary{fakeDAVDrive: base, uploadError: errors.New("unexpected EOF"), errorMutation: errors.New("context deadline exceeded")}
	b := &backupDAVDrive{primary: primary, backup: api, root: "0", paths: map[string]string{"0": "", "i": "target.txt"}, entries: map[string]davEntry{}}
	f, _ := os.CreateTemp(t.TempDir(), "body-*")
	defer f.Close()
	f.Write([]byte("data"))
	if _, e := b.Upload(context.Background(), "0", "stage.txt", f); e == nil {
		t.Fatal("unknown upload success")
	}
	if e := b.Remove(context.Background(), davEntry{Item: Item{ID: "i", Name: "target.txt"}}); e == nil {
		t.Fatal("unknown delete success")
	}
	if writes.Load() != 0 {
		t.Fatal("uncertain operation replayed")
	}
}

func TestOpenListProxyBodyUsedWhenRawURLFails(t *testing.T) {
	base := newFakeDAV(t)
	api, _ := newOpenListFixture(t, base)
	f, _ := os.CreateTemp(t.TempDir(), "file-*")
	defer f.Close()
	f.Write([]byte("proxy recovered contents"))
	entry, e := base.Upload(context.Background(), "0", "proof.txt", f)
	if e != nil {
		t.Fatal(e)
	}
	b := &backupDAVDrive{primary: base, backup: api, paths: map[string]string{"0": "", entry.ID: "proof.txt"}, entries: map[string]davEntry{}}
	routes := b.ReadRoutes(context.Background(), entry)
	routes = routes[len(routes)-2:]
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad raw URL", 503) }))
	defer bad.Close()
	routes[0] = fixtureRoute("OpenList 服务", bad.URL, "")
	r := fixtureReader(t, entry.Size, routes)
	got, e := io.ReadAll(r)
	if e != nil || string(got) != "proxy recovered contents" {
		t.Fatal("OpenList proxy fallback failed", e, string(got))
	}
}
func TestDownloadRedirectNeverForwardsOpenListToken(t *testing.T) {
	var leaked atomic.Bool
	body := []byte("safe download")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		w.Write(body)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "private-openlist" {
			t.Error("trusted source token not applied")
		}
		http.Redirect(w, r, target.URL, 302)
	}))
	defer origin.Close()
	r := fixtureReader(t, int64(len(body)), []readRoute{{Name: "OpenList 代理正文", Resolve: func(context.Context) (readTarget, error) {
		return readTarget{URL: origin.URL, Headers: http.Header{"Authorization": []string{"private-openlist"}}}, nil
	}}})
	got, e := io.ReadAll(r)
	if e != nil || !bytes.Equal(got, body) || leaked.Load() {
		t.Fatal("redirect credential boundary failed", e)
	}
}
func TestOpenListRejectsUnknownListingsAndCrossOriginCredentialRedirect(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		respondJSON(w, 200, map[string]any{"code": 200, "data": map[string]any{"content": []any{}, "total": 0}})
	}))
	defer other.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/fs/list" {
			http.Redirect(w, r, other.URL, 307)
			return
		}
		respondJSON(w, 200, map[string]any{"code": 200, "data": map[string]any{"unknown": true}})
	}))
	defer s.Close()
	c, _ := newOpenListClient(openListInput{Address: s.URL, Root: "/", Token: "private"})
	if _, e := c.listing(context.Background(), ""); e == nil {
		t.Fatal("cross-origin redirect accepted")
	}
	if leaked.Load() {
		t.Fatal("API token forwarded")
	}
	unknown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{"code": 200, "data": map[string]any{"unknown": true}})
	}))
	defer unknown.Close()
	c, _ = newOpenListClient(openListInput{Address: unknown.URL, Root: "/"})
	if _, e := c.listing(context.Background(), ""); e == nil {
		t.Fatal("unknown listing treated as empty")
	}
}

func TestConcurrentFilesQueueButMetadataDoesNotWait(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	fs.transfers = newTransferRuntime(transferOptions{ConcurrentUploads: 2, ConcurrentDownloads: 2, TotalConnections: 8, UploadStorageMode: "disk"})
	stalled := &countedStalledUploads{fakeDAVDrive: base, started: make(chan struct{}, 8), release: make(chan struct{})}
	fs.mounts[0].drive = stalled
	client := &http.Client{Timeout: 5 * time.Second}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			u := server.URL + "/dav/" + "%E8%81%94%E9%80%9A%E4%BA%91%E7%9B%98-1" + fmt.Sprintf("/f%d.bin", i)
			r, _ := http.NewRequest("PUT", u, strings.NewReader("body"))
			r.SetBasicAuth("tester", "test-password-123")
			resp, e := client.Do(r)
			if e != nil {
				errs <- e
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 201 {
				errs <- fmt.Errorf("PUT %d", resp.StatusCode)
			}
		})
	}
	for i := 0; i < 2; i++ {
		select {
		case <-stalled.started:
		case <-time.After(time.Second):
			close(stalled.release)
			t.Fatal("file scheduler did not start")
		}
	}
	deadline := time.Now().Add(time.Second)
	for fs.transfers.uploads.limiter.status()["queued"] != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if q := fs.transfers.uploads.limiter.status(); q["active"] != 2 || q["queued"] != 2 {
		close(stalled.release)
		t.Fatal("file queue incorrect", q)
	}
	wantDAV(t, server.URL, "PROPFIND", "/联通云盘-1", "", 207, map[string]string{"Depth": "1"})
	close(stalled.release)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if fs.transfers.uploads.limiter.status()["active"] != 0 {
		t.Fatal("file lease leaked")
	}
}

type countedStalledUploads struct {
	*fakeDAVDrive
	started chan struct{}
	release chan struct{}
}

type mobileUploadBudgetBridge struct{ endpoint string }

func (b mobileUploadBudgetBridge) Request(context.Context, string, string, []byte, map[string]string) ([]byte, error) {
	return json.Marshal(map[string]any{"code": 0, "data": map[string]any{"resultCode": "0", "uploadResult": map[string]any{"newContentIDList": map[string]any{"newContent": []any{map[string]any{"contentID": "new", "contentName": "test.bin", "isNeedUpload": "1"}}}, "redirectionUrl": b.endpoint, "uploadTaskID": "task"}}})
}
func TestMobileUploadUsesSharedBodyBudgetAndCancelsWait(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.Write([]byte("unexpected")) }))
	defer server.Close()
	cfg := defaultConfig("mobile")
	cfg.Transport = "bridge"
	cfg.Mobile = MobileConfig{AppID: "fixture", AppSecret: "fixture-secret", AccessToken: "fixture-token", DeviceID: "device"}
	cl, _ := newClient(cfg, "")
	cl.bridge = mobileUploadBudgetBridge{endpoint: server.URL}
	budget := newFairLimiter(1)
	release, _ := budget.acquire(context.Background())
	defer release()
	d := &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}, budget: budget, partMiB: 5}
	f, _ := os.CreateTemp(t.TempDir(), "mobile-*")
	defer f.Close()
	f.Write([]byte("sample"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, e := d.Upload(ctx, "root", "test.bin", f); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("mobile did not cancel shared budget wait", e)
	}
	if hits.Load() != 0 || budget.status()["active"] != 1 {
		t.Fatal("mobile bypassed or released another transfer's budget")
	}
}

func (d *countedStalledUploads) Upload(ctx context.Context, p, n string, f *os.File) (davEntry, error) {
	d.started <- struct{}{}
	select {
	case <-d.release:
	case <-ctx.Done():
		return davEntry{}, ctx.Err()
	}
	return d.fakeDAVDrive.Upload(ctx, p, n, f)
}

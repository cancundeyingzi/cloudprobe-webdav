package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

type fakeDAVDrive struct {
	mu          sync.Mutex
	entries     map[string]davEntry
	parents     map[string]string
	contents    map[string][]byte
	seq         int
	server      *httptest.Server
	failUpload  bool
	ignoreRange bool
}

func newFakeDAV(t *testing.T) *fakeDAVDrive {
	d := &fakeDAVDrive{entries: map[string]davEntry{}, parents: map[string]string{}, contents: map[string][]byte{}}
	d.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		b, ok := d.contents[strings.TrimPrefix(r.URL.Path, "/")]
		d.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		if d.ignoreRange {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(b)
			return
		}
		http.ServeContent(w, r, "file.bin", time.Unix(100000, 0), bytes.NewReader(b))
	}))
	t.Cleanup(d.server.Close)
	return d
}
func (d *fakeDAVDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []davEntry{}
	for id, it := range d.entries {
		if d.parents[id] == parent {
			out = append(out, it)
		}
	}
	return out, nil
}
func (d *fakeDAVDrive) URL(ctx context.Context, it davEntry) (string, error) {
	return d.server.URL + "/" + it.ID, nil
}
func (d *fakeDAVDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	id := fmt.Sprint(d.seq)
	it := davEntry{Item: Item{ID: id, Name: name, Directory: true}, Modified: time.Now()}
	d.entries[id] = it
	d.parents[id] = parent
	return it, nil
}
func (d *fakeDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	if d.failUpload {
		return davEntry{}, errors.New("provider rejected upload")
	}
	f.Seek(0, 0)
	b, e := io.ReadAll(f)
	if e != nil {
		return davEntry{}, e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	id := fmt.Sprint(d.seq)
	it := davEntry{Item: Item{ID: id, FID: "fid-" + id, Name: name, Size: int64(len(b))}, Modified: time.Now()}
	d.entries[id] = it
	d.parents[id] = parent
	d.contents[id] = b
	return it, nil
}
func (d *fakeDAVDrive) Remove(ctx context.Context, it davEntry) error {
	d.mu.Lock()
	children := []davEntry{}
	for id, v := range d.entries {
		if d.parents[id] == it.ID {
			children = append(children, v)
		}
	}
	delete(d.entries, it.ID)
	delete(d.contents, it.ID)
	delete(d.parents, it.ID)
	d.mu.Unlock()
	for _, c := range children {
		d.Remove(ctx, c)
	}
	return nil
}
func (d *fakeDAVDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.entries[it.ID]
	if !ok {
		return os.ErrNotExist
	}
	v.Name = name
	d.entries[it.ID] = v
	if parent != "" {
		d.parents[it.ID] = parent
	}
	return nil
}

func testDAVService(t *testing.T, writable bool) (*httptest.Server, *fakeDAVDrive, *davFS) {
	d := newFakeDAV(t)
	fs := &davFS{mounts: []*davMount{{Alias: "联通云盘-1", Root: "0", Provider: "wopan", Writable: writable, drive: d}}, spool: t.TempDir(), maxUpload: 64 << 20, cache: map[string]davCache{}}
	fs.transfers = newTransferRuntime(transferOptions{UploadStorageMode: "disk"})
	salt := []byte("1234567890123456")
	hash, _ := pbkdf2.Key(sha256.New, "test-password-123", salt, 100000, 32)
	s := &davService{fs: fs, handler: &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS()}, username: "tester", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash), slots: make(chan struct{}, 8)}
	server := httptest.NewServer(s)
	t.Cleanup(server.Close)
	return server, d, fs
}
func davRequest(t *testing.T, server, method, target string, body io.Reader, headers map[string]string) (int, []byte, http.Header) {
	t.Helper()
	u, _ := url.Parse(server)
	u.Path = "/dav" + target
	r, e := http.NewRequest(method, u.String(), body)
	if e != nil {
		t.Fatal(e)
	}
	r.SetBasicAuth("tester", "test-password-123")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	return resp.StatusCode, b, resp.Header
}
func wantDAV(t *testing.T, server, method, target, body string, want int, headers map[string]string) []byte {
	t.Helper()
	code, b, _ := davRequest(t, server, method, target, strings.NewReader(body), headers)
	if code != want {
		t.Fatalf("%s %s returned %d, want %d: %s", method, target, code, want, b)
	}
	return b
}
func TestWebDAVClientCRUDAndUnicode(t *testing.T) {
	s, d, _ := testDAVService(t, true)
	root := "/联通云盘-1"
	wantDAV(t, s.URL, "OPTIONS", "/", "", 200, nil)
	b := wantDAV(t, s.URL, "PROPFIND", "/", "", 207, map[string]string{"Depth": "1"})
	if !bytes.Contains(b, []byte("%E8%81%94")) && !bytes.Contains(b, []byte("联通云盘")) {
		t.Fatal("root mount missing")
	}
	wantDAV(t, s.URL, "MKCOL", root+"/测试目录", "", 201, nil)
	wantDAV(t, s.URL, "PUT", root+"/测试目录/说明 & 100%.txt", "第一版中文文本", 201, nil)
	b = wantDAV(t, s.URL, "GET", root+"/测试目录/说明 & 100%.txt", "", 200, nil)
	if string(b) != "第一版中文文本" {
		t.Fatal("content differs")
	}
	wantDAV(t, s.URL, "PUT", root+"/测试目录/说明 & 100%.txt", "保存后的第二版", 201, nil)
	b = wantDAV(t, s.URL, "GET", root+"/测试目录/说明 & 100%.txt", "", 200, nil)
	if string(b) != "保存后的第二版" {
		t.Fatal("overwrite did not commit")
	}
	dest := s.URL + "/dav" + root + "/测试目录/改名.txt"
	u, _ := url.Parse(dest)
	wantDAV(t, s.URL, "MOVE", root+"/测试目录/说明 & 100%.txt", "", 201, map[string]string{"Destination": u.String()})
	u, _ = url.Parse(s.URL + "/dav" + root + "/测试目录/副本.txt")
	wantDAV(t, s.URL, "COPY", root+"/测试目录/改名.txt", "", 201, map[string]string{"Destination": u.String()})
	b = wantDAV(t, s.URL, "GET", root+"/测试目录/副本.txt", "", 200, nil)
	if string(b) != "保存后的第二版" {
		t.Fatal("copy differs")
	}
	wantDAV(t, s.URL, "DELETE", root+"/测试目录/副本.txt", "", 204, nil)
	wantDAV(t, s.URL, "DELETE", root+"/测试目录", "", 204, nil)
	a, _ := d.List(context.Background(), "0")
	if len(a) != 0 {
		t.Fatal("cloud entries survived removal")
	}
}
func TestWebDAVRangeHeadAndLargeFiles(t *testing.T) {
	s, _, _ := testDAVService(t, true)
	file := "/联通云盘-1/video.bin"
	data := bytes.Repeat([]byte("0123456789abcdef"), 800000) // exceeds the old 10 MiB verification limit
	code, b, _ := davRequest(t, s.URL, "PUT", file, bytes.NewReader(data), nil)
	if code != 201 {
		t.Fatalf("upload: %d %s", code, b)
	}
	code, b, h := davRequest(t, s.URL, "HEAD", file, nil, nil)
	if code != 200 || len(b) != 0 || h.Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatal("HEAD metadata incorrect", code, h)
	}
	code, b, h = davRequest(t, s.URL, "GET", file, nil, map[string]string{"Range": "bytes=10485770-10485869"})
	if code != 206 || !bytes.Equal(b, data[10485770:10485870]) || h.Get("Content-Range") == "" {
		t.Fatalf("range: %d %s", code, b)
	}
	code, b, _ = davRequest(t, s.URL, "GET", file, nil, map[string]string{"Range": "bytes=-31"})
	if code != 206 || !bytes.Equal(b, data[len(data)-31:]) {
		t.Fatal("suffix range incorrect")
	}
	wantDAV(t, s.URL, "GET", file, "", 416, map[string]string{"Range": fmt.Sprintf("bytes=%d-", len(data)+1)})
}
func TestWebDAVRejectedUploadPreservesOriginal(t *testing.T) {
	s, d, fs := testDAVService(t, true)
	file := "/联通云盘-1/original.txt"
	wantDAV(t, s.URL, "PUT", file, "keep original", 201, nil)
	d.failUpload = true
	b := wantDAV(t, s.URL, "PUT", file, "replacement", 502, nil)
	if !bytes.Contains(b, []byte("provider rejected upload")) {
		t.Fatal("upstream error lost")
	}
	fs.invalidate(fs.mounts[0])
	b = wantDAV(t, s.URL, "GET", file, "", 200, nil)
	if string(b) != "keep original" {
		t.Fatal("original was changed by failed upload")
	}
	files, _ := os.ReadDir(fs.spool)
	if len(files) != 0 {
		t.Fatal("local spool was not removed")
	}
}

type failingDAVBody struct{ read bool }

func (r *failingDAVBody) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.ErrUnexpectedEOF
	}
	r.read = true
	return copy(p, []byte("incomplete")), nil
}
func (r *failingDAVBody) Close() error { return nil }
func TestWebDAVInterruptedUploadNeverCommits(t *testing.T) {
	s, d, fs := testDAVService(t, true)
	serverHandler := s.Config.Handler
	r := httptest.NewRequest("PUT", s.URL+"/dav/联通云盘-1/incomplete.txt", nil)
	r.SetBasicAuth("tester", "test-password-123")
	r.Body = &failingDAVBody{}
	r.ContentLength = 100
	w := httptest.NewRecorder()
	serverHandler.ServeHTTP(w, r)
	if w.Code < 400 {
		t.Fatal("interrupted upload reported success")
	}
	a, _ := d.List(context.Background(), "0")
	if len(a) != 0 {
		t.Fatal("incomplete data reached cloud")
	}
	files, _ := os.ReadDir(fs.spool)
	if len(files) != 0 {
		t.Fatal("spool leaked")
	}
}
func TestWebDAVAuthenticationReadOnlyAndBoundaries(t *testing.T) {
	s, _, _ := testDAVService(t, false)
	r, _ := http.NewRequest("PROPFIND", s.URL+"/dav/", nil)
	r.Header.Set("Depth", "1")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("missing authentication challenge")
	}
	for _, method := range []string{"PUT", "MKCOL", "DELETE", "LOCK", "PROPPATCH"} {
		wantDAV(t, s.URL, method, "/联通云盘-1/a.txt", "", 403, nil)
	}
	wantDAV(t, s.URL, "PROPFIND", "/", "", 403, map[string]string{"Depth": "infinity"})
	wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1/../private", "", 400, map[string]string{"Depth": "0"})
	wantDAV(t, s.URL, "MOVE", "/联通云盘-1/a.txt", "", 400, map[string]string{"Destination": "http://external.invalid/dav/a"})
	wantDAV(t, s.URL, "COPY", "/联通云盘-1/a.txt", "", 501, map[string]string{"Destination": s.URL + "/dav/other/b"})
}
func TestWebDAVLocksAndConditionalSave(t *testing.T) {
	s, _, _ := testDAVService(t, true)
	file := "/联通云盘-1/locked.txt"
	wantDAV(t, s.URL, "PUT", file, "one", 201, nil)
	code, b, h := davRequest(t, s.URL, "LOCK", file, strings.NewReader(`<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype><d:owner>client</d:owner></d:lockinfo>`), map[string]string{"Depth": "0", "Timeout": "Second-600"})
	if code != 200 || h.Get("Lock-Token") == "" {
		t.Fatalf("lock: %d %s", code, b)
	}
	token := h.Get("Lock-Token")
	wantDAV(t, s.URL, "PUT", file, "blocked", 423, nil)
	wantDAV(t, s.URL, "PUT", file, "two", 201, map[string]string{"If": "(" + token + ")"})
	wantDAV(t, s.URL, "UNLOCK", file, "", 204, map[string]string{"Lock-Token": token})
	wantDAV(t, s.URL, "PUT", file, "stale", 412, map[string]string{"If-Match": `"wrong-etag"`})
	wantDAV(t, s.URL, "PUT", file, "duplicate", 412, map[string]string{"If-None-Match": "*"})
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/new.txt", "new", 201, map[string]string{"If-None-Match": "*"})
}
func TestWebDAVUploadLimitsAndNoFalseRange(t *testing.T) {
	s, d, fs := testDAVService(t, true)
	fs.maxUpload = 16
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/too-big.txt", strings.Repeat("x", 17), 413, nil)
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/a.txt", "0123456789", 201, nil)
	d.ignoreRange = true
	reader := &davReader{ctx: context.Background(), size: 10, pos: 5, url: func() (string, error) {
		entries, _ := d.List(context.Background(), "0")
		return d.server.URL + "/" + entries[0].ID, nil
	}}
	defer reader.Close()
	if _, e := reader.Read(make([]byte, 3)); e == nil {
		t.Fatal("ignored upstream Range must fail")
	}
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/a.txt", "xx", 501, map[string]string{"Content-Range": "bytes 0-1/10"})
}
func TestWebDAVPrivateCredentialProtection(t *testing.T) {
	plain := []byte(`{"token":"private-test-token"}`)
	protected, e := protectLocal(plain)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := unprotectLocal(protected)
	if e != nil || !bytes.Equal(decoded, plain) {
		t.Fatal("local protection did not round trip")
	}
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": {Key: "a", Provider: "wopan", credentials: map[string]any{"user_id": "user-1", "token": "old"}}}}
	if e = g.saveDAVCredential(g.accounts["a"], "access-private", "refresh-private"); e != nil {
		t.Fatal(e)
	}
	next := map[string]guiAccount{"b": {Key: "b", Provider: "wopan", credentials: map[string]any{"user_id": "user-1", "token": "old"}}}
	g.applyDAVCredentials(next)
	if next["b"].credentials["token"] != "access-private" {
		t.Fatal("refreshed credentials not restored")
	}
	if strings.Contains(g.redactDAV("access-private refresh-private"), "private") {
		t.Fatal("refresh secrets leaked")
	}
	if _, e = os.Stat(filepath.Join(g.data, "webdav-credentials.bin")); e != nil {
		t.Fatal(e)
	}
}

func TestWebDAVFailedCopyRestoresOverwrittenDestination(t *testing.T) {
	s, d, _ := testDAVService(t, true)
	source, destination := "/联通云盘-1/source.txt", "/联通云盘-1/destination.txt"
	wantDAV(t, s.URL, "PUT", source, "source content", 201, nil)
	wantDAV(t, s.URL, "PUT", destination, "destination original", 201, nil)
	d.failUpload = true
	u, _ := url.Parse(s.URL)
	u.Path = "/dav" + destination
	wantDAV(t, s.URL, "COPY", source, "", 502, map[string]string{"Destination": u.String(), "Overwrite": "T"})
	got := wantDAV(t, s.URL, "GET", destination, "", 200, nil)
	if string(got) != "destination original" {
		t.Fatal("failed copy lost destination original")
	}
	got = wantDAV(t, s.URL, "GET", source, "", 200, nil)
	if string(got) != "source content" {
		t.Fatal("failed copy changed source")
	}
}

func TestWebDAVSuccessfulMoveOverwriteCleansBackup(t *testing.T) {
	s, d, _ := testDAVService(t, true)
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/source.txt", "source", 201, nil)
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/dest.txt", "old", 201, nil)
	u, _ := url.Parse(s.URL)
	u.Path = "/dav/联通云盘-1/dest.txt"
	wantDAV(t, s.URL, "MOVE", "/联通云盘-1/source.txt", "", 204, map[string]string{"Destination": u.String(), "Overwrite": "T"})
	got := wantDAV(t, s.URL, "GET", "/联通云盘-1/dest.txt", "", 200, nil)
	if string(got) != "source" {
		t.Fatal("move content incorrect")
	}
	a, _ := d.List(context.Background(), "0")
	if len(a) != 1 {
		t.Fatal("move retained unnecessary backup")
	}
}

func TestWopanCookieParsingAndAccountMatching(t *testing.T) {
	for _, raw := range []string{"WoCloud-Web-Token=abc%2Bdef%3D; other=value", `WoCloud-Web-Token="abc%2Bdef%3D"`, "abc%2Bdef%3D"} {
		v, e := webTokenFromInput(raw)
		if e != nil || v != "abc+def=" {
			t.Fatal("cookie normalization failed", e)
		}
	}
	if _, e := webTokenFromInput("other=private"); e == nil {
		t.Fatal("unrelated cookies accepted")
	}
	cred := map[string]any{"mobile": "+8613800000000", "user_id": "opaque-id"}
	if !sameWopanAccount(cred, "13800000000") || sameWopanAccount(cred, "13900000000") {
		t.Fatal("account identity guard incorrect")
	}
}

func TestUpdatedServerCredentialsAreNotOverwrittenByOldOverride(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": {Provider: "wopan", credentials: map[string]any{"user_id": "uid", "token": "original"}}}}
	if e := g.saveDAVCredential(cloneGUIAccount(g.accounts["a"]), "refreshed", "refresh"); e != nil {
		t.Fatal(e)
	}
	if e := g.saveDAVWebCredential(cloneGUIAccount(g.accounts["a"]), "website-private-token"); e != nil {
		t.Fatal(e)
	}
	newAccounts := map[string]guiAccount{"a": {Provider: "wopan", credentials: map[string]any{"user_id": "uid", "token": "new-server-token"}}}
	g.applyDAVCredentials(newAccounts)
	if newAccounts["a"].credentials["token"] != "new-server-token" {
		t.Fatal("new server credentials overwritten")
	}
	if newAccounts["a"].credentials["wopan_web_token"] != "website-private-token" || !newAccounts["a"].WebBound {
		t.Fatal("independent website fallback lost")
	}
}

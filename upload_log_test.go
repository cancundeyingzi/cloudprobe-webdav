package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelWopanExactBytesRetryAndGlobalBound(t *testing.T) {
	const partSize = 64 << 10
	data := make([]byte, 8*partSize+123)
	for i := range data {
		data[i] = byte((i/partSize)*29 + i%251)
	}
	f, err := os.CreateTemp(t.TempDir(), "spool-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.Write(data)
	var active, peak atomic.Int64
	var mu sync.Mutex
	seen := map[int][]byte{}
	attempts := map[int]int{}
	gate := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if n == 2 {
			once.Do(func() { close(gate) })
		}
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
		if r.ContentLength <= 0 || len(r.TransferEncoding) > 0 {
			t.Error("missing fixed length")
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			http.Error(w, "multipart", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("uniqueId") != "stable-session" || r.FormValue("fileInfo") != "stable-encrypted-info" {
			t.Error("session changed across concurrent/retried parts")
		}
		index, _ := strconv.Atoi(r.FormValue("partIndex"))
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			http.Error(w, "file", 400)
			return
		}
		body, _ := io.ReadAll(file)
		file.Close()
		mu.Lock()
		attempts[index]++
		attempt := attempts[index]
		seen[index] = body
		mu.Unlock()
		if index == 2 && attempt == 1 {
			http.Error(w, "transient", 503)
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"complete-fid"}}`)
	}))
	defer srv.Close()
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	ctx, cancel := context.WithTimeout(newRunTrace(context.Background(), l, "PUT", "/mount/test.bin"), 10*time.Second)
	defer cancel()
	u := &wopanUploadSession{endpoint: srv.URL, access: "unused", name: "stage.bin", parent: "0", encrypted: "stable-encrypted-info", unique: "stable-session", source: "联通网页授权", size: int64(len(data)), partSize: partSize, concurrency: 4, slots: make(chan struct{}, 2), client: srv.Client()}
	fid, err := u.run(ctx, f)
	if err != nil || fid != "complete-fid" {
		t.Fatalf("upload result %q %v", fid, err)
	}
	if peak.Load() != 2 {
		t.Fatalf("global concurrency bound: %d", peak.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	var joined []byte
	for i := 1; i <= 9; i++ {
		joined = append(joined, seen[i]...)
	}
	if !bytes.Equal(joined, data) || attempts[2] != 2 {
		t.Fatal("part offsets, final short part or retry incorrect")
	}
	tr := ctx.Value(runTraceKey{}).(*runTrace)
	if tr.confirmed.Load() != int64(len(data)) || tr.sent.Load() != int64(len(data)+partSize) {
		t.Fatal("retry byte accounting incorrect")
	}
}

func TestParallelWopanFatalErrorCancelsSiblings(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "spool-*")
	defer f.Close()
	f.Write(make([]byte, 512<<10))
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"code":"500020001","msg":"no permission"}`)
	}))
	defer srv.Close()
	u := &wopanUploadSession{endpoint: srv.URL, name: "stage.bin", source: "联通网页授权", size: 512 << 10, partSize: 64 << 10, concurrency: 4, slots: make(chan struct{}, 8), client: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := u.run(ctx, f)
	if err == nil || !strings.Contains(err.Error(), "500020001") {
		t.Fatalf("fatal error was hidden: %v", err)
	}
	if requests.Load() > 4 {
		t.Fatal("fatal failure started additional parts or replayed upload")
	}
}

func TestParallelWopanCanceledQueueDoesNotSend(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "spool-*")
	defer f.Close()
	f.Write([]byte("sample"))
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u := &wopanUploadSession{endpoint: "http://127.0.0.1:1", name: "stage.bin", size: 6, partSize: 3, concurrency: 2, slots: slots, client: http.DefaultClient}
	if _, err := u.run(ctx, f); err == nil {
		t.Fatal("canceled upload reported success")
	}
	if len(slots) != 1 {
		t.Fatal("borrowed another upload's slot")
	}
}

func TestRuntimeLogPersistenceRedactionAndPagination(t *testing.T) {
	r := &Redactor{}
	r.Add("private-user-token")
	l := newRuntimeLog(t.TempDir(), r.Text)
	defer l.close()
	for i := 0; i < 815; i++ {
		l.add(runLogEntry{Operation: "读取目录", Source: "网易代理", Result: "成功", Path: "/mount/readme.txt", Detail: "private-user-token https://node.test/opaque-signature/file?key=very-private", Items: []runLogItem{{Name: "说明.txt", Size: 123}}})
	}
	b, err := os.ReadFile(l.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-user-token", "opaque-signature", "very-private"} {
		if bytes.Contains(b, []byte(secret)) {
			t.Fatalf("runtime log leaked %s", secret)
		}
	}
	if len(bytes.Split(bytes.TrimSpace(b), []byte("\n"))) != 815 || len(l.entries) != 800 {
		t.Fatal("persistence or memory bound incorrect")
	}
	g := &guiApp{host: "localhost:12345", nonce: "ui-nonce", runLog: l}
	req := httptest.NewRequest("GET", "http://localhost:12345/api/run-log?after=0", nil)
	req.Header.Set("X-CloudProbe-UI", g.nonce)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	var response struct {
		Entries []runLogEntry `json:"entries"`
		Gap     bool          `json:"gap"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &response); err != nil || !response.Gap || len(response.Entries) != 300 || response.Entries[0].ID != 16 {
		t.Fatal("incremental API lost bounded history information", err)
	}
	req = httptest.NewRequest("GET", "http://localhost:12345/api/run-log/export", nil)
	req.Header.Set("X-CloudProbe-UI", g.nonce)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), b) {
		t.Fatal("export omitted older records")
	}
	req = httptest.NewRequest("GET", "http://localhost:12345/api/run-log/export", nil)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("export did not require local UI session")
	}
}

func TestRuntimeLogAssociatesRequestAndDirectoryItems(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	ctx := scopedDAV(newRunTrace(context.Background(), l, "PROPFIND", "/mount/parent/file"), &davMount{Alias: "mount", Provider: "wopan"}, "")
	logList(directoryLogContext(ctx), "网易代理", "query_files", time.Now(), []davEntry{{Item: Item{Name: "readme.txt", Size: 12}}, {Item: Item{Name: "nested", Directory: true}}}, nil)
	logURL(ctx, "网易代理", "get_download_url", time.Now(), "https://node.test/private-signature?token=hidden", nil)
	if len(l.entries) != 2 {
		t.Fatal("events missing")
	}
	if e := l.entries[0]; e.Path != "/mount/parent" || e.Count != 2 || len(e.Items) != 2 || e.RequestID == "" || e.Mount != "mount" {
		t.Fatal("directory association incorrect", e)
	}
	if e := l.entries[1]; e.Source != "网易代理" || !strings.Contains(e.Detail, "node.test") || strings.Contains(e.Detail, "private-signature") {
		t.Fatal("download URL incorrectly exposed or attributed", e)
	}
}

func TestWebDAVRuntimeSummaryCountsUploadBody(t *testing.T) {
	s, _, _ := testDAVService(t, true)
	service := s.Config.Handler.(*davService)
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	service.manager = &davManager{app: &guiApp{runLog: l, accounts: map[string]guiAccount{}}}
	data := "actual-upload-content-123456789"
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/readme.txt", data, 201, nil)
	service.manager.mu.Lock()
	activity := append([]davActivity{}, service.manager.activity...)
	service.manager.mu.Unlock()
	if len(activity) != 1 || activity[0].ReceivedBytes != int64(len(data)) || activity[0].Bytes == int64(len(data)) || activity[0].RequestID == "" {
		t.Fatal("PUT was measured using response length", activity)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	found := false
	for _, e := range l.entries {
		if e.Operation == "WebDAV 请求" && e.Result == "201" {
			found = true
			if e.ReceivedBytes != int64(len(data)) || e.RequestID != activity[0].RequestID {
				t.Fatal("summary bytes or correlation incorrect")
			}
		}
	}
	if !found {
		t.Fatal("no runtime summary")
	}
}

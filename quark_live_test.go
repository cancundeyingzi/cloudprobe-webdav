package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLiveQuarkWebDAVCRUD(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK") != "1" {
		t.Skip("设置 CLOUDPROBE_LIVE_QUARK 后在专用测试目录实测夸克")
	}
	profile := os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE")
	if profile == "" {
		t.Fatal("missing isolated profile")
	}
	app := &guiApp{data: profile, accounts: map[string]guiAccount{}, downloads: map[string]guiDownload{}}
	app.quark = newQuarkManager(app)
	defer app.quark.Stop()
	if !app.quark.loggedClients() {
		t.Fatal("夸克资料中没有可用账号")
	}
	client, selectionErr := quarkLiveClient(app.quark)
	if selectionErr != nil {
		t.Fatal(selectionErr)
	}
	runtime := newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "disk", UploadRecoveryMode: "off"})
	drive := newQuarkDrive(client, runtime, 4, "auto")
	server, _, fs := testDAVService(t, true)
	fs.transfers = runtime
	fs.mounts[0].drive = drive
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].Identity = davIdentityHash("quark", client.snapshot().UID)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	httpClient := &http.Client{Timeout: 4 * time.Minute}
	root := "/联通云盘-1/CloudProbe-quark-test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:8]
	report := map[string]any{"version": version, "date": time.Now().Format(time.RFC3339), "test_path": root, "checks": []string{}, "real_account": true, "cache_mode": "disk", "cleanup_ok": false}
	created := false
	request := func(callCtx context.Context, method, path string, body io.Reader, headers map[string]string) (int, []byte, http.Header, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + path
		req, err := http.NewRequestWithContext(callCtx, method, u.String(), body)
		if err != nil {
			return 0, nil, nil, err
		}
		req.SetBasicAuth("tester", "test-password-123")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		response, err := httpClient.Do(req)
		if err != nil {
			return 0, nil, nil, err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		return response.StatusCode, data, response.Header, err
	}
	t.Cleanup(func() {
		if created {
			cleanupCtx, end := context.WithTimeout(context.Background(), 90*time.Second)
			defer end()
			status, _, _, err := request(cleanupCtx, "DELETE", root, nil, nil)
			report["cleanup_ok"] = err == nil && (status == 204 || status == 404)
			if report["cleanup_ok"] != true {
				t.Errorf("专用夸克测试目录清理失败，保留了明确测试路径；status=%d error=%v", status, err)
			}
		}
		if path := os.Getenv("CLOUDPROBE_LIVE_QUARK_REPORT"); path != "" {
			encoded, _ := json.MarshalIndent(report, "", "  ")
			if err := os.MkdirAll(filepath.Dir(path), 0700); err == nil {
				_ = os.WriteFile(path, encoded, 0600)
			}
		}
	})
	check := func(name, method, path string, payload []byte, want int, headers map[string]string) []byte {
		t.Helper()
		status, data, _, err := request(ctx, method, path, bytes.NewReader(payload), headers)
		if err != nil || status != want {
			t.Fatalf("%s failed: status=%d want=%d error=%v detail=%s", name, status, want, err, client.scrub(string(data[:min(len(data), 1000)])))
		}
		report["checks"] = append(report["checks"].([]string), name)
		return data
	}
	quota, err := drive.Quota(ctx)
	if err != nil {
		t.Fatal(err)
	}
	report["quota_available"] = quota.available()
	report["quota_total"] = quota.Total
	created = true // Only this newly generated, unique path is ever cleaned.
	check("mkdir", "MKCOL", root, nil, 201, nil)
	textPath := root + "/说明 & 100%.txt"
	initial := []byte("CloudProbe 夸克真实文本测试\n")
	check("text upload", "PUT", textPath, initial, 201, nil)
	if body := check("text download", "GET", textPath, nil, 200, nil); !bytes.Equal(body, initial) {
		t.Fatal("text hash mismatch")
	}
	replacement := []byte("CloudProbe overwrite with verified content\n")
	check("overwrite", "PUT", textPath, replacement, 201, nil)
	if body := check("overwrite readback", "GET", textPath, nil, 200, nil); !bytes.Equal(body, replacement) {
		t.Fatal("overwrite mismatch")
	}
	check("head", "HEAD", textPath, nil, 200, nil)
	if body := check("range", "GET", textPath, nil, 206, map[string]string{"Range": "bytes=3-17"}); !bytes.Equal(body, replacement[3:18]) {
		t.Fatal("range mismatch")
	}
	renamed := root + "/renamed.txt"
	dest, _ := url.Parse(server.URL + "/dav")
	dest.Path = "/dav" + renamed
	check("rename", "MOVE", textPath, nil, 201, map[string]string{"Destination": dest.String()})
	check("new subfolder", "MKCOL", root+"/moved", nil, 201, nil)
	moved := root + "/moved/renamed-again.txt"
	dest.Path = "/dav" + moved
	check("move and rename", "MOVE", renamed, nil, 201, map[string]string{"Destination": dest.String()})
	copied := root + "/copied.txt"
	dest.Path = "/dav" + copied
	check("copy", "COPY", moved, nil, 201, map[string]string{"Destination": dest.String()})
	if body := check("copy readback", "GET", copied, nil, 200, nil); !bytes.Equal(body, replacement) {
		t.Fatal("copy mismatch")
	}
	quotaBody := []byte(`<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:quota-used-bytes/><D:quota-available-bytes/></D:prop></D:propfind>`)
	body := check("RFC4331 quota", "PROPFIND", root, quotaBody, 207, map[string]string{"Depth": "0"})
	if !bytes.Contains(body, []byte("quota-used-bytes")) || !bytes.Contains(body, []byte("200 OK")) {
		t.Fatal("quota properties missing")
	}
	large := make([]byte, 23<<20+73)
	if _, err = rand.Read(large); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(large)
	largePath := root + "/large.bin"
	started := time.Now()
	check("large upload", "PUT", largePath, large, 201, nil)
	report["large_upload_seconds"] = time.Since(started).Seconds()
	downloaded := check("large download", "GET", largePath, nil, 200, nil)
	got := sha256.Sum256(downloaded)
	if got != hash {
		t.Fatal("large file SHA256 mismatch")
	}
	report["large_sha256"] = hex.EncodeToString(hash[:])
	report["large_bytes"] = len(large)
	check("delete", "DELETE", copied, nil, 204, nil)
	report["status"] = "PASS"
}
func TestLiveQuarkCleanupRecordedDirectory(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK_CLEANUP") != "1" {
		t.Skip("只清理指定的本次测试记录目录")
	}
	profile, reportFile := os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_QUARK_REPORT")
	raw, err := os.ReadFile(reportFile)
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]any
	if json.Unmarshal(raw, &report) != nil {
		t.Fatal("invalid report")
	}
	name := filepath.Base(valueString(report["test_path"]))
	if !strings.HasPrefix(name, "CloudProbe-quark-test-") {
		t.Fatal("refusing unowned path")
	}
	app := &guiApp{data: profile, accounts: map[string]guiAccount{}}
	app.quark = newQuarkManager(app)
	var client *quarkClient
	app.quark.mu.Lock()
	for _, q := range app.quark.clients {
		client = q
		break
	}
	app.quark.mu.Unlock()
	if client == nil {
		t.Fatal("no account")
	}
	drive := newQuarkDrive(client, nil, 4, "auto")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	entries, err := drive.List(ctx, "0")
	if err != nil {
		t.Fatal(err)
	}
	matches := []davEntry{}
	for _, it := range entries {
		if it.Name == name {
			matches = append(matches, it)
		}
	}
	if len(matches) > 1 {
		t.Fatal("ambiguous test path")
	}
	if len(matches) == 1 {
		if err = drive.Remove(ctx, matches[0]); err != nil {
			t.Fatal(err)
		}
	}
	report["cleanup_ok"] = true
	out, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(reportFile, out, 0600); err != nil {
		t.Fatal(err)
	}
}

type quarkDesktopFaultTransport struct {
	base         http.RoundTripper
	desktop, web atomic.Int64
}

func (t *quarkDesktopFaultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Hostname() == "drive-pc.quark.cn" {
		t.desktop.Add(1)
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
		return &http.Response{StatusCode: 501, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":501,"code":501,"message":"injected desktop unavailability"}`)), Request: r}, nil
	}
	if r.URL.Hostname() == "drive.quark.cn" {
		t.web.Add(1)
	}
	return t.base.RoundTrip(r)
}
func TestLiveQuarkWebFallback(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK_WEB") != "1" {
		t.Skip("设置 CLOUDPROBE_LIVE_QUARK_WEB 后实测网页回退")
	}
	app := &guiApp{data: os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE"), accounts: map[string]guiAccount{}}
	app.quark = newQuarkManager(app)
	q, selectionErr := quarkLiveClient(app.quark)
	if selectionErr != nil {
		t.Fatal(selectionErr)
	}
	base := q.metadata.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	fault := &quarkDesktopFaultTransport{base: base}
	q.metadata.Transport = fault
	runtime := newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "memory"})
	drive := newQuarkDrive(q, runtime, 4, "auto")
	server, _, fs := testDAVService(t, true)
	fs.transfers = runtime
	fs.mounts[0].drive = drive
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].UploadStorageMode = "disk"
	fs.mounts[0].Identity = davIdentityHash("quark", q.snapshot().UID)
	root := "/联通云盘-1/CloudProbe-quark-web-test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:8]
	report := map[string]any{"version": version, "real_web_channel": true, "desktop_fault_injected": true, "test_path": root, "cleanup_ok": false}
	request := func(ctx context.Context, method, path string, body []byte, headers map[string]string) (int, []byte, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + path
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.SetBasicAuth("tester", "test-password-123")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return resp.StatusCode, data, err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, _, err := request(ctx, "DELETE", root, nil, nil)
		report["cleanup_ok"] = err == nil && (status == 204 || status == 404)
		report["injected_desktop_requests"] = fault.desktop.Load()
		report["real_web_requests"] = fault.web.Load()
		if report["cleanup_ok"] != true {
			t.Error("web fallback test directory cleanup failed")
		}
		if path := os.Getenv("CLOUDPROBE_LIVE_QUARK_WEB_REPORT"); path != "" {
			b, _ := json.MarshalIndent(report, "", "  ")
			_ = os.WriteFile(path, b, 0600)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	expect := func(method, path string, body []byte, want int, headers map[string]string) []byte {
		t.Helper()
		status, data, err := request(ctx, method, path, body, headers)
		if err != nil || status != want {
			t.Fatalf("web fallback %s status=%d want=%d error=%v detail=%s", method, status, want, err, q.scrub(string(data[:min(len(data), 500)])))
		}
		return data
	}
	if _, err := drive.Quota(ctx); err != nil {
		t.Fatal(err)
	}
	expect("MKCOL", root, nil, 201, nil)
	content := bytes.Repeat([]byte("real-web-fallback"), 4096)
	file := root + "/web.txt"
	expect("PUT", file, content, 201, nil)
	if body := expect("GET", file, nil, 200, nil); !bytes.Equal(body, content) {
		t.Fatal("real web fallback data mismatch")
	}
	if body := expect("GET", file, nil, 206, map[string]string{"Range": "bytes=9-1023"}); !bytes.Equal(body, content[9:1024]) {
		t.Fatal("real web range mismatch")
	}
	dest, _ := url.Parse(server.URL)
	dest.Path = "/dav" + root + "/renamed.txt"
	expect("MOVE", file, nil, 201, map[string]string{"Destination": dest.String()})
	expect("DELETE", root+"/renamed.txt", nil, 204, nil)
	lockURL, _ := url.Parse(server.URL)
	lockURL.Path = "/dav" + root + "/locked.txt"
	lockBody := `<?xml version="1.0"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:owner>CloudProbe test</D:owner></D:lockinfo>`
	req, _ := http.NewRequestWithContext(ctx, "LOCK", lockURL.String(), strings.NewReader(lockBody))
	req.SetBasicAuth("tester", "test-password-123")
	req.Header.Set("Depth", "0")
	req.Header.Set("Timeout", "Second-90")
	response, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	token := response.Header.Get("Lock-Token")
	if response.StatusCode != 201 || token == "" {
		t.Fatalf("real empty-file LOCK failed: %d", response.StatusCode)
	}
	expect("PUT", root+"/locked.txt", []byte("saved with lock"), 201, map[string]string{"If": "(" + token + ")"})
	expect("UNLOCK", root+"/locked.txt", nil, 204, map[string]string{"Lock-Token": token})
	if body := expect("GET", root+"/locked.txt", nil, 200, nil); string(body) != "saved with lock" {
		t.Fatal("locked save differs")
	}
	report["lock_empty_file_save_unlock"] = "PASS"
	if fault.desktop.Load() < 3 || fault.web.Load() < 3 {
		t.Fatal("fallback did not use both injection and real web path")
	}
	report["status"] = "PASS"
	report["bytes"] = len(content)
}

func quarkLiveClient(manager *quarkManager) (*quarkClient, error) {
	if key := os.Getenv("CLOUDPROBE_LIVE_QUARK_ACCOUNT_KEY"); key != "" {
		return manager.client(key)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.clients) != 1 {
		return nil, errors.New("实网测试有多个或没有夸克账号，请显式选择账号，未猜测目标")
	}
	for _, client := range manager.clients {
		return client, nil
	}
	return nil, errors.New("未选择夸克账号")
}

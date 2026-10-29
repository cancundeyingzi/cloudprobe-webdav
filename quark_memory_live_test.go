package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLiveQuarkMemoryWebDAV(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK_MEMORY") != "1" {
		t.Skip("显式选择真实账号后验证夸克内存 WebDAV")
	}
	app := &guiApp{data: os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE"), accounts: map[string]guiAccount{}}
	app.quark = newQuarkManager(app)
	client, err := quarkLiveClient(app.quark)
	if err != nil {
		t.Fatal("未唯一选择已授权测试账号")
	}
	runtime := newTransferRuntime(transferOptions{TransferScheduling: "client", UploadMemoryMiB: 8, TotalUploadMemoryMiB: 16, UploadPrefetchMiB: 4, TotalConnections: 2, UploadRecoveryMode: "off"})
	drive := newQuarkDrive(client, runtime, 1, "auto")
	server, _, fs := testDAVService(t, true)
	fs.transfers = runtime
	fs.mounts[0].drive, fs.mounts[0].Alias, fs.mounts[0].Provider, fs.mounts[0].UploadStorageMode = drive, "夸克测试", "quark", "memory"
	fs.mounts[0].Identity = davIdentityHash("quark", client.snapshot().UID)
	root := "/夸克测试/CloudProbe-memory-test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:8]
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	report := map[string]any{"version": version, "test_path": root, "strict_memory": true, "cleanup_ok": false}
	forceWeb := os.Getenv("CLOUDPROBE_LIVE_QUARK_MEMORY_WEB") == "1"
	var webPreparations atomic.Int64
	if forceWeb {
		base := client.metadata.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		client.metadata.Transport = phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "drive-pc.quark.cn" && strings.HasSuffix(r.URL.Path, "/file/upload/pre") {
				return &http.Response{StatusCode: 501, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":501,"code":501,"message":"injected desktop preparation failure"}`)), Request: r}, nil
			}
			if r.URL.Host == "drive.quark.cn" && strings.HasSuffix(r.URL.Path, "/file/upload/pre") {
				webPreparations.Add(1)
			}
			return base.RoundTrip(r)
		})
	}
	request := func(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (int, []byte, http.Header, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + path
		r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
		if err != nil {
			return 0, nil, nil, err
		}
		r.SetBasicAuth("tester", "test-password-123")
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			return 0, nil, nil, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		return resp.StatusCode, data, resp.Header, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		status, _, _, err := request(cleanup, "DELETE", root, nil, nil)
		report["cleanup_ok"] = err == nil && (status == 204 || status == 404)
		if path := os.Getenv("CLOUDPROBE_LIVE_QUARK_MEMORY_REPORT"); path != "" {
			data, _ := json.MarshalIndent(report, "", "  ")
			_ = os.WriteFile(path, data, 0600)
		}
		if report["cleanup_ok"] != true {
			t.Error("专用内存测试目录清理未确认")
		}
	}()
	if status, _, _, err := request(ctx, "MKCOL", root, nil, nil); err != nil || status != 201 {
		t.Fatal("专用测试目录创建失败")
	}
	sizes := []int{6 << 20, 11 << 20, 36 << 20}
	if forceWeb {
		sizes = []int{6 << 20}
	}
	payloads := make([][]byte, len(sizes))
	etags := make([]string, len(sizes))
	for i, size := range sizes {
		payloads[i] = make([]byte, size+17)
		if _, err = rand.Read(payloads[i]); err != nil {
			t.Fatal("合成正文生成失败")
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	failed := false
	uploadStarted := time.Now()
	for i, payload := range payloads {
		wg.Go(func() {
			status, _, headers, err := request(ctx, "PUT", fmt.Sprintf("%s/file-%d.bin", root, i), bytes.NewReader(payload), nil)
			if err != nil || status != 201 {
				mu.Lock()
				failed = true
				mu.Unlock()
				t.Errorf("内存上传失败：file=%d status=%d", i, status)
				return
			}
			etags[i] = headers.Get("ETag")
		})
	}
	wg.Wait()
	report["upload_seconds"], report["files"] = time.Since(uploadStarted).Seconds(), len(payloads)
	if failed {
		return
	}
	weakValidators := 0
	for _, etag := range etags {
		if strings.HasPrefix(etag, "W/") {
			weakValidators++
		}
	}
	report["weak_validators"] = weakValidators
	for i, payload := range payloads {
		path := fmt.Sprintf("%s/file-%d.bin", root, i)
		status, data, _, err := request(ctx, "GET", path, nil, nil)
		if err != nil || status != 200 || sha256.Sum256(data) != sha256.Sum256(payload) {
			t.Fatalf("完整回读不符：file=%d status=%d", i, status)
		}
		status, _, headers, err := request(ctx, "HEAD", path, nil, nil)
		if err != nil || status != 200 || etags[i] == "" || headers.Get("ETag") != etags[i] {
			t.Fatalf("最终 ETag 不一致：file=%d", i)
		}
		rangeHeaders := map[string]string{"Range": "bytes=9-1023"}
		if !strings.HasPrefix(etags[i], "W/") {
			rangeHeaders["If-Range"] = etags[i]
		}
		status, data, _, err = request(ctx, "GET", path, nil, rangeHeaders)
		if err != nil || status != 206 || !bytes.Equal(data, payload[9:1024]) {
			t.Fatalf("Range 不符：file=%d status=%d", i, status)
		}
		if strings.HasPrefix(etags[i], "W/") {
			status, data, _, err = request(ctx, "GET", path, nil, map[string]string{"Range": "bytes=9-1023", "If-Range": etags[i]})
			if err != nil || status != 200 || sha256.Sum256(data) != sha256.Sum256(payload) {
				t.Fatal("弱验证器错误地允许拼接或完整回退内容不符")
			}
		}
	}
	moved := root + "/renamed.bin"
	destination, _ := url.Parse(server.URL)
	destination.Path = "/dav" + moved
	moveMatch := etags[0]
	if strings.HasPrefix(moveMatch, "W/") {
		moveMatch = "*"
	}
	if status, _, _, err := request(ctx, "MOVE", root+"/file-0.bin", nil, map[string]string{"Destination": destination.String(), "If-Match": moveMatch}); err != nil || status != 201 {
		t.Fatal("条件改名失败")
	}
	quota := `<d:propfind xmlns:d="DAV:"><d:prop><d:quota-used-bytes/><d:quota-available-bytes/></d:prop></d:propfind>`
	status, body, _, err := request(ctx, "PROPFIND", "/夸克测试", bytes.NewBufferString(quota), map[string]string{"Depth": "0", "Content-Type": "application/xml"})
	if err != nil || status != 207 || !bytes.Contains(body, []byte("quota-used-bytes>")) || !bytes.Contains(body, []byte("quota-available-bytes>")) || bytes.Contains(body, []byte("404 Not Found")) {
		t.Fatal("配额扩展未返回有效容量")
	}
	files, _ := os.ReadDir(fs.spool)
	stats := runtime.memory.status()
	if len(files) != 0 || stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 16<<20 || stats["allocated_bytes"].(int64) > 16<<20 {
		t.Fatal("纯内存缓存未正确归还或超出预算")
	}
	if forceWeb && webPreparations.Load() == 0 {
		t.Fatal("未使用网页备用上传通道")
	}
	report["web_backup_verified"], report["web_preparations"] = forceWeb, webPreparations.Load()
	report["status"], report["sha256_all_equal"], report["range_ok"], report["etag_ok"], report["quota_ok"], report["rename_ok"], report["spool_files"], report["memory"] = "PASS", true, true, true, true, true, len(files), stats
	t.Log("夸克严格纯内存上传、完整回读、Range、最终 ETag、条件改名及容量通过")
}

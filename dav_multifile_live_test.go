package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
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
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

type liveFallbackBridge struct {
	inner    *OriginalBridge
	failList atomic.Bool
}

func (b *liveFallbackBridge) Request(ctx context.Context, method, p string, body []byte, headers map[string]string) ([]byte, error) {
	if b.failList.Load() && strings.HasSuffix(p, "/query_files") {
		return nil, errors.New("测试注入：网易目录接口暂不可用")
	}
	return b.inner.Request(ctx, method, p, body, headers)
}

type liveFallbackDrive struct {
	*wopanDAVDrive
	badURL   string
	failBody atomic.Bool
}

func (d *liveFallbackDrive) ReadRoutes(ctx context.Context, it davEntry) []readRoute {
	routes := d.wopanDAVDrive.ReadRoutes(ctx, it)
	if d.failBody.Load() {
		routes[0].Resolve = func(context.Context) (readTarget, error) { return readTarget{URL: d.badURL}, nil }
	}
	return routes
}

func TestLiveMultiFileTransfersAndFallback(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_MULTI") != "1" {
		t.Skip("显式指定私有测试资料后才运行多文件上传、下载及故障回退")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	if profile == "" || bundle == "" {
		t.Fatal("缺少独立测试资料")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	bridge, e := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll"), filepath.Join(profile, "original-sdk"))
	if e != nil {
		t.Fatal(e)
	}
	defer bridge.Close()
	g := &guiApp{data: profile, bridge: bridge, accounts: map[string]guiAccount{}}
	log := newRuntimeLog(profile, g.redactDAV)
	g.runLog = log
	defer log.close()
	data, e := g.call(ctx, "GET", "/v1/drive/alltokens", map[string]any{})
	if e != nil {
		t.Fatal("读取绑定失败")
	}
	all := list(data["wopan_credentials"])
	if len(all) == 0 {
		t.Fatal("没有联通绑定")
	}
	a := guiAccount{Key: "multi-test", Provider: "wopan", credentials: object(all[0])}
	g.accounts[a.Key] = a
	g.applyDAVCredentials(g.accounts)
	a = g.accounts[a.Key]
	if !a.WebBound {
		t.Fatal("未恢复官网授权")
	}
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = valueString(a.credentials["token"])
	cl, e := newClient(cfg, "")
	if e != nil {
		t.Fatal(e)
	}
	faultBridge := &liveFallbackBridge{inner: bridge}
	cl.bridge = faultBridge
	driver := newWopanDAVDrive(cl, a.credentials, "0", nil)
	driver.uploadConcurrency = 4
	driver.uploadMode = "auto"
	driver.uploadPartMiB = 5
	if e = driver.probe(ctx); e != nil {
		t.Fatal(cl.redact.Text(e.Error()))
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "injected body gateway failure", 503) }))
	defer bad.Close()
	d := &liveFallbackDrive{wopanDAVDrive: driver, badURL: bad.URL}
	runtime := newTransferRuntime(transferOptions{})
	driver.uploadBudget = runtime.connections
	mount := &davMount{Alias: "联通云盘-1", Root: "0", Provider: "wopan", Writable: true, drive: d, health: &routeHealth{}}
	fs := &davFS{mounts: []*davMount{mount}, spool: filepath.Join(profile, "multi-spool"), maxUpload: 128 << 20, cache: map[string]davCache{}, transfers: runtime}
	password := randomKey()
	salt := []byte(randomKey()[:16])
	hash, _ := pbkdf2.Key(sha256.New, password, salt, 100000, 32)
	svc := &davService{manager: &davManager{app: g}, fs: fs, handler: &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS()}, username: "multi-test", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash)}
	server := httptest.NewServer(svc)
	defer server.Close()
	client := &http.Client{Timeout: 12 * time.Minute}
	call := func(c context.Context, method, p string, body []byte, headers map[string]string) (int, []byte, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + p
		r, e := http.NewRequestWithContext(c, method, u.String(), bytes.NewReader(body))
		if e != nil {
			return 0, nil, e
		}
		r.SetBasicAuth("multi-test", password)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		resp, e := client.Do(r)
		if e != nil {
			return 0, nil, e
		}
		defer resp.Body.Close()
		raw, e := io.ReadAll(resp.Body)
		return resp.StatusCode, raw, e
	}
	root := "/联通云盘-1/CloudProbe-Multi-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	if code, _, e := call(ctx, "MKCOL", root, nil, nil); e != nil || code != 201 {
		t.Fatal("创建专用目录失败", code, e)
	}
	reportPath := os.Getenv("CLOUDPROBE_LIVE_REPORT")
	if reportPath == "" {
		reportPath = filepath.Join(profile, "multifile-verification.json")
	}
	report := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "transfer_settings": runtime.options, "upload_threads": 4, "random_data": true, "openlist_live_configured": false}
	defer func() {
		faultBridge.failList.Store(false)
		d.failBody.Store(false)
		cleanup, c := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
		defer c()
		code, _, e := call(cleanup, "DELETE", root, nil, nil)
		report["test_directory_removed"], report["success"] = e == nil && code == 204, !t.Failed() && e == nil && code == 204
		_ = writePrivateJSON(reportPath, report)
		if e != nil || code != 204 {
			t.Errorf("专用目录清理失败 %d", code)
		}
	}()
	names := []string{"text.txt", "archive.zip", "video.mkv", "random.bin"}
	sizes := []int{4096, 12 << 20, 18 << 20, 24 << 20}
	payloads := make([][]byte, 4)
	for i := range payloads {
		payloads[i] = make([]byte, sizes[i])
		if _, e = rand.Read(payloads[i]); e != nil {
			t.Fatal(e)
		}
	}
	copy(payloads[0], []byte("CloudProbe ordinary text contents\n"))
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := []map[string]any{}
	errs := make(chan error, 4)
	started := time.Now()
	var maxUpload, maxDownload, maxConnections atomic.Int64
	stopMonitor := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				for limit, value := range map[*fairLimiter]*atomic.Int64{runtime.uploads.limiter: &maxUpload, runtime.downloads.limiter: &maxDownload, runtime.connections: &maxConnections} {
					n := int64(limit.status()["active"])
					for old := value.Load(); n > old && !value.CompareAndSwap(old, n); old = value.Load() {
					}
				}
			}
		}
	}()
	defer func() { close(stopMonitor); <-monitorDone }()
	for i := range names {
		wg.Go(func() {
			start := time.Now()
			code, raw, e := call(ctx, "PUT", root+"/"+names[i], payloads[i], nil)
			if e != nil || code != 201 {
				errs <- fmt.Errorf("PUT %s HTTP %d：%v %s", names[i], code, e, cl.redact.Text(string(raw)))
				return
			}
			mu.Lock()
			results = append(results, map[string]any{"name": names[i], "bytes": sizes[i], "upload_ms": time.Since(start).Milliseconds(), "sha256": fmt.Sprintf("%x", sha256.Sum256(payloads[i]))})
			mu.Unlock()
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	report["parallel_upload_ms"], report["upload_files"] = time.Since(started).Milliseconds(), results
	t.Logf("4 个文件上传完成，最大同时上传 %d", maxUpload.Load())
	faultBridge.failList.Store(true)
	fs.invalidate(mount)
	code, listing, e := call(ctx, "PROPFIND", root, nil, map[string]string{"Depth": "1"})
	if e != nil || code != 207 {
		t.Fatal("目录备用未工作", code, e)
	}
	for _, name := range names {
		if !bytes.Contains(listing, []byte(name)) {
			t.Fatal("目录丢失普通文件", name)
		}
	}
	report["forced_proxy_list_failure_fell_back"] = true
	d.failBody.Store(true)
	errs = make(chan error, 4)
	started = time.Now()
	for i := range names {
		wg.Go(func() {
			code, raw, e := call(ctx, "GET", root+"/"+names[i], nil, nil)
			if e != nil || code != 200 || sha256.Sum256(raw) != sha256.Sum256(payloads[i]) {
				errs <- fmt.Errorf("GET %s HTTP %d：%v", names[i], code, e)
			}
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	report["parallel_download_ms"], report["all_four_sha256_match"], report["forced_body_503_fell_back"] = time.Since(started).Milliseconds(), true, true
	d.failBody.Store(false)
	code, raw, e := call(ctx, "GET", root+"/random.bin", nil, map[string]string{"Range": "bytes=12345-12444"})
	if e != nil || code != 206 || !bytes.Equal(raw, payloads[3][12345:12445]) {
		t.Fatal("客户端 Range 读取失败", code, e)
	}
	report["client_range_exact_bytes"] = true
	entries := snapshotRunEntries(log)
	sources := map[string]int{}
	fall := 0
	for _, entry := range entries {
		if entry.Operation == "下载正文" && entry.Result == "开始" {
			sources[entry.Source]++
		}
		if entry.Operation == "切换备用接口" {
			fall++
		}
	}
	report["download_sources"], report["fallback_events"], report["max_simultaneous_upload_files"], report["max_simultaneous_download_files"], report["max_body_connections"] = sources, fall, maxUpload.Load(), maxDownload.Load(), maxConnections.Load()
	if maxUpload.Load() > 3 || maxDownload.Load() > 4 || maxConnections.Load() > 64 {
		t.Fatal("实网并发超过预算")
	}
	if sources["联通直连 V2"]+sources["联通直连 V1"] == 0 {
		t.Fatal("正文未使用备用直连接口")
	}
	t.Logf("多文件下载和故障回退通过：最大下载文件 %d，正文连接 %d；四份 SHA256 一致", maxDownload.Load(), maxConnections.Load())
}

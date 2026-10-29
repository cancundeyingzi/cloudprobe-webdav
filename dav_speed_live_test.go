package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

func TestLiveWopanUploadConcurrencyBenchmark(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_BENCHMARK") != "1" {
		t.Skip("仅在明确批准的独立测试目录进行 1/2/4 并发实测")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	if profile == "" || bundle == "" {
		t.Fatal("缺少独立 profile / bundle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	b, err := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll"), filepath.Join(profile, "original-sdk"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	g := &guiApp{data: profile, bridge: b, accounts: map[string]guiAccount{}}
	l := newRuntimeLog(profile, g.redactDAV)
	g.runLog = l
	defer l.close()
	data, err := g.call(ctx, "GET", "/v1/drive/alltokens", map[string]any{})
	if err != nil {
		t.Fatal("读取绑定云盘失败")
	}
	accounts := list(data["wopan_credentials"])
	if len(accounts) == 0 {
		t.Fatal("没有联通绑定")
	}
	a := guiAccount{Key: "benchmark", Provider: "wopan", Name: "联通云盘", credentials: object(accounts[0])}
	g.accounts[a.Key] = a
	g.applyDAVCredentials(g.accounts)
	a = g.accounts[a.Key]
	if !a.WebBound {
		t.Fatal("独立测试 profile 未恢复官网备用登录；不修改原 profile 或重新登录")
	}
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = valueString(a.credentials["token"])
	cl, err := newClient(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	cl.bridge = b
	d := newWopanDAVDrive(cl, a.credentials, "0", nil)
	d.uploadSlots = make(chan struct{}, 8)
	if err = d.probe(ctx); err != nil {
		t.Fatal(cl.redact.Text(err.Error()))
	}
	if !d.usingWeb {
		t.Fatal("测试没有使用官网授权，不做不等价的速度对照")
	}
	fs := &davFS{mounts: []*davMount{{Alias: "联通云盘-1", Root: "0", Provider: "wopan", Writable: true, drive: d}}, spool: filepath.Join(profile, "webdav-spool"), maxUpload: 64 << 20, cache: map[string]davCache{}}
	password := randomKey()
	salt := []byte(randomKey()[:16])
	hash, _ := pbkdf2.Key(sha256.New, password, salt, 100000, 32)
	mgr := &davManager{app: g}
	svc := &davService{manager: mgr, fs: fs, handler: &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS()}, username: "benchmark", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash), slots: make(chan struct{}, 8)}
	server := httptest.NewServer(svc)
	defer server.Close()
	client := &http.Client{Timeout: 6 * time.Minute}
	dir := "CloudProbe-Speed-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	root := "/联通云盘-1/" + dir
	call := func(method, p string, body []byte) (int, []byte, string, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + p
		r, e := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if e != nil {
			return 0, nil, "", e
		}
		r.SetBasicAuth("benchmark", password)
		resp, e := client.Do(r)
		if e != nil {
			return 0, nil, "", e
		}
		defer resp.Body.Close()
		v, e := io.ReadAll(resp.Body)
		return resp.StatusCode, v, resp.Header.Get("X-CloudProbe-Request-ID"), e
	}
	report := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "provider": "wopan", "credential_source": "official web login", "file_bytes": 32 << 20, "part_bytes": 8 << 20, "test_directory": dir, "random_data_per_case": true, "cases": []map[string]any{}}
	cases := []map[string]any{}
	defer func() {
		code, _, _, e := call("DELETE", root, nil)
		report["test_directory_removed"] = e == nil && code == 204
		report["cases"] = cases
		report["success"] = !t.Failed() && e == nil && code == 204
		file := os.Getenv("CLOUDPROBE_LIVE_REPORT")
		if file == "" {
			file = filepath.Join(profile, "upload-concurrency-benchmark.json")
		}
		_ = writePrivateJSON(file, report)
		if e != nil || code != 204 {
			t.Errorf("测试目录清理未成功：HTTP %d", code)
		}
	}()
	code, _, _, err := call("MKCOL", root, nil)
	if err != nil || code != 201 {
		t.Fatalf("创建专用测试目录失败：HTTP %d", code)
	}
	for _, concurrency := range []int{1, 2, 4} {
		d.uploadConcurrency = concurrency
		payload := make([]byte, 32<<20)
		if _, err = rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("random-c%d.bin", concurrency)
		t.Logf("开始 %d 并发：32 MiB 随机正文", concurrency)
		started := time.Now()
		status, body, requestID, e := call("PUT", root+"/"+name, payload)
		elapsed := time.Since(started)
		row := map[string]any{"concurrency": concurrency, "http_status": status, "total_ms": elapsed.Milliseconds(), "overall_bytes_per_second": rateFor(int64(len(payload)), elapsed), "sha256_expected": fmt.Sprintf("%x", sha256.Sum256(payload))}
		cases = append(cases, row)
		if e != nil || status != 201 {
			t.Fatalf("并发 %d 上传失败 HTTP %d：%s", concurrency, status, cl.redact.Text(string(body)))
		}
		l.mu.Lock()
		parts := []map[string]any{}
		for _, entry := range l.entries {
			if entry.RequestID != requestID {
				continue
			}
			if entry.Operation == "云端上传" && entry.Result == "成功" {
				row["cloud_ms"], row["cloud_bytes_per_second"], row["upload_node"], row["sent_bytes"], row["confirmed_bytes"] = entry.DurationMs, entry.BytesPerSecond, entry.Endpoint, entry.SentBytes, entry.ConfirmedBytes
			}
			if entry.Operation == "上传分片" && entry.Result == "已确认" {
				parts = append(parts, map[string]any{"part": entry.Part, "attempt": entry.Attempt, "send_ms": entry.SendMs, "confirm_wait_ms": entry.ConfirmWaitMs, "queue_ms": entry.QueueMs, "duration_ms": entry.DurationMs})
			}
		}
		l.mu.Unlock()
		row["parts"] = parts
		status, downloaded, _, e := call("GET", root+"/"+name, nil)
		row["sha256_matches"] = e == nil && status == 200 && sha256.Sum256(payload) == sha256.Sum256(downloaded)
		if row["sha256_matches"] != true {
			t.Fatal("下载回验不一致")
		}
		if row["confirmed_bytes"] != int64(len(payload)) || len(parts) != 4 {
			t.Fatal("分片确认数量或字节数不完整")
		}
		t.Logf("%d 并发上传并回验通过：总耗时 %.2fs，整体 %.1f KiB/s，节点 %s", concurrency, elapsed.Seconds(), rateFor(int64(len(payload)), elapsed)/1024, row["upload_node"])
		if code, _, _, err = call("DELETE", root+"/"+name, nil); err != nil || code != 204 {
			t.Fatal("测试文件清理失败")
		}
	}
	// Publish only paths inside the owned test directory. Root listings can
	// contain the user's other file names and remain in the private profile.
	l.mu.Lock()
	owned := []runLogEntry{}
	for _, entry := range l.entries {
		if strings.HasPrefix(entry.Path, root+"/") || entry.Path == root {
			owned = append(owned, entry)
		}
	}
	l.mu.Unlock()
	if target := os.Getenv("CLOUDPROBE_LIVE_REPORT"); target != "" {
		_ = writePrivateJSON(filepath.Join(filepath.Dir(target), "benchmark-operation-log.json"), owned)
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

func snapshotRunEntries(l *runtimeLog) []runLogEntry {
	l.mu.Lock()
	fi, err := l.file.Stat()
	if err != nil {
		l.mu.Unlock()
		return nil
	}
	reader := io.NewSectionReader(l.file, 0, fi.Size())
	l.mu.Unlock()
	dec := json.NewDecoder(reader)
	var rows []runLogEntry
	for {
		var row runLogEntry
		if dec.Decode(&row) != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows
}

func TestLiveWopanTransportAndConcurrentOperations(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_TRANSPORT") != "1" {
		t.Skip("显式授权后才进行 256 MiB 真实上传连接及并行操作对照")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	if profile == "" || bundle == "" {
		t.Fatal("缺少独立登录资料和组件目录")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
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
		t.Fatal("读取绑定失败")
	}
	accounts := list(data["wopan_credentials"])
	if len(accounts) == 0 {
		t.Fatal("没有联通绑定")
	}
	a := guiAccount{Key: "transport-test", Provider: "wopan", credentials: object(accounts[0])}
	g.accounts[a.Key] = a
	g.applyDAVCredentials(g.accounts)
	a = g.accounts[a.Key]
	if !a.WebBound {
		t.Fatal("私有测试资料未恢复官网授权")
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
	d.uploadConcurrency = 4
	d.uploadSlots = make(chan struct{}, 8)
	if err = d.probe(ctx); err != nil {
		t.Fatal(cl.redact.Text(err.Error()))
	}
	if !d.usingWeb {
		t.Fatal("未使用官网授权，停止对照")
	}
	fs := &davFS{mounts: []*davMount{{Alias: "联通云盘-1", Root: "0", Provider: "wopan", Writable: true, drive: d}}, spool: filepath.Join(profile, "transport-spool"), maxUpload: 512 << 20, cache: map[string]davCache{}}
	password := randomKey()
	salt := []byte(randomKey()[:16])
	hash, _ := pbkdf2.Key(sha256.New, password, salt, 100000, 32)
	svc := &davService{manager: &davManager{app: g}, fs: fs, handler: &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS()}, username: "transport-test", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash), slots: make(chan struct{}, 8)}
	server := httptest.NewServer(svc)
	defer server.Close()
	client := &http.Client{Timeout: 13 * time.Minute}
	request := func(callCtx context.Context, method, target string, body io.Reader, length int64) (*http.Response, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + target
		r, e := http.NewRequestWithContext(callCtx, method, u.String(), body)
		if e != nil {
			return nil, e
		}
		r.ContentLength = length
		r.SetBasicAuth("transport-test", password)
		if method == "PROPFIND" {
			r.Header.Set("Depth", "1")
		}
		return client.Do(r)
	}
	status := func(callCtx context.Context, method, target string, body []byte) (int, error) {
		resp, e := request(callCtx, method, target, bytes.NewReader(body), int64(len(body)))
		if e != nil {
			return 0, e
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}
	root := "/联通云盘-1/CloudProbe-Transport-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	report := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "file_bytes": 256 << 20, "part_bytes": 8 << 20, "concurrency": 4, "same_random_content_and_suffix": true, "cases": []map[string]any{}}
	cases := []map[string]any{}
	reportPath := os.Getenv("CLOUDPROBE_LIVE_REPORT")
	if reportPath == "" {
		reportPath = filepath.Join(profile, "transport-verification.json")
	}
	if code, e := status(ctx, "MKCOL", root, nil); e != nil || code != 201 {
		t.Fatalf("新建测试目录 HTTP %d", code)
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
		defer c()
		code, e := status(cleanup, "DELETE", root, nil)
		report["cases"], report["test_directory_removed"], report["success"] = cases, e == nil && code == 204, !t.Failed() && e == nil && code == 204
		_ = writePrivateJSON(reportPath, report)
		if e != nil || code != 204 {
			t.Errorf("测试目录清理未完成 HTTP %d", code)
		}
	}()
	source, err := os.CreateTemp(profile, "random-source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(source.Name())
	defer source.Close()
	h := sha256.New()
	if _, err = io.CopyN(io.MultiWriter(source, h), rand.Reader, 256<<20); err != nil {
		t.Fatal(err)
	}
	expected := hex.EncodeToString(h.Sum(nil))
	source.Sync()
	for _, mode := range []string{"http1", "http2"} {
		d.uploadMode = mode
		name := "random-" + mode + ".bin"
		if code, e := status(ctx, "PUT", root+"/delete-during-upload.txt", []byte("owned disposable test file")); e != nil || code != 201 {
			t.Fatal("创建并行删除样本失败")
		}
		if _, err = source.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		row := map[string]any{"mode": mode, "file_bytes": 256 << 20, "expected_sha256": expected}
		cases = append(cases, row)
		t.Logf("开始 %s：256 MiB 随机文件，4 个工作任务", mode)
		started := time.Now()
		transferCtx, transferCancel := context.WithTimeout(ctx, 12*time.Minute)
		type uploadResult struct {
			status    int
			requestID string
			err       error
		}
		done := make(chan uploadResult, 1)
		go func() {
			resp, e := request(transferCtx, "PUT", root+"/"+name, io.NewSectionReader(source, 0, 256<<20), 256<<20)
			if e != nil {
				done <- uploadResult{err: e}
				return
			}
			defer resp.Body.Close()
			body, readErr := readBounded(resp.Body, 64<<10)
			if resp.StatusCode != 201 && readErr == nil {
				readErr = fmt.Errorf("%s", cl.redact.Text(string(body)))
			}
			done <- uploadResult{status: resp.StatusCode, requestID: resp.Header.Get("X-CloudProbe-Request-ID"), err: readErr}
		}()
		var requestID string
		wait := time.NewTicker(100 * time.Millisecond)
		for requestID == "" {
			select {
			case <-wait.C:
				l.mu.Lock()
				for _, event := range l.entries {
					if event.Operation == "上传连接" && event.Path == root+"/"+name {
						requestID = event.RequestID
						break
					}
				}
				l.mu.Unlock()
			case result := <-done:
				wait.Stop()
				transferCancel()
				t.Fatalf("上传在并行检查前结束：HTTP %d %v", result.status, result.err)
			case <-transferCtx.Done():
				wait.Stop()
				transferCancel()
				t.Fatal("等待上传开始超时")
			}
		}
		wait.Stop()
		probeCtx, probeCancel := context.WithTimeout(ctx, 6*time.Second)
		probeStart := time.Now()
		code, e := status(probeCtx, "DELETE", root+"/delete-during-upload.txt", nil)
		row["parallel_delete_ms"], row["parallel_delete_status"] = time.Since(probeStart).Milliseconds(), code
		if e != nil || code != 204 {
			probeCancel()
			transferCancel()
			<-done
			t.Fatalf("上传中删除独立文件失败 HTTP %d", code)
		}
		probeStart = time.Now()
		code, e = status(probeCtx, "PROPFIND", root, nil)
		row["parallel_listing_ms"], row["parallel_listing_status"] = time.Since(probeStart).Milliseconds(), code
		if e != nil || code != 207 {
			probeCancel()
			transferCancel()
			<-done
			t.Fatal("上传中目录查询失败")
		}
		probeStart = time.Now()
		code, e = status(probeCtx, "DELETE", root, nil)
		row["ancestor_conflict_ms"], row["ancestor_conflict_status"] = time.Since(probeStart).Milliseconds(), code
		probeCancel()
		if e != nil || code != 423 {
			transferCancel()
			<-done
			t.Fatal("祖先目录冲突未被及时拒绝")
		}
		t.Logf("%s 上传期间：删除独立文件 %v ms，列目录 %v ms，祖先目录冲突 %v ms", mode, row["parallel_delete_ms"], row["parallel_listing_ms"], row["ancestor_conflict_ms"])
		var result uploadResult
		ticker := time.NewTicker(30 * time.Second)
		waiting := true
		for waiting {
			select {
			case result = <-done:
				waiting = false
			case <-ticker.C:
				t.Logf("%s 上传仍在进行，已用 %.0f 秒", mode, time.Since(started).Seconds())
			}
		}
		ticker.Stop()
		transferCancel()
		elapsed := time.Since(started)
		row["http_status"], row["total_ms"], row["overall_bytes_per_second"] = result.status, elapsed.Milliseconds(), rateFor(256<<20, elapsed)
		if result.err != nil || result.status != 201 {
			t.Fatalf("%s 上传失败 HTTP %d：%v", mode, result.status, result.err)
		}
		entries := snapshotRunEntries(l)
		connections, firstWave, protocols := map[string]map[string]any{}, map[string]bool{}, map[string]bool{}
		partResults := []map[string]any{}
		retries := 0
		for _, event := range entries {
			if event.RequestID != result.requestID {
				continue
			}
			if event.Operation == "上传连接" {
				_, port, _ := net.SplitHostPort(event.LocalAddress)
				connections[event.ConnectionID] = map[string]any{"id": event.ConnectionID, "local_port": port, "remote_endpoint": event.RemoteAddress, "protocol": event.Protocol, "worker": event.Worker}
				protocols[event.Protocol] = true
				if event.Part <= 4 && event.Attempt == 1 {
					firstWave[event.ConnectionID] = true
				}
			}
			if event.Operation == "分片重试" {
				retries++
			}
			if event.Operation == "上传分片" && event.Result == "已确认" {
				partResults = append(partResults, map[string]any{"part": event.Part, "attempt": event.Attempt, "connection_id": event.ConnectionID, "protocol": event.Protocol, "send_ms": event.SendMs, "confirm_wait_ms": event.ConfirmWaitMs, "duration_ms": event.DurationMs})
			}
			if event.Operation == "云端上传" && event.Result == "成功" {
				row["cloud_ms"], row["cloud_bytes_per_second"], row["sent_bytes"], row["confirmed_bytes"], row["upload_node"] = event.DurationMs, event.BytesPerSecond, event.SentBytes, event.ConfirmedBytes, event.Endpoint
			}
		}
		row["connections"], row["first_wave_connections"], row["protocols"], row["retry_count"], row["parts"] = connections, len(firstWave), protocols, retries, partResults
		if mode == "http1" && (len(firstWave) != 4 || !protocols["HTTP/1.1"]) {
			t.Fatal("实网没有建立四条 HTTP/1.1 连接")
		}
		resp, e := request(ctx, "GET", root+"/"+name, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		gotHash := sha256.New()
		downloaded, readErr := io.Copy(gotHash, resp.Body)
		resp.Body.Close()
		row["sha256_matches"] = readErr == nil && resp.StatusCode == 200 && downloaded == 256<<20 && hex.EncodeToString(gotHash.Sum(nil)) == expected
		if row["sha256_matches"] != true {
			t.Fatal("完整下载 SHA256 不一致")
		}
		if row["confirmed_bytes"] != int64(256<<20) || len(partResults) != 32 {
			t.Fatal("分片确认不完整")
		}
		t.Logf("%s 通过：整体 %.2f MiB/s；云端 %v ms；初始连接 %d；重试 %d；SHA256 一致", mode, rateFor(256<<20, elapsed)/(1<<20), row["cloud_ms"], len(firstWave), retries)
		if code, e = status(ctx, "DELETE", root+"/"+name, nil); e != nil || code != 204 {
			t.Fatal("测试文件清理失败")
		}
		report["cases"] = cases
		_ = writePrivateJSON(reportPath, report)
	}
	// No root listing names, credentials, or local IP addresses enter the report.
	report["raw_run_log_stays_in_private_profile"] = true
}

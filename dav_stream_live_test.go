package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type repeatUploadByte byte

func (b repeatUploadByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func TestLiveMemoryStreamingAndAggregateRecovery(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_STREAM") != "1" {
		t.Skip("显式启用后才在专用目录测试真实联通纯内存上传、下载及总体暂停")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	if profile == "" || bundle == "" {
		t.Fatal("缺少独立实测资料目录")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	bridge, err := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll"), filepath.Join(profile, "original-sdk"))
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	g := &guiApp{bridge: bridge, data: profile, accounts: map[string]guiAccount{}}
	g.runLog = newRuntimeLog(profile, g.redactDAV)
	defer g.runLog.close()
	if err = g.loadAccounts(ctx); err != nil {
		t.Fatal("读取绑定账号失败")
	}
	var account guiAccount
	for _, key := range g.accountOrder {
		if g.accounts[key].Provider == "wopan" {
			account = g.accounts[key]
			break
		}
	}
	if account.Key == "" {
		t.Fatal("没有联通账号")
	}
	client, err := g.accountClient(account.Key)
	if err != nil {
		t.Fatal(err)
	}
	client.folder = ""
	drive := newWopanDAVDrive(client, account.credentials, "0", func(access, refresh string) error { return g.saveDAVCredential(account, access, refresh) })
	trace := newRunTrace(ctx, g.runLog, "纯内存实测", "/CloudProbe-owned-test")
	if err = drive.probe(trace); err != nil {
		t.Fatal(client.redact.Text(err.Error()))
	}
	directory := "CloudProbe-Stream-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	root, err := drive.Mkdir(trace, "0", directory)
	if err != nil {
		t.Fatal(client.redact.Text(err.Error()))
	}
	cleaned := false
	defer func() {
		cleanup, c := context.WithTimeout(context.WithoutCancel(trace), 40*time.Second)
		defer c()
		if e := drive.Remove(cleanup, root); e != nil {
			t.Error("专用测试目录清理失败：" + client.redact.Text(e.Error()))
		} else {
			cleaned = true
		}
		report := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "result": map[bool]string{true: "PASS", false: "FAIL"}[!t.Failed()], "test_directory_cleaned": cleaned, "file_body_disk_spools": 0}
		_ = writePrivateJSON(filepath.Join(profile, "stream-live-summary.json"), report)
	}()
	options := transferOptions{UploadMemoryMiB: 32, TotalUploadMemoryMiB: 64, UploadPrefetchMiB: 10, UploadLowSpeedMB: 1000, UploadLowSeconds: 1, UploadRecoveryTries: 1}.defaults()
	runtime := newTransferRuntime(options)
	drive.uploadConcurrency = 4
	drive.uploadPartMiB = 5
	drive.uploadBudget = runtime.connections
	drive.uploadMode = "auto"
	server, _, fs := testDAVService(t, true)
	fs.mounts = []*davMount{{Alias: "联通云盘-1", Root: root.ID, Provider: "wopan", Identity: davIdentity(account), Writable: true, drive: drive}}
	fs.transfers = runtime
	fs.spool = filepath.Join(profile, "must-not-have-upload-spools")
	// Attach private operation logging to the same real WebDAV handler.
	service := server.Config.Handler.(*davService)
	service.manager = &davManager{app: g}
	sizes := []int64{6 << 20, 11 << 20, 36 << 20}
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, len(sizes))
	for i, size := range sizes {
		wg.Go(func() {
			req, e := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%s/dav/联通云盘-1/file-%d.bin", server.URL, i), io.LimitReader(repeatUploadByte(49+i), size))
			if e != nil {
				errs <- e
				return
			}
			req.ContentLength = size
			req.SetBasicAuth("tester", "test-password-123")
			resp, e := http.DefaultClient.Do(req)
			if e == nil {
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					e = fmt.Errorf("测试 PUT 返回 %d：%s", resp.StatusCode, client.redact.Text(string(data)))
				}
			}
			errs <- e
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Error(client.redact.Text(e.Error()))
		}
	}
	if t.Failed() {
		return
	}
	uploadTime := time.Since(started)
	for i, size := range sizes {
		req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/dav/联通云盘-1/file-%d.bin", server.URL, i), nil)
		req.SetBasicAuth("tester", "test-password-123")
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(client.redact.Text(e.Error()))
		}
		got := sha256.New()
		n, e := io.Copy(got, resp.Body)
		resp.Body.Close()
		want := sha256.New()
		io.Copy(want, io.LimitReader(repeatUploadByte(49+i), size))
		if e != nil || resp.StatusCode != 200 || n != size || hex.EncodeToString(got.Sum(nil)) != hex.EncodeToString(want.Sum(nil)) {
			t.Fatal("真实下载长度或 SHA256 不一致")
		}
	}
	if _, e := os.Stat(fs.spool); !os.IsNotExist(e) {
		t.Fatal("实网纯内存上传创建了磁盘正文暂存")
	}
	memory := runtime.memory.status()
	if memory["used_bytes"].(int64) != 0 || memory["peak_bytes"].(int64) > 64<<20 || memory["allocated_bytes"].(int64) > 64<<20 {
		t.Fatal("实网内存超预算或未归还")
	}
	recovery := runtime.recovery.status()
	if recovery["attempts"].(int) == 0 {
		t.Fatal("测试专用高阈值未触发总体暂停")
	}
	report := map[string]any{"version": version, "provider": "wopan", "files": 3, "uploaded_bytes": sizes[0] + sizes[1] + sizes[2], "upload_duration_ms": uploadTime.Milliseconds(), "sha256_all_match": true, "file_body_disk_spools": 0, "upload_memory": memory, "aggregate_recovery": recovery, "test_threshold_mb": 1000, "test_window_seconds": 1, "production_defaults_restored_separately": true}
	if err = writePrivateJSON(filepath.Join(profile, "stream-live-details.json"), report); err != nil {
		t.Fatal(err)
	}
	t.Logf("真实联通：3 文件并发纯内存上传/下载 SHA256 一致；正文缓存峰值 %.1f MiB；总体暂停 %d 次；上传 %.1f 秒", float64(memory["peak_bytes"].(int64))/(1<<20), recovery["attempts"].(int), uploadTime.Seconds())
}

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Explicitly gated protocol experiment: only an isolated synthetic directory is
// touched. Whether deferred hashes are supported is a recorded result, not an
// assumption used to enable a production path.
func TestLiveQuarkDeferredHashProbe(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK_DEFERRED_HASH") != "1" {
		t.Skip("按需验证夸克延后哈希协议")
	}
	app := &guiApp{data: os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE"), accounts: map[string]guiAccount{}}
	app.quark = newQuarkManager(app)
	client, err := quarkLiveClient(app.quark)
	if err != nil {
		t.Fatal("未能选择已授权的测试账号")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	runtime := newTransferRuntime(transferOptions{UploadRecoveryMode: "off"})
	drive := newQuarkDrive(client, runtime, 1, "auto")
	dir, err := drive.Mkdir(ctx, "0", "CloudProbe-deferred-hash-"+randomKey()[:12])
	if err != nil {
		t.Fatal("未能建立专用实验目录")
	}
	report := map[string]any{"version": version, "deferred_hash_supported": false, "cleanup_ok": false}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		report["cleanup_ok"] = drive.Remove(cleanup, dir) == nil
		if path := os.Getenv("CLOUDPROBE_LIVE_QUARK_DEFERRED_REPORT"); path != "" {
			data, _ := json.MarshalIndent(report, "", "  ")
			_ = os.MkdirAll(filepath.Dir(path), 0700)
			_ = os.WriteFile(path, data, 0600)
		}
		if report["cleanup_ok"] != true {
			t.Error("实验目录清理未确认")
		}
	}()
	payload := make([]byte, 12<<20+37)
	if _, err = rand.Read(payload); err != nil {
		t.Fatal("生成合成正文失败")
	}
	u := &quarkUpload{drive: drive, route: quarkDesktop, parent: dir.ID, name: "probe.bin", size: int64(len(payload)), mime: "application/octet-stream"}
	if err = u.prepareSession(ctx); err != nil {
		t.Fatal("实验会话准备失败")
	}
	parts := max(int64(1), (u.size+u.partSize-1)/u.partSize)
	tags := make([]string, parts)
	var sent, confirmed atomic.Int64
	for index := int64(1); index <= parts; index++ {
		off := (index - 1) * u.partSize
		tag, partErr := u.part(ctx, bytes.NewReader(payload), index, parts, off, min(u.partSize, u.size-off), &sent, &confirmed)
		if partErr != nil {
			report["failed_phase"] = "part_before_hash"
			var upstream *upstreamHTTPError
			if errors.As(partErr, &upstream) {
				report["http_status"] = upstream.code
			}
			t.Log("延后哈希未通过分片阶段，记录为不支持")
			return
		}
		tags[index-1] = tag
	}
	u.md5 = fmt.Sprintf("%x", md5.Sum(payload))
	u.sha1 = fmt.Sprintf("%x", sha1.Sum(payload))
	if err = u.submitHash(ctx); err != nil {
		report["failed_phase"] = "hash_after_parts"
		return
	}
	if err = u.commitOSS(ctx, tags); err != nil {
		report["failed_phase"] = "oss_commit"
		return
	}
	if _, err = client.call(ctx, u.route, "POST", "/file/upload/finish", nil, map[string]any{"task_id": u.task, "obj_key": u.key}); err != nil {
		report["failed_phase"] = "finish"
		return
	}
	entry, err := drive.verifyQuarkUpload(ctx, dir.ID, u.name, u.size, "", u.sha1, true)
	if err != nil {
		report["failed_phase"] = "listing_verification"
		return
	}
	reader := newTransferReader(ctx, &davMount{drive: drive, health: &routeHealth{}}, entry, runtime)
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || sha256.Sum256(data) != sha256.Sum256(payload) {
		t.Fatal("实验正文回读校验失败")
	}
	report["deferred_hash_supported"], report["bytes"], report["parts"] = true, len(payload), parts
	t.Log("延后哈希、合并及完整正文校验通过")
}

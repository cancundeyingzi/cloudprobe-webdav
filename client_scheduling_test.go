package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSchedulingFollowsFilesButKeepsBodyBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runtime := newTransferRuntime(transferOptions{TransferScheduling: "client", ConcurrentUploads: 1, ConcurrentDownloads: 1, TotalConnections: 1})
	for _, kind := range []string{"上传", "下载"} {
		var releases []func()
		for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
			release, err := runtime.file(ctx, kind, key)
			if err != nil {
				t.Fatalf("client mode queued a file: %v", err)
			}
			releases = append(releases, release)
		}
		for _, release := range releases {
			release()
		}
	}
	if runtime.uploads.limiter.status()["limit"] != 0 || runtime.downloads.limiter.status()["limit"] != 0 {
		t.Fatal("client mode applied a file count cap")
	}
	release, err := runtime.connections.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	if extra, err := runtime.connections.acquire(waitCtx); err == nil {
		extra()
		t.Fatal("client mode bypassed body request budget")
	}
	release()
	if runtime.memory.status()["limit_bytes"] != int64(128<<20) {
		t.Fatal("upload memory protection changed")
	}
}

func TestClientSchedulingDoesNotMultiplyDownloadRequests(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	payload := bytes.Repeat([]byte("request-following-"), 400000)
	drive.entries["file"] = davEntry{Item: Item{ID: "file", FID: "immutable-v1", Name: "large.bin", Size: int64(len(payload))}}
	drive.parents["file"] = "0"
	drive.contents["file"] = payload
	var requests atomic.Int64
	upstream := drive.server.Config.Handler
	drive.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		upstream.ServeHTTP(w, r)
	})
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "client"})
	body := wantDAV(t, server.URL, "GET", "/联通云盘-1/large.bin", "", 200, nil)
	if !bytes.Equal(body, payload) || requests.Load() != 1 {
		t.Fatalf("one client GET produced %d upstream requests or corrupted content", requests.Load())
	}
	requests.Store(0)
	body = wantDAV(t, server.URL, "GET", "/联通云盘-1/large.bin", "", 206, map[string]string{"Range": "bytes=17-1023"})
	if !bytes.Equal(body, payload[17:1024]) || requests.Load() != 1 {
		t.Fatalf("client Range was multiplied or changed: requests=%d", requests.Load())
	}
	requests.Store(0)
	wantDAV(t, server.URL, "HEAD", "/联通云盘-1/large.bin", "", 200, nil)
	if requests.Load() != 0 {
		t.Fatal("HEAD fetched upstream body")
	}
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "server"})
	body = wantDAV(t, server.URL, "GET", "/联通云盘-1/large.bin", "", 200, nil)
	if !bytes.Equal(body, payload) || requests.Load() < 2 {
		t.Fatal("server download acceleration was not preserved")
	}
}

func TestClientSchedulingAllowsSevenSimultaneousHTTPUploads(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "client", ConcurrentUploads: 1, UploadStorageMode: "disk"})
	stalled := &countedStalledUploads{fakeDAVDrive: base, started: make(chan struct{}, 8), release: make(chan struct{})}
	fs.mounts[0].drive = stalled
	var wg sync.WaitGroup
	errs := make(chan error, 7)
	client := &http.Client{Timeout: 5 * time.Second}
	for i := 0; i < 7; i++ {
		wg.Go(func() {
			req, err := http.NewRequest("PUT", server.URL+fmt.Sprintf("/dav/%%E8%%81%%94%%E9%%80%%9A%%E4%%BA%%91%%E7%%9B%%98-1/client-%d.bin", i), strings.NewReader("test-body"))
			if err != nil {
				errs <- err
				return
			}
			req.SetBasicAuth("tester", "test-password-123")
			resp, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != 201 {
				errs <- fmt.Errorf("PUT status %d", resp.StatusCode)
			}
		})
	}
	started := 0
	timer := time.NewTimer(3 * time.Second)
	for started < 7 {
		select {
		case <-stalled.started:
			started++
		case <-timer.C:
			close(stalled.release)
			wg.Wait()
			t.Fatalf("only %d of 7 client uploads entered concurrently", started)
		}
	}
	timer.Stop()
	close(stalled.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if q := fs.transfers.uploads.limiter.status(); q["active"] != 0 || q["queued"] != 0 {
		t.Fatal("client upload leases leaked", q)
	}
}

func TestSchedulingDefaultsPreserveExistingProfiles(t *testing.T) {
	old := transferOptions{}.defaults()
	if old.TransferScheduling != "server" || old.ConcurrentUploads != 3 || old.ConcurrentDownloads != 4 || old.DownloadConcurrency != 8 {
		t.Fatal("legacy defaults changed")
	}
	old.TransferScheduling = "unknown"
	if old.validate() == nil {
		t.Fatal("invalid scheduling option accepted")
	}
	data := t.TempDir()
	app := &guiApp{data: data}
	fresh := newDAVManager(app)
	if fresh.config.TransferScheduling != "client" {
		t.Fatal("new GUI profile must prefer client scheduling")
	}
	if err := os.WriteFile(filepath.Join(data, "webdav.json"), []byte(`{"concurrent_uploads":2,"concurrent_downloads":5}`), 0600); err != nil {
		t.Fatal(err)
	}
	restored := newDAVManager(app).config.transferOptions.defaults()
	if restored.TransferScheduling != "server" || restored.ConcurrentUploads != 2 || restored.ConcurrentDownloads != 5 {
		t.Fatal("existing GUI profile must preserve its behavior")
	}
	encoded, err := json.Marshal(transferOptions{TransferScheduling: "client"}.defaults())
	if err != nil {
		t.Fatal(err)
	}
	var decoded transferOptions
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.TransferScheduling != "client" {
		t.Fatal("scheduling choice did not survive persistence")
	}
}

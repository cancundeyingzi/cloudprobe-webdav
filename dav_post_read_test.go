package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestDAVPostSharesDownloadScheduling(t *testing.T) {
	server, _, fs := testDAVService(t, true)
	file := "/联通云盘-1/file.bin"
	wantDAV(t, server.URL, "PUT", file, "body", http.StatusCreated, nil)
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "server", ConcurrentDownloads: 1, UploadStorageMode: "disk"})
	release, err := fs.transfers.file(context.Background(), "下载", "other-file")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan int, 1)
	go func() { code, _, _ := davRequest(t, server.URL, "POST", file, nil, nil); done <- code }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for fs.transfers.downloads.limiter.status()["queued"] == 0 {
		select {
		case code := <-done:
			t.Fatalf("POST bypassed occupied download slot: %d", code)
		case <-deadline.C:
			t.Fatal("POST did not enter the download queue")
		case <-ticker.C:
		}
	}
	if fs.transfers.uploads.limiter.status()["active"] != 0 {
		t.Fatal("POST was scheduled as upload")
	}
	release()
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatal(code)
		}
	case <-deadline.C:
		t.Fatal("POST did not resume after download slot release")
	}
}

func TestDAVConditionalPostRefreshesCachedMetadata(t *testing.T) {
	server, drive, _ := testDAVService(t, true)
	file := "/联通云盘-1/file.bin"
	wantDAV(t, server.URL, "PUT", file, "old", http.StatusCreated, nil)
	_, _, head := davRequest(t, server.URL, "HEAD", file, nil, nil)
	drive.mu.Lock()
	for id, entry := range drive.entries {
		if entry.Name == "file.bin" {
			entry.FID += "-new"
			drive.entries[id] = entry
			drive.contents[id] = []byte("new")
		}
	}
	drive.mu.Unlock()
	code, _, _ := davRequest(t, server.URL, "POST", file, nil, map[string]string{"If-Match": head.Get("ETag")})
	if code != http.StatusPreconditionFailed {
		t.Fatalf("conditional POST used stale metadata: %d", code)
	}
	code, body, _ := davRequest(t, server.URL, "POST", file, nil, nil)
	if code != http.StatusOK || string(body) != "new" {
		t.Fatalf("ordinary POST download changed: %d %q", code, body)
	}
}

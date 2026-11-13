package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type countedDAVDrive struct {
	davDrive
	lists atomic.Int64
}

func (d *countedDAVDrive) List(ctx context.Context, dir string) ([]davEntry, error) {
	d.lists.Add(1)
	return d.davDrive.List(ctx, dir)
}

func TestDAVRangeReadsReuseDirectoriesAndConditionsRefreshTarget(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	counter := &countedDAVDrive{davDrive: drive}
	fs.mounts[0].drive = counter
	root := "/联通云盘-1"
	for _, dir := range []string{"/a", "/a/b", "/a/b/c"} {
		wantDAV(t, server.URL, "MKCOL", root+dir, "", http.StatusCreated, nil)
	}
	file := root + "/a/b/c/file.bin"
	wantDAV(t, server.URL, "PUT", file, strings.Repeat("x", 4096), http.StatusCreated, nil)
	_, _, header := davRequest(t, server.URL, "HEAD", file, nil, nil)
	before := counter.lists.Load()
	for index := 0; index < 10; index++ {
		wantDAV(t, server.URL, "GET", file, "", http.StatusPartialContent, map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", index*100, index*100+99)})
	}
	if calls := counter.lists.Load() - before; calls != 0 {
		t.Fatalf("cached Range requests caused %d directory queries", calls)
	}
	drive.mu.Lock()
	for id, entry := range drive.entries {
		if entry.Name == "file.bin" {
			entry.Modified = entry.Modified.Add(time.Hour)
			entry.FID += "-changed"
			drive.entries[id] = entry
		}
	}
	drive.mu.Unlock()
	code, _, _ := davRequest(t, server.URL, "GET", file, nil, map[string]string{"If-Match": header.Get("ETag")})
	if code != http.StatusPreconditionFailed {
		t.Fatalf("conditional GET ignored external change: %d", code)
	}
	if calls := counter.lists.Load() - before; calls != 1 {
		t.Fatalf("conditional read refreshed %d directories, want only the parent", calls)
	}
}

func TestDAVCacheInvalidationIsScopedToMount(t *testing.T) {
	cache := davDirCache{}
	cache.put("one", "0", []davEntry{{Item: Item{ID: "a"}}}, 0)
	cache.put("two", "0", []davEntry{{Item: Item{ID: "b"}}}, 0)
	cache.invalidate("one")
	cache.put("one", "0", nil, 0)
	if _, _, ok := cache.get("one", "0"); ok {
		t.Fatal("stale response repopulated an invalidated mount")
	}
	if _, _, ok := cache.get("two", "0"); !ok {
		t.Fatal("unrelated mount lost its cache")
	}
}

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
	if calls := counter.lists.Load() - before; calls != 4 {
		t.Fatalf("conditional read queried %d directories, want each ancestor once", calls)
	}
}

func TestDAVConditionalReadsResolveReplacedAncestors(t *testing.T) {
	for _, test := range []struct {
		header string
		status int
	}{
		{"If-Match", http.StatusPreconditionFailed},
		{"If-None-Match", http.StatusOK},
		{"If-Range", http.StatusOK},
		{"If-Modified-Since", http.StatusOK},
		{"If-Unmodified-Since", http.StatusPreconditionFailed},
	} {
		t.Run(test.header, func(t *testing.T) {
			server, drive, _ := testDAVService(t, true)
			ctx := context.Background()
			oldDir, err := drive.Mkdir(ctx, "0", "a")
			if err != nil {
				t.Fatal(err)
			}
			drive.mu.Lock()
			drive.entries["old-file"] = davEntry{Item: Item{ID: "old-file", FID: "old-version", Name: "f.bin", Size: 3}, Modified: time.Unix(100000, 0)}
			drive.parents["old-file"], drive.contents["old-file"] = oldDir.ID, []byte("old")
			drive.mu.Unlock()
			file := "/联通云盘-1/a/f.bin"
			code, _, head := davRequest(t, server.URL, "HEAD", file, nil, nil)
			if code != http.StatusOK {
				t.Fatal(code)
			}
			if err := drive.Move(ctx, oldDir, "", "b"); err != nil {
				t.Fatal(err)
			}
			newDir, err := drive.Mkdir(ctx, "0", "a")
			if err != nil {
				t.Fatal(err)
			}
			drive.mu.Lock()
			drive.entries["new-file"] = davEntry{Item: Item{ID: "new-file", FID: "new-version", Name: "f.bin", Size: 3}, Modified: time.Unix(200000, 0)}
			drive.parents["new-file"], drive.contents["new-file"] = newDir.ID, []byte("new")
			drive.mu.Unlock()
			value := head.Get("ETag")
			if strings.Contains(test.header, "Since") {
				value = head.Get("Last-Modified")
			}
			headers := map[string]string{test.header: value}
			if test.header == "If-Range" {
				headers["Range"] = "bytes=0-0"
			}
			code, body, _ := davRequest(t, server.URL, "GET", file, nil, headers)
			if code != test.status || code == http.StatusOK && string(body) != "new" {
				t.Fatalf("replaced ancestor: status=%d body=%q, want %d and current content", code, body, test.status)
			}
		})
	}
}

func TestDAVDirectoryCacheExpiresUnvisitedEntries(t *testing.T) {
	now := time.Now()
	cache := davDirCache{now: func() time.Time { return now }}
	mount := &davMount{Alias: "one"}
	cache.put(mount, "old", []davEntry{{Item: Item{ID: "old"}}}, 0)
	now = now.Add(davDirectoryTTL + time.Second)
	cache.put(mount, "new", []davEntry{{Item: Item{ID: "new"}}}, 0)
	if len(cache.entries) != 1 || cache.items != 1 {
		t.Fatal("expired, unvisited directory is still retained")
	}
	if _, _, ok := cache.get(mount, "old"); ok {
		t.Fatal("expired directory hit")
	}
	if _, _, ok := cache.get(mount, "new"); !ok {
		t.Fatal("current directory was lost")
	}
}

func TestDAVDirectoryCacheBoundsCapacityAndReplacements(t *testing.T) {
	cache := davDirCache{}
	mount := &davMount{Alias: "one"}
	for index := 0; index <= davCacheMaxDirectories; index++ {
		cache.put(mount, fmt.Sprint(index), []davEntry{{Item: Item{ID: "entry"}}}, 0)
	}
	if _, _, ok := cache.get(mount, "0"); ok {
		t.Fatal("oldest directory exceeded capacity")
	}
	if _, _, ok := cache.get(mount, fmt.Sprint(davCacheMaxDirectories)); !ok {
		t.Fatal("newest directory was not cached")
	}
	for index := 0; index < 20; index++ {
		cache.put(mount, "replace", make([]davEntry, davCacheMaxEntries/2), 0)
	}
	cache.put(mount, "another", make([]davEntry, davCacheMaxEntries/2), 0)
	if _, _, ok := cache.get(mount, "replace"); !ok {
		t.Fatal("replacements consumed additional capacity")
	}
	cache.put(mount, "one-more", []davEntry{{Item: Item{ID: "one"}}}, 0)
	if cache.items > davCacheMaxEntries || len(cache.entries) > davCacheMaxDirectories {
		t.Fatal("capacity exceeded")
	}
	cache.invalidate("one")
	if cache.items != 0 || cache.bytes != 0 || cache.order.Len() != 0 {
		t.Fatal("invalidation leaked accounting or snapshots")
	}
}

func TestDAVDirectoryCacheSkipsOversizedSnapshots(t *testing.T) {
	mount := &davMount{Alias: "one"}
	cache := davDirCache{}
	cache.put(mount, "too-many", make([]davEntry, davCacheMaxEntries+1), 0)
	cache.put(mount, "too-large", []davEntry{{Item: Item{ID: "large", Name: strings.Repeat("x", davCacheMaxBytes+1)}}}, 0)
	if len(cache.entries) != 0 {
		t.Fatal("oversized snapshots were cached")
	}
	cache.put(mount, "valid", []davEntry{{Item: Item{ID: "valid"}}}, 0)
	if _, _, ok := cache.get(mount, "valid"); !ok {
		t.Fatal("oversized snapshots prevented later caching")
	}
}

func TestDAVCacheInvalidationIsScopedToMount(t *testing.T) {
	cache := davDirCache{}
	one, two := &davMount{Alias: "one"}, &davMount{Alias: "two"}
	cache.put(one, "0", []davEntry{{Item: Item{ID: "a"}}}, 0)
	cache.put(two, "0", []davEntry{{Item: Item{ID: "b"}}}, 0)
	cache.invalidate("one")
	cache.put(one, "0", nil, 0)
	if _, _, ok := cache.get(one, "0"); ok {
		t.Fatal("stale response repopulated an invalidated mount")
	}
	if _, _, ok := cache.get(two, "0"); !ok {
		t.Fatal("unrelated mount lost its cache")
	}
	if _, _, ok := cache.get(&davMount{Alias: "two"}, "0"); ok {
		t.Fatal("a replacement mount consumed the previous driver's snapshot")
	}
}

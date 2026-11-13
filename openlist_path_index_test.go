package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type replacingBackupListDrive struct {
	davDrive
	generation int
}

func (d *replacingBackupListDrive) List(context.Context, string) ([]davEntry, error) {
	d.generation++
	entries := make([]davEntry, 20000)
	for i := range entries {
		entries[i] = davEntry{Item: Item{ID: fmt.Sprintf("generation-%d-file-%d", d.generation, i), Name: fmt.Sprintf("file-%d.bin", i), Size: 1}}
	}
	return entries, nil
}

func TestOpenListPathIndexReplacesCompleteDirectorySnapshots(t *testing.T) {
	drive := &replacingBackupListDrive{}
	backup := &backupDAVDrive{primary: drive, root: "0"}
	mount := &davMount{Alias: "fixture", Root: "0", drive: backup}
	fs := &davFS{mounts: []*davMount{mount}}
	now := time.Unix(100000, 0)
	fs.dirCache.now = func() time.Time { return now }
	for generation := 1; generation <= 3; generation++ {
		entries, err := fs.listing(context.Background(), mount, "0")
		if err != nil || len(entries) != 20000 || drive.generation != generation {
			t.Fatalf("normal List path: generation=%d driver=%d entries=%d err=%v", generation, drive.generation, len(entries), err)
		}
		if len(backup.index.nodes) > 20000 || backup.index.bytes > backupPathMaxBytes {
			t.Fatalf("obsolete snapshots retained: nodes=%d bytes=%d", len(backup.index.nodes), backup.index.bytes)
		}
		now = now.Add(davDirectoryTTL + time.Second)
	}
	if _, err := backup.relative("generation-1-file-0"); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("first generation still resolves", err)
	}
	if fs.dirCache.items != 20000 || len(fs.dirCache.entries) != 1 {
		t.Fatal("directory cache did not replace its expired snapshot")
	}
}

func TestOpenListPathIndexBoundsEntriesBytesAndAncestorRetention(t *testing.T) {
	b := &backupDAVDrive{root: "0", index: backupPathIndex{maxEntries: 4, maxBytes: 2400}}
	for i := range 100 {
		b.remember("0", []davEntry{{Item: Item{ID: fmt.Sprint(i), Name: strings.Repeat("x", 150) + fmt.Sprint(i)}}})
		if len(b.index.nodes) > 4 || b.index.bytes > 2400 || b.index.order.Len() != len(b.index.nodes) {
			t.Fatal("index budget or accounting exceeded", len(b.index.nodes), b.index.bytes)
		}
	}
	b.remember("0", []davEntry{{Item: Item{ID: "oversize", Name: strings.Repeat("x", 4000)}}})
	if _, err := b.relative("oversize"); err == nil {
		t.Fatal("oversized path cached")
	}
	root := b.rootLocation()
	parent := &backupPathNode{owner: b, ancestor: root, id: "parent", parent: "0", relative: "a", directory: true}
	child := &backupPathNode{owner: b, ancestor: parent, id: "child", parent: "parent", relative: "a/f"}
	if backupPathNodeBytes(child) <= backupPathNodeBytes(parent)+len(child.relative) {
		t.Fatal("child budget excludes retained ancestor nodes")
	}
	if directoryCacheBytes([]davEntry{{backupPath: child}}) < backupPathNodeBytes(child) {
		t.Fatal("directory cache budget excludes retained path chain")
	}
}

func TestOpenListPathIndexRefreshInvalidatesCachedDescendantAfterAncestorEviction(t *testing.T) {
	for _, operation := range []string{"refresh", "delete"} {
		t.Run(operation, func(t *testing.T) {
			b := &backupDAVDrive{root: "0", index: backupPathIndex{maxEntries: 1}}
			b.remember("0", []davEntry{{Item: Item{ID: "directory", Name: "folder", Directory: true}}})
			b.remember("directory", []davEntry{{Item: Item{ID: "file", Name: "file.bin"}}})
			if _, err := b.relative("directory"); err == nil {
				t.Fatal("fixture did not evict ancestor")
			}
			if operation == "refresh" {
				b.rememberListing("0", nil)
			} else {
				b.forget(davEntry{Item: Item{ID: "directory"}})
			}
			if _, err := b.relative("file"); !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("invalidation missed evicted ancestor retained by child", err)
			}
			if len(b.index.nodes) != 0 || b.index.bytes != 0 || b.index.mapSlots != 0 || b.index.order.Len() != 0 {
				t.Fatal("empty index retained capacity accounting", b.index.bytes, b.index.mapSlots)
			}
		})
	}
}

type fixedBackupListDrive struct {
	davDrive
	entries []davEntry
	started chan struct{}
	release chan struct{}
}

func (d *fixedBackupListDrive) List(context.Context, string) ([]davEntry, error) {
	if d.started != nil {
		close(d.started)
		<-d.release
	}
	return append([]davEntry(nil), d.entries...), nil
}

func TestOpenListPathIndexRejectsInvalidSnapshots(t *testing.T) {
	for _, entries := range [][]davEntry{
		{{Item: Item{ID: "one", Name: "same"}}, {Item: Item{ID: "two", Name: "same"}}},
		{{Item: Item{ID: "same", Name: "one"}}, {Item: Item{ID: "same", Name: "two"}}},
		{{Item: Item{ID: "one", Name: "../escape"}}},
		{{Item: Item{Name: "missing-id"}}},
	} {
		d := &fixedBackupListDrive{entries: entries}
		b := &backupDAVDrive{primary: d, root: "0"}
		b.remember("0", []davEntry{{Item: Item{ID: "old", Name: "old"}}})
		if got, err := b.List(context.Background(), "0"); err == nil || got != nil {
			t.Fatal("malformed native listing accepted", entries, err)
		}
		if _, err := b.relative("old"); !errors.Is(err, errBackupPathUnavailable) {
			t.Fatal("malformed listing retained an older validated snapshot", err)
		}
	}
}

func TestOpenListPathIndexInflightListingCannotRestoreMutatedLocation(t *testing.T) {
	entry := davEntry{Item: Item{ID: "file", Name: "old"}}
	d := &fixedBackupListDrive{entries: []davEntry{entry}, started: make(chan struct{}), release: make(chan struct{})}
	b := &backupDAVDrive{primary: d, root: "0"}
	b.remember("0", []davEntry{entry})
	result := make(chan error, 1)
	go func() {
		_, err := b.List(context.Background(), "0")
		result <- err
	}()
	<-d.started
	b.forget(entry)
	close(d.release)
	if err := <-result; !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("listing started before mutation was accepted afterward", err)
	}
	if _, err := b.relative(entry.ID); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("stale listing resurrected removed path", err)
	}
}

func backupIndexFixture(t *testing.T, nested bool) (*davFS, *backupDAVDrive, *fakeDAVDrive, davEntry) {
	t.Helper()
	base := newFakeDAV(t)
	parent := "0"
	if nested {
		dir, err := base.Mkdir(context.Background(), "0", "folder")
		if err != nil {
			t.Fatal(err)
		}
		parent = dir.ID
	}
	entry := davEntry{Item: Item{ID: "native-file", Name: "file.bin", Size: 3}}
	base.entries[entry.ID] = entry
	base.parents[entry.ID] = parent
	base.contents[entry.ID] = []byte("old")
	api, _ := newOpenListFixture(t, base)
	b := &backupDAVDrive{primary: base, backup: api, root: "0"}
	fs := &davFS{mounts: []*davMount{{Alias: "test", Root: "0", drive: b}}}
	return fs, b, base, entry
}

func TestOpenListPathIndexEvictionPreservesInflightFallback(t *testing.T) {
	fs, b, _, _ := backupIndexFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := fs.lookup(ctx, "/test/folder/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	routes := b.ReadRoutes(ctx, node.entry)
	b.index.maxEntries = 1
	b.remember("0", []davEntry{{Item: Item{ID: "other", Name: "other.bin"}}})
	if _, err := b.relative(node.entry.ID); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("fixture failed to evict original file", err)
	}
	if _, err := routes[len(routes)-2].Resolve(ctx); err != nil {
		t.Fatal("capacity eviction broke the active request's verified location", err)
	}
	// A bare ID has no authority to guess its old path after eviction.
	bare := node.entry
	bare.backupPath = nil
	if _, err := b.readLocation(context.Background(), bare); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("unmapped native ID unexpectedly acquired a path", err)
	}
	// The ordinary cached directory walk can reconstruct exactly the selected IDs.
	rebuilt, err := fs.lookup(ctx, "/test/folder/file.bin")
	if err != nil || rebuilt.entry.ID != node.entry.ID {
		t.Fatal("normal lookup did not rebuild an evicted location", err)
	}
	rebuiltRoutes := b.ReadRoutes(ctx, rebuilt.entry)
	if _, err := rebuiltRoutes[len(rebuiltRoutes)-1].Resolve(ctx); err != nil {
		t.Fatal("rebuilt location cannot use proxy fallback", err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		b.index.mu.Lock()
		active := len(b.index.active)
		b.index.mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed request retained selected locations", active)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOpenListPathIndexRefreshInvalidatesEvictedAncestorAndReplacement(t *testing.T) {
	fs, b, base, original := backupIndexFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, err := fs.lookup(ctx, "/test/folder/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	routes := b.ReadRoutes(ctx, node.entry)
	b.index.maxEntries = 1
	b.remember("0", []davEntry{{Item: Item{ID: "other", Name: "other.bin"}}})
	parent := base.parents[original.ID]
	if err := base.Remove(ctx, base.entries[parent]); err != nil {
		t.Fatal(err)
	}
	replacement, err := base.Mkdir(ctx, "0", "folder")
	if err != nil {
		t.Fatal(err)
	}
	base.mu.Lock()
	base.entries["replacement"] = davEntry{Item: Item{ID: "replacement", Name: original.Name, Size: original.Size}}
	base.parents["replacement"] = replacement.ID
	base.contents["replacement"] = []byte("new")
	base.mu.Unlock()
	if _, err := b.List(ctx, "0"); err != nil {
		t.Fatal(err)
	}
	if _, err := routes[len(routes)-2].Resolve(ctx); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("deleted ancestor allowed same-name/same-size replacement fallback", err)
	}
	fs.invalidate(fs.mounts[0])
	current, err := fs.lookup(ctx, "/test/folder/file.bin")
	if err != nil || current.entry.ID != "replacement" {
		t.Fatal("new request failed to resolve replacement by its actual native ID", err)
	}
}

func TestOpenListPathIndexMutationInvalidatesSubtree(t *testing.T) {
	for _, mutation := range []string{"remove", "rename"} {
		t.Run(mutation, func(t *testing.T) {
			fs, b, _, _ := backupIndexFixture(t, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			file, err := fs.lookup(ctx, "/test/folder/file.bin")
			if err != nil {
				t.Fatal(err)
			}
			dir, err := fs.lookup(ctx, "/test/folder")
			if err != nil {
				t.Fatal(err)
			}
			routes := b.ReadRoutes(ctx, file.entry)
			if mutation == "remove" {
				err = b.Remove(ctx, dir.entry)
			} else {
				err = b.Move(ctx, dir.entry, "", "renamed")
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.relative(file.entry.ID); !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("mutation retained descendant mapping", err)
			}
			if _, err := routes[len(routes)-1].Resolve(ctx); !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("mutation retained inflight descendant location", err)
			}
			if mutation == "rename" {
				fs.invalidate(fs.mounts[0])
				current, err := fs.lookup(ctx, "/test/renamed/file.bin")
				if err != nil || current.entry.ID != file.entry.ID {
					t.Fatal("rename changed native identity or blocked ordinary path reconstruction", err)
				}
				currentRoutes := b.ReadRoutes(ctx, current.entry)
				if _, err := currentRoutes[len(currentRoutes)-2].Resolve(ctx); err != nil {
					t.Fatal("renamed file cannot use freshly verified fallback", err)
				}
			}
		})
	}
}

func TestOpenListPathIndexMountReplacementKeepsRequestOwner(t *testing.T) {
	fs, old, _, _ := backupIndexFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshot := fs.freshReadContext(ctx)
	before, err := fs.lookup(snapshot, "/test/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	otherFS, replacement, base, _ := backupIndexFixture(t, false)
	base.mu.Lock()
	base.contents["native-file"] = []byte("new")
	base.mu.Unlock()
	fs.mountMu.Lock()
	fs.mounts = otherFS.mounts
	fs.mountMu.Unlock()
	after, err := fs.lookup(ctx, "/test/file.bin")
	if err != nil || after.entry.backupPath.owner != replacement {
		t.Fatal("new request reused prior mount's location", err)
	}
	if _, err := replacement.readLocation(ctx, before.entry); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("old entry was accepted by another mount with matching IDs/names", err)
	}
	inflight, err := fs.lookup(snapshot, "/test/file.bin")
	if err != nil || inflight.mount != before.mount || inflight.entry.backupPath.owner != old {
		t.Fatal("inflight snapshot switched mount owner", err)
	}
	routes := old.ReadRoutes(ctx, inflight.entry)
	if _, err := routes[len(routes)-2].Resolve(ctx); err != nil {
		t.Fatal("mount replacement broke request bound to the old mount", err)
	}
}

type pausedBackupListCall struct {
	resume chan struct{}
}

type pausedBackupListDrive struct {
	*fakeDAVDrive
	calls chan pausedBackupListCall
}

func (d *pausedBackupListDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	entries, err := d.fakeDAVDrive.List(ctx, parent)
	if err != nil {
		return nil, err
	}
	call := pausedBackupListCall{resume: make(chan struct{})}
	select {
	case d.calls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-call.resume:
		return entries, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestOpenListPathIndexCompletedWritesSupersedeOlderListing(t *testing.T) {
	for _, operation := range []string{"upload", "mkdir"} {
		t.Run(operation, func(t *testing.T) {
			base := newFakeDAV(t)
			old := davEntry{Item: Item{ID: "old-file", Name: "target.bin", Size: 3}}
			base.entries[old.ID], base.parents[old.ID] = old, "0"
			drive := &pausedBackupListDrive{fakeDAVDrive: base, calls: make(chan pausedBackupListCall)}
			b := &backupDAVDrive{primary: drive, root: "0"}
			b.remember("0", []davEntry{old})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := b.List(ctx, "0"); result <- err }()
			call := <-drive.calls
			var created davEntry
			var err error
			if operation == "upload" {
				file, createErr := os.CreateTemp(t.TempDir(), "body-*")
				if createErr != nil {
					t.Fatal(createErr)
				}
				defer file.Close()
				if _, err := file.WriteString("new"); err != nil {
					t.Fatal(err)
				}
				created, err = b.Upload(ctx, "0", old.Name, file)
			} else {
				created, err = b.Mkdir(ctx, "0", "new-folder")
			}
			if err != nil {
				t.Fatal(err)
			}
			close(call.resume)
			if err := <-result; !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("old directory response overrode a completed write", err)
			}
			if _, err := b.relative(created.ID); err != nil {
				t.Fatal("completed write's location was removed by old response", err)
			}
			if operation == "upload" {
				if _, err := b.relative(old.ID); !errors.Is(err, errBackupPathUnavailable) {
					t.Fatal("old upload target ID was resurrected", err)
				}
			}
			if len(b.index.pending) != 0 {
				t.Fatal("finished listing retained publication state")
			}
		})
	}
}

func TestOpenListPathIndexRejectsReversedCompleteResponses(t *testing.T) {
	base := newFakeDAV(t)
	old := davEntry{Item: Item{ID: "old", Name: "file.bin", Size: 3}}
	base.entries[old.ID], base.parents[old.ID] = old, "0"
	drive := &pausedBackupListDrive{fakeDAVDrive: base, calls: make(chan pausedBackupListCall)}
	b := &backupDAVDrive{primary: drive, root: "0"}
	b.remember("0", []davEntry{old})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	older := make(chan error, 1)
	go func() { _, err := b.List(ctx, "0"); older <- err }()
	first := <-drive.calls
	base.mu.Lock()
	delete(base.entries, old.ID)
	current := davEntry{Item: Item{ID: "new", Name: old.Name, Size: old.Size}}
	base.entries[current.ID], base.parents[current.ID] = current, "0"
	base.mu.Unlock()
	newer := make(chan error, 1)
	go func() { _, err := b.List(ctx, "0"); newer <- err }()
	second := <-drive.calls
	close(second.resume)
	if err := <-newer; err != nil {
		t.Fatal("newer directory response failed", err)
	}
	close(first.resume)
	if err := <-older; !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("older directory response replaced newer publication", err)
	}
	if _, err := b.relative(current.ID); err != nil {
		t.Fatal("latest object location lost", err)
	}
	if _, err := b.relative(old.ID); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("older response restored old object identity", err)
	}
	if b.index.pending != nil {
		t.Fatal("publication state retained after final directory call")
	}
}

func TestOpenListPathIndexOpenedDirectoryReaddirSurvivesEviction(t *testing.T) {
	for _, openListID := range []bool{false, true} {
		t.Run(fmt.Sprint(openListID), func(t *testing.T) {
			fs, b, base, _ := backupIndexFixture(t, true)
			primary := &failingBackupPrimary{fakeDAVDrive: base, failList: openListID}
			b.primary = primary
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			file, err := fs.OpenFile(ctx, "/test/folder", os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			dir := file.(*davFile).node.entry
			b.index.maxEntries = 1
			b.remember("0", []davEntry{{Item: Item{ID: "other", Name: "other.bin"}}})
			if _, err := b.relative(dir.ID); !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("fixture did not evict opened directory", err)
			}
			primary.failList = true
			items, err := file.Readdir(-1)
			if err != nil || len(items) != 1 || items[0].Name() != "file.bin" {
				t.Fatal("opened directory lost its pinned fallback location", items, err)
			}
		})
	}
}

func TestOpenListPathIndexDetachedListingCannotRebindAfterMutation(t *testing.T) {
	for _, operation := range []string{"delete", "rename"} {
		t.Run(operation, func(t *testing.T) {
			base := newFakeDAV(t)
			original := davEntry{Item: Item{ID: "target", Name: "file.bin", Size: 3}}
			base.entries[original.ID], base.parents[original.ID] = original, "0"
			drive := &fixedBackupListDrive{davDrive: base, entries: []davEntry{original, {Item: Item{ID: "filler", Name: "filler"}}}}
			b := &backupDAVDrive{primary: drive, root: "0", index: backupPathIndex{maxEntries: 1}}
			old, err := b.List(context.Background(), "0")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.relative(original.ID); err == nil {
				t.Fatal("old result remained controlled by the index")
			}
			drive.entries = []davEntry{original}
			current, err := b.List(context.Background(), "0")
			if err != nil {
				t.Fatal(err)
			}
			if operation == "delete" {
				err = b.Remove(context.Background(), current[0])
			} else {
				err = b.Move(context.Background(), current[0], "", "renamed.bin")
			}
			if err != nil {
				t.Fatal(err)
			}
			root := davEntry{Item: Item{ID: "0", Directory: true}}
			if _, err := b.bindLocation(context.Background(), root, old[0]); !errors.Is(err, errBackupPathUnavailable) {
				t.Fatal("detached old result resurrected deleted/renamed path", err)
			}
		})
	}
}

func TestOpenListPathIndexStaleDirectoryCacheRefreshesBeforeSelection(t *testing.T) {
	fs, b, base, original := backupIndexFixture(t, false)
	counter := &countedDAVDrive{davDrive: base}
	b.primary = counter
	if _, err := fs.listing(context.Background(), fs.mounts[0], "0"); err != nil {
		t.Fatal(err)
	}
	b.index.maxEntries = 1
	b.remember("0", []davEntry{{Item: Item{ID: "filler", Name: "filler"}}})
	base.mu.Lock()
	delete(base.entries, original.ID)
	current := davEntry{Item: Item{ID: "new", Name: original.Name, Size: original.Size}}
	base.entries[current.ID], base.parents[current.ID] = current, "0"
	base.mu.Unlock()
	if _, err := b.List(context.Background(), "0"); err != nil {
		t.Fatal(err)
	}
	before := counter.lists.Load()
	node, err := fs.lookup(context.Background(), "/test/file.bin")
	if err != nil || node.entry.ID != current.ID {
		t.Fatal("old cached location was selected after a newer publication", node.entry.ID, err)
	}
	if counter.lists.Load()-before != 1 {
		t.Fatal("stale path did not refresh exactly its parent directory")
	}
}

func TestOpenListPathIndexUnchangedRefreshPreservesActiveNodes(t *testing.T) {
	fs, b, _, _ := backupIndexFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	file, err := fs.lookup(ctx, "/test/folder/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	routes := b.ReadRoutes(ctx, file.entry)
	b.index.maxEntries = 1
	b.remember("0", []davEntry{{Item: Item{ID: "filler", Name: "filler"}}})
	for range 20 {
		dirs, err := b.List(ctx, "0")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.List(b.parentContext(ctx, dirs[0]), dirs[0].ID); err != nil {
			t.Fatal(err)
		}
		b.ReadRoutes(ctx, file.entry)
		if err := file.entry.backupPath.usable(); err != nil {
			t.Fatal("unchanged refresh invalidated a selected request node", err)
		}
	}
	if _, err := routes[len(routes)-2].Resolve(ctx); err != nil {
		t.Fatal("unchanged directory refresh broke active fallback", err)
	}
	b.index.mu.Lock()
	registrations := len(b.index.active[file.entry.backupPath])
	b.index.mu.Unlock()
	if registrations != 1 {
		t.Fatal("same request repeatedly registered the same node", registrations)
	}
}

func TestOpenListPathIndexUnlocatedNativeListingCannotRebindAfterDeletion(t *testing.T) {
	fs, b, base, original := backupIndexFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir, err := fs.lookup(ctx, "/test/folder")
	if err != nil {
		t.Fatal(err)
	}
	b.index.maxEntries = 1
	b.remember("0", []davEntry{{Item: Item{ID: "filler", Name: "filler"}}})
	if _, err := b.relative(dir.entry.ID); err == nil {
		t.Fatal("fixture did not evict the directory mapping")
	}
	// Native verification queries only a parent ID, without a path context.
	old, err := fs.listing(ctx, fs.mounts[0], dir.entry.ID)
	if err != nil || len(old) != 1 || old[0].ID != original.ID {
		t.Fatal("unlocated native identity verification failed", old, err)
	}
	if old[0].backupPath == nil || old[0].backupPath.usable() == nil {
		t.Fatal("unlocated native result did not carry an unavailable marker")
	}
	// A later correctly located query and mutation must not authorize the old
	// unlocated result merely because its name and parent are still available.
	current, err := b.List(b.parentContext(ctx, dir.entry), dir.entry.ID)
	if err != nil || len(current) != 1 {
		t.Fatal("located refresh failed", err)
	}
	if err := b.Remove(ctx, current[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := b.bindLocation(ctx, dir.entry, old[0]); !errors.Is(err, errBackupPathUnavailable) {
		t.Fatal("unlocated old listing resurrected the deleted native ID", err)
	}
	// The cached unavailable result forces one scoped parent refresh, which
	// correctly reports the deletion rather than assigning a current version.
	counter := &countedDAVDrive{davDrive: base}
	b.primary = counter
	if _, err := fs.lookup(ctx, "/test/folder/file.bin"); !os.IsNotExist(err) {
		t.Fatal("cached unlocated result hid the deletion", err)
	}
	if counter.lists.Load() != 1 {
		t.Fatal("unlocated cached result did not refresh exactly its parent")
	}
}

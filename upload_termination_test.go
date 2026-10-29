package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

type auditSpoolHolder struct {
	base  http.RoundTripper
	spool string
	once  sync.Once
	held  *os.File
	err   error
}

func (h *auditSpoolHolder) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "PUT" {
		h.once.Do(func() {
			files, e := filepath.Glob(filepath.Join(h.spool, "upload-*"))
			if e != nil {
				h.err = e
				return
			}
			if len(files) == 1 {
				h.held, h.err = os.Open(files[0])
			}
		})
	}
	return h.base.RoundTrip(r)
}

func TestTerminationDiskSuccessMustRetainFailedBodyCleanup(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows delete sharing semantics")
	}
	_, drive := newQuarkFixture(t)
	server, _, fs := testDAVService(t, true)
	fs.mounts[0].drive = drive
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].Identity = "audit-account"
	fs.mounts[0].UploadStorageMode = "disk"
	holder := &auditSpoolHolder{base: drive.bodyHTTP.Transport, spool: fs.spool}
	client := *drive.bodyHTTP
	client.Transport = holder
	drive.bodyHTTP = &client
	defer func() {
		if holder.held != nil {
			holder.held.Close()
		}
	}()
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", "completed-content", 201, nil)
	holder.once.Do(func() {})
	if holder.err != nil || holder.held == nil {
		t.Fatal("failure injection not established", holder.err)
	}
	body, err := os.ReadFile(holder.held.Name())
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
	t.Logf("HTTP=201 cloud_body=%q retained_local_body=%q checkpoint_count=%d", wantDAV(t, server.URL, "GET", "/联通云盘-1/file.bin", "", 200, nil), body, len(files))
	owners, _ := filepath.Glob(filepath.Join(fs.spool, "owners", "*.json"))
	if len(owners) != 1 {
		t.Fatal("failed body cleanup has no independent ownership", owners)
	}
	holder.held.Close()
	id := strings.TrimSuffix(filepath.Base(owners[0]), ".json")
	lease, e := lockUploadCheckpoint(filepath.Join(fs.spool, "owners", "lease-"+id[:2]))
	if e != nil {
		t.Fatal(e)
	}
	defer lease.Close()
	owner, e := readSpoolOwner(fs.spool, id)
	if e != nil {
		t.Fatal(e)
	}
	e = owner.cleanup()
	lease.Close()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(holder.held.Name()); !os.IsNotExist(e) {
		t.Fatal("pending body not reclaimed", e)
	}
}

func TestTerminationAbortedInputMustOwnFailedCleanup(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows delete sharing semantics")
	}
	_, _, fs := testDAVService(t, true)
	_, drive := newQuarkFixture(t)
	fs.mounts[0].drive = drive
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].Identity = "audit-account"
	fs.mounts[0].UploadStorageMode = "disk"
	f, err := fs.OpenFile(context.Background(), "/联通云盘-1/partial.bin", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	d := f.(*davFile)
	if _, err = d.Write([]byte("incomplete-body")); err != nil {
		t.Fatal(err)
	}
	held, err := os.Open(d.tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	d.failed = true
	if err = d.Close(); err == nil {
		t.Fatal("incomplete upload accepted")
	}
	files, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
	_, exists := os.Stat(held.Name())
	t.Logf("close_error=%v body_exists=%t checkpoint_count=%d", err, exists == nil, len(files))
	owners, _ := filepath.Glob(filepath.Join(fs.spool, "owners", "*.json"))
	if exists != nil || len(owners) != 1 {
		t.Fatal("aborted body lost ownership", exists, owners)
	}
}

func TestTerminationMemoryReplacementMustRetainOldCloudOwnership(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.add("0", "file.bin", []byte("original-target"), false)
	spool := t.TempDir()
	server, fs := memoryCheckpointService(t, drive, spool)
	name := "/联通云盘-1/file.bin"
	old, err := fs.lookup(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	parent, base, err := fs.parent(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("a"), 128<<10)
	f, err := fs.openMemoryFile(context.Background(), name, parent, base, &old.entry, int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = f.memory.FinishInput(); err != nil {
		t.Fatal(err)
	}
	result := <-f.uploadDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	cp := checkpointFrom(f.ctx)
	backup := "CloudProbe-backup-audit.bin"
	cp.mu.Lock()
	cp.data.Backup = backup
	err = cp.saveLocked()
	cp.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = drive.Move(context.Background(), old.entry, "", backup); err != nil {
		t.Fatal(err)
	}
	stage := cp.data.Stage
	f.memory.Close()
	f.releaseCheckpoint()
	server.Close()
	// This is the durable crash boundary after the old target has been backed
	// up but before the already-uploaded staged object is renamed to target.
	server, _ = memoryCheckpointService(t, drive, spool)
	changed := bytes.Repeat([]byte("b"), len(body))
	wantDAV(t, server.URL, "PUT", name, string(changed), 409, nil)
	invalid, err := readUploadCheckpoint(cp.file)
	if err != nil {
		t.Fatal(err)
	}
	if !invalid.data.Invalid || invalid.data.Backup != backup || invalid.data.Uploaded == nil {
		t.Fatal("fixture did not retain pending commit before restart")
	}
	wantDAV(t, server.URL, "PUT", name, string(changed), 201, nil)
	entries, err := drive.List(context.Background(), "0")
	if err != nil {
		t.Fatal(err)
	}
	var leftovers []string
	for _, it := range entries {
		if it.Name == backup || it.Name == stage {
			leftovers = append(leftovers, it.Name)
		}
	}
	files, _ := filepath.Glob(filepath.Join(spool, "resume", "*.bin"))
	t.Logf("replacement_HTTP=201 old_cloud_objects=%s checkpoint_count=%d", strings.Join(leftovers, ","), len(files))
	history, _ := filepath.Glob(filepath.Join(spool, "resume", "history", "*.bin"))
	if len(leftovers) != 0 || len(history) != 1 {
		t.Fatal("old cloud transaction not retired safely", leftovers, history)
	}
	archived, e := readCheckpointHistory(history[0])
	if e != nil || archived.Data.Backup != backup || archived.Data.Uploaded == nil {
		t.Fatal("old ownership not retained in history", e)
	}
}

func TestTerminationResetDuringPendingCommitMustPreserveRecovery(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.add("0", "file.bin", []byte("original-target"), false)
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "audit"}
	server, fs := memoryCheckpointService(t, drive, filepath.Join(g.data, "webdav-spool"))
	name := "/联通云盘-1/file.bin"
	old, err := fs.lookup(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	parent, base, err := fs.parent(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("a"), 128<<10)
	f, err := fs.openMemoryFile(context.Background(), name, parent, base, &old.entry, int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = f.memory.FinishInput(); err != nil {
		t.Fatal(err)
	}
	result := <-f.uploadDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	cp := checkpointFrom(f.ctx)
	backup := "CloudProbe-backup-reset-audit.bin"
	cp.mu.Lock()
	cp.data.Backup = backup
	err = cp.saveLocked()
	cp.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = drive.Move(context.Background(), old.entry, "", backup); err != nil {
		t.Fatal(err)
	}
	f.memory.Close()
	f.releaseCheckpoint()
	fs.invalidate(fs.mounts[0])
	if w := lifecycleRequest(g, "reset", cp.data.Key); w.Code < 400 {
		t.Fatal("reset accepted without original mount")
	}
	if _, e := os.Stat(cp.file); e != nil {
		t.Fatal("unresolved transaction record lost", e)
	}
	g.dav = &davManager{server: server.Config, mounts: fs.mounts}
	if w := lifecycleRequest(g, "reset", cp.data.Key); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	code, _, _ := davRequest(t, server.URL, "GET", name, nil, nil)
	_, recordErr := os.Stat(cp.file)
	entries, err := drive.List(context.Background(), "0")
	if err != nil {
		t.Fatal(err)
	}
	backupExists := false
	for _, it := range entries {
		if it.Name == backup && sameCloudObject(it, old.entry) {
			backupExists = true
		}
	}
	t.Logf("reset_HTTP=200 original_target_HTTP=%d backup_exists=%t checkpoint_deleted=%t", code, backupExists, os.IsNotExist(recordErr))
	if code != 200 || backupExists || !os.IsNotExist(recordErr) {
		t.Fatal("reset did not restore original target", code, backupExists, recordErr)
	}
	if got := wantDAV(t, server.URL, "GET", name, "", 200, nil); string(got) != "original-target" {
		t.Fatal("original contents not restored")
	}
	history, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "history", "*.bin"))
	if len(history) != 1 {
		t.Fatal("old transaction history missing")
	}
	if w := lifecycleRequest(g, "reset", cp.data.Key); w.Code < 400 {
		t.Fatal("nonexistent task was reset again")
	}
}

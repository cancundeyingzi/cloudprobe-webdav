package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func lifecycleAutoService(t *testing.T) (*httptest.Server, *davFS) {
	fixture, drive := newQuarkFixture(t)
	original := fixture.server.Config.Handler
	fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/file/upload/pre") {
			recorder := httptest.NewRecorder()
			original.ServeHTTP(recorder, r)
			var value map[string]any
			json.Unmarshal(recorder.Body.Bytes(), &value)
			value["data"].(map[string]any)["part_size"] = 16 << 20
			respondJSON(w, recorder.Code, value)
			return
		}
		original.ServeHTTP(w, r)
	})
	server, fs := memoryCheckpointService(t, drive, t.TempDir())
	fs.mounts[0].UploadStorageMode = "auto"
	return server, fs
}

func TestLifecycleLegacyEmptyCandidateDoesNotBlockOverwrite(t *testing.T) {
	server, fs := lifecycleAutoService(t)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", "first-body", 201, nil)
	d := &davFile{fs: fs, ctx: context.Background(), name: "/联通云盘-1/file.bin", node: davNode{mount: fs.mounts[0], parent: "0", entry: davEntry{Item: Item{Name: "file.bin"}}}}
	cp, release, err := d.openCheckpointFor("memory:10", 10, "memory")
	if err != nil {
		t.Fatal(err)
	}
	if err = cp.setSession(map[string]any{"Kind": "quark", "PartSize": 16 << 20}); err != nil {
		t.Fatal(err)
	}
	if err = cp.publish(context.Background()); err != nil {
		t.Fatal(err)
	}
	release()
	wantDAV(t, server.URL, "PUT", d.name, "other-body", 201, nil)
	files, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
	if len(files) != 0 {
		t.Fatal("legacy candidate remained", files)
	}
}

func TestLifecycleValidMemoryProgressSurvivesBudgetFallback(t *testing.T) {
	server, fs := lifecycleAutoService(t)
	drive := fs.mounts[0].drive.(*quarkDAVDrive)
	client := *drive.bodyHTTP
	client.Transport = &checkpointTransport{base: drive.bodyHTTP.Transport, cancel: func() {}}
	drive.bodyHTTP = &client
	settings := transferOptions{TransferScheduling: "client", UploadStorageMode: "auto", UploadMemoryMiB: 32, TotalUploadMemoryMiB: 32, UploadPrefetchMiB: 1, UploadRecoveryMode: "off"}
	fs.transfers = newTransferRuntime(settings)
	body := bytes.Repeat([]byte("x"), 17<<20)
	code, _, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/file.bin", bytes.NewReader(body), nil)
	if code < 400 {
		t.Fatal("expected part failure")
	}
	files, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	before, err := readUploadCheckpoint(files[0])
	if err != nil || len(before.data.Parts) == 0 {
		t.Fatal("fixture has no saved progress", err)
	}
	small := settings
	small.UploadMemoryMiB = 5
	small.TotalUploadMemoryMiB = 5
	fs.transfers = newTransferRuntime(small)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", string(body), 409, nil)
	after, err := readUploadCheckpoint(files[0])
	if err != nil || after.data.Stage != before.data.Stage || len(after.data.Parts) != len(before.data.Parts) {
		t.Fatal("valid progress discarded", err)
	}
	files, _ = filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
	if len(files) != 1 {
		t.Fatal("silent second task created")
	}
	fs.transfers = newTransferRuntime(settings)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", string(body), 201, nil)
}

func TestLifecycleRetiredPauseCannotResurrectOrModifyNewOwner(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	key := strings.Repeat("b", 64)
	file := filepath.Join(g.data, "webdav-spool", "resume", key+".bin")
	c := &uploadCheckpoint{file: file, data: uploadCheckpointData{Version: 1, Key: key, Parts: map[int64]string{}}}
	if err := c.saveLocked(); err != nil {
		t.Fatal(err)
	}
	lease, err := lockUploadCheckpoint(filepath.Join(filepath.Dir(file), "lease-bb"))
	if err != nil {
		t.Fatal(err)
	}
	a := c.activate(func() { t.Error("retired callback invoked") })
	// The same handle captured by the HTTP pause path is delayed until after
	// retirement, a real reset request, and a new owner using the same key.
	ready, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { close(ready); <-release; done <- a.pause() }()
	<-ready
	c.releaseLease(lease)
	if w := lifecycleRequest(g, "reset", key); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("reset retained record", err)
	}
	next := &uploadCheckpoint{file: file, data: uploadCheckpointData{Version: 1, Key: key, Stage: "new-generation", Parts: map[int64]string{}}}
	if err = next.saveLocked(); err != nil {
		t.Fatal(err)
	}
	lease, err = lockUploadCheckpoint(filepath.Join(filepath.Dir(file), "lease-bb"))
	if err != nil {
		t.Fatal(err)
	}
	defer next.releaseLease(lease)
	current := next.activate(func() {})
	close(release)
	if err = <-done; !errors.Is(err, errCheckpointRetired) {
		t.Fatal("stale pause accepted", err)
	}
	a.retire()
	if value, ok := activeCheckpoints.Load(file); !ok || value != current {
		t.Fatal("old retirement removed new owner")
	}
	loaded, err := readUploadCheckpoint(file)
	if err != nil || loaded.data.Paused || loaded.data.Stage != "new-generation" {
		t.Fatal("old handle changed new record", err)
	}
	if err = next.complete(); err != nil {
		t.Fatal(err)
	}
	if err = current.pause(); !errors.Is(err, errCheckpointRetired) {
		t.Fatal("pause accepted after completion", err)
	}
	if _, err = os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("completed record resurrected", err)
	}
}

func TestLifecycleRetirementSharesPauseCriticalSection(t *testing.T) {
	c := testCheckpoint(t)
	c.data.Key = "fixture"
	if err := c.saveLocked(); err != nil {
		t.Fatal(err)
	}
	lease, err := lockUploadCheckpoint(filepath.Join(filepath.Dir(c.file), "lease"))
	if err != nil {
		t.Fatal(err)
	}
	a := c.activate(func() {})
	c.mu.Lock()
	started, retired := make(chan struct{}), make(chan struct{})
	go func() { close(started); c.releaseLease(lease); close(retired) }()
	<-started
	select {
	case <-retired:
		c.mu.Unlock()
		t.Fatal("retirement bypassed task mutex")
	default:
	}
	c.mu.Unlock()
	select {
	case <-retired:
	case <-time.After(time.Second):
		t.Fatal("retirement blocked")
	}
	if err = a.pause(); !errors.Is(err, errCheckpointRetired) {
		t.Fatal(err)
	}
	if err = c.saveLocked(); !errors.Is(err, errCheckpointRetired) {
		t.Fatal("retired owner can still write", err)
	}
}

func TestLifecyclePauseBeforeCommitPreservesStagedObject(t *testing.T) {
	_, base, fs := testDAVService(t, true)
	f, _ := os.CreateTemp(t.TempDir(), "body")
	defer f.Close()
	f.WriteString("body")
	entry, err := base.Upload(context.Background(), "0", "CloudProbe-upload-stage", f)
	if err != nil {
		t.Fatal(err)
	}
	cp := testCheckpoint(t)
	a := cp.activate(func() {})
	if err = a.pause(); err != nil {
		t.Fatal(err)
	}
	a.retire()
	d := &davFile{fs: fs, ctx: context.WithValue(context.Background(), uploadCheckpointKey{}, cp), name: "/联通云盘-1/target", node: davNode{mount: fs.mounts[0], parent: "0", entry: davEntry{Item: Item{Name: "target"}}}}
	err = d.commitUpload(entry, 4, nil)
	var status *davStatusError
	if !errors.As(err, &status) || status.Code != 423 {
		t.Fatal("paused upload continued commit", err)
	}
	entries, _ := base.List(context.Background(), "0")
	if len(entries) != 1 || entries[0].Name != entry.Name {
		t.Fatal("pause changed final target", entries)
	}
}

func TestLifecycleResetRejectsUnownedSpoolAndExplicitForget(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	key := strings.Repeat("c", 64)
	file := filepath.Join(g.data, "webdav-spool", "resume", key+".bin")
	for _, name := range []string{"../upload-outside", "unrelated.txt", "upload-x:stream"} {
		c := &uploadCheckpoint{file: file, data: uploadCheckpointData{Version: 1, Key: key, Parts: map[int64]string{}, Spool: name}}
		if err := c.saveLocked(); err != nil {
			t.Fatal(err)
		}
		if w := lifecycleRequest(g, "reset", key); w.Code < 400 {
			t.Fatal("unowned spool accepted", name)
		}
		if _, err := os.Stat(file); err != nil {
			t.Fatal("association deleted", err)
		}
		if w := lifecycleRequest(g, "forget", key); w.Code != 409 {
			t.Fatal("force path bypassed valid cleanup")
		}
	}
	os.WriteFile(file, []byte("corrupt"), 0600)
	if w := lifecycleRequest(g, "reset", key); w.Code < 400 {
		t.Fatal("corrupt record silently discarded")
	}
	if w := lifecycleRequest(g, "forget", key); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestLifecycleAutoFallbackReleasesCandidateAndAllowsOverwrite(t *testing.T) {
	server, fs := lifecycleAutoService(t)
	for _, body := range []string{"first-body", "other-body"} {
		wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", body, 201, nil)
		if got := wantDAV(t, server.URL, "GET", "/联通云盘-1/file.bin", "", 200, nil); string(got) != body {
			t.Fatal("wrong uploaded content")
		}
		files, _ := filepath.Glob(filepath.Join(fs.spool, "resume", "*.bin"))
		if len(files) != 0 {
			t.Errorf("successful fallback retained %d tasks", len(files))
		}
	}
}

func TestLifecycleAutoFallbackUsesLastTaskSlot(t *testing.T) {
	server, fs := lifecycleAutoService(t)
	dir := filepath.Join(fs.spool, "resume")
	os.MkdirAll(dir, 0700)
	for i := 0; i < 255; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%064x.bin", i)), []byte("reserved fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", "body", 201, nil)
	files, _ := filepath.Glob(filepath.Join(dir, "*.bin"))
	if len(files) != 255 {
		t.Fatal("mode switch consumed an extra slot", len(files))
	}
}

func lifecycleRequest(g *guiApp, action, id string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "http://localhost/api/upload-resume", strings.NewReader(fmt.Sprintf(`{"id":%q,"action":%q}`, id, action)))
	r.Header.Set("X-CloudProbe-UI", g.nonce)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

func TestLifecycleResetReclaimsCrashSpool(t *testing.T) {
	_, drive := newQuarkFixture(t)
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	fs := &davFS{spool: filepath.Join(g.data, "webdav-spool")}
	os.MkdirAll(fs.spool, 0700)
	body, err := os.CreateTemp(fs.spool, "upload-*")
	if err != nil {
		t.Fatal(err)
	}
	body.Write(bytes.Repeat([]byte("crash-body-"), 1024))
	d := &davFile{fs: fs, ctx: context.Background(), tmp: body, name: "/mount/target", node: davNode{mount: &davMount{Identity: "account", Provider: "quark", drive: drive}, parent: "0", entry: davEntry{Item: Item{Name: "target"}}}}
	cp, release, err := d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	body.Close()
	release() // process-exit state: body survives, OS lease is released
	w := lifecycleRequest(g, "reset", cp.data.Key)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = os.Stat(body.Name()); !os.IsNotExist(err) {
		t.Fatal("reset orphaned crash body", err)
	}
	if _, err = os.Stat(cp.file); !os.IsNotExist(err) {
		t.Fatal("checkpoint still exists", err)
	}
}

func TestLifecycleResetFailurePreservesCleanupAssociation(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	key := strings.Repeat("a", 64)
	cp := &uploadCheckpoint{file: filepath.Join(g.data, "webdav-spool", "resume", key+".bin"), data: uploadCheckpointData{Version: 1, Key: key, Parts: map[int64]string{}, Spool: "upload-blocked"}}
	if err := cp.saveLocked(); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(g.data, "webdav-spool", "upload-blocked")
	os.Mkdir(blocked, 0700)
	os.WriteFile(filepath.Join(blocked, "unrelated"), []byte("keep"), 0600)
	w := lifecycleRequest(g, "reset", key)
	if w.Code < 400 {
		t.Fatal("cleanup failure reported success")
	}
	loaded, err := readUploadCheckpoint(cp.file)
	if err != nil || loaded.data.Spool != "upload-blocked" {
		t.Fatal("cleanup association lost", err)
	}
	f, err := os.Open(filepath.Join(blocked, "unrelated"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if b, _ := io.ReadAll(f); string(b) != "keep" {
		t.Fatal("unrelated contents changed")
	}
}

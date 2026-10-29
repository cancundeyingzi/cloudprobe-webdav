package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

func terminationAPI(g *guiApp, method, endpoint string, input any, auth bool) *httptest.ResponseRecorder {
	b, _ := json.Marshal(input)
	r := httptest.NewRequest(method, "http://localhost/api/"+endpoint, bytes.NewReader(b))
	if auth {
		r.Header.Set("X-CloudProbe-UI", g.nonce)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

type retirementListFailure struct{ davDrive }

func (d retirementListFailure) List(context.Context, string) ([]davEntry, error) {
	return nil, errors.New("native listing unavailable")
}

func TestRetirementCannotUseOpenListFallbackAsAbsenceEvidence(t *testing.T) {
	p := newPendingRetirement(t)
	backup, _ := newOpenListFixture(t, newFakeDAV(t))
	var calls atomic.Int64
	backup.client = &http.Client{Transport: protocolRoundTripper(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, errors.New("fallback invoked") })}
	p.fs.mounts[0].drive = &backupDAVDrive{primary: retirementListFailure{p.drive}, backup: backup, root: "0", paths: map[string]string{"0": ""}, entries: map[string]davEntry{}}
	if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code < 400 {
		t.Fatal("native failure accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("retirement consulted read-only fallback")
	}
	if _, err := os.Stat(p.cp.file); err != nil {
		t.Fatal("native failure dropped recovery state", err)
	}
}

func TestBodyOwnershipProcessHelper(t *testing.T) {
	dir := os.Getenv("CLOUDPROBE_BODY_OWNER_DIR")
	if dir == "" {
		t.Skip("subprocess-only helper")
	}
	f, o, err := newOwnedSpool(context.Background(), dir, "/crash-body")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	defer o.lease.Close()
	if _, err = f.Write([]byte("crash-owned-body")); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	fmt.Println(o.data.ID)
	<-time.After(20 * time.Second)
}

func TestBodyOwnershipForcedTerminationCleanup(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "owner-test"}
	dir := filepath.Join(g.data, "webdav-spool")
	cmd := exec.Command(os.Args[0], "-test.run=^TestBodyOwnershipProcessHelper$", "-test.timeout=25s")
	cmd.Env = append(os.Environ(), "CLOUDPROBE_BODY_OWNER_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- strings.TrimSpace(line) }()
	var id string
	select {
	case id = <-ready:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("child did not persist owner")
	}
	if !validOwnerID(id) {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("invalid child owner ID", id)
	}
	if w := terminationAPI(g, "POST", "upload-spool", map[string]any{"id": id}, true); w.Code != 409 {
		t.Fatal("live child body was removed")
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if w := terminationAPI(g, "POST", "upload-spool", map[string]any{"id": id}, true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = os.Stat(filepath.Join(dir, "upload-"+id)); !os.IsNotExist(err) {
		t.Fatal("killed-process body not reclaimed", err)
	}
}

func TestBodyOwnershipExistsBeforeReceiveAndSurvivesCrash(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "owner-test"}
	_, _, fs := testDAVService(t, true)
	fs.spool = filepath.Join(g.data, "webdav-spool")
	f, err := fs.OpenFile(context.Background(), "/联通云盘-1/body.bin", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	d := f.(*davFile)
	defer d.tmp.Close()
	defer d.spoolOwner.lease.Close()
	id := d.spoolOwner.data.ID
	if _, err = readSpoolOwner(fs.spool, id); err != nil {
		t.Fatal("body accepted without owner", err)
	}
	if w := terminationAPI(g, "GET", "upload-spool", nil, false); w.Code != 403 {
		t.Fatal("cleanup inventory exposed without auth")
	}
	if w := terminationAPI(g, "POST", "upload-spool", map[string]any{"id": id}, true); w.Code != 409 {
		t.Fatal("active body was cleaned", w.Code)
	}
	d.Write([]byte("partial-body"))
	d.tmp.Close()
	d.spoolOwner.lease.Close() // crashed request
	fresh := &guiApp{data: g.data, host: g.host, nonce: "new-session"}
	if w := terminationAPI(fresh, "POST", "upload-spool", map[string]any{"id": id}, true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = os.Stat(d.tmp.Name()); !os.IsNotExist(err) {
		t.Fatal("crashed body survived cleanup", err)
	}
	if _, err = os.Stat(d.spoolOwner.file); !os.IsNotExist(err) {
		t.Fatal("owner survived cleanup", err)
	}
}

func TestBodyOwnershipRejectsForgedPath(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "owner-test"}
	dir := filepath.Join(g.data, "webdav-spool")
	f, o, err := newOwnedSpool(context.Background(), dir, "/target")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	o.lease.Close()
	outside := filepath.Join(g.data, "outside")
	os.WriteFile(outside, []byte("keep"), 0600)
	o.data.Name = "../outside"
	if err = writePrivateJSON(o.file, o.data); err != nil {
		t.Fatal(err)
	}
	if w := terminationAPI(g, "POST", "upload-spool", map[string]any{"id": o.data.ID}, true); w.Code < 400 {
		t.Fatal("forged owner accepted")
	}
	if b, err := os.ReadFile(outside); err != nil || string(b) != "keep" {
		t.Fatal("outside file changed", err)
	}
	if _, err = os.Stat(o.file); err != nil {
		t.Fatal("invalid ownership record lost", err)
	}
}

func TestBodyOwnershipRegistryCannotBypassSpoolNameValidation(t *testing.T) {
	dir := t.TempDir()
	f, o, err := newOwnedSpool(context.Background(), dir, "/target")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("keep")
	f.Close()
	o.lease.Close()
	if err = cleanupRegisteredSpool(dir, o.data.ID, ""); err == nil {
		t.Fatal("bare owner ID accepted as a spool filename")
	}
	if b, err := os.ReadFile(filepath.Join(dir, o.data.Name)); err != nil || string(b) != "keep" {
		t.Fatal("invalid association deleted body", err)
	}
	if _, err = readSpoolOwner(dir, o.data.ID); err != nil {
		t.Fatal("invalid association deleted owner", err)
	}
}

type pendingRetirement struct {
	g        *guiApp
	server   *httptest.Server
	fs       *davFS
	cloud    *quarkFixture
	drive    *quarkDAVDrive
	cp       *uploadCheckpoint
	original davEntry
	payload  []byte
}

func newPendingRetirement(t *testing.T) *pendingRetirement {
	t.Helper()
	cloud, drive := newQuarkFixture(t)
	cloud.add("0", "file.bin", []byte("original-target"), false)
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "retire-test"}
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
	body := bytes.Repeat([]byte("staged-content"), 10000)
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
	cp.mu.Lock()
	cp.data.Backup = "CloudProbe-backup-retire.bin"
	err = cp.saveLocked()
	cp.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = drive.Move(context.Background(), old.entry, "", cp.data.Backup); err != nil {
		t.Fatal(err)
	}
	f.memory.Close()
	f.releaseCheckpoint()
	fs.invalidate(fs.mounts[0])
	g.dav = &davManager{server: server.Config, mounts: fs.mounts}
	return &pendingRetirement{g, server, fs, cloud, drive, cp, old.entry, body}
}

type retirementFaultDrive struct {
	davDrive
	failMove, failRemove, lostMove, lostRemove bool
	moves, removes                             int
}

func (d *retirementFaultDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	d.moves++
	if d.failMove {
		return errors.New("injected restore failure")
	}
	err := d.davDrive.Move(ctx, it, parent, name)
	if err == nil && d.lostMove {
		return errors.New("lost restore response")
	}
	return err
}
func (d *retirementFaultDrive) Remove(ctx context.Context, it davEntry) error {
	d.removes++
	if d.failRemove {
		return errors.New("injected cleanup failure")
	}
	err := d.davDrive.Remove(ctx, it)
	if err == nil && d.lostRemove {
		return errors.New("lost delete response")
	}
	return err
}

func TestRetirementFailurePreservesAssociationsAndRetries(t *testing.T) {
	for _, phase := range []string{"restore", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			p := newPendingRetirement(t)
			fault := &retirementFaultDrive{davDrive: p.drive, failMove: phase == "restore", failRemove: phase == "cleanup"}
			p.fs.mounts[0].drive = fault
			before, _ := os.ReadFile(p.cp.file)
			if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code < 400 {
				t.Fatal("failed retirement reported success")
			}
			after, _ := os.ReadFile(p.cp.file)
			if !bytes.Equal(before, after) {
				t.Fatal("failed retirement changed active association")
			}
			history, _ := filepath.Glob(filepath.Join(p.fs.spool, "resume", "history", "*.bin"))
			if len(history) != 1 {
				t.Fatal("retirement receipt missing")
			}
			fault.failMove = false
			fault.failRemove = false
			if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if got := wantDAV(t, p.server.URL, "GET", p.cp.data.Target, "", 200, nil); string(got) != "original-target" {
				t.Fatal("original not restored")
			}
			entries, _ := p.drive.List(context.Background(), "0")
			if len(entries) != 1 {
				t.Fatal("owned cloud artifacts retained", entries)
			}
		})
	}
}

func TestRetirementRejectsExternalChangesAndWrongAccount(t *testing.T) {
	for _, kind := range []string{"target", "backup", "account"} {
		t.Run(kind, func(t *testing.T) {
			p := newPendingRetirement(t)
			fault := &retirementFaultDrive{davDrive: p.drive}
			p.fs.mounts[0].drive = fault
			switch kind {
			case "target":
				p.cloud.add("0", "file.bin", []byte("external"), false)
			case "backup":
				p.cloud.mu.Lock()
				old := p.cloud.files[p.original.ID]
				old.Content = []byte("changed-backup")
				p.cloud.files[old.ID] = old
				p.cloud.mu.Unlock()
			case "account":
				p.fs.mounts[0].Identity = "different-account"
			}
			if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code < 400 {
				t.Fatal("unsafe retirement accepted")
			}
			if fault.moves != 0 || fault.removes != 0 {
				t.Fatal("external state mutated", fault.moves, fault.removes)
			}
			if _, err := os.Stat(p.cp.file); err != nil {
				t.Fatal("recovery record lost", err)
			}
		})
	}
}

func TestRetirementLostResponsesAndCommittedTarget(t *testing.T) {
	for _, kind := range []string{"restore-response", "delete-response", "already-committed"} {
		t.Run(kind, func(t *testing.T) {
			p := newPendingRetirement(t)
			fault := &retirementFaultDrive{davDrive: p.drive, lostMove: kind == "restore-response", lostRemove: kind == "delete-response"}
			p.fs.mounts[0].drive = fault
			if kind == "already-committed" {
				if err := p.drive.Move(context.Background(), *p.cp.data.Uploaded, "", "file.bin"); err != nil {
					t.Fatal(err)
				}
			}
			if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			want := []byte("original-target")
			if kind == "already-committed" {
				want = p.payload
			}
			if got := wantDAV(t, p.server.URL, "GET", p.cp.data.Target, "", 200, nil); !bytes.Equal(got, want) {
				t.Fatal("wrong final target")
			}
		})
	}
}

func TestRetirementRespectsDAVLockAndHistoryAuthorization(t *testing.T) {
	p := newPendingRetirement(t)
	service := p.server.Config.Handler.(*davService)
	token, err := service.handler.LockSystem.Create(p.cp.data.Updated, webdav.LockDetails{Root: p.cp.data.Target, Duration: -1, ZeroDepth: true})
	if err != nil {
		t.Fatal(err)
	}
	if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code < 400 {
		t.Fatal("client lock bypassed")
	}
	service.handler.LockSystem.Unlock(p.cp.data.Updated, token)
	if w := lifecycleRequest(p.g, "reset", p.cp.data.Key); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := terminationAPI(p.g, "GET", "upload-history", nil, false); w.Code != 403 {
		t.Fatal("history exposed")
	}
	w := terminationAPI(p.g, "GET", "upload-history", nil, true)
	if w.Code != 200 || strings.Contains(w.Body.String(), "fixture-auth") {
		t.Fatal("history leaked session")
	}
	var data struct {
		History []struct {
			ID string `json:"id"`
		} `json:"history"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &data); err != nil || len(data.History) != 1 {
		t.Fatal("history not discoverable", err)
	}
	id := data.History[0].ID
	if w = terminationAPI(p.g, "POST", "upload-history", map[string]any{"id": id}, true); w.Code < 400 {
		t.Fatal("history removed without acknowledgement")
	}
	if w = terminationAPI(p.g, "POST", "upload-history", map[string]any{"id": id, "reviewed": true}, true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testCheckpoint(t *testing.T) *uploadCheckpoint {
	t.Helper()
	c := &uploadCheckpoint{file: filepath.Join(t.TempDir(), "task.bin"), data: uploadCheckpointData{Version: 1, Parts: map[int64]string{}}}
	return c
}

func TestCheckpointMobileResumeRanges(t *testing.T) {
	c := testCheckpoint(t)
	body := []byte("abcdefghijk")
	var mu sync.Mutex
	calls := map[string]int{}
	saved := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		key := r.Header.Get("Range")
		if r.Header.Get("uploadtaskID") != "same-task" {
			t.Error("task changed")
		}
		mu.Lock()
		defer mu.Unlock()
		calls[key]++
		if key == "bytes=4-7" && calls[key] == 1 {
			http.Error(w, "interrupted", 400)
			return
		}
		saved[key] = b
		io.WriteString(w, `{"resultCode":"0"}`)
	}))
	defer srv.Close()
	d := &mobileDAVDrive{}
	ctx := context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	if err := d.uploadResumableParts(ctx, srv.URL, "same-task", "name", int64(len(body)), 4, bytes.NewReader(body)); err == nil {
		t.Fatal("expected interruption")
	}
	c = reloadCheckpoint(t, c)
	ctx = context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	if err := d.uploadResumableParts(ctx, srv.URL, "same-task", "name", int64(len(body)), 4, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["bytes=0-3"] != 1 || calls["bytes=4-7"] != 2 || calls["bytes=8-10"] != 1 {
		t.Fatal(calls)
	}
	if !bytes.Equal(append(append(saved["bytes=0-3"], saved["bytes=4-7"]...), saved["bytes=8-10"]...), body) {
		t.Fatal("incorrect resumed bytes")
	}
}

func TestCheckpointCloudDriveFullPUTRetryKeepsWANProgress(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	fixture.add("0", "file.bin", []byte("previous"), false)
	spool := t.TempDir()
	configure := func(d *quarkDAVDrive) *httptest.Server {
		server, _, fs := testDAVService(t, true)
		fs.mounts[0].Provider = "quark"
		fs.mounts[0].Identity = "actual-account"
		fs.mounts[0].drive = d
		fs.spool = spool
		return server
	}
	client := *drive.bodyHTTP
	client.Transport = &checkpointTransport{base: drive.bodyHTTP.Transport, cancel: func() {}}
	drive.bodyHTTP = &client
	first := configure(drive)
	body := bytes.Repeat([]byte("cloud-drive-replayed-body"), 10000)
	code, _, _ := davRequest(t, first.URL, "PUT", "/联通云盘-1/file.bin", bytes.NewReader(body), nil)
	if code < 400 {
		t.Fatal("first request should fail")
	}
	first.Close()
	restarted := newQuarkDrive(drive.client, newTransferRuntime(transferOptions{UploadStorageMode: "disk"}), 1, "auto")
	restarted.retryWait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	second := configure(restarted)
	wantDAV(t, second.URL, "PUT", "/联通云盘-1/file.bin", string(body), 201, nil)
	got := wantDAV(t, second.URL, "GET", "/联通云盘-1/file.bin", "", 200, nil)
	if !bytes.Equal(got, body) {
		t.Fatal("final file differs")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.partCalls[1] != 1 || len(fixture.uploads) != 1 {
		t.Fatalf("full LAN retry replayed WAN body: %v", fixture.partCalls)
	}
	files, _ := filepath.Glob(filepath.Join(spool, "resume", "*.bin"))
	if len(files) != 0 {
		t.Fatal("completed checkpoint retained")
	}
}

func TestCheckpointProcessHelper(t *testing.T) {
	file := os.Getenv("CLOUDPROBE_CHECKPOINT_HELPER")
	if file == "" {
		t.Skip("subprocess helper")
	}
	lease, err := lockUploadCheckpoint(filepath.Join(filepath.Dir(file), "process-lease"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	c := reloadCheckpoint(t, &uploadCheckpoint{file: file})
	f, err := os.Open(os.Getenv("CLOUDPROBE_CHECKPOINT_BODY"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx := context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	u := &wopanUploadSession{endpoint: os.Getenv("CLOUDPROBE_CHECKPOINT_URL"), unique: "process-session", name: "stage", size: 12, partSize: 4, concurrency: 1}
	if _, err = u.run(ctx, f); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointSurvivesForcedProcessTermination(t *testing.T) {
	c := testCheckpoint(t)
	if err := c.saveLocked(); err != nil {
		t.Fatal(err)
	}
	body := filepath.Join(t.TempDir(), "body")
	os.WriteFile(body, []byte("abcdefghijkl"), 0600)
	secondPart := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var mu sync.Mutex
	calls := map[int]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ParseMultipartForm(1<<20) != nil {
			return
		}
		defer r.MultipartForm.RemoveAll()
		i, _ := strconv.Atoi(r.FormValue("partIndex"))
		mu.Lock()
		calls[i]++
		n := calls[i]
		mu.Unlock()
		if i == 2 && n == 1 {
			close(secondPart)
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"done"}}`)
	}))
	defer srv.Close()
	command := func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointProcessHelper$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), "CLOUDPROBE_CHECKPOINT_HELPER="+c.file, "CLOUDPROBE_CHECKPOINT_BODY="+body, "CLOUDPROBE_CHECKPOINT_URL="+srv.URL)
		return cmd
	}
	first := command()
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Process.Kill() }()
	select {
	case <-secondPart:
	case <-time.After(10 * time.Second):
		t.Fatal("child did not reach part two")
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	unblock()
	if out, err := command().CombinedOutput(); err != nil {
		t.Fatalf("restart: %v %s", err, out)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("killed process lost durable progress: %v", calls)
	}
}
func reloadCheckpoint(t *testing.T, c *uploadCheckpoint) *uploadCheckpoint {
	t.Helper()
	b, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatal(err)
	}
	b, err = unprotectLocal(b)
	if err != nil {
		t.Fatal(err)
	}
	next := &uploadCheckpoint{file: c.file}
	if err = json.Unmarshal(b, &next.data); err != nil {
		t.Fatal(err)
	}
	return next
}

func TestCheckpointWopanRestartSkipsAcknowledgedParts(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 21)
	file, err := os.CreateTemp(t.TempDir(), "body")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	file.Write(data)
	var mu sync.Mutex
	calls := map[int]int{}
	parts := map[int][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			http.Error(w, "bad", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("uniqueId") != "original-session" {
			t.Error("session changed")
		}
		i, _ := strconv.Atoi(r.FormValue("partIndex"))
		f, _, _ := r.FormFile("file")
		b, _ := io.ReadAll(f)
		f.Close()
		mu.Lock()
		defer mu.Unlock()
		calls[i]++
		if i == 2 && calls[i] == 1 {
			http.Error(w, "interrupted", 400)
			return
		}
		parts[i] = b
		io.WriteString(w, `{"code":"0000","data":{"fid":"final-id"}}`)
	}))
	defer server.Close()
	c := testCheckpoint(t)
	ctx := context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	u := &wopanUploadSession{endpoint: server.URL, unique: "original-session", name: "stage", size: int64(len(data)), partSize: 80, concurrency: 1, client: server.Client()}
	if _, err := u.run(ctx, file); err == nil {
		t.Fatal("failure not injected")
	}
	c = reloadCheckpoint(t, c)
	ctx = context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	u = &wopanUploadSession{endpoint: server.URL, unique: "different-session", name: "stage", size: int64(len(data)), partSize: 90, concurrency: 1, client: server.Client()}
	if _, err := u.run(ctx, file); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[1] != 1 || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("retransmitted acknowledged parts: %v", calls)
	}
	if !bytes.Equal(append(append(parts[1], parts[2]...), parts[3]...), data) {
		t.Fatal("resumed content differs")
	}
}

type checkpointTransport struct {
	base   http.RoundTripper
	cancel context.CancelFunc
	failed bool
}

func (r *checkpointTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == "PUT" && req.URL.Query().Get("partNumber") == "2" && !r.failed {
		r.failed = true
		r.cancel()
		if req.Context().Err() == nil {
			return &http.Response{StatusCode: 400, Header: http.Header{}, Body: io.NopCloser(bytes.NewBufferString("injected failure"))}, nil
		}
		return nil, context.Canceled
	}
	return r.base.RoundTrip(req)
}

func TestCheckpointQuarkReloadReusesSessionAndETags(t *testing.T) {
	fixture, d := newQuarkFixture(t)
	file, _ := os.CreateTemp(t.TempDir(), "body")
	defer file.Close()
	body := bytes.Repeat([]byte("recover-quark"), 20000)
	file.Write(body)
	c := testCheckpoint(t)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), uploadCheckpointKey{}, c))
	defer cancel()
	client := *d.client.bodies
	client.Transport = &checkpointTransport{base: d.client.bodies.Transport, cancel: cancel}
	d.bodyHTTP = &client
	if _, err := d.Upload(ctx, "0", "stage.txt", file); err == nil {
		t.Fatal("upload was not interrupted")
	}
	c = reloadCheckpoint(t, c)
	ctx = context.WithValue(context.Background(), uploadCheckpointKey{}, c)
	entry, err := d.Upload(ctx, "0", "stage.txt", file)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.partCalls[1] != 1 || len(fixture.uploads) != 1 {
		t.Fatalf("session or first part replayed: %v", fixture.partCalls)
	}
	if !bytes.Equal(fixture.files[entry.ID].Content, body) {
		t.Fatal("resumed bytes differ")
	}
}

func TestCheckpointDiskIdentityCorruptionAndLease(t *testing.T) {
	_, drive := newQuarkFixture(t)
	f, _ := os.CreateTemp(t.TempDir(), "body")
	defer f.Close()
	f.WriteString("first-content")
	fs := &davFS{spool: t.TempDir()}
	d := &davFile{fs: fs, ctx: context.Background(), tmp: f, name: "/mount/name", node: davNode{mount: &davMount{Identity: "account", Provider: "quark", drive: drive}, parent: "0", entry: davEntry{Item: Item{Name: "name"}}}}
	c, release, err := d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.openCheckpoint(); err == nil {
		t.Fatal("parallel lease accepted")
	}
	stage := c.data.Stage
	release()
	c, release, err = d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if c.data.Stage != stage {
		t.Fatal("restart changed staging identity")
	}
	release()
	d.node.mount.Identity = "other-account"
	other, done, err := d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if other.file == c.file {
		t.Fatal("different accounts shared session")
	}
	done()
	d.node.mount.Identity = "account"
	f.WriteAt([]byte("other-content"), 0)
	other, done, err = d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if other.file == c.file {
		t.Fatal("same-size different bytes shared session")
	}
	done()
	original := []byte("broken")
	if err = os.WriteFile(other.file, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.openCheckpoint(); err == nil {
		t.Fatal("corrupt record accepted")
	}
	got, _ := os.ReadFile(other.file)
	if !bytes.Equal(got, original) {
		t.Fatal("corrupt record overwritten")
	}
}

func TestCheckpointSaveFailureStopsConfirmation(t *testing.T) {
	c := testCheckpoint(t)
	if err := os.Mkdir(c.file, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.confirm(1, "tag"); err == nil {
		t.Fatal("failed persistence ignored")
	}
	if _, ok := c.part(1); ok {
		t.Fatal("unpersisted part acknowledged")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := copyWithContext(ctx, io.Discard, bytes.NewReader([]byte("body"))); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCheckpointCommitRestartAndExternalChange(t *testing.T) {
	for _, phase := range []string{"backup", "committed", "external-change"} {
		t.Run(phase, func(t *testing.T) {
			server, base, fs := testDAVService(t, true)
			name := "/联通云盘-1/target"
			wantDAV(t, server.URL, "PUT", name, "old", 201, nil)
			oldNode, err := fs.lookup(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			file, _ := os.CreateTemp(t.TempDir(), "body")
			defer file.Close()
			file.WriteString("new-content")
			stage, err := base.Upload(context.Background(), "0", "CloudProbe-upload-fixture", file)
			if err != nil {
				t.Fatal(err)
			}
			c := testCheckpoint(t)
			c.data.Stage = stage.Name
			c.data.Uploaded = &stage
			c.data.Size = 11
			c.data.Original = &oldNode.entry
			c.data.Backup = "CloudProbe-backup-fixture"
			var current *davEntry
			if phase == "external-change" {
				base.mu.Lock()
				entry := base.entries[oldNode.entry.ID]
				entry.Size = 42
				base.entries[entry.ID] = entry
				base.mu.Unlock()
				current = &entry
			} else {
				if err = base.Move(context.Background(), oldNode.entry, "", c.data.Backup); err != nil {
					t.Fatal(err)
				}
				if phase == "committed" {
					if err = base.Move(context.Background(), stage, "", "target"); err != nil {
						t.Fatal(err)
					}
					current = &stage
				}
			}
			fs.invalidate(fs.mounts[0])
			d := &davFile{fs: fs, ctx: context.Background(), name: name, node: davNode{mount: fs.mounts[0], parent: "0", entry: davEntry{Item: Item{Name: "target"}}}, original: current}
			created, err := d.uploadWithCheckpoint(c)
			if phase == "external-change" {
				if err == nil {
					t.Fatal("external change accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if d.committed == nil {
				if err = d.commitUpload(created, 11, d.original); err != nil {
					t.Fatal(err)
				}
			}
			if got := wantDAV(t, server.URL, "GET", name, "", 200, nil); string(got) != "new-content" {
				t.Fatalf("wrong recovered target: %s", got)
			}
			entries, _ := base.List(context.Background(), "0")
			if len(entries) != 1 {
				t.Fatalf("backup cleanup incomplete: %v", entries)
			}
		})
	}
}

func TestCheckpointManagementAuthorizationPauseAndReset(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture-nonce"}
	key := strings.Repeat("a", 64)
	c := &uploadCheckpoint{file: filepath.Join(g.data, "webdav-spool", "resume", key+".bin"), data: uploadCheckpointData{Version: 1, Key: key, Parts: map[int64]string{1: "etag"}, Session: json.RawMessage(`{"PartSize":4,"Secret":"must-not-export"}`), Size: 10}}
	if err := c.saveLocked(); err != nil {
		t.Fatal(err)
	}
	request := func(method, body, nonce string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://localhost/api/upload-resume", strings.NewReader(body))
		r.Header.Set("X-CloudProbe-UI", nonce)
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w
	}
	if request("GET", "", "").Code != 403 {
		t.Fatal("task metadata exposed without UI auth")
	}
	w := request("GET", "", g.nonce)
	if w.Code != 200 || strings.Contains(w.Body.String(), "must-not-export") {
		t.Fatal("task listing leaked session")
	}
	for _, action := range []string{"pause", "resume"} {
		w = request("POST", `{"id":"`+key+`","action":"`+action+`"}`, g.nonce)
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		loaded, err := readUploadCheckpoint(c.file)
		if err != nil || loaded.data.Paused != (action == "pause") {
			t.Fatal("pause state not persisted", err)
		}
	}
	if w = request("POST", `{"id":"`+key+`","action":"reset"}`, g.nonce); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if _, err := os.Stat(c.file); !os.IsNotExist(err) {
		t.Fatal("reset did not remove checkpoint")
	}
}

type expiredCheckpointDrive struct {
	*fakeDAVDrive
	calls  int
	names  []string
	code   int
	reason string
}

func (d *expiredCheckpointDrive) Upload(ctx context.Context, p, n string, f *os.File) (davEntry, error) {
	d.calls++
	d.names = append(d.names, n)
	if d.calls == 1 {
		return davEntry{}, &quarkPartError{HTTP: &upstreamHTTPError{code: d.code}, Code: d.reason}
	}
	return d.fakeDAVDrive.Upload(ctx, p, n, f)
}
func TestCheckpointOnlyExplicitMissingSessionRebuilds(t *testing.T) {
	for _, code := range []int{404, 403, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			_, base, fs := testDAVService(t, true)
			drive := &expiredCheckpointDrive{fakeDAVDrive: base, code: code, reason: "NoSuchUpload"}
			fs.mounts[0].drive = drive
			f, _ := os.CreateTemp(t.TempDir(), "body")
			defer f.Close()
			f.WriteString("new")
			cp := testCheckpoint(t)
			cp.data.Stage = "CloudProbe-upload-before"
			cp.data.Size = 3
			d := &davFile{fs: fs, ctx: context.Background(), name: "/联通云盘-1/new", tmp: f, node: davNode{mount: fs.mounts[0], parent: "0", entry: davEntry{Item: Item{Name: "new"}}}}
			_, err := d.uploadWithCheckpoint(cp)
			if code == 404 {
				if err != nil || drive.calls != 2 || drive.names[0] == drive.names[1] {
					t.Fatalf("expired session not rebuilt safely: %v", err)
				}
			} else {
				if err == nil || drive.calls != 1 {
					t.Fatal("unconfirmed expiry discarded session")
				}
			}
		})
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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

func memoryCheckpointService(t *testing.T, drive *quarkDAVDrive, spool string) (*httptest.Server, *davFS) {
	server, _, fs := testDAVService(t, true)
	fs.spool = spool
	fs.mounts[0].Identity = "stable-memory-account"
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].drive = drive
	fs.mounts[0].UploadStorageMode = "memory"
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "memory", UploadMemoryMiB: 5, TotalUploadMemoryMiB: 5, UploadPrefetchMiB: 1, UploadRecoveryMode: "off"})
	return server, fs
}
func assertMemoryMetadataOnly(t *testing.T, spool string, body []byte) {
	t.Helper()
	err := filepath.WalkDir(spool, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(filepath.Dir(path)) != "resume" {
			t.Errorf("body spool created: %s", path)
		}
		if !strings.HasSuffix(d.Name(), ".bin") && !strings.HasPrefix(d.Name(), "lease-") && d.Name() != "catalog-lease" {
			t.Errorf("unexpected disk artifact: %s", path)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(b) > 4<<20 {
			t.Errorf("metadata exceeds budget: %d", len(b))
		}
		if len(body) > 64 && bytes.Contains(b, body[:64]) {
			t.Error("plaintext body reached disk")
		}
		if strings.HasSuffix(d.Name(), ".bin") {
			plain, err := unprotectLocal(b)
			if err != nil {
				return err
			}
			if len(body) > 64 && bytes.Contains(plain, body[:64]) {
				t.Error("body stored inside checkpoint")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMemoryCheckpointFullPUTReplayAndChangedContent(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(strconv.FormatBool(changed), func(t *testing.T) {
			fixture, drive := newQuarkFixture(t)
			fixture.add("0", "file.bin", []byte("original"), false)
			spool := t.TempDir()
			server, fs := memoryCheckpointService(t, drive, spool)
			client := *drive.bodyHTTP
			client.Transport = &checkpointTransport{base: drive.bodyHTTP.Transport, cancel: func() {}}
			drive.bodyHTTP = &client
			body := bytes.Repeat([]byte("plaintext-must-stay-in-RAM-"), 250000)
			code, _, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/file.bin", bytes.NewReader(body), nil)
			if code < 400 {
				t.Fatal("expected injected interruption")
			}
			server.Close()
			assertMemoryMetadataOnly(t, spool, body)
			if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
				t.Fatal("failed upload leaked buffers")
			}
			restarted := newQuarkDrive(drive.client, newTransferRuntime(transferOptions{}), 1, "auto")
			restarted.retryWait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
			server, fs = memoryCheckpointService(t, restarted, spool)
			if changed {
				body = append([]byte(nil), body...)
				body[65536+17] ^= 1 // change a previously attempted but unacknowledged part
				wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", string(body), 409, nil)
				if got := wantDAV(t, server.URL, "GET", "/联通云盘-1/file.bin", "", 200, nil); string(got) != "original" {
					t.Fatal("mismatched replay committed")
				}
			}
			wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", string(body), 201, nil)
			if got := wantDAV(t, server.URL, "GET", "/联通云盘-1/file.bin", "", 200, nil); !bytes.Equal(got, body) {
				t.Fatal("recovered content differs")
			}
			fixture.mu.Lock()
			calls, uploads := fixture.partCalls[1], len(fixture.uploads)
			fixture.mu.Unlock()
			if !changed && (calls != 1 || uploads != 1) {
				t.Fatalf("confirmed part replayed: calls=%d sessions=%d", calls, uploads)
			}
			if changed && uploads != 2 {
				t.Fatal("changed body did not start a fresh session")
			}
			stats := fs.transfers.memory.status()
			if stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 5<<20 {
				t.Fatal(stats)
			}
			assertMemoryMetadataOnly(t, spool, body)
		})
	}
}

func TestMemoryCheckpointCompletedUploadDrainsReplayBeforeCommit(t *testing.T) {
	fixture, drive := newQuarkFixture(t)
	spool := t.TempDir()
	_, fs := memoryCheckpointService(t, drive, spool)
	body := bytes.Repeat([]byte("verified-memory-content"), 5000)
	parent, base, err := fs.parent(context.Background(), "/联通云盘-1/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	f, err := fs.openMemoryFile(context.Background(), "/联通云盘-1/file.bin", parent, base, nil, int64(len(body)))
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
	f.memory.Close()
	f.releaseCheckpoint() // simulate a restart before final rename
	fixture.mu.Lock()
	calls := fixture.partCalls[1]
	fixture.mu.Unlock()
	server, fs := memoryCheckpointService(t, drive, spool)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", string(body), 201, nil)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.partCalls[1] != calls {
		t.Fatal("already-uploaded object retransmitted")
	}
	if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
		t.Fatal("replay drain retained memory")
	}
}

func TestMemoryCheckpointWopanRevalidatesAndReleasesSkippedParts(t *testing.T) {
	body := bytes.Repeat([]byte("memory-network-recovery-"), 25)
	var mu sync.Mutex
	calls := map[int]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		i, _ := strconv.Atoi(r.FormValue("partIndex"))
		mu.Lock()
		calls[i]++
		n := calls[i]
		mu.Unlock()
		if i == 2 && n == 1 {
			http.Error(w, "stop", 400)
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"done"}}`)
	}))
	defer srv.Close()
	cp := testCheckpoint(t)
	cp.data.Mode = "memory"
	cp.data.Hashes = map[int64]string{}
	cp.data.Size = int64(len(body))
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			cp = reloadCheckpoint(t, cp)
		}
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), uploadCheckpointKey{}, cp), 5*time.Second)
		if attempt == 1 {
			plan, err := planMemoryUpload(ctx, &wopanDAVDrive{}, "0", "stage", int64(len(body)), transferOptions{UploadPartMiB: 16})
			if err != nil || plan.partSize != 80 {
				t.Fatal("changed settings lost original part layout", plan.partSize, err)
			}
		}
		budget := newByteBudget(160)
		source := newMemoryUploadSource(ctx, int64(len(body)), 80, 80, budget, nil)
		written := make(chan error, 1)
		go func() {
			_, err := source.Write(body)
			if err == nil {
				err = source.FinishInput()
			}
			written <- err
		}()
		u := &wopanUploadSession{endpoint: srv.URL, unique: "stable", name: "stage", size: int64(len(body)), partSize: 80, concurrency: 1, client: srv.Client()}
		_, err := u.run(source.ctx, source)
		if attempt == 0 && err == nil {
			t.Fatal("interruption missing")
		}
		if attempt == 1 && err != nil {
			t.Fatal(err)
		}
		if err != nil {
			source.Abort(err)
		}
		<-written
		source.Close()
		cancel()
		if budget.status()["used_bytes"].(int64) != 0 || budget.status()["peak_bytes"].(int64) > 160 {
			t.Fatal("memory budget violated")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[1] != 1 {
		t.Fatal("first confirmed part replayed")
	}
}

func TestMemoryCheckpointSaveFailureRetainsPart(t *testing.T) {
	cp := testCheckpoint(t)
	cp.data.Mode = "memory"
	cp.data.Hashes = map[int64]string{}
	ctx := context.WithValue(context.Background(), uploadCheckpointKey{}, cp)
	budget := newByteBudget(8)
	s := newMemoryUploadSource(ctx, 4, 4, 4, budget, nil)
	defer s.Close()
	s.Write([]byte("body"))
	if _, _, err := prepareCheckpointPart(ctx, s, 1, 0, 4); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cp.file); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cp.file, 0700); err != nil {
		t.Fatal(err)
	}
	if err := cp.confirm(1, "tag"); err == nil {
		t.Fatal("save failure ignored")
	}
	got := make([]byte, 4)
	if _, err := s.ReadAt(got, 0); err != nil || string(got) != "body" {
		t.Fatal("body released before checkpoint saved")
	}
	if _, ok := cp.part(1); ok {
		t.Fatal("unsaved confirmation reused")
	}
	if _, err := json.Marshal(cp.data); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryCheckpointMissingFingerprintCannotReuseACK(t *testing.T) {
	cp := testCheckpoint(t)
	cp.data.Mode = "memory"
	cp.data.Hashes = map[int64]string{}
	cp.data.Parts[1] = "old-tag"
	ctx := context.WithValue(context.Background(), uploadCheckpointKey{}, cp)
	s := newMemoryUploadSource(ctx, 4, 4, 4, newByteBudget(4), nil)
	defer s.Close()
	s.Write([]byte("new!"))
	if _, reuse, err := prepareCheckpointPart(ctx, s, 1, 0, 4); err == nil || reuse {
		t.Fatal("missing fingerprint accepted as identical bytes")
	}
}

func TestMemoryCheckpointInstantUploadVerifiesFullReplayHash(t *testing.T) {
	for _, correct := range []bool{true, false} {
		t.Run(strconv.FormatBool(correct), func(t *testing.T) {
			fixture, drive := newQuarkFixture(t)
			body := bytes.Repeat([]byte("verified-instant-memory-content"), 200000)
			original := fixture.server.Config.Handler
			fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/file/upload/pre") {
					var request map[string]any
					if json.NewDecoder(r.Body).Decode(&request) != nil {
						t.Error("invalid prepare")
						return
					}
					data := append([]byte(nil), body...)
					if !correct {
						data[0] ^= 1
					}
					entry := fixture.add("0", valueString(request["file_name"]), data, false)
					respondJSON(w, 200, map[string]any{"code": 0, "status": 200, "data": map[string]any{"finish": true, "fid": entry.ID}})
					return
				}
				original.ServeHTTP(w, r)
			})
			server, fs := memoryCheckpointService(t, drive, t.TempDir())
			code := 502
			if correct {
				code = 201
			}
			wantDAV(t, server.URL, "PUT", "/联通云盘-1/instant.bin", string(body), code, nil)
			if correct {
				if got := wantDAV(t, server.URL, "GET", "/联通云盘-1/instant.bin", "", 200, nil); !bytes.Equal(got, body) {
					t.Fatal("instant bytes differ")
				}
			} else {
				wantDAV(t, server.URL, "GET", "/联通云盘-1/instant.bin", "", 404, nil)
			}
			if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
				t.Fatal("instant upload leaked memory")
			}
			assertMemoryMetadataOnly(t, fs.spool, body)
		})
	}
}

func TestMemoryCheckpointCanceledReplayReleasesBudget(t *testing.T) {
	cp := testCheckpoint(t)
	cp.data.Mode = "memory"
	cp.data.Hashes = map[int64]string{1: fmt.Sprintf("%x", sha256.Sum256([]byte("body")))}
	cp.data.Uploaded = &davEntry{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), uploadCheckpointKey{}, cp))
	budget := newByteBudget(4)
	source := newMemoryUploadSource(ctx, 8, 4, 4, budget, nil)
	if _, err := source.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- source.drainCheckpoint(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled replay blocked")
	}
	source.Close()
	if budget.status()["used_bytes"].(int64) != 0 {
		t.Fatal("canceled replay leaked memory")
	}
}

func memoryProcessBody() []byte { return bytes.Repeat([]byte("memory-process-payload-"), 40) }
func TestMemoryCheckpointProcessHelper(t *testing.T) {
	file := os.Getenv("CLOUDPROBE_MEMORY_CHECKPOINT_HELPER")
	if file == "" {
		t.Skip("subprocess-only helper")
	}
	lease, err := lockUploadCheckpoint(filepath.Join(filepath.Dir(file), "lease-bb"))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	cp := reloadCheckpoint(t, &uploadCheckpoint{file: file})
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), uploadCheckpointKey{}, cp), 15*time.Second)
	defer cancel()
	body := memoryProcessBody()
	budget := newByteBudget(160)
	s := newMemoryUploadSource(ctx, int64(len(body)), 80, 80, budget, nil)
	written := make(chan error, 1)
	go func() {
		_, e := s.Write(body)
		if e == nil {
			e = s.FinishInput()
		}
		written <- e
	}()
	u := &wopanUploadSession{endpoint: os.Getenv("CLOUDPROBE_MEMORY_CHECKPOINT_URL"), unique: "original-process-session", name: "stage", size: int64(len(body)), partSize: 80, concurrency: 1}
	_, err = u.run(s.ctx, s)
	if err != nil {
		s.Abort(err)
	}
	writeErr := <-written
	s.Close()
	if err != nil || writeErr != nil {
		t.Fatal(err, writeErr)
	}
	if budget.status()["used_bytes"].(int64) != 0 {
		t.Fatal("child retained memory")
	}
}

func TestMemoryCheckpointForcedProcessRestartWithoutBodyFiles(t *testing.T) {
	spool := t.TempDir()
	key := strings.Repeat("b", 64)
	cp := &uploadCheckpoint{file: filepath.Join(spool, "resume", key+".bin"), data: uploadCheckpointData{Version: 1, Key: key, Mode: "memory", Size: int64(len(memoryProcessBody())), Parts: map[int64]string{}, Hashes: map[int64]string{}}}
	if err := cp.saveLocked(); err != nil {
		t.Fatal(err)
	}
	seen := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var mu sync.Mutex
	calls := map[int]int{}
	parts := map[int][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ParseMultipartForm(1<<20) != nil {
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("uniqueId") != "original-process-session" {
			t.Error("cloud session changed across process restart")
		}
		i, _ := strconv.Atoi(r.FormValue("partIndex"))
		f, _, _ := r.FormFile("file")
		body, _ := io.ReadAll(f)
		f.Close()
		mu.Lock()
		calls[i]++
		n := calls[i]
		parts[i] = body
		mu.Unlock()
		if i == 2 && n == 1 {
			close(seen)
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
		cmd := exec.Command(os.Args[0], "-test.run=^TestMemoryCheckpointProcessHelper$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), "CLOUDPROBE_MEMORY_CHECKPOINT_HELPER="+cp.file, "CLOUDPROBE_MEMORY_CHECKPOINT_URL="+srv.URL)
		return cmd
	}
	first := command()
	var output bytes.Buffer
	first.Stdout = &output
	first.Stderr = &output
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer first.Process.Kill()
	exited := make(chan error, 1)
	go func() { exited <- first.Wait() }()
	select {
	case <-seen:
	case err := <-exited:
		unblock()
		t.Fatalf("child exited early: %v %s", err, output.String())
	case <-time.After(10 * time.Second):
		unblock()
		_ = first.Process.Kill()
		<-exited
		t.Fatalf("first process did not reach second part: %s", output.String())
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-exited
	unblock()
	assertMemoryMetadataOnly(t, spool, memoryProcessBody())
	if out, err := command().CombinedOutput(); err != nil {
		t.Fatalf("restart failed: %v %s", err, out)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[1] != 1 || calls[2] != 2 {
		t.Fatal("WAN prefix replayed after restart", calls)
	}
	var joined []byte
	for i := 1; i <= len(parts); i++ {
		joined = append(joined, parts[i]...)
	}
	if !bytes.Equal(joined, memoryProcessBody()) {
		t.Fatal("reassembled cloud content differs")
	}
	assertMemoryMetadataOnly(t, spool, memoryProcessBody())
}

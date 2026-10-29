package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

// Regression cases promoted from the 1.6.0 audit. No live cloud access.
func maintenanceWait(t *testing.T, label string, ready func() bool) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(until) {
			t.Fatal("reproduction setup timed out: " + label)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFixMemoryAndRequestBudgetsCannotDeadlock(t *testing.T) {
	const part int64 = 5 << 20
	o := transferOptions{UploadMemoryMiB: 5, TotalUploadMemoryMiB: 5, UploadPrefetchMiB: 1, TotalConnections: 1}.defaults()
	if err := o.validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := newMemoryUploadSource(ctx, 2*part, part, part, newByteBudget(part), nil)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.Context().Err() == nil {
			io.WriteString(w, `{"code":"0000","data":{"fid":"fixture"}}`)
		}
	}))
	defer node.Close()
	u := &wopanUploadSession{endpoint: node.URL, name: "fixture.bin", unique: "maintenance-budget", size: 2 * part, partSize: part, concurrency: 2, mode: "http1", budget: newFairLimiter(1), connections: &uploadConnections{}, retrySlots: make(chan struct{}, 1)}
	poolA, poolB := u.newPool(), u.newPool()
	defer poolA.close()
	defer poolB.close()
	var sent, confirmed, retries atomic.Int64
	result := make(chan error, 3)
	future := &maintenanceReadySource{memoryUploadSource: source, entered: make(chan struct{})}
	go func() { _, e := u.part(ctx, future, 2, 2, 2, poolB, &sent, &confirmed, &retries); result <- e }()
	<-future.entered
	if u.budget.status()["active"] != 0 {
		t.Error("future part consumed HTTP permit before input was available")
	}
	go func() {
		_, e := source.Write(bytes.Repeat([]byte("x"), int(2*part)))
		if e == nil {
			e = source.FinishInput()
		}
		result <- e
	}()
	maintenanceWait(t, "first part fills bounded cache", func() bool { return source.Received() == part })
	go func() { _, e := u.part(ctx, source, 1, 2, 1, poolA, &sent, &confirmed, &retries); result <- e }()
	for range 3 {
		if e := <-result; e != nil {
			t.Error(e)
		}
	}
	if confirmed.Load() != 2*part {
		t.Error("legal low-budget upload did not finish", confirmed.Load())
	}
	source.Close()
}

func TestFixBodyCloseMustUnblockPendingRead(t *testing.T) {
	source := newMemoryUploadSource(context.Background(), 1024, 1024, 1024, newByteBudget(1024), nil)
	entered := make(chan struct{})
	body := newUploadBody(context.Background(), func(ctx context.Context) io.Reader {
		return &maintenanceSignalReader{Reader: uploadSection(ctx, source, 0, 1024), entered: entered}
	})
	readDone, closeDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(readDone); body.Read(make([]byte, 128)) }()
	<-entered
	go func() { defer close(closeDone); body.Close() }()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		source.Abort(context.Canceled)
		t.Error("Close failed to interrupt a pending per-request read")
	}
	<-readDone
	<-closeDone
	if source.ctx.Err() != nil {
		t.Error("closing one request canceled the whole upload")
	}
	if _, e := source.Write(bytes.Repeat([]byte("x"), 1024)); e != nil {
		t.Fatal(e)
	}
	retry := newUploadBody(context.Background(), func(ctx context.Context) io.Reader { return uploadSection(ctx, source, 0, 1024) })
	data, e := io.ReadAll(retry)
	retry.Close()
	if e != nil || len(data) != 1024 {
		t.Error("same cached bytes unavailable for retry", e)
	}
	source.Close()
}

func TestFixMissingAccountIdentityMustNotRefreshOtherAccounts(t *testing.T) {
	a := guiAccount{Key: "a", Provider: "wopan", credentials: map[string]any{"token": "fixture-account-a-token"}}
	b := guiAccount{Key: "b", Provider: "wopan", credentials: map[string]any{"token": "fixture-account-b-token"}}
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": a, "b": b}}
	if e := g.saveDAVCredential(a, "fixture-refreshed-a-token", "fixture-refresh-a"); e == nil {
		t.Error("unstable account identity accepted for persistence")
	}
	if valueString(g.accounts["b"].credentials["token"]) != "fixture-account-b-token" {
		t.Error("account B token overwritten")
	}
	if e := g.saveDAVWebCredential(a, "fixture-website-token"); e == nil {
		t.Error("website token saved without a stable identity")
	}
	if davIdentity(a) != "" || davIdentity(b) != "" {
		t.Error("unknown identities must not generate shared persistent keys")
	}
}

func TestFixPartialNativeMoveMustNotReplayThroughOpenList(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fixture-token-012345678901234567"
	cl, err := newClient(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	d := newWopanDAVDrive(cl, nil, "0", nil)
	d.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
	var renames, moves, backupWrites atomic.Int64
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		var request struct {
			Header struct {
				Key string `json:"key"`
			} `json:"header"`
		}
		json.NewDecoder(r.Body).Decode(&request)
		code := "0000"
		switch request.Header.Key {
		case wopan.KeyRenameFileOrDirectory:
			renames.Add(1)
		case wopan.KeyMoveFile:
			moves.Add(1)
			code = "500020001"
		default:
			t.Error("unexpected SDK request", request.Header.Key)
		}
		body := fmt.Sprintf(`{"STATUS":"200","RSP":{"RSP_CODE":"%s","DATA":{}}}`, code)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			backupWrites.Add(1)
		}
		respondJSON(w, 200, map[string]any{"code": 404, "message": "old path was already renamed", "data": map[string]any{}})
	}))
	defer api.Close()
	c, err := newOpenListClient(openListInput{Enabled: true, Address: api.URL, Root: "/mount"})
	if err != nil {
		t.Fatal(err)
	}
	b := &backupDAVDrive{primary: d, backup: c, root: "0", paths: map[string]string{"0": "", "f": "old.txt", "dest": "destination"}, entries: map[string]davEntry{}}
	err = b.Move(context.Background(), davEntry{Item: Item{ID: "f", Name: "old.txt"}}, "dest", "new.txt")
	if renames.Load() != 1 || moves.Load() != 1 || err == nil {
		t.Fatal("partial execution setup failed", renames.Load(), moves.Load(), err)
	}
	if backupWrites.Load() != 0 {
		t.Errorf("BUG: native rename succeeded, native move failed, yet OpenList was asked to write again (%d request)", backupWrites.Load())
	}
}

func TestFixClosedUploadConnectionsMustNotBeRetainedForever(t *testing.T) {
	registry := &uploadConnections{}
	for range 500 {
		a, b := net.Pipe()
		tracked := registry.track(a)
		id := registry.id(tracked)
		if id != registry.id(tracked) {
			t.Error("active connection lost its stable ID")
		}
		tracked.Close()
		b.Close()
	}
	if len(registry.ids) != 0 {
		t.Error("closed connections retained", len(registry.ids))
	}
}

func TestFixFailedLockCreationMustReleaseItsLock(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	drive.failUpload = true
	body := `<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:owner>review</D:owner></D:lockinfo>`
	first, _, _ := davRequest(t, server.URL, "LOCK", "/联通云盘-1/ghost.txt", strings.NewReader(body), nil)
	if first != 502 {
		t.Fatal("setup expected failed cloud write", first)
	}
	if _, err := fs.Stat(context.Background(), "/联通云盘-1/ghost.txt"); !os.IsNotExist(err) {
		t.Fatal("fixture file unexpectedly exists", err)
	}
	second, _, _ := davRequest(t, server.URL, "LOCK", "/联通云盘-1/ghost.txt", strings.NewReader(body), nil)
	if second == 423 {
		t.Error("BUG: failed LOCK returned 502 but left a persistent ghost lock; subsequent LOCK on absent file returns 423")
	}
	drive.failUpload = false
	code, _, headers := davRequest(t, server.URL, "LOCK", "/联通云盘-1/ghost.txt", strings.NewReader(body), nil)
	if code != 201 || headers.Get("Lock-Token") == "" {
		t.Fatal("successful LOCK no longer works after failure", code)
	}
	wantDAV(t, server.URL, "UNLOCK", "/联通云盘-1/ghost.txt", "", 204, map[string]string{"Lock-Token": headers.Get("Lock-Token")})
}

type maintenanceLostAckDrive struct {
	*fakeDAVDrive
	lost bool
}

func (d *maintenanceLostAckDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	if err := d.fakeDAVDrive.Move(ctx, it, parent, name); err != nil {
		return err
	}
	if !d.lost && strings.HasPrefix(it.Name, "CloudProbe-upload-") && name == "target.txt" {
		d.lost = true
		return errors.New("response lost after successful cloud rename")
	}
	return nil
}
func TestFixLostCommitResponseMustNotBlindlyRestoreSameName(t *testing.T) {
	server, base, fs := testDAVService(t, true)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/target.txt", "old", 201, nil)
	d := &maintenanceLostAckDrive{fakeDAVDrive: base}
	fs.mounts[0].drive = d
	status, _, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/target.txt", strings.NewReader("new"), nil)
	if status != 201 || !d.lost {
		t.Fatal("confirmed commit was not recovered", status)
	}
	items, _ := base.List(context.Background(), "0")
	if len(items) != 1 || items[0].Name != "target.txt" {
		t.Fatal("reconciliation introduced duplicates or left backup", items)
	}
	code, data, _ := davRequest(t, server.URL, "GET", "/联通云盘-1/target.txt", nil, nil)
	if code != 200 || string(data) != "new" {
		t.Error("committed content cannot be read", code)
	}
}

type maintenanceReadySource struct {
	*memoryUploadSource
	entered chan struct{}
}

func (s *maintenanceReadySource) WaitReadable(ctx context.Context, off int64) error {
	close(s.entered)
	return s.memoryUploadSource.WaitReadable(ctx, off)
}

type maintenanceSignalReader struct {
	io.Reader
	entered chan struct{}
	once    sync.Once
}

func (r *maintenanceSignalReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.Reader.Read(p)
}

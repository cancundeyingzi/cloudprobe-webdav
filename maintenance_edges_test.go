package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"golang.org/x/net/webdav"
)

func TestMaintenanceHTTPAttemptTimeoutPreservesCachedPart(t *testing.T) {
	const part int64 = 5 << 20
	var requests atomic.Int64
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, e := io.Copy(io.Discard, r.Body); e != nil {
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"fixture"}}`)
	}))
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	source := newMemoryUploadSource(ctx, part, part, part, newByteBudget(part), nil)
	if _, e := source.Write(make([]byte, 1024)); e != nil {
		t.Fatal(e)
	}
	client := node.Client()
	client.Timeout = 40 * time.Millisecond
	// A short prefix may still sit in net/http's write buffer while the client
	// supplies no body bytes. Count attempts before RoundTrip, not at the server.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client.Transport = protocolRoundTripper(func(r *http.Request) (*http.Response, error) { requests.Add(1); return transport.RoundTrip(r) })
	u := &wopanUploadSession{endpoint: node.URL, name: "slow-client.bin", unique: "attempt-timeout", size: part, partSize: part, concurrency: 1, mode: "http1", client: client, retryDelay: time.Millisecond}
	done := make(chan error, 1)
	go func() { _, e := u.run(ctx, source); done <- e }()
	until := time.After(time.Second)
	for requests.Load() < 2 {
		select {
		case e := <-done:
			t.Fatalf("attempt ended before retry: %T %v; source=%v; attempts=%d", e, e, source.ctx.Err(), requests.Load())
		case <-until:
			t.Fatalf("attempt timeout did not release request; attempts=%d source=%v", requests.Load(), source.ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if source.ctx.Err() != nil {
		t.Fatal("attempt timeout canceled retained file cache")
	}
	if _, e := source.Write(make([]byte, part-1024)); e != nil {
		t.Fatal(e)
	}
	if e := source.FinishInput(); e != nil {
		t.Fatal(e)
	}
	if e := <-done; e != nil {
		t.Fatal("upload could not continue after client supplied remaining bytes", e)
	}
	source.Close()
	if source.global.status()["used_bytes"].(int64) != 0 {
		t.Fatal("timeout retry leaked buffers")
	}
}

func TestMaintenanceStableIdentityRestoreAndLegacyIsolation(t *testing.T) {
	a := guiAccount{Key: "a", Provider: "wopan", credentials: map[string]any{"username": "fixture-account-a", "token": "original-a"}}
	b := guiAccount{Key: "b", Provider: "wopan", credentials: map[string]any{"username": "fixture-account-b", "token": "original-b"}}
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{"a": a, "b": b}}
	oldKey := davIdentityHash("wopan", "fixture-account-a")
	if e := g.saveDAVCredential(cloneGUIAccount(a), "updated-a", "refresh-a"); e != nil {
		t.Fatal(e)
	}
	if e := g.saveDAVWebCredential(cloneGUIAccount(a), "fixture-web-a"); e != nil {
		t.Fatal(e)
	}
	if davIdentity(g.accounts["a"]) != oldKey || g.accounts["b"].credentials["token"] != "original-b" || g.accounts["b"].credentials["wopan_web_token"] != nil {
		t.Fatal("normal account identity or isolation changed")
	}
	next := map[string]guiAccount{"a": {Provider: "wopan", credentials: map[string]any{"username": "fixture-account-a", "token": "original-a"}}, "b": {Provider: "wopan", credentials: map[string]any{"username": "fixture-account-b", "token": "original-b"}}}
	g.applyDAVCredentials(next)
	if next["a"].credentials["token"] != "updated-a" || next["a"].credentials["wopan_web_token"] != "fixture-web-a" || next["b"].credentials["token"] != "original-b" {
		t.Fatal("existing valid encrypted credentials did not restore")
	}
	legacy := map[string]davCredential{davIdentityHash("wopan", ""): {Access: "wrong-shared-token", WebToken: "wrong-shared-web"}}
	plain, _ := json.Marshal(legacy)
	protected, e := protectLocal(plain)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(filepath.Join(g.data, "webdav-credentials.bin"), protected, 0600)
	unknown := map[string]guiAccount{"x": {Provider: "wopan", credentials: map[string]any{"token": "only-x"}}, "y": {Provider: "wopan", credentials: map[string]any{"token": "only-y"}}}
	g.applyDAVCredentials(unknown)
	if unknown["x"].credentials["token"] != "only-x" || unknown["y"].credentials["token"] != "only-y" || unknown["x"].WebBound || unknown["y"].WebBound {
		t.Fatal("unsafe legacy empty-identity override applied")
	}
}

type maintenanceCommitModeDrive struct {
	*fakeDAVDrive
	mode string
	done bool
}

func (d *maintenanceCommitModeDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	if !d.done && d.mode == "backup-ack" && it.Name == "target.txt" && strings.HasPrefix(name, "CloudProbe-backup-") {
		d.done = true
		if e := d.fakeDAVDrive.Move(ctx, it, parent, name); e != nil {
			return e
		}
		return errors.New("lost backup acknowledgement")
	}
	if !d.done && strings.HasPrefix(it.Name, "CloudProbe-upload-") && name == "target.txt" {
		switch d.mode {
		case "before-commit":
			d.done = true
			return errors.New("commit not executed")
		case "foreign-target":
			d.done = true
			d.mu.Lock()
			d.seq++
			id := fmt.Sprint(d.seq)
			entry := davEntry{Item: Item{ID: id, FID: "foreign-version", Name: name, Size: 7}, Modified: time.Now()}
			d.entries[id] = entry
			d.parents[id] = "0"
			d.contents[id] = []byte("foreign")
			d.mu.Unlock()
			return errors.New("external target appeared")
		}
	}
	return d.fakeDAVDrive.Move(ctx, it, parent, name)
}
func TestMaintenanceCommitRecoveryModes(t *testing.T) {
	for _, mode := range []string{"before-commit", "backup-ack", "foreign-target"} {
		t.Run(mode, func(t *testing.T) {
			server, base, fs := testDAVService(t, true)
			wantDAV(t, server.URL, "PUT", "/联通云盘-1/target.txt", "old", 201, nil)
			d := &maintenanceCommitModeDrive{fakeDAVDrive: base, mode: mode}
			fs.mounts[0].drive = d
			status, _, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/target.txt", strings.NewReader("new"), nil)
			wantCode, wantText := 502, "old"
			if mode == "backup-ack" {
				wantCode, wantText = 201, "new"
			}
			if mode == "foreign-target" {
				wantText = "foreign"
			}
			if status != wantCode {
				t.Fatal("unexpected commit result", status, wantCode)
			}
			code, body, _ := davRequest(t, server.URL, "GET", "/联通云盘-1/target.txt", nil, nil)
			if code != 200 || string(body) != wantText {
				t.Fatal("wrong target after reconciliation", code, string(body))
			}
			entries, _ := base.List(context.Background(), "0")
			targets, backups := 0, 0
			for _, it := range entries {
				if it.Name == "target.txt" {
					targets++
				}
				if strings.HasPrefix(it.Name, "CloudProbe-backup-") {
					backups++
				}
			}
			if targets != 1 || mode == "foreign-target" && backups != 1 {
				t.Fatal("duplicates introduced or recovery backup lost", targets, backups)
			}
		})
	}
}

func TestMaintenanceConfirmedRejectionDoesNotUseOpenListForWrites(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fixture-token-012345678901234567"
	cl, _ := newClient(cfg, "")
	d := newWopanDAVDrive(cl, nil, "0", nil)
	d.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"STATUS":"200","RSP":{"RSP_CODE":"500020001","DATA":{}}}`)), Request: r}, nil
	})})
	var writes atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writes.Add(1)
		respondJSON(w, 200, map[string]any{"code": 200, "data": map[string]any{}})
	}))
	defer api.Close()
	c, _ := newOpenListClient(openListInput{Enabled: true, Address: api.URL, Root: "/mount"})
	b := &backupDAVDrive{primary: d, backup: c, root: "0", paths: map[string]string{"0": "", "f": "old.txt", "dest": "destination"}, entries: map[string]davEntry{}}
	if e := b.Move(context.Background(), davEntry{Item: Item{ID: "f", Name: "old.txt"}}, "dest", "new.txt"); e == nil {
		t.Fatal("native rejection became success through unsafe OpenList fallback")
	}
	if writes.Load() != 0 {
		t.Fatal("OpenList wrote without stable object identities", writes.Load())
	}
}

func TestMaintenanceExecutionStageControlsReplay(t *testing.T) {
	denied := &davNotExecuted{errors.New("explicit rejection")}
	if !safePreexecutionFailure(denied) {
		t.Fatal("explicit pre-execution rejection lost fallback")
	}
	if safePreexecutionFailure(&davMutationError{Phase: "second RPC", Partial: true, Err: denied}) {
		t.Fatal("partial state inherited nested replay permission")
	}
	if safePreexecutionFailure(errors.New("post-write check mentions HTTP 403 and 500020001")) {
		t.Fatal("error text granted replay permission")
	}
	if confirmedMutationRejection(&apiBusinessError{Code: "500020001", Message: "unknown provider code"}) {
		t.Fatal("a provider-specific code was trusted for an unrelated API")
	}
}

func TestMaintenanceTrackedTLSConnectionRetires(t *testing.T) {
	registry := &uploadConnections{}
	a, b := net.Pipe()
	defer b.Close()
	tracked := registry.track(a)
	tlsConn := tls.Client(tracked, &tls.Config{ServerName: "fixture.invalid"})
	if registry.id(tlsConn) != registry.id(tracked) {
		t.Fatal("TLS wrapper changed connection identity")
	}
	tlsConn.Close()
	if len(registry.ids) != 0 {
		t.Fatal("TLS close retained underlying connection")
	}
}

func TestMaintenanceSingleBufferMultipleWorkers(t *testing.T) {
	server, _, fs := installMemoryFixture(t)
	fs.transfers = newTransferRuntime(transferOptions{UploadMemoryMiB: 5, TotalUploadMemoryMiB: 5, UploadPrefetchMiB: 1, TotalConnections: 1, UploadRecoveryMode: "off"})
	data := bytes.Repeat([]byte("x"), 8<<20)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/tight.bin", string(data), 201, nil)
	code, got, _ := davRequest(t, server.URL, "GET", "/联通云盘-1/tight.bin", nil, nil)
	if code != 200 || !bytes.Equal(got, data) {
		t.Fatal("low-budget pipeline changed file data")
	}
	if fs.transfers.memory.status()["peak_bytes"].(int64) > 5<<20 {
		t.Fatal("low-budget fix exceeded memory limit")
	}
}

func TestMaintenanceMobileResponseReadKeepsAttemptContextAlive(t *testing.T) {
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, "<result ")
		w.(http.Flusher).Flush()
		time.Sleep(15 * time.Millisecond)
		io.WriteString(w, `resultCode="0"/>`)
	}))
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sent, confirmed atomic.Int64
	d := &mobileDAVDrive{}
	if e := d.sendUploadPart(ctx, &http.Client{Timeout: 500 * time.Millisecond}, node.URL, "task", "data.bin", 1024, 0, 1024, 1, 1, true, bytes.NewReader(make([]byte, 1024)), &sent, &confirmed); e != nil {
		t.Fatal("successful response was canceled before body was read", e)
	}
	if confirmed.Load() != 1024 {
		t.Fatal("mobile confirmation not preserved")
	}
}

func TestMaintenanceMobilePartialMoveCannotReplay(t *testing.T) {
	cfg := defaultConfig("mobile")
	cfg.Transport = "bridge"
	cfg.Mobile = MobileConfig{AppID: "fixture", AppSecret: "fixture-secret", AccessToken: "fixture-token", DeviceID: "device"}
	cl, _ := newClient(cfg, "")
	var calls atomic.Int64
	cl.bridge = quotaBridgeFunc(func(ctx context.Context, method, path string, body []byte, h map[string]string) ([]byte, error) {
		calls.Add(1)
		var req map[string]any
		json.Unmarshal(body, &req)
		if strings.HasSuffix(valueString(req["path"]), "moveContentCatalog") {
			return nil, &apiHTTPError{Code: 403, Err: errors.New("fixture HTTP 403 rejection")}
		}
		return []byte(`{"code":0,"data":{"resultCode":"0"}}`), nil
	})
	d := &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}}
	e := d.Move(context.Background(), davEntry{Item: Item{ID: "f", Name: "old.txt"}}, "dest", "new.txt")
	var partial *davMutationError
	if calls.Load() != 2 || !errors.As(e, &partial) || !partial.Partial || safePreexecutionFailure(e) {
		t.Fatal("mobile partial rename/move incorrectly permits replay", calls.Load(), e)
	}
}

type maintenanceStatFS struct {
	webdav.FileSystem
	target string
	first  error
	calls  atomic.Int64
}

func (f *maintenanceStatFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	if name == f.target && f.calls.Add(1) == 1 {
		return nil, f.first
	}
	return f.FileSystem.Stat(ctx, name)
}

func TestMaintenanceLockNeverTruncatesExistingFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"stat-error", errors.New("transient directory failure")}, {"create-race", os.ErrNotExist}} {
		t.Run(tc.name, func(t *testing.T) {
			server, _, fs := testDAVService(t, true)
			target := "/联通云盘-1/keep.txt"
			wantDAV(t, server.URL, "PUT", target, "keep original", 201, nil)
			service := server.Config.Handler.(*davService)
			service.handler.FileSystem = &maintenanceStatFS{FileSystem: fs, target: target, first: tc.err}
			body := `<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
			code, _, _ := davRequest(t, server.URL, "LOCK", target, strings.NewReader(body), nil)
			if code < 400 {
				t.Fatal("uncertain creation was reported successful", code)
			}
			code, data, _ := davRequest(t, server.URL, "GET", target, nil, nil)
			if code != 200 || string(data) != "keep original" {
				t.Fatal("LOCK truncated existing cloud content", code, string(data))
			}
		})
	}
}

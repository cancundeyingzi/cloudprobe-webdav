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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

func warningStartFixture(t *testing.T, mode string) (*davManager, davStartInput, string, *atomic.Int64) {
	t.Helper()
	body := "download-original-content"
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "old.txt", time.Unix(100000, 0), strings.NewReader(body))
	}))
	t.Cleanup(node.Close)
	g := &guiApp{data: t.TempDir(), logged: true, bridge: &OriginalBridge{}, accounts: map[string]guiAccount{"a": {Key: "a", Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "fixture-user", "token": "fixture-private-token-long"}}}}
	d := newDAVManager(g)
	g.dav = d
	calls := &atomic.Int64{}
	d.wopanClientSetup = func(drive *wopanDAVDrive) {
		drive.w.Phone = "fixture"
		drive.w.ZoneURL = node.URL
		drive.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
		drive.client.bridge = quotaBridgeFunc(func(ctx context.Context, method, path string, b []byte, h map[string]string) ([]byte, error) {
			if mode == "unreadable" {
				return nil, errors.New("read channel unavailable")
			}
			switch path {
			case "/v1/api/wopan/query_files":
				return []byte(fmt.Sprintf(`{"code":0,"data":{"meta":{"code":"200"},"nextPage":false,"fileInfoList":[{"id":"f1","fid":"fid1","name":"old.txt","fileSize":%d,"type":"1"}]}}`, len(body))), nil
			case "/v1/api/wopan/get_download_url":
				return []byte(fmt.Sprintf(`{"code":0,"data":{"downLoadInfos":[{"fid":"fid1","downloadUrl":%q}]}}`, node.URL)), nil
			default:
				return nil, fmt.Errorf("unexpected proxy request %s", path)
			}
		})
		drive.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			code, body := 403, `{"STATUS":"403"}`
			if mode == "unverified" {
				code, body = 200, `{"STATUS":"200","RSP":{"RSP_CODE":"12345","RSP_DESC":"fixture-private-token-long upstream unavailable","DATA":{}}}`
			}
			if mode == "healthy" {
				code, body = 200, `{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":{"files":[],"systemDirs":[]}}}`
			}
			return &http.Response{StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		})})
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	in := davStartInput{Listen: "127.0.0.1", Port: port, Username: "tester", Password: "test-password-123", MaxUploadMiB: 16, Mounts: []davMountInput{{Account: "a", Alias: "盘", Root: "0", Writable: true}}, transferOptions: transferOptions{UploadStorageMode: "disk"}}
	if mode == "openlist-denied" {
		in.Mounts[0].OpenList = &openListInput{Enabled: true, Address: node.URL, Root: "/", Token: "fixture-openlist-token", VerifyWrite: true}
	}
	t.Cleanup(d.Stop)
	return d, in, body, calls
}

func TestDAVStartsWithReadableCloudWhenWriteProbeFails(t *testing.T) {
	for _, mode := range []string{"denied", "unverified", "healthy", "readonly", "openlist-denied"} {
		t.Run(mode, func(t *testing.T) {
			d, in, body, calls := warningStartFixture(t, mode)
			if mode == "readonly" {
				in.Mounts[0].Writable = false
			}
			status, err := d.Start(context.Background(), in)
			if err != nil {
				t.Fatal("readable mount could not start", err)
			}
			if status["running"] != true {
				t.Fatal("service not running")
			}
			base := fmt.Sprintf("http://127.0.0.1:%d", in.Port)
			wantDAV(t, base, "PROPFIND", "/盘/", "", 207, map[string]string{"Depth": "1"})
			if got := wantDAV(t, base, "GET", "/盘/old.txt", "", 200, nil); string(got) != body {
				t.Fatal("download changed")
			}
			wantDAV(t, base, "HEAD", "/盘/old.txt", "", 200, nil)
			if got := wantDAV(t, base, "GET", "/盘/old.txt", "", 206, map[string]string{"Range": "bytes=3-7"}); string(got) != body[3:8] {
				t.Fatal("range changed")
			}
			warning := d.mounts[0].writeWarningSnapshot()
			if mode == "healthy" || mode == "readonly" {
				if warning != nil {
					t.Fatal("healthy/read-only selection incorrectly warned", warning)
				}
				if mode == "readonly" && calls.Load() != 0 {
					t.Fatal("read-only selection checked write channel")
				}
				return
			}
			kind := "denied"
			if mode == "unverified" {
				kind = "unverified"
			}
			if warning == nil || warning.Kind != kind || warning.Reason == "" || warning.Advice == "" {
				t.Fatalf("missing precise warning: %+v", warning)
			}
			if mode == "openlist-denied" {
				if _, ok := d.mounts[0].drive.(*backupDAVDrive); ok {
					t.Fatal("unverified fallback activated")
				}
				if !strings.Contains(warning.Reason, "OpenList") {
					t.Fatal("disabled fallback not explained")
				}
				if !d.config.Mounts[0].OpenListEnabled {
					t.Fatal("requested fallback setting lost")
				}
			}
			encoded, _ := json.Marshal(d.Status())
			if bytes.Contains(encoded, []byte("fixture-private-token-long")) {
				t.Fatal("warning leaked token")
			}
			code, reply, _ := davRequest(t, base, "PUT", "/盘/old.txt", strings.NewReader("replace"), nil)
			if code < 400 || len(reply) == 0 {
				t.Fatalf("write reported success: %d %s", code, reply)
			}
			if mode == "denied" && code != 403 {
				t.Fatalf("permission response = %d %s", code, reply)
			}
			if got := wantDAV(t, base, "GET", "/盘/old.txt", "", 200, nil); string(got) != body {
				t.Fatal("failed upload changed original")
			}
			if d.Status()["running"] != true {
				t.Fatal("write failure stopped service")
			}
		})
	}
}

func TestDAVStillRejectsUnreadableCloudAndCanceledStart(t *testing.T) {
	d, in, _, _ := warningStartFixture(t, "unreadable")
	if _, err := d.Start(context.Background(), in); err == nil {
		t.Fatal("unreadable mount accepted")
	}
	if d.Status()["running"] != false {
		t.Fatal("failed start left service running")
	}
	if _, err := os.Stat(filepath.Join(d.app.data, "webdav.json")); !os.IsNotExist(err) {
		t.Fatal("failed start persisted configuration", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Start(ctx, in); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled start ignored", err)
	}
}

type warningPermissionDrive struct {
	*fakeDAVDrive
	allow atomic.Bool
}

func (d *warningPermissionDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	if !d.allow.Load() {
		return davEntry{}, &quarkAPIError{HTTP: 403, Message: "cloud denies this path"}
	}
	return d.fakeDAVDrive.Upload(ctx, parent, name, f)
}

func TestDAVRuntimeWriteDenialPreservesDownloadsAndRecovers(t *testing.T) {
	srv, base, fs := testDAVService(t, true)
	wantDAV(t, srv.URL, "PUT", "/联通云盘-1/old.txt", "old", 201, nil)
	drive := &warningPermissionDrive{fakeDAVDrive: base}
	fs.mounts[0].drive = drive
	wantDAV(t, srv.URL, "PUT", "/联通云盘-1/old.txt", "replacement", 403, nil)
	warning := fs.mounts[0].writeWarningSnapshot()
	if warning == nil || warning.Kind != "denied" {
		t.Fatal("cloud denial was not surfaced", warning)
	}
	if got := wantDAV(t, srv.URL, "GET", "/联通云盘-1/old.txt", "", 200, nil); string(got) != "old" {
		t.Fatal("original was modified")
	}
	drive.allow.Store(true)
	wantDAV(t, srv.URL, "PUT", "/联通云盘-1/other.txt", "ok", 201, nil)
	if fs.mounts[0].writeWarningSnapshot() == nil {
		t.Fatal("unrelated path cleared warning")
	}
	wantDAV(t, srv.URL, "PUT", "/联通云盘-1/old.txt", "new", 201, nil)
	if fs.mounts[0].writeWarningSnapshot() != nil {
		t.Fatal("successful retry retained stale warning")
	}
	if got := wantDAV(t, srv.URL, "GET", "/联通云盘-1/old.txt", "", 200, nil); string(got) != "new" {
		t.Fatal("write recovery failed")
	}
}

func TestDAVWriteWarningClassificationAndConcurrentStatus(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, errors.New("HTTP 403? not confirmed"), &os.PathError{Op: "open", Path: "local-spool", Err: os.ErrPermission}, &davMutationError{Phase: "move", Partial: true, Err: &quarkAPIError{HTTP: 403, Message: "denied after rename"}}} {
		if cloudWriteDenied(err) {
			t.Fatal("uncertain/local error mislabeled as cloud permission", err)
		}
	}
	m := &davMount{Alias: "test"}
	d := &davManager{mounts: []*davMount{m}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				m.warnWrite("/test/file", "PUT", os.ErrPermission, func(s string) string { return s })
				_ = d.Status()
				m.clearWriteWarning("/test/file", "PUT")
			}
		}()
	}
	wg.Wait()
}

// Local UI acceptance uses the real manager, HTTP handlers and embedded UI,
// with cloud requests confined to the fixture above. Never enabled by CI.
func TestDAVWriteWarningUIFixture(t *testing.T) {
	if os.Getenv("CLOUDPROBE_WRITE_WARNING_UI") != "localhost-fixture-only" {
		t.Skip("interactive local fixture")
	}
	d, in, _, _ := warningStartFixture(t, "denied")
	g := d.app
	g.nonce = "local-write-warning-fixture"
	g.accountOrder = []string{"a"}
	a := g.accounts["a"]
	a.Name = "模拟云盘（写入被拒绝）"
	g.accounts["a"] = a
	g.user = "本机隔离测试"
	g.stage = "模拟云盘：可下载，写入被拒绝"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.shutdown = cancel
	srv := httptest.NewServer(g)
	defer srv.Close()
	g.host = strings.TrimPrefix(srv.URL, "http://")
	if _, err := d.Start(ctx, in); err != nil {
		t.Fatal(err)
	}
	fmt.Println("WRITE_WARNING_UI=" + srv.URL)
	<-ctx.Done()
}

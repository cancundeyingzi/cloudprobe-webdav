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

	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"golang.org/x/net/websocket"
)

func TestIndependentAccountsMergeBothLoginOrdersAndRestore(t *testing.T) {
	for _, webFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(webFirst), func(t *testing.T) {
			g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
			bound := guiAccount{Key: "original-mount-key", Provider: "wopan", Root: "0", credentials: map[string]any{"mobile": "+8613800000000", "token": "bound-token"}}
			web := guiAccount{Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "13800000000", "wopan_web_token": "web-token", "wopan_web_managed": "1"}}
			first, second := bound, web
			if webFirst {
				first, second = web, bound
			}
			if err := g.acceptCloudAccount(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			key := g.accountOrder[0]
			identity := davIdentity(g.accounts[key])
			if err := g.acceptCloudAccount(context.Background(), second); err != nil {
				t.Fatal(err)
			}
			if len(g.accounts) != 1 || g.accountOrder[0] != key || davIdentity(g.accounts[key]) != identity {
				t.Fatal("same account duplicated or mount identity changed")
			}
			a := g.accounts[key]
			if !a.WebBound || len(a.Channels) != 2 || valueString(a.credentials["token"]) != "bound-token" {
				t.Fatal("channel lost")
			}
			// An old legacy web token must not replace a newer independently saved one.
			g.mu.Lock()
			if err := g.persistDAVCredentialsLocked(identity, "old-legacy-token"); err != nil {
				t.Fatal(err)
			}
			g.mu.Unlock()
			restored := &guiApp{data: g.data, accounts: map[string]guiAccount{}}
			restored.restoreCloudAccounts()
			if len(restored.accounts) != 1 || valueString(restored.accounts[key].credentials["wopan_web_token"]) != "web-token" {
				t.Fatal("restart lost newer channel")
			}
			cl, err := restored.accountClient(key)
			if err != nil || cl.bridge != nil {
				t.Fatal("independent client requires NetEase", err)
			}
			public, _ := json.Marshal(restored.publicStatus())
			if bytes.Contains(public, []byte("web-token")) || bytes.Contains(public, []byte("bound-token")) {
				t.Fatal("public status leaked credentials")
			}
		})
	}
}

func TestIndependentAccountStoreDamageAndCancellationPreserveState(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	file := filepath.Join(g.data, "cloud-accounts.bin")
	os.WriteFile(file, []byte("broken"), 0600)
	g.restoreCloudAccounts()
	a := guiAccount{Provider: "mobile", credentials: map[string]any{"user_id": "13800000000", "mobile_web_token": "new"}}
	if err := g.acceptCloudAccount(context.Background(), a); err == nil {
		t.Fatal("damaged vault replaced")
	}
	raw, _ := os.ReadFile(file)
	if string(raw) != "broken" || len(g.accounts) != 0 {
		t.Fatal("damaged vault or account state changed")
	}
	g = &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.acceptCloudAccount(ctx, a); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled login persisted", err)
	}
	if len(g.accounts) != 0 {
		t.Fatal("canceled account published")
	}
	a.credentials["user_id"] = "138****0000"
	b := cloneGUIAccount(a)
	delete(a.credentials, "user_id")
	delete(b.credentials, "user_id")
	a.credentials["username"] = "同名用户"
	b.credentials["username"] = "同名用户"
	if sameCloudAccount(a, b) {
		t.Fatal("display names accepted as cross-channel identity")
	}
}

func TestIndependentWopanStartsWithoutNetEaseAndReads(t *testing.T) {
	d, in, _, _ := warningStartFixture(t, "healthy")
	g := d.app
	g.logged = false
	g.bridge = nil
	g.accounts["a"] = guiAccount{Key: "a", Provider: "wopan", Root: "0", WebBound: true, credentials: map[string]any{"user_id": "fixture-user", "wopan_web_token": "only-independent-web-token"}}
	d.wopanClientSetup = func(drive *wopanDAVDrive) {
		if drive.client.bridge != nil || !drive.usingWeb {
			t.Fatal("wrong independent channel")
		}
		drive.w.Phone = "fixture-user"
		drive.w.ZoneURL = "https://example.test"
		drive.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
		drive.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":{"files":[],"systemDirs":[]}}}`)), Request: r}, nil
		})})
	}
	status, err := d.Start(context.Background(), in)
	if err != nil || status["running"] != true {
		t.Fatal("independent WebDAV start failed", err)
	}
	wantDAV(t, fmt.Sprintf("http://127.0.0.1:%d", in.Port), "PROPFIND", "/盘/", "", 207, map[string]string{"Depth": "1"})
}

func TestPhoneWindowIgnoresLauncherExitWhileBrowserConnected(t *testing.T) {
	server := httptest.NewServer(websocket.Server{Handshake: func(*websocket.Config, *http.Request) error { return nil }, Handler: func(conn *websocket.Conn) {
		defer conn.Close()
		for {
			var req map[string]any
			if websocket.JSON.Receive(conn, &req) != nil {
				return
			}
			if req["method"] == "Browser.close" {
				return
			}
			websocket.JSON.Send(conn, map[string]any{"id": req["id"], "result": map[string]any{"cookies": []map[string]any{{"name": "__pus", "value": "official-token", "domain": ".quark.cn", "expires": -1}}}})
		}
	}})
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := websocket.Dial("ws"+strings.TrimPrefix(server.URL, "http"), "", "http://127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	exited := make(chan error)
	close(exited)
	b := &quarkPhoneBrowser{rpc: newQuarkBrowserRPC(ctx, conn), exited: exited}
	defer b.rpc.Close()
	snapshot, err := b.Poll(ctx)
	if err != nil || snapshot.Cookies["__pus"] != "official-token" {
		t.Fatal("launcher exit mistaken for window closure", err)
	}
}

func TestMobileWebOperationsVerifyIdentityAndCompletion(t *testing.T) {
	var item map[string]any
	var uploaded []byte
	complete := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/part" {
			uploaded, _ = io.ReadAll(r.Body)
			w.WriteHeader(200)
			return
		}
		if r.Header.Get("Authorization") != "Basic fixture-token" || r.Header.Get("Mcloud-Sign") == "" {
			t.Error("missing website authorization or signature")
		}
		var in map[string]any
		json.NewDecoder(r.Body).Decode(&in)
		data := map[string]any{}
		switch r.URL.Path {
		case "/file/list":
			items := []any{}
			if item != nil {
				items = append(items, item)
			}
			data["items"] = items
			data["nextPageCursor"] = ""
		case "/file/create":
			item = map[string]any{"fileId": "stable-id", "name": in["name"], "type": in["type"], "size": in["size"]}
			data = map[string]any{"fileId": "stable-id", "fileName": in["name"], "uploadId": "session", "partInfos": []any{map[string]any{"partNumber": 1, "uploadUrl": "http://" + r.Host + "/part"}}}
		case "/file/complete":
			complete = true
		case "/file/update":
			if in["fileId"] != "stable-id" {
				t.Error("wrong ID")
			}
			item["name"] = in["name"]
		case "/file/batchMove":
		case "/recyclebin/batchTrash":
			item = nil
		default:
			t.Error("unexpected method", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "code": "0", "data": data})
	}))
	defer server.Close()
	d := &mobileWebDrive{token: "fixture-token", host: server.URL, http: server.Client()}
	f, err := os.CreateTemp(t.TempDir(), "body")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.WriteString("independent-content")
	e, err := d.Upload(context.Background(), "/", "file.txt", f)
	if err != nil || !complete || string(uploaded) != "independent-content" || e.ID != "stable-id" {
		t.Fatal("upload not confirmed", err)
	}
	if err = d.Move(context.Background(), e, "/", "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	e.Name = "renamed.txt"
	if err = d.Remove(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	entries, err := d.List(context.Background(), "/")
	if err != nil || len(entries) != 0 {
		t.Fatal("remove not reflected", err)
	}
}

type independentFailDrive struct {
	davDrive
	err   error
	calls int
}

func (d *independentFailDrive) Mkdir(context.Context, string, string) (davEntry, error) {
	d.calls++
	return davEntry{}, d.err
}
func TestCloudChannelsRetryOnlyDefiniteRejection(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			backup := newFakeDAV(t)
			primary := &independentFailDrive{davDrive: newFakeDAV(t), err: &apiHTTPError{Code: 403, Err: errors.New("denied")}}
			if uncertain {
				primary.err = &davMutationError{Phase: "create", Err: errors.New("response lost")}
			}
			d := &cloudChannelDrive{channels: []cloudChannel{{"web", "/", primary}, {"bound", "root", backup}}, nodes: map[string]cloudChannelNode{}}
			_, err := d.Mkdir(context.Background(), "/", "folder")
			entries, _ := backup.List(context.Background(), "root")
			if uncertain {
				if err == nil || len(entries) != 0 {
					t.Fatal("uncertain write replayed")
				}
			} else if err != nil || len(entries) != 1 {
				t.Fatal("known rejection did not use backup", err)
			}
		})
	}
}

func TestWopanChannelRefreshAppliesToExistingDrive(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	a := guiAccount{Key: "a", Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": "13800000000", "wopan_web_token": "old-token"}}
	g.accounts[a.Key] = a
	cl, err := g.cloudAccountClient(a, "", MobileConfig{}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	d := g.newWopanAccountDrive(cl, a, "0", nil)
	updated := cloneGUIAccount(a)
	updated.credentials["wopan_web_token"] = "new-token"
	g.accounts[a.Key] = updated
	d.directMu.Lock()
	changed := d.syncAccountCredential()
	token, _ := d.w.GetToken()
	d.directMu.Unlock()
	if !changed || token != "new-token" || !d.credentialNeedsProbe {
		t.Fatal("new authorization unavailable to mounted drive")
	}
}

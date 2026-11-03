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
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCorruptDAVConfigBlocksStartUntilExplicitRecovery(t *testing.T) {
	for _, bad := range []string{`{broken`, `null`, `[]`, `{"port":"bad"}`} {
		t.Run(bad, func(t *testing.T) {
			d, in, _, calls := warningStartFixture(t, "healthy")
			file := filepath.Join(d.app.data, "webdav.json")
			if err := os.WriteFile(file, []byte(bad), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := d.Start(context.Background(), in); err == nil {
				t.Fatal("corrupt configuration accepted")
			}
			got, _ := os.ReadFile(file)
			if string(got) != bad || calls.Load() != 0 || d.Status()["config_error"] == "" {
				t.Fatal("original file/error not preserved")
			}
			backup, err := d.recoverConfig(context.Background(), true)
			if err != nil || backup == "" {
				t.Fatal("recovery failed", err)
			}
			got, _ = os.ReadFile(filepath.Join(d.app.data, backup))
			if string(got) != bad {
				t.Fatal("backup changed")
			}
			if _, err = d.Start(context.Background(), in); err != nil {
				t.Fatal("recovered start failed", err)
			}
		})
	}
}

func TestDAVConfigReloadAndUnreadablePathPreserveSource(t *testing.T) {
	g := &guiApp{data: t.TempDir()}
	file := filepath.Join(g.data, "webdav.json")
	os.Mkdir(file, 0700)
	d := newDAVManager(g)
	if d.Status()["config_error"] == "" {
		t.Fatal("directory read error hidden")
	}
	if _, err := d.recoverConfig(context.Background(), true); err == nil {
		t.Fatal("non-file configuration reset")
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(file, []byte(`{"username":"restored","port":50146}`), 0600)
	if _, err := d.recoverConfig(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if d.config.Username != "restored" || d.Status()["config_error"] != "" {
		t.Fatal("reload did not restore valid settings")
	}
}

func TestQuarkAmbiguousMutationsAreNotReplayed(t *testing.T) {
	for _, op := range []string{"mkdir", "rename", "delete"} {
		t.Run(op, func(t *testing.T) {
			f, d := newQuarkFixture(t)
			ctx := context.Background()
			var entry davEntry
			if op != "mkdir" {
				f.add("0", "old.txt", []byte("old"), false)
				items, err := d.List(ctx, "0")
				if err != nil {
					t.Fatal(err)
				}
				entry = items[0]
			}
			base := d.client.metadata.Transport
			var writes atomic.Int64
			d.client.metadata.Transport = protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
				res, err := base.RoundTrip(r)
				if err != nil {
					return nil, err
				}
				if r.Method == "POST" {
					writes.Add(1)
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					res.StatusCode = 404
					res.Body = io.NopCloser(strings.NewReader("gateway lost the upstream response"))
					res.ContentLength = -1
					res.Header.Set("Content-Type", "text/plain")
				}
				return res, nil
			})
			var err error
			switch op {
			case "mkdir":
				_, err = d.Mkdir(ctx, "0", "new")
			case "rename":
				err = d.Move(ctx, entry, "", "new.txt")
			case "delete":
				err = d.Remove(ctx, entry)
			}
			if writes.Load() != 1 {
				t.Fatal("uncertain mutation replayed", writes.Load())
			}
			if op == "rename" && err != nil {
				t.Fatal("verified rename not recovered", err)
			}
			if op != "rename" && err == nil {
				t.Fatal("unproven create/delete reported success")
			}
		})
	}
	for _, code := range []int{401, 403, 404, 405, 501} {
		if quarkMutationRejected(&quarkAPIError{HTTP: code, Rejected: true}) {
			t.Fatal("ambiguous status permits replay", code)
		}
	}
}

func TestGUIBrowseSnapshotsBindDirectoryIdentityAndVersion(t *testing.T) {
	g := &guiApp{accounts: map[string]guiAccount{"a": {Provider: "wopan", credentials: map[string]any{"user_id": "first"}}}}
	f := newFakeDAV(t)
	original := davEntry{Item: Item{ID: "id", FID: "version1", Name: "file.txt", Size: 3}, Parent: "A"}
	f.entries["id"] = original
	f.parents["id"] = "A"
	f.contents["id"] = []byte("old")
	native := []davEntry{original}
	one, err := g.storeBrowseSnapshot("a", "A", []Item{original.Item}, native)
	if err != nil {
		t.Fatal(err)
	}
	native[0].Name = "mutated"
	_, err = g.storeBrowseSnapshot("a", "B", []Item{{ID: "other", Name: "other"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := g.snapshotEntry("a", "A", one, "id")
	if err != nil || selected.Name != "file.txt" {
		t.Fatal("snapshot was overwritten", err)
	}
	if _, err = g.snapshotEntry("a", "B", one, "id"); err == nil {
		t.Fatal("cross-directory snapshot accepted")
	}
	if _, err = verifyGUIEntry(context.Background(), f, "A", selected); err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.FID = "version2"
	f.entries["id"] = changed
	if _, err = verifyGUIEntry(context.Background(), f, "A", selected); err == nil {
		t.Fatal("changed version accepted")
	}
	g.accounts["a"] = guiAccount{Provider: "wopan", credentials: map[string]any{"user_id": "second"}}
	if _, err = g.snapshotEntry("a", "A", one, "id"); err == nil {
		t.Fatal("replaced account reused old snapshot")
	}
	for i := 0; i < 40; i++ {
		g.storeBrowseSnapshot("a", "A", []Item{{ID: fmt.Sprint(i)}}, nil)
	}
	if len(g.browseSnapshots) > 32 {
		t.Fatal("snapshot count unbounded")
	}
}

func TestGUIDownloadRejectsWrongBodyAndWeakVersionNeedsConsent(t *testing.T) {
	f := newFakeDAV(t)
	entry := davEntry{Item: Item{ID: "id", Name: "file.txt", Size: 3}, Parent: "0"}
	f.entries["id"] = entry
	f.parents["id"] = "0"
	f.contents["id"] = []byte("new")
	_, err := verifyGUIEntry(context.Background(), f, "0", entry)
	var weak *guiVersionUnavailable
	if !errors.As(err, &weak) {
		t.Fatal("weak version silently accepted", err)
	}
	if _, err = verifyGUIEntry(context.Background(), f, "0", entry, true); err != nil {
		t.Fatal("explicit current-content download rejected", err)
	}
	entry.ContentVersion = "sha1:" + strings.Repeat("0", 40) + ":3"
	cl := &Client{folder: t.TempDir(), cfg: Config{DownloadMaxBytes: 100}}
	if _, err = guiFetchRedundant(context.Background(), f, nil, cl, entry, "file.txt"); err == nil {
		t.Fatal("body hash mismatch accepted")
	}
	if _, err = os.Stat(filepath.Join(cl.folder, "file.txt")); !os.IsNotExist(err) {
		t.Fatal("invalid body left downloadable", err)
	}
}

func TestOpenListConcurrentProcessesPreserveAllKeys(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var cmds []*exec.Cmd
	var outputs []*bytes.Buffer
	for _, prefix := range []string{"a", "b"} {
		c := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOpenListTransactionChild$")
		c.Env = append(os.Environ(), "CLOUDPROBE_OPENLIST_TEST_DIR="+dir, "CLOUDPROBE_OPENLIST_TEST_PREFIX="+prefix)
		b := &bytes.Buffer{}
		c.Stdout = b
		c.Stderr = b
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, c)
		outputs = append(outputs, b)
	}
	for i, c := range cmds {
		if err := c.Wait(); err != nil {
			t.Fatalf("child %d: %v %s", i, err, outputs[i].String())
		}
	}
	g := &guiApp{data: dir}
	for _, prefix := range []string{"a", "b"} {
		for i := 0; i < 12; i++ {
			key := fmt.Sprintf("%s-%d", prefix, i)
			c, err := g.openListConfig(key, nil, "identity")
			if err != nil || c == nil || c.Token != key {
				t.Fatal("concurrent save lost credentials", key, err)
			}
		}
	}
	first, _ := g.openListConfig("a-0", nil, "identity")
	second, _ := g.openListConfig("a-0", nil, "identity")
	first.Token = "updated"
	if err := g.saveOpenList("a-0", *first); err != nil {
		t.Fatal(err)
	}
	if err := g.saveOpenList("a-0", *second); err == nil {
		t.Fatal("stale same-key update accepted")
	}
}

func TestOpenListTransactionChild(t *testing.T) {
	dir := os.Getenv("CLOUDPROBE_OPENLIST_TEST_DIR")
	prefix := os.Getenv("CLOUDPROBE_OPENLIST_TEST_PREFIX")
	if dir == "" || prefix == "" {
		return
	}
	g := &guiApp{data: dir}
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if err := g.saveOpenList(key, openListSaved{openListInput: openListInput{Enabled: true, Token: key}, Identity: "identity"}); err != nil {
			t.Fatal(err)
		}
	}
}

func integrityGUIFixture(t *testing.T) (*guiApp, *quarkFixture, string) {
	f, d := newQuarkFixture(t)
	key := quarkAccountKey("account")
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "integrity-fixture", downloads: map[string]guiDownload{}, accounts: map[string]guiAccount{key: {Key: key, Name: "模拟夸克", Provider: "quark", Root: "0", credentials: map[string]any{"user_id": "account"}}}, accountOrder: []string{key}}
	g.quark = &quarkManager{app: g, clients: map[string]*quarkClient{key: d.client}}
	return g, f, key
}

func integrityGUIRequest(t *testing.T, g *guiApp, endpoint string, input any) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(input)
	r := httptest.NewRequest("POST", "http://"+g.host+"/api/"+endpoint, bytes.NewReader(b))
	r.Header.Set("X-CloudProbe-UI", g.nonce)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

func TestGUIFetchUsesOriginalSnapshotAcrossDirectoryQueries(t *testing.T) {
	g, f, key := integrityGUIFixture(t)
	file := f.add("A", "a.txt", []byte("old"), false)
	f.add("B", "b.txt", []byte("other"), false)
	r := integrityGUIRequest(t, g, "list", map[string]any{"account": key, "directory": "A"})
	if r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	var listing struct {
		Snapshot string `json:"snapshot"`
	}
	json.Unmarshal(r.Body.Bytes(), &listing)
	if listing.Snapshot == "" {
		t.Fatal("list omitted snapshot")
	}
	if r = integrityGUIRequest(t, g, "list", map[string]any{"account": key, "directory": "B"}); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	input := map[string]any{"account": key, "directory": "A", "snapshot": listing.Snapshot, "id": file.ID, "name": "chosen.txt"}
	if r = integrityGUIRequest(t, g, "fetch", input); r.Code != 200 {
		t.Fatal("old directory snapshot lost", r.Body.String())
	}
	f.mu.Lock()
	changed := f.files[file.ID]
	changed.Content = []byte("new")
	f.files[file.ID] = changed
	f.mu.Unlock()
	if r = integrityGUIRequest(t, g, "fetch", input); r.Code < 400 {
		t.Fatal("updated same-ID content accepted")
	}
}

func TestConfigResetAPIRequiresExplicitConfirmation(t *testing.T) {
	g, _, _ := integrityGUIFixture(t)
	file := filepath.Join(g.data, "webdav.json")
	os.WriteFile(file, []byte("{bad"), 0600)
	g.dav = newDAVManager(g)
	if r := integrityGUIRequest(t, g, "webdav/config/reset", map[string]any{}); r.Code < 400 {
		t.Fatal("unconfirmed reset accepted")
	}
	if b, _ := os.ReadFile(file); string(b) != "{bad" {
		t.Fatal("unconfirmed reset changed file")
	}
	if r := integrityGUIRequest(t, g, "webdav/config/reset", map[string]any{"confirm": true}); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
}

func TestIntegrityUIFixture(t *testing.T) {
	if os.Getenv("CLOUDPROBE_INTEGRITY_UI") != "local-only" {
		t.Skip("local UI fixture")
	}
	g, f, _ := integrityGUIFixture(t)
	g.logged = true
	g.user = "本机模拟账号"
	g.bridge = &OriginalBridge{}
	f.add("0", "snapshot.txt", []byte("snapshot content"), false)
	os.WriteFile(filepath.Join(g.data, "webdav.json"), []byte("{bad"), 0600)
	g.dav = newDAVManager(g)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.shutdown = cancel
	srv := httptest.NewServer(g)
	defer srv.Close()
	g.host = strings.TrimPrefix(srv.URL, "http://")
	fmt.Println("INTEGRITY_UI=" + srv.URL)
	fmt.Println("INTEGRITY_DATA=" + g.data)
	<-ctx.Done()
}

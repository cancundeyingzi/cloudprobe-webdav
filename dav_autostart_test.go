package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func asyncFixture(t *testing.T) (*davManager, davStartInput) {
	t.Helper()
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	d := newDAVManager(g)
	g.dav = d
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	t.Cleanup(d.Stop)
	return d, davStartInput{background: true, Port: port, Username: "tester", Password: "test-password-123", transferOptions: transferOptions{UploadStorageMode: "auto"}}
}
func awaitLoads(t *testing.T, d *davManager, predicate func(map[string]davLoadState) bool) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		s := cloneLoadStates(d.loads)
		d.mu.Unlock()
		if predicate(s) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mount load did not finish", d.Status()["account_loads"])
}
func addAsyncAccount(d *davManager, key string) {
	d.app.mu.Lock()
	defer d.app.mu.Unlock()
	d.app.accounts[key] = guiAccount{Key: key, Provider: "wopan", Root: "0", credentials: map[string]any{"user_id": key}}
}
func TestDAVAsyncEmptyAndIndependentLoads(t *testing.T) {
	d, in := asyncFixture(t)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var fail atomic.Bool
	fail.Store(true)
	fake := newFakeDAV(t)
	d.mountLoader = func(ctx context.Context, s davMountInput, a guiAccount) (*davMount, error) {
		if a.Key == "slow" {
			select {
			case blocked <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
			}
		}
		if a.Key == "bad" && fail.Load() {
			return nil, errors.New("授权过期")
		}
		return &davMount{Alias: s.Alias, Root: s.Root, Provider: a.Provider, Identity: davIdentity(a), drive: fake}, nil
	}
	if _, err := d.Start(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", in.Port)
	body := wantDAV(t, base, "PROPFIND", "/", "", 207, map[string]string{"Depth": "1"})
	if strings.Count(string(body), "<D:response>") > 1 {
		t.Fatal("empty root had children")
	}
	for _, key := range []string{"slow", "good", "bad"} {
		addAsyncAccount(d, key)
	}
	awaitLoads(t, d, func(s map[string]davLoadState) bool {
		return s["good"].State == "mounted" && s["bad"].State == "failed" && s["slow"].State == "loading"
	})
	if len(d.fs.mountSnapshot()) != 1 {
		t.Fatal("slow/bad mount blocked or leaked")
	}
	cfg, err := readDAVConfig(filepath.Join(d.app.data, "webdav.json"))
	if err != nil || len(cfg.Mounts) != 3 {
		t.Fatal("failed/pending config lost", err)
	}
	fail.Store(false)
	if err := d.retryMount("bad"); err != nil {
		t.Fatal(err)
	}
	awaitLoads(t, d, func(s map[string]davLoadState) bool { return s["bad"].State == "mounted" })
	d.Stop()
	close(release)
	addAsyncAccount(d, "later")
	time.Sleep(1100 * time.Millisecond)
	if d.Status()["running"] != false {
		t.Fatal("manual stop was undone")
	}
}
func TestDAVAutoRestorePreservesSelectionsAndPassword(t *testing.T) {
	d, in := asyncFixture(t)
	addAsyncAccount(d, "excluded")
	in.Password = ""
	if _, err := d.Start(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	password := d.Status()["generated_password"]
	if password == "" {
		t.Fatal("missing generated password")
	}
	d.Stop()
	restored := newDAVManager(d.app)
	d.app.dav = restored
	t.Cleanup(restored.Stop)
	restored.AutoStart(context.Background())
	if restored.Status()["running"] != true || restored.Status()["generated_password"] != password {
		t.Fatal("startup/password did not restore")
	}
	time.Sleep(1100 * time.Millisecond)
	if len(restored.fs.mountSnapshot()) != 0 {
		t.Fatal("excluded account auto-mounted")
	}
}
func TestDAVAsyncRestoresMultipleReadonlyRoots(t *testing.T) {
	d, in := asyncFixture(t)
	addAsyncAccount(d, "same")
	in.Mounts = []davMountInput{{Account: "same", Alias: "one", Root: "0"}, {Account: "same", Alias: "two", Root: "sub"}}
	fake := newFakeDAV(t)
	var calls atomic.Int64
	d.mountLoader = func(ctx context.Context, s davMountInput, a guiAccount) (*davMount, error) {
		calls.Add(1)
		return &davMount{Alias: s.Alias, Root: s.Root, Identity: davIdentity(a), drive: fake}, nil
	}
	if _, err := d.Start(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	awaitLoads(t, d, func(s map[string]davLoadState) bool {
		return s["same"].State == "mounted" && len(d.fs.mountSnapshot()) == 2
	})
	time.Sleep(1100 * time.Millisecond)
	if calls.Load() != 2 {
		t.Fatal("unchanged accounts were reloaded", calls.Load())
	}
	d.app.mu.Lock()
	a := d.app.accounts["same"]
	a.credentials = map[string]any{"user_id": "same", "token": "renewed"}
	d.app.accounts["same"] = a
	d.app.mu.Unlock()
	awaitLoads(t, d, func(s map[string]davLoadState) bool { return s["same"].State == "mounted" && calls.Load() == 4 })
	d.app.mu.Lock()
	delete(d.app.accounts, "same")
	d.app.mu.Unlock()
	awaitLoads(t, d, func(s map[string]davLoadState) bool { return len(d.fs.mountSnapshot()) == 0 })
}

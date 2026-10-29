package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLifecycleWindowsResetRetriesLockedSpool(t *testing.T) {
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	key := strings.Repeat("d", 64)
	c := &uploadCheckpoint{file: filepath.Join(g.data, "webdav-spool", "resume", key+".bin"), data: uploadCheckpointData{Version: 1, Key: key, Parts: map[int64]string{}, Spool: "upload-owned"}}
	if err := c.saveLocked(); err != nil {
		t.Fatal(err)
	}
	body := filepath.Join(g.data, "webdav-spool", c.data.Spool)
	if err := os.WriteFile(body, []byte("body"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(body)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if w := lifecycleRequest(g, "reset", key); w.Code < 400 {
		t.Fatal("locked cleanup reported success")
	}
	if _, err = os.Stat(c.file); err != nil {
		t.Fatal("failed cleanup lost association", err)
	}
	f.Close()
	if w := lifecycleRequest(g, "reset", key); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = os.Stat(body); !os.IsNotExist(err) {
		t.Fatal("body survived retry", err)
	}
	if _, err = os.Stat(c.file); !os.IsNotExist(err) {
		t.Fatal("record survived retry", err)
	}
}

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func spoolRetryFixture(t *testing.T) (*davFile, *guiApp, *uploadCheckpoint, string) {
	t.Helper()
	g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "fixture"}
	fs := &davFS{spool: filepath.Join(g.data, "webdav-spool")}
	if err := os.MkdirAll(fs.spool, 0700); err != nil {
		t.Fatal(err)
	}
	makeBody := func() *os.File {
		f, err := os.CreateTemp(fs.spool, "upload-*")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(bytes.Repeat([]byte("spool-retry"), 1024)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}
	old := makeBody()
	d := &davFile{fs: fs, ctx: context.Background(), tmp: old, name: "/mount/file", node: davNode{mount: &davMount{Identity: "account", Provider: "quark", drive: &quarkDAVDrive{}}, parent: "0", entry: davEntry{Item: Item{Name: "file"}}}}
	c, release, err := d.openCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	release()
	old.Close()
	d.tmp = makeBody()
	return d, g, c, old.Name()
}

func assertSpoolRetryBlocked(t *testing.T, d *davFile, c *uploadCheckpoint) error {
	t.Helper()
	before, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatal(err)
	}
	_, release, err := d.openCheckpoint()
	if release != nil {
		release()
	}
	if err == nil {
		t.Error("recovery continued after failed old-body cleanup")
	}
	after, readErr := os.ReadFile(c.file)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(before, after) {
		t.Error("failed cleanup changed persisted checkpoint")
	}
	loaded, readErr := readUploadCheckpoint(c.file)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if loaded.data.Spool != c.data.Spool {
		t.Errorf("old body association replaced: %q -> %q", c.data.Spool, loaded.data.Spool)
	}
	return err
}

func TestSpoolRetryMissingOldBodyAndSameBody(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "same-file"}[same], func(t *testing.T) {
			d, _, c, old := spoolRetryFixture(t)
			if same {
				f, err := os.OpenFile(old, os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				d.tmp = f
			} else if err := os.Remove(old); err != nil {
				t.Fatal(err)
			}
			next, release, err := d.openCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			release()
			if next.data.Spool != filepath.Base(d.tmp.Name()) {
				t.Fatal("new body association not saved")
			}
			if same {
				if _, err = os.Stat(old); err != nil {
					t.Fatal("active current body removed", err)
				}
			}
			if next.file != c.file {
				t.Fatal("retry lost task identity")
			}
		})
	}
}

func TestSpoolRetryRejectsInvalidOrNonregularAssociation(t *testing.T) {
	for _, name := range []string{"../upload-outside", "upload-directory"} {
		t.Run(name, func(t *testing.T) {
			d, _, c, _ := spoolRetryFixture(t)
			loaded, err := readUploadCheckpoint(c.file)
			if err != nil {
				t.Fatal(err)
			}
			loaded.data.Spool = name
			if err = loaded.saveLocked(); err != nil {
				t.Fatal(err)
			}
			if name == "upload-directory" {
				if err = os.Mkdir(filepath.Join(d.fs.spool, name), 0700); err != nil {
					t.Fatal(err)
				}
			}
			assertSpoolRetryBlocked(t, d, loaded)
		})
	}
}

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGUIResultRetentionPreservesRecentAndUnmanagedFiles(t *testing.T) {
	data := t.TempDir()
	now := time.Now()
	root := filepath.Join(data, "results")
	for index := 0; index < 53; index++ {
		name := "run-" + now.Add(-time.Duration(index)*time.Minute).Format("20060102-150405") + fmt.Sprintf("-%d", index)
		if err := os.MkdirAll(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	old := "run-" + now.Add(-8*24*time.Hour).Format("20060102-150405") + "-100"
	for _, name := range []string{old, "personal-notes"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneGUIResults(data, now); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 51 {
		t.Fatalf("expected 50 recent results and personal directory: %d %v", len(entries), err)
	}
	if _, err := os.Stat(filepath.Join(root, "personal-notes")); err != nil {
		t.Fatal("unmanaged directory was removed", err)
	}
	if _, err := os.Stat(filepath.Join(root, old)); !os.IsNotExist(err) {
		t.Fatal("expired result was retained", err)
	}
}

func TestBrowserRetentionPreservesActiveProfileAndCleansAbandonedOne(t *testing.T) {
	data := t.TempDir()
	profile, cleanup, err := newBrowserProfile(data, "quark")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	root := filepath.Join(data, "official-quark-browser")
	old := filepath.Join(root, "phone-0123456789abcdef")
	personal := filepath.Join(root, "personal")
	for _, name := range []string{old, personal} {
		if err := os.MkdirAll(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	_ = pruneBrowserProfiles(data)
	if _, err := os.Stat(profile); err != nil {
		t.Fatal("active profile was removed", err)
	}
	if _, err := os.Stat(personal); err != nil {
		t.Fatal("unmanaged profile was removed", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("abandoned profile was not removed", err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(profile); !os.IsNotExist(err) {
		t.Fatal("closed profile was not removed", err)
	}
}

func TestDataRetentionRefusesLinkedRoot(t *testing.T) {
	data, outside := t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(data, "results")); err != nil {
		t.Skip("symlink creation is unavailable")
	}
	if err := pruneGUIResults(data, time.Now()); err == nil {
		t.Fatal("linked data root was accepted for cleanup")
	}
}

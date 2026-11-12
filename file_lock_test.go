package main

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestKeyLeaseSeparatesTasksAndRemovesReleasedFiles(t *testing.T) {
	dir := t.TempDir()
	firstKey := hex.EncodeToString(make([]byte, 32))
	secondKey := hex.EncodeToString(append([]byte{1}, make([]byte, 31)...))
	first, err := acquireKeyLease(context.Background(), dir, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := acquireKeyLease(context.Background(), dir, secondKey)
	if err != nil {
		first.Close()
		t.Fatal("same-prefix independent task was blocked", err)
	}
	if _, err = acquireKeyLease(context.Background(), dir, firstKey); err == nil {
		t.Fatal("same task was not exclusive")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(lockKeyPath(dir, firstKey)); !os.IsNotExist(err) {
		t.Fatal("released lock file remains", err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupLegacyLockStripes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lease-ab")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := cleanupLegacyLockStripes(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("legacy stripe remains", err)
	}
}

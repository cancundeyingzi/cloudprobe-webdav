package main

import (
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestKeyLeaseSeparatesTasksAndRemovesReleasedFiles(t *testing.T) {
	dir := t.TempDir()
	firstKey := hex.EncodeToString(make([]byte, 32))
	secondBytes := make([]byte, 32)
	secondBytes[31] = 1
	secondKey := hex.EncodeToString(secondBytes)
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

func TestKeyLeaseAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	key := hex.EncodeToString(make([]byte, 32))
	lease, err := acquireKeyLease(context.Background(), dir, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lease.Close() })
	runChild := func(expected string) {
		command := exec.Command(os.Args[0], "-test.run=^TestKeyLeaseProcessHelper$")
		command.Env = append(os.Environ(), "CLOUDPROBE_KEY_LEASE_DIR="+dir, "CLOUDPROBE_KEY_LEASE_EXPECT="+expected)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("child lease check: %v %s", err, output)
		}
	}
	runChild("busy")
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	runChild("available")
	if err := cleanupLegacyLockStripes(filepath.Join(dir, "not-created")); err != nil {
		t.Fatal("clean startup failed before uploads exist", err)
	}
}

func TestKeyLeaseProcessHelper(t *testing.T) {
	dir := os.Getenv("CLOUDPROBE_KEY_LEASE_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	lease, err := acquireKeyLease(context.Background(), dir, hex.EncodeToString(make([]byte, 32)))
	if lease != nil {
		defer lease.Close()
	}
	if (err != nil) != (os.Getenv("CLOUDPROBE_KEY_LEASE_EXPECT") == "busy") {
		t.Fatal("unexpected cross-process lock result", err)
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

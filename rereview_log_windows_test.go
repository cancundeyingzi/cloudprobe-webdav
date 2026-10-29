package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReReviewWindowsRetentionRecoversAfterFileReleased(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	defer l.close()
	l.maxFileBytes, l.maxTotalBytes, l.maxFiles = 512, 1024, 2
	l.add(runLogEntry{Operation: "held-segment"})
	file, err := os.Open(l.path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for range 30 {
		l.add(runLogEntry{Operation: "fill", Detail: strings.Repeat("x", 200)})
	}
	status := l.status()
	if status["disk_paused"] != true || status["retention_error"] == "" {
		t.Errorf("retention failure is not reported as recoverable pause: %v", status)
	}
	if dropped, ok := status["dropped_entries"].(uint64); !ok || dropped == 0 {
		t.Errorf("paused log did not count records missing from disk: %v", status)
	}
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > l.maxFiles {
		t.Fatalf("paused rotation exceeded segment count: %d", len(entries))
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		l.add(runLogEntry{Operation: "recovered-disk-record"})
		data, readErr := os.ReadFile(l.path)
		if readErr == nil && bytes.Contains(data, []byte("recovered-disk-record")) {
			status = l.status()
			if status["error"] != "" || status["disk_paused"] != false || status["retention_error"] != "" {
				t.Fatalf("recovered logger still reports failure: %v", status)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("disk logging did not recover after Windows handle closed")
}

func TestReReviewWindowsStartupRetentionAndNonblockingWarning(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("full-%t", full), func(t *testing.T) {
			profile := t.TempDir()
			dir := filepath.Join(profile, "run-logs")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			count := 1
			if full {
				count = 8
			}
			var held []*os.File
			defer func() {
				for _, f := range held {
					_ = f.Close()
				}
			}()
			for i := range count {
				name := filepath.Join(dir, fmt.Sprintf("20200101-000000-00000000-%04d.jsonl", i))
				if err := os.WriteFile(name, []byte("old\n"), 0600); err != nil {
					t.Fatal(err)
				}
				old := time.Now().Add(-8 * 24 * time.Hour)
				if err := os.Chtimes(name, old, old); err != nil {
					t.Fatal(err)
				}
				f, err := os.Open(name)
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, f)
			}
			l := newRuntimeLog(profile, nil)
			defer l.close()
			l.add(runLogEntry{Operation: "startup-record"})
			status := l.status()
			if status["error"] != "" || status["disk_paused"] != full || status["retention_error"] == "" {
				t.Fatalf("wrong startup retention state: %v", status)
			}
			if !full {
				data, err := os.ReadFile(l.path)
				if err != nil || !bytes.Contains(data, []byte("startup-record")) {
					t.Fatalf("cleanup warning blocked writable log: %s %v", data, err)
				}
			}
			for _, f := range held {
				_ = f.Close()
			}
			deadline := time.Now().Add(4 * time.Second)
			for time.Now().Before(deadline) {
				l.add(runLogEntry{Operation: "startup-recovered"})
				status = l.status()
				if status["disk_paused"] == false && status["retention_error"] == "" {
					data, err := os.ReadFile(l.path)
					if err != nil || !bytes.Contains(data, []byte("startup-recovered")) {
						t.Fatalf("recovered startup did not write: %v", err)
					}
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("startup retention did not recover")
		})
	}
}

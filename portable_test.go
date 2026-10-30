package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPortableDataStaysWithExecutable(t *testing.T) {
	base := t.TempDir()
	t.Chdir(t.TempDir())
	data, err := preparePortableData(base, "data")
	if err != nil {
		t.Fatal(err)
	}
	canonicalBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	if data != filepath.Join(canonicalBase, "data") {
		t.Fatalf("unexpected data path %s", data)
	}
	for _, p := range []string{"..", filepath.Join("..", "escape"), t.TempDir(), base} {
		if _, err = preparePortableData(base, p); err == nil {
			t.Fatalf("accepted external/root path %s", p)
		}
	}
	files, err := os.ReadDir(data)
	if err != nil || len(files) != 0 {
		t.Fatalf("write probe left residue: %v %v", files, err)
	}
}

func TestPortableDataRejectsLinkEscapeAndUnwritablePath(t *testing.T) {
	base := t.TempDir()
	if err := os.WriteFile(filepath.Join(base, "blocked"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := preparePortableData(base, "blocked/data"); err == nil {
		t.Fatal("file ancestor accepted")
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(base, "link")); err != nil {
		t.Skip("symlink unavailable", err)
	}
	if _, err := preparePortableData(base, "link/data"); err == nil {
		t.Fatal("external link accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "data")); !os.IsNotExist(err) {
		t.Fatal("wrote outside software directory", err)
	}
}

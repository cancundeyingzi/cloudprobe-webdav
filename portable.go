package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// Relative paths are relative to the executable, never the caller's cwd.
// Check existing ancestors too, so a junction/symlink cannot redirect data.
func portablePath(base, requested string) (string, error) {
	base, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(requested) {
		requested = filepath.Join(base, requested)
	}
	requested = filepath.Clean(requested)
	inside := func(p string) bool {
		rel, e := filepath.Rel(base, p)
		return e == nil && rel != "." && filepath.IsLocal(rel)
	}
	if !inside(requested) {
		return "", fmt.Errorf("资料必须保存在软件目录内：%s", base)
	}
	for p := requested; p != base; p = filepath.Dir(p) {
		_, e := os.Lstat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return "", e
		}
		resolved, e := filepath.EvalSymlinks(p)
		if e != nil {
			return "", e
		}
		if resolved != base && !inside(resolved) {
			return "", fmt.Errorf("资料路径通过链接指向软件目录之外：%s", p)
		}
		break
	}
	return requested, nil
}

func preparePortableData(base, requested string) (string, error) {
	p, err := portablePath(base, requested)
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(p, 0700); err != nil {
		return "", fmt.Errorf("软件资料目录不可写，不会改存其他位置：%w", err)
	}
	f, err := os.CreateTemp(p, ".write-check-*")
	if err != nil {
		return "", fmt.Errorf("软件资料目录不可写，不会改存其他位置：%w", err)
	}
	name := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return "", closeErr
	}
	if removeErr != nil {
		return "", removeErr
	}
	return p, nil
}

func defaultLocalPath(name string) string {
	exe, err := os.Executable()
	if err != nil {
		return filepath.Join("data", name)
	}
	return filepath.Join(filepath.Dir(exe), "data", name)
}

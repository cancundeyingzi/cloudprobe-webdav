package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"time"
)

var managedBrowserName = regexp.MustCompile(`^(login|phone)-[0-9a-f]{16}$`)
var managedResultName = regexp.MustCompile(`^run-\d{8}-\d{6}-\d+$`)

func openManagedDataRoot(data, name string) (*os.Root, error) {
	root, err := os.OpenRoot(data)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("资料目录不是普通目录，未清理：%s", name)
	}
	return root.OpenRoot(name)
}

func managedDataEntries(root *os.Root) ([]os.DirEntry, error) {
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	return dir.ReadDir(-1)
}

func pruneGUIResults(data string, now time.Time) error {
	root, err := openManagedDataRoot(data, "results")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	entries, err := managedDataEntries(root)
	if err != nil {
		return err
	}
	type resultDir struct {
		name    string
		created time.Time
	}
	var results []resultDir
	for _, entry := range entries {
		if !entry.IsDir() || !managedResultName.MatchString(entry.Name()) {
			continue
		}
		created, err := time.ParseInLocation("20060102-150405", entry.Name()[4:19], time.Local)
		if err == nil {
			results = append(results, resultDir{entry.Name(), created})
		}
	}
	slices.SortFunc(results, func(a, b resultDir) int { return b.created.Compare(a.created) })
	var failures []error
	for index, result := range results {
		if index >= 50 || now.Sub(result.created) > 7*24*time.Hour {
			if err := root.RemoveAll(result.name); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func browserRootName(provider string) (string, error) {
	switch provider {
	case "wopan", "quark", "mobile":
		return "official-" + provider + "-browser", nil
	}
	return "", errors.New("不支持的浏览器资料类型")
}

func removeBrowserProfile(data, rootName, name string) error {
	if !managedBrowserName.MatchString(name) {
		return os.ErrInvalid
	}
	root, err := openManagedDataRoot(data, rootName)
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("浏览器资料路径不是普通目录，未清理")
	}
	lease, err := tryLockFile(filepath.Join(data, rootName, name, ".cloudprobe-profile.lock"))
	if err != nil {
		return fmt.Errorf("浏览器资料仍在使用或无法加锁，已保留 %s：%w", name, err)
	}
	_ = lease.Close()
	return root.RemoveAll(name)
}

func pruneBrowserProfiles(data string) error {
	var failures []error
	for _, provider := range []string{"quark", "wopan", "mobile"} {
		name, _ := browserRootName(provider)
		root, err := openManagedDataRoot(data, name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			failures = append(failures, err)
			continue
		}
		entries, err := managedDataEntries(root)
		root.Close()
		if err != nil {
			failures = append(failures, err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() && managedBrowserName.MatchString(entry.Name()) {
				if err := removeBrowserProfile(data, name, entry.Name()); err != nil {
					failures = append(failures, err)
				}
			}
		}
	}
	return errors.Join(failures...)
}

func newBrowserProfile(data, provider string) (string, func() error, error) {
	rootName, err := browserRootName(provider)
	if err != nil {
		return "", nil, err
	}
	name := "login-" + randomKey()[:16]
	profile, err := portablePath(data, filepath.Join(rootName, name))
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(profile, 0700); err != nil {
		return "", nil, err
	}
	lease, err := tryLockFile(filepath.Join(profile, ".cloudprobe-profile.lock"))
	if err != nil {
		return "", nil, err
	}
	var once sync.Once
	cleanup := func() error {
		var cleanupErr error
		once.Do(func() {
			_ = lease.Close()
			cleanupErr = removeBrowserProfile(data, rootName, name)
		})
		return cleanupErr
	}
	return profile, cleanup, nil
}

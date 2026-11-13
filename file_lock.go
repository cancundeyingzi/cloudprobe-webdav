package main

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func lockFileWait(ctx context.Context, path string) (*os.File, error) {
	for attempt := 0; attempt < 50; attempt++ {
		file, err := tryLockFile(path)
		if err == nil {
			return file, nil
		}
		if attempt == 49 {
			return nil, err
		}
		if err = davWait(ctx, lockRetryDelay); err != nil {
			return nil, err
		}
	}
	return nil, os.ErrPermission
}

func lockKeyPath(dir, key string) string {
	return filepath.Join(dir, "lock-"+key)
}

func acquireKeyLease(ctx context.Context, dir, key string) (*keyLease, error) {
	if !validOwnerID(key) {
		return nil, os.ErrInvalid
	}
	catalog, err := lockFileWait(ctx, filepath.Join(dir, "catalog-lease"))
	if err != nil {
		return nil, err
	}
	defer catalog.Close()
	file, err := tryLockFile(lockKeyPath(dir, key))
	if err != nil {
		return nil, err
	}
	if info, statErr := file.Stat(); statErr != nil {
		_ = file.Close()
		return nil, statErr
	} else if !info.Mode().IsRegular() || info.Size() != 0 {
		_ = file.Close()
		return nil, errors.New("任务锁文件不是空文件，已保留")
	}
	return &keyLease{file: file, path: lockKeyPath(dir, key), dir: dir}, nil
}

type keyLease struct {
	file *os.File
	path string
	dir  string
	once sync.Once
}

func (lease *keyLease) Close() error {
	var err error
	lease.once.Do(func() {
		catalog, lockErr := lockFileWait(context.Background(), filepath.Join(lease.dir, "catalog-lease"))
		closeErr := lease.file.Close()
		if lockErr != nil {
			err = lockErr
		} else {
			if removeErr := os.Remove(lease.path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = removeErr
			}
			_ = catalog.Close()
		}
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	})
	return err
}

func cleanupLegacyLockStripes(dir string) error {
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	catalog, err := lockFileWait(context.Background(), filepath.Join(dir, "catalog-lease"))
	if err != nil {
		return err
	}
	defer catalog.Close()
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() {
			continue
		}
		key := strings.TrimPrefix(name, "lease-")
		legacy := strings.HasPrefix(name, "lease-") && len(key) == 2
		stale := strings.HasPrefix(name, "lock-") && validOwnerID(strings.TrimPrefix(name, "lock-"))
		if !legacy && !stale {
			continue
		}
		if legacy {
			if _, err = hex.DecodeString(key); err != nil {
				continue
			}
		}
		path := filepath.Join(dir, name)
		file, lockErr := tryLockFile(path)
		if lockErr != nil {
			continue
		}
		info, statErr := file.Stat()
		_ = file.Close()
		if statErr == nil && info.Size() == 0 {
			if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return removeErr
			}
		}
	}
	return nil
}

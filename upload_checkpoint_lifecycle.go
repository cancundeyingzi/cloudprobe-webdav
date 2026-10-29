package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errCheckpointRetired = errors.New("上传任务句柄已退役，请刷新任务列表")

// Activity identity, retirement, management writes and terminal deletion all
// share checkpoint.mu. The OS lease is released only after retirement.
func (c *uploadCheckpoint) activate(cancel context.CancelFunc) *activeCheckpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.data.Paused {
		return nil
	}
	c.retireActivityLocked()
	a := &activeCheckpoint{checkpoint: c, cancel: cancel}
	c.activity = a
	activeCheckpoints.Store(c.file, a)
	return a
}

func (c *uploadCheckpoint) ensureUsable() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errCheckpointRetired
	}
	if c.data.Paused {
		return &davStatusError{Code: 423, Message: "续传任务已暂停，未继续提交目标"}
	}
	return nil
}
func (c *uploadCheckpoint) retireActivityLocked() {
	if c.activity != nil {
		c.activity.retired = true
		activeCheckpoints.CompareAndDelete(c.file, c.activity)
		c.activity = nil
	}
}
func (a *activeCheckpoint) retire() {
	if a == nil {
		return
	}
	c := a.checkpoint
	c.mu.Lock()
	defer c.mu.Unlock()
	a.retired = true
	if c.activity == a {
		c.retireActivityLocked()
	}
}
func (a *activeCheckpoint) pause() error {
	c := a.checkpoint
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.retired || c.closed || c.activity != a {
		return errCheckpointRetired
	}
	c.data.Paused = true
	err := c.saveLocked()
	a.cancel()
	return err
}
func (c *uploadCheckpoint) releaseLease(lease *os.File) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.retireActivityLocked()
	_ = lease.Close()
}
func (c *uploadCheckpoint) complete() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.retireActivityLocked()
	if err := os.Remove(c.file); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
func (c *uploadCheckpoint) hasProgress() bool {
	return len(c.data.Parts) > 0 || len(c.data.Hashes) > 0 || c.data.Uploaded != nil || c.data.Backup != "" || c.data.Spool != ""
}

// Provisional memory candidates do not consume a persisted-task slot. The
// quota is acquired only after the chosen mode is known to be usable.
func (c *uploadCheckpoint) publish(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errCheckpointRetired
	}
	if !c.provisional {
		return nil
	}
	catalog, err := lockCheckpointCatalog(ctx, filepath.Join(filepath.Dir(c.file), "catalog-lease"))
	if err != nil {
		return err
	}
	defer catalog.Close()
	entries, err := os.ReadDir(filepath.Dir(c.file))
	if err != nil {
		return err
	}
	count := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".bin") {
			count++
		}
	}
	if count >= 256 {
		return errors.New("未完成续传任务达到 256 个上限，请先处理或清理旧任务")
	}
	c.provisional = false
	if err = c.saveLocked(); err != nil {
		c.provisional = true
		return err
	}
	return nil
}

// Caller owns the record's OS lease. Refuse ambiguous names, directories and
// links. os.Root confines removal to the controlled spool directory.
func (c *uploadCheckpoint) discard() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errCheckpointRetired
	}
	if err := c.cleanupSpool(""); err != nil {
		return err
	}
	c.closed = true
	c.retireActivityLocked()
	if err := os.Remove(c.file); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Caller owns the OS lease and exclusive access to this checkpoint (mu if
// published). keep is the current request's body, which must not be deleted.
// This routine never changes the persisted association, even on failure.
func (c *uploadCheckpoint) cleanupSpool(keep string) error {
	name := c.data.Spool
	if name != "" {
		if !strings.HasPrefix(name, "upload-") || len(name) <= len("upload-") || filepath.Base(name) != name || !filepath.IsLocal(name) || strings.ContainsAny(name, "/\\:\x00") {
			return errors.New("续传记录的暂存文件名无效，保留记录等待核验")
		}
		root, err := os.OpenRoot(filepath.Dir(filepath.Dir(c.file)))
		if err != nil {
			return err
		}
		defer root.Close()
		info, err := root.Lstat(name)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("关联暂存路径不是普通文件，保留记录等待核验")
			}
			if name == keep {
				return nil
			}
			if err = root.Remove(name); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("清理关联正文失败，已保留续传记录：%w", err)
			}
		}
	}
	return nil
}

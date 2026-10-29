package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/net/webdav"
)

type checkpointHistory struct {
	Version int                  `json:"version"`
	State   string               `json:"state"`
	Data    uploadCheckpointData `json:"data"`
}

func (c *uploadCheckpoint) hasCloudState() bool {
	return len(c.data.Session) > 0 || c.data.Uploaded != nil || c.data.Backup != ""
}
func (c *uploadCheckpoint) archive(ctx context.Context, state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errCheckpointRetired
	}
	id := ""
	if c.data.Uploaded != nil {
		id = c.data.Uploaded.ID
	}
	key, _ := json.Marshal([]string{c.data.Stage, c.data.Backup, id})
	digest := sha256.Sum256(key)
	dir := filepath.Join(filepath.Dir(c.file), "history")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	name := filepath.Join(dir, c.data.Key+"-"+hex.EncodeToString(digest[:])+".bin")
	catalog, err := lockCheckpointCatalog(ctx, filepath.Join(dir, "catalog-lease"))
	if err != nil {
		return err
	}
	defer catalog.Close()
	if _, err = os.Stat(name); os.IsNotExist(err) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return e
		}
		count := 0
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".bin") {
				count++
			}
		}
		if count >= 256 {
			return errors.New("历史云端任务达到 256 个上限，请先核验并移除已处理历史")
		}
	} else if err != nil {
		return err
	}
	b, err := json.Marshal(checkpointHistory{Version: 1, State: state, Data: c.data})
	if err != nil {
		return err
	}
	if len(b) > 5<<20 {
		return errors.New("历史云端任务记录超过容量上限")
	}
	b, err = protectLocal(b)
	if err != nil {
		return err
	}
	return writePrivateBytes(name, b)
}

// The caller owns the checkpoint lease and reserves the target path. Never
// erase old-generation ownership before cloud reconciliation and its receipt.
func (c *uploadCheckpoint) retireCloud(ctx context.Context, m *davMount, parent, targetName string) error {
	if !c.hasCloudState() {
		c.cloudSettled = true
		return nil
	}
	if err := c.archive(ctx, "待核验旧云端事务"); err != nil {
		return err
	}
	if c.data.Uploaded == nil && c.data.Backup == "" {
		// There is no verified object identity to delete. Preserve the opaque
		// session in history for operator inspection rather than guessing by name.
		if err := c.archive(ctx, "未确认云端会话已保留"); err != nil {
			return err
		}
		c.cloudSettled = true
		return nil
	}
	if m == nil || !m.Writable || m.Identity == "" || c.data.Account != m.Identity || c.data.Provider != m.Provider || c.data.Parent != parent || c.data.TargetName != targetName {
		return errors.New("无法绑定原云盘账号和目标目录，保留旧事务记录")
	}
	// Absence is destructive evidence here: a read-only fallback must never
	// substitute its path identities or visibility for the native namespace.
	driver := m.drive
	for {
		backup, ok := driver.(*backupDAVDrive)
		if !ok {
			break
		}
		driver = backup.primary
	}
	listing := func() ([]davEntry, error) { return driver.List(directoryLogContext(ctx), parent) }
	entries, err := listing()
	if err != nil {
		return err
	}
	target, err := uniqueNamedEntry(entries, targetName)
	if err != nil {
		return err
	}
	var backup *davEntry
	if c.data.Backup != "" {
		backup, err = uniqueNamedEntry(entries, c.data.Backup)
		if err != nil {
			return err
		}
		if c.data.Original == nil {
			return errors.New("备份记录缺少原对象身份，保留事务")
		}
	}
	if backup != nil && !sameCheckpointObject(*backup, *c.data.Original) {
		return errors.New("旧备份身份或内容版本已变化，保留事务")
	}
	finalIsUploaded := target != nil && c.data.Uploaded != nil && sameCheckpointObject(*target, *c.data.Uploaded)
	if target == nil && c.data.Original != nil {
		if backup == nil {
			return errors.New("原目标和备份均未确认，不能丢弃恢复关系")
		}
		moveErr := driver.Move(ctx, *backup, "", targetName)
		entries, err = listing()
		if err != nil {
			return errors.Join(moveErr, err)
		}
		target, err = uniqueNamedEntry(entries, targetName)
		if err != nil {
			return err
		}
		remaining, e := uniqueNamedEntry(entries, c.data.Backup)
		if e != nil {
			return e
		}
		if target == nil || !sameCheckpointObject(*target, *c.data.Original) || remaining != nil {
			return errors.Join(moveErr, errors.New("原目标恢复未核验，保留事务"))
		}
		backup = nil
	} else if target != nil && !finalIsUploaded {
		if c.data.Original == nil || !sameCheckpointObject(*target, *c.data.Original) {
			return errors.New("目标已被其他操作修改，不能自动终结旧事务")
		}
		if backup != nil {
			return errors.New("目标与备份同时指向原对象，保留异常状态以待核验")
		}
	}
	removeVerified := func(entry davEntry) error {
		removeErr := driver.Remove(ctx, entry)
		current, e := listing()
		if e != nil {
			return errors.Join(removeErr, e)
		}
		for _, it := range current {
			if sameCloudObject(it, entry) {
				return errors.Join(removeErr, errors.New("旧对象清理尚未确认，保留事务"))
			}
		}
		return nil
	}
	if finalIsUploaded && backup != nil {
		if err = removeVerified(*backup); err != nil {
			return err
		}
	}
	entries, err = listing()
	if err != nil {
		return err
	}
	stage, err := uniqueNamedEntry(entries, c.data.Stage)
	if err != nil {
		return err
	}
	state := "旧云端事务已核验收敛"
	if stage != nil && stage.Name != targetName {
		if c.data.Uploaded == nil {
			state = "未确认云端临时对象已保留"
		} else {
			if !sameCheckpointObject(*stage, *c.data.Uploaded) {
				return errors.New("临时对象身份或版本已变化，保留事务")
			}
			if err = removeVerified(*stage); err != nil {
				return err
			}
		}
	}
	if err = c.archive(ctx, state); err != nil {
		return err
	}
	c.cloudSettled = true
	return nil
}

func (g *guiApp) retireCheckpointCloud(ctx context.Context, c *uploadCheckpoint) error {
	if c.data.Uploaded == nil && c.data.Backup == "" {
		return c.retireCloud(ctx, nil, "", "")
	}
	if g.dav == nil {
		return errors.New("请启动原账号的 WebDAV 挂载后核验待提交事务，记录已保留")
	}
	g.dav.controlMu.RLock()
	defer g.dav.controlMu.RUnlock()
	g.dav.mu.Lock()
	server := g.dav.server
	g.dav.mu.Unlock()
	if server == nil {
		return errors.New("请启动原账号的 WebDAV 挂载后核验待提交事务，记录已保留")
	}
	s, ok := server.Handler.(*davService)
	if !ok {
		return errors.New("无法取得原挂载服务")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, davRequestKey{}, &davRequestState{})
	ctx = newRunTrace(ctx, g.runLog, "放弃续传", c.data.Target)
	release, err := s.fs.reserve(ctx, c.data.Target)
	if err != nil {
		return err
	}
	defer release()
	if s.handler != nil && s.handler.LockSystem != nil {
		token, e := s.handler.LockSystem.Create(time.Now(), webdav.LockDetails{Root: c.data.Target, Duration: time.Minute, ZeroDepth: true})
		if e != nil {
			return &davStatusError{Code: 423, Message: "目标仍被客户端锁定，保留旧事务"}
		}
		defer s.handler.LockSystem.Unlock(time.Now(), token)
	}
	s.fs.refreshPath(c.data.Target)
	parent, name, err := s.fs.parent(ctx, c.data.Target)
	if err != nil {
		return err
	}
	ctx = scopedDAV(ctx, parent.mount, c.data.Target)
	defer s.fs.invalidate(parent.mount)
	if c.data.Account == "" && c.data.Mode == "memory" {
		key, _ := json.Marshal([]string{parent.mount.Provider, parent.mount.Identity, parent.entry.ID, name, fmt.Sprintf("memory:%d", c.data.Size)})
		digest := sha256.Sum256(key)
		if hex.EncodeToString(digest[:]) == c.data.Key {
			c.data.Account = parent.mount.Identity
			c.data.Parent = parent.entry.ID
			c.data.TargetName = name
		}
	}
	return c.retireCloud(ctx, parent.mount, parent.entry.ID, name)
}

func readCheckpointHistory(file string) (checkpointHistory, error) {
	var result checkpointHistory
	f, err := os.Open(file)
	if err != nil {
		return result, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (6<<20)+1))
	if err != nil {
		return result, err
	}
	if len(b) > 6<<20 {
		return result, os.ErrInvalid
	}
	b, err = unprotectLocal(b)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(b, &result)
	if err == nil && result.Version != 1 {
		err = os.ErrInvalid
	}
	return result, err
}

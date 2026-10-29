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
	"sync"
	"time"
)

// Only acknowledged cloud parts are durable. Local bodies remain ordinary
// request spools: CloudDrive2 may resend them without retransmitting cloud parts.
type uploadCheckpointData struct {
	Version         int              `json:"version"`
	Key             string           `json:"key"`
	Stage           string           `json:"stage"`
	Size            int64            `json:"size"`
	Updated         time.Time        `json:"updated"`
	Session         json.RawMessage  `json:"session,omitempty"`
	Parts           map[int64]string `json:"parts"`
	Uploaded        *davEntry        `json:"uploaded,omitempty"`
	Original        *davEntry        `json:"original,omitempty"`
	Backup          string           `json:"backup,omitempty"`
	Target          string           `json:"target"`
	Provider        string           `json:"provider"`
	Paused          bool             `json:"paused"`
	OriginalVersion string           `json:"original_version,omitempty"`
	UploadedVersion string           `json:"uploaded_version,omitempty"`
	Spool           string           `json:"spool,omitempty"`
	Mode            string           `json:"mode,omitempty"`
	Hashes          map[int64]string `json:"part_hashes"`
	Invalid         bool             `json:"invalid,omitempty"`
	Account         string           `json:"account_identity,omitempty"`
	Parent          string           `json:"parent_id,omitempty"`
	TargetName      string           `json:"target_name,omitempty"`
}
type uploadCheckpoint struct {
	mu           sync.Mutex
	file         string
	data         uploadCheckpointData
	closed       bool
	provisional  bool
	activity     *activeCheckpoint
	cloudSettled bool
}
type uploadCheckpointKey struct{}

func checkpointFrom(ctx context.Context) *uploadCheckpoint {
	c, _ := ctx.Value(uploadCheckpointKey{}).(*uploadCheckpoint)
	return c
}

func (c *uploadCheckpoint) saveLocked() error {
	if c.closed {
		return errCheckpointRetired
	}
	if c.provisional {
		return nil
	}
	c.data.Updated = time.Now().UTC()
	b, err := json.Marshal(c.data)
	if err != nil {
		return err
	}
	if len(b) > 4<<20 {
		return errors.New("上传续传记录超过容量上限")
	}
	b, err = protectLocal(b)
	if err != nil {
		return err
	}
	return writePrivateBytes(c.file, b)
}
func (c *uploadCheckpoint) session(v any) (bool, error) {
	if c == nil {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data.Session) == 0 {
		return false, nil
	}
	return true, json.Unmarshal(c.data.Session, v)
}
func (c *uploadCheckpoint) setSession(v any) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.data.Session = b
	return c.saveLocked()
}
func (c *uploadCheckpoint) part(index int64) (string, bool) {
	if c == nil {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data.Parts[index]
	return v, ok
}
func (c *uploadCheckpoint) confirm(index int64, tag string) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.data.Invalid {
		return errors.New("续传会话已失效，停止确认分片")
	}
	c.data.Parts[index] = tag
	if err := c.saveLocked(); err != nil {
		delete(c.data.Parts, index)
		return err
	}
	return nil
}
func checkpointProgress(ctx context.Context, n int64, index int64) {
	if tr, _ := ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
		tr.confirmed.Add(n)
	}
	logDAV(ctx, runLogEntry{Operation: "复用云端分片", Source: "上传恢复记录", Result: "已确认分片", Part: index, ConfirmedBytes: n, Detail: "局域网正文已重新校验，此分片不再通过公网上传"})
}

func resumableDrive(d davDrive) bool {
	switch v := d.(type) {
	case *wopanDAVDrive, *mobileDAVDrive, *quarkDAVDrive:
		return true
	case *backupDAVDrive:
		return resumableDrive(v.primary)
	}
	return false
}

func (d *davFile) openCheckpoint() (*uploadCheckpoint, func(), error) {
	m := d.node.mount
	if m.Identity == "" || !resumableDrive(m.drive) {
		return nil, func() {}, nil
	}
	h := sha256.New()
	if _, err := copyWithContext(d.ctx, h, io.NewSectionReader(d.tmp, 0, fiSize(d.tmp))); err != nil {
		return nil, nil, err
	}
	return d.openCheckpointFor(hex.EncodeToString(h.Sum(nil)), fiSize(d.tmp), "disk")
}

func (d *davFile) openCheckpointFor(signature string, size int64, mode string) (*uploadCheckpoint, func(), error) {
	m := d.node.mount
	if m.Identity == "" || !resumableDrive(m.drive) {
		return nil, func() {}, nil
	}
	// Account, cloud parent, target and original version isolate independent jobs.
	keyBytes, _ := json.Marshal([]string{m.Provider, m.Identity, d.node.parent, d.node.entry.Name, signature})
	keyHash := sha256.Sum256(keyBytes)
	key := hex.EncodeToString(keyHash[:])
	dir := filepath.Join(d.fs.spool, "resume")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, nil, err
	}
	// Fixed lock stripes bound lock-file growth and survive process termination.
	lease, err := lockUploadCheckpoint(filepath.Join(dir, "lease-"+key[:2]))
	if err != nil {
		return nil, nil, fmt.Errorf("续传任务正在使用或无法加锁：%w", err)
	}
	c := &uploadCheckpoint{file: filepath.Join(dir, key+".bin")}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { c.releaseLease(lease) }) }
	f, err := os.Open(c.file)
	if err == nil {
		b, readErr := io.ReadAll(io.LimitReader(f, (5<<20)+1))
		_ = f.Close()
		if readErr != nil || len(b) > 5<<20 {
			release()
			return nil, nil, errors.New("续传记录无法读取或超过上限，已保留原文件")
		}
		plain, e := unprotectLocal(b)
		if e != nil {
			release()
			return nil, nil, e
		}
		if json.Unmarshal(plain, &c.data) != nil || c.data.Version != 1 || c.data.Key != key || c.data.Size != size || c.data.Parts == nil || !strings.HasPrefix(c.data.Stage, "CloudProbe-upload-") || safeFilename(c.data.Stage) != nil {
			release()
			return nil, nil, errors.New("续传记录校验失败，已保留原文件")
		}
		c.restoreVersions()
		if c.data.Account != "" && (c.data.Account != m.Identity || c.data.Parent != d.node.parent || c.data.TargetName != d.node.entry.Name) {
			release()
			return nil, nil, errors.New("续传任务资源身份不一致，保留记录")
		}
		c.data.Account = m.Identity
		c.data.Parent = d.node.parent
		c.data.TargetName = d.node.entry.Name
		c.data.Target = d.name
		if mode == "memory" && (c.data.Mode != "memory" || c.data.Hashes == nil) {
			release()
			return nil, nil, errors.New("内存续传记录缺少分片指纹，拒绝复用")
		}
		if mode == "memory" && c.data.Invalid && !c.data.Paused {
			if err := c.retireCloud(d.ctx, m, d.node.parent, d.node.entry.Name); err != nil {
				release()
				return nil, nil, err
			}
			// Reconciliation may have restored the old target or found an already
			// committed one. Bind the new generation to that freshly checked state.
			d.fs.invalidate(m)
			current, lookupErr := d.fs.lookup(d.ctx, d.name)
			if lookupErr != nil && !os.IsNotExist(lookupErr) {
				release()
				return nil, nil, lookupErr
			}
			d.original = nil
			if lookupErr == nil {
				copy := current.entry
				d.original = &copy
			}
			c.data.Session = nil
			c.data.Parts = map[int64]string{}
			c.data.Hashes = map[int64]string{}
			c.data.Uploaded = nil
			c.data.UploadedVersion = ""
			c.data.Backup = ""
			c.data.Original = d.original
			c.data.OriginalVersion = ""
			if d.original != nil {
				c.data.OriginalVersion = d.original.ContentVersion
			}
			c.data.Stage = "CloudProbe-upload-" + randomKey()[:24] + filepath.Ext(d.node.entry.Name)
			c.data.Invalid = false
			c.cloudSettled = false
		}

		if c.data.Paused {
			release()
			return nil, nil, davLocalError(d.ctx, 423, "云端续传任务已暂停，请在管理界面继续")
		}
		// Old versions persisted empty memory candidates before checking the
		// budget. They have no body progress to protect and may refer to an old
		// target version after a successful disk fallback.
		if mode == "memory" && !c.hasProgress() {
			if err := os.Remove(c.file); err != nil && !os.IsNotExist(err) {
				release()
				return nil, nil, err
			}
			c.provisional = true
			c.data.Session = nil
			c.data.Original = d.original
			c.data.OriginalVersion = ""
			if d.original != nil {
				c.data.OriginalVersion = d.original.ContentVersion
			}
			c.data.Stage = "CloudProbe-upload-" + randomKey()[:24] + filepath.Ext(d.node.entry.Name)
		}

		if mode == "disk" {
			if err := c.cleanupSpool(filepath.Base(d.tmp.Name())); err != nil {
				release()
				return nil, nil, err
			}
			c.data.Spool = filepath.Base(d.tmp.Name())
		}
		if err := c.saveLocked(); err != nil {
			release()
			return nil, nil, err
		}
		return c, release, nil
	}
	if !os.IsNotExist(err) {
		release()
		return nil, nil, err
	}
	c.provisional = true
	c.data = uploadCheckpointData{Version: 1, Key: key, Size: size, Mode: mode, Hashes: map[int64]string{}, Stage: "CloudProbe-upload-" + randomKey()[:24] + filepath.Ext(d.node.entry.Name), Parts: map[int64]string{}, Original: d.original, Target: d.name, Provider: m.Provider, Account: m.Identity, Parent: d.node.parent, TargetName: d.node.entry.Name}
	if mode == "disk" {
		c.data.Spool = filepath.Base(d.tmp.Name())
	}
	if d.original != nil {
		c.data.OriginalVersion = d.original.ContentVersion
	}
	err = nil
	if mode == "disk" {
		err = c.publish(d.ctx)
	}
	if err != nil {
		release()
		return nil, nil, err
	}
	return c, release, nil
}

func lockCheckpointCatalog(ctx context.Context, path string) (*os.File, error) {
	for attempt := 0; attempt < 50; attempt++ {
		f, err := lockUploadCheckpoint(path)
		if err == nil {
			return f, nil
		}
		if attempt == 49 {
			return nil, err
		}
		if err = davWait(ctx, 20*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, os.ErrPermission
}

func (d *davFile) uploadWithCheckpoint(c *uploadCheckpoint) (davEntry, error) {
	m := d.node.mount
	if c == nil {
		return m.drive.Upload(d.ctx, d.node.parent, "CloudProbe-upload-"+randomKey()[:12]+filepath.Ext(d.node.entry.Name), d.tmp)
	}
	if err := c.ensureUsable(); err != nil {
		return davEntry{}, davCloudError(d.ctx, err)
	}
	d.ctx = context.WithValue(d.ctx, uploadCheckpointKey{}, c)
	baseContext := d.ctx
	ctx, cancel := context.WithCancel(baseContext)
	d.ctx = ctx
	activity := c.activate(cancel)
	if activity == nil {
		d.ctx = baseContext
		cancel()
		return davEntry{}, errCheckpointRetired
	}
	defer func() { activity.retire(); d.ctx = baseContext; cancel() }()
	// An already verified staged object needs no body retransmission. Recheck its
	// identity before feeding it to the existing guarded commit path.
	if c.data.Uploaded != nil {
		entries, err := m.drive.List(directoryLogContext(d.ctx), d.node.parent)
		if err != nil {
			return davEntry{}, err
		}
		var staged, backup *davEntry
		for _, it := range entries {
			if it.Size == c.data.Size && sameCheckpointObject(it, *c.data.Uploaded) {
				if it.Name == d.node.entry.Name {
					if err := d.confirmCommittedObject(*c.data.Uploaded, c.data.Size); err != nil {
						return davEntry{}, err
					}
					if c.data.Original != nil && c.data.Backup != "" {
						for _, old := range entries {
							if old.Name == c.data.Backup && sameCheckpointObject(old, *c.data.Original) {
								if err := m.drive.Remove(d.ctx, old); err != nil {
									return davEntry{}, err
								}
							}
						}
					}
					return it, nil
				}
				if it.Name == c.data.Stage {
					copy := it
					staged = &copy
				}
			}
			if c.data.Original != nil && it.Name == c.data.Backup && sameCheckpointObject(it, *c.data.Original) {
				copy := it
				backup = &copy
			}
		}
		if staged != nil {
			if backup != nil && d.original == nil {
				if err := d.checkUploadTarget(nil); err != nil {
					return davEntry{}, err
				}
				if err := m.drive.Move(d.ctx, *backup, "", d.node.entry.Name); err != nil {
					return davEntry{}, err
				}
				d.original = c.data.Original
			}
			if err := d.checkUploadTarget(c.data.Original); err != nil {
				return davEntry{}, err
			}
			d.original = c.data.Original
			return *staged, nil
		}
		return davEntry{}, errors.New("已上传临时对象已移动或消失，保留续传记录等待核验；未重放上传")
	}
	if err := d.checkUploadTarget(c.data.Original); err != nil {
		return davEntry{}, err
	}
	entry, err := m.drive.Upload(d.ctx, d.node.parent, c.data.Stage, d.tmp)
	var expired *quarkPartError
	if ctx.Err() == nil && errors.As(err, &expired) && expired.Code == "NoSuchUpload" && expired.HTTP.code == 404 {
		// A completed multipart upload also disappears from OSS. Never reset
		// before excluding the already-created staged object in the cloud listing.
		entries, listErr := m.drive.List(directoryLogContext(ctx), d.node.parent)
		if listErr != nil {
			return davEntry{}, listErr
		}
		staged, lookupErr := uniqueNamedEntry(entries, c.data.Stage)
		if lookupErr != nil {
			return davEntry{}, lookupErr
		}
		if staged == nil {
			c.mu.Lock()
			c.data.Session = nil
			c.data.Parts = map[int64]string{}
			c.data.Stage = "CloudProbe-upload-" + randomKey()[:24] + filepath.Ext(d.node.entry.Name)
			saveErr := c.saveLocked()
			c.mu.Unlock()
			if saveErr != nil {
				return davEntry{}, saveErr
			}
			logDAV(ctx, runLogEntry{Operation: "重建云端会话", Source: "夸克 OSS", Result: "旧会话明确失效", Detail: "云端已无原上传任务且未发现已完成临时对象；使用本次局域网正文重新上传，不复用旧分片"})
			entry, err = m.drive.Upload(ctx, d.node.parent, c.data.Stage, d.tmp)
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return davEntry{}, err
	}
	c.mu.Lock()
	c.data.Uploaded = &entry
	c.data.UploadedVersion = entry.ContentVersion
	err = c.saveLocked()
	c.mu.Unlock()
	return entry, err
}

func (c *uploadCheckpoint) restoreVersions() {
	if c.data.Original != nil {
		c.data.Original.ContentVersion = c.data.OriginalVersion
	}
	if c.data.Uploaded != nil {
		c.data.Uploaded.ContentVersion = c.data.UploadedVersion
	}
}
func sameCheckpointObject(a, b davEntry) bool {
	x, _ := (davInfo{a}).ETag(context.Background())
	y, _ := (davInfo{b}).ETag(context.Background())
	return a.Directory == b.Directory && sameCloudObject(a, b) && x == y
}

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

func (d *davFile) checkUploadTarget(old *davEntry) error {
	f, m := d.fs, d.node.mount
	f.invalidate(m)
	parent, _, err := f.parent(d.ctx, d.name)
	if err != nil {
		return err
	}
	if parent.entry.ID != d.node.parent {
		return davLocalError(d.ctx, http.StatusConflict, "上传期间目标父目录已变化，未覆盖目标")
	}
	current, err := f.lookup(d.ctx, d.name)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if old == nil && err == nil || old != nil && os.IsNotExist(err) {
		return davLocalError(d.ctx, http.StatusConflict, "上传期间目标已变化，未覆盖目标")
	}
	if old != nil {
		before, _ := (davInfo{*old}).ETag(d.ctx)
		after, _ := (davInfo{current.entry}).ETag(d.ctx)
		if before != after || current.entry.Directory {
			return davLocalError(d.ctx, http.StatusConflict, "上传期间目标内容已变化，未覆盖目标")
		}
	}
	return nil
}

func (d *davFile) confirmCommittedObject(created davEntry, size int64) error {
	for attempt := 0; attempt < 5; attempt++ {
		d.fs.invalidate(d.node.mount)
		entries, err := d.fs.listing(directoryLogContext(d.ctx), d.node.mount, d.node.parent)
		if err != nil {
			return err
		}
		entry, err := uniqueNamedEntry(entries, d.node.entry.Name)
		if err != nil {
			return davCloudError(d.ctx, err)
		}
		if entry != nil {
			if entry.Directory || entry.Size != size || !sameCloudObject(*entry, created) || created.ContentVersion != "" && entry.ContentVersion != created.ContentVersion {
				return davCloudError(d.ctx, errors.New("提交目标身份、大小或内容版本不符，未确认成功"))
			}
			d.committed = entry
			return nil
		}
		if err := davWait(d.ctx, 300*time.Millisecond); err != nil {
			return err
		}
	}
	return davCloudError(d.ctx, errors.New("云端提交后未确认最终对象，未报告成功"))
}

// The vendored DAV handler asks for the verified final object after Close.
func (d *davFile) CommittedInfo() (os.FileInfo, error) {
	if d.committed == nil {
		return nil, errors.New("尚未确认最终上传对象")
	}
	return davInfo{*d.committed}, nil
}

// Names and sizes are insufficient after a lost acknowledgement. Prefer the
// cloud object ID; a supplied immutable version must also remain consistent.
func sameCloudObject(a, b davEntry) bool {
	return a.ID != "" && a.ID == b.ID && (a.FID == "" || b.FID == "" || a.FID == b.FID)
}
func uniqueNamedEntry(entries []davEntry, name string) (*davEntry, error) {
	var found *davEntry
	for i := range entries {
		if entries[i].Name == name {
			if found != nil {
				return nil, errors.New("目录存在同名条目，无法核对提交结果")
			}
			entry := entries[i]
			found = &entry
		}
	}
	return found, nil
}
func (d *davFile) commitListing() ([]davEntry, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(d.ctx), 10*time.Second)
	defer cancel()
	return d.node.mount.drive.List(directoryLogContext(ctx), d.node.parent)
}
func (d *davFile) confirmBackupRename(old davEntry, name string) error {
	entries, err := d.commitListing()
	if err != nil {
		return err
	}
	backup, err := uniqueNamedEntry(entries, name)
	if err != nil {
		return err
	}
	target, err := uniqueNamedEntry(entries, old.Name)
	if err != nil {
		return err
	}
	if backup != nil && sameCloudObject(*backup, old) && target == nil {
		return nil
	}
	return errors.New("未能按文件身份确认原文件备份，保留当前云端状态")
}

// Returns true only when the staged object is already the unique target. A
// rollback is allowed only after confirming both target absence and the original
// staged/backup objects, never merely because an RPC returned an error.
func (d *davFile) reconcileCommit(created davEntry, size int64, old *davEntry, backupName string, moveErr error) (bool, error) {
	entries, err := d.commitListing()
	if err != nil {
		return false, fmt.Errorf("提交响应失败且无法核验目录；保留临时文件及备份 %s：%v；%w", backupName, err, moveErr)
	}
	target, err := uniqueNamedEntry(entries, d.node.entry.Name)
	if err != nil {
		return false, fmt.Errorf("提交结果不唯一；备份 %s 保留：%w", backupName, err)
	}
	if target != nil {
		if !target.Directory && target.Size == size && sameCloudObject(*target, created) {
			logDAV(d.ctx, runLogEntry{Operation: "提交结果核对", Source: "云盘目录接口", Result: "已确认", Detail: "改名响应丢失，但目标 ID、版本和大小已确认；继续原提交流程"})
			return true, nil
		}
		return false, fmt.Errorf("提交后目标已被占用或版本不符；备份 %s 保留：%w", backupName, moveErr)
	}
	if old == nil {
		return false, fmt.Errorf("提交未确认，已保留唯一临时文件：%w", moveErr)
	}
	staged, err := uniqueNamedEntry(entries, created.Name)
	if err != nil {
		return false, err
	}
	backup, err := uniqueNamedEntry(entries, backupName)
	if err != nil {
		return false, err
	}
	if staged == nil || backup == nil || !sameCloudObject(*staged, created) || !sameCloudObject(*backup, *old) {
		return false, fmt.Errorf("无法确认可安全恢复的临时文件和备份 %s，保留云端状态：%w", backupName, moveErr)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(d.ctx), 10*time.Second)
	defer cancel()
	if err = d.node.mount.drive.Move(ctx, *backup, "", old.Name); err != nil {
		return false, fmt.Errorf("提交失败，恢复响应也未确认；原内容保留于 %s 或原名称，请核对：%v；%w", backupName, err, moveErr)
	}
	d.fs.invalidate(d.node.mount)
	return false, fmt.Errorf("临时文件提交失败，原内容已恢复：%w", moveErr)
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"
)

type memoryUploadResult struct {
	entry davEntry
	err   error
}

func (f *davFS) openMemoryFile(ctx context.Context, name string, parent davNode, base string, old *davEntry, size int64) (*davFile, error) {
	if size < 0 {
		return nil, davLocalError(ctx, http.StatusLengthRequired, "纯内存上传需要 Content-Length")
	}
	if size > f.maxUpload {
		return nil, davLocalError(ctx, http.StatusRequestEntityTooLarge, "文件超过上传上限")
	}
	if !streamDriveAvailable(parent.mount.drive) || strings.HasPrefix(parent.entry.ID, "openlist:") {
		return nil, &memoryUnavailable{"此上传路径不支持纯内存流水线"}
	}
	clean, _ := davPath(name)
	ctx = scopedDAV(ctx, parent.mount, clean)
	o := f.transfers.options
	stageName := "CloudProbe-upload-" + randomKey()[:12] + path.Ext(base)
	d := &davFile{fs: f, ctx: ctx, name: clean, node: davNode{mount: parent.mount, parent: parent.entry.ID, entry: davEntry{Item: Item{Name: base}, Modified: time.Now()}}, writing: true, original: old, spoolStarted: time.Now(), uploadDone: make(chan memoryUploadResult, 1)}
	cp, release, err := d.openCheckpointFor(fmt.Sprintf("memory:%d", size), size, "memory")
	if err != nil {
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			release()
		}
	}()
	if cp != nil {
		ctx = context.WithValue(ctx, uploadCheckpointKey{}, cp)
		d.ctx = ctx
		stageName = cp.data.Stage
		if cp.data.Uploaded == nil {
			if err = d.checkUploadTarget(cp.data.Original); err != nil {
				return nil, err
			}
		}
	}
	var plan memoryUploadPlan
	if cp != nil && cp.data.Uploaded != nil {
		var session struct{ PartSize int64 }
		if json.Unmarshal(cp.data.Session, &session) != nil || session.PartSize <= 0 {
			return nil, errors.New("已完成内存任务缺少有效分片布局")
		}
		plan = memoryUploadPlan{partSize: session.PartSize, workers: 1, run: func(ctx context.Context, s *memoryUploadSource) (davEntry, error) {
			if err := s.drainCheckpoint(ctx); err != nil {
				return davEntry{}, err
			}
			return *cp.data.Uploaded, nil
		}}
	} else {
		plan, err = planMemoryUpload(ctx, parent.mount.drive, parent.entry.ID, stageName, size, o)
	}
	if err != nil {
		return nil, err
	}
	part, workers := plan.partSize, plan.workers
	if part <= 0 || part > 256<<20 || f.transfers.memory.reservation(part) > int64(o.UploadMemoryMiB)<<20 {
		return nil, &memoryUnavailable{"云端单片超过单文件内存预算；请增大内存或选择磁盘兼容"}
	}
	window := min(int64(o.UploadMemoryMiB)<<20, part*int64(workers)+(int64(o.UploadPrefetchMiB)<<20))
	window = max(f.transfers.memory.reservation(part), window)
	source := newMemoryUploadSource(ctx, size, part, window, f.transfers.memory, f.transfers.recovery)
	if plan.hashes {
		source.enableHashes()
	}
	d.memory = source
	d.releaseCheckpoint = func() {
		if cp != nil {
			activeCheckpoints.Delete(cp.file)
		}
		release()
	}
	if cp != nil {
		activeCheckpoints.Store(cp.file, activeCheckpoint{cp, func() { source.Abort(errors.New("上传续传任务已暂停")) }})
	}
	logDAV(ctx, runLogEntry{Operation: "内存流水上传", Source: "WebDAV → 云盘", Result: "开始", Bytes: size, Detail: fmt.Sprintf("文件正文仅驻内存；单文件窗口 %d B；全局预算 %d B；分片 %d B；预读目标 %d MiB；临时名称 %s", window, int64(o.TotalUploadMemoryMiB)<<20, part, o.UploadPrefetchMiB, stageName)})
	go func() {
		entry, err := plan.run(source.ctx, source)
		if err == nil {
			err = source.waitInput(source.ctx)
		}
		if err == nil && cp != nil {
			cp.mu.Lock()
			cp.data.Uploaded = &entry
			cp.data.UploadedVersion = entry.ContentVersion
			err = cp.saveLocked()
			cp.mu.Unlock()
		}
		if err != nil {
			err = invalidateMemorySession(source.ctx, parent.mount.drive, parent.entry.ID, err)
			source.Abort(err)
		}
		d.uploadDone <- memoryUploadResult{entry, err}
	}()
	ready = true
	return d, nil
}
func nativeWopanDrive(d davDrive) *wopanDAVDrive {
	if b, ok := d.(*backupDAVDrive); ok {
		return nativeWopanDrive(b.primary)
	}
	w, _ := d.(*wopanDAVDrive)
	return w
}

func (d *davFile) closeMemory() (err error) {
	if d.releaseCheckpoint != nil {
		defer d.releaseCheckpoint()
	}
	source := d.memory
	var inputErr error
	state := davState(d.ctx)
	if d.failed || d.ctx.Err() != nil || state != nil && (state.aborted || state.err != nil || state.contentLength >= 0 && state.bodyBytes != state.contentLength) {
		inputErr = errors.New("客户端上传不完整或已取消，未提交目标文件")
	} else {
		inputErr = source.FinishInput()
	}
	if inputErr != nil {
		source.Abort(inputErr)
	}
	result := <-d.uploadDone // Cancel and join every HTTP reader before releasing RAM.
	source.Close()
	d.fs.invalidate(d.node.mount)
	entry := runLogEntry{Operation: "内存流水上传", Source: "WebDAV → 云盘", Result: "已接收并上传", Bytes: source.Received(), DurationMs: time.Since(d.spoolStarted).Milliseconds(), Detail: "正文未创建磁盘暂存文件；已确认分片立即释放，未确认分片保留重试；全部后台读取已结束"}
	defer func() {
		if err != nil {
			entry.Result = "失败"
			entry.Error = err.Error()
		}
		logDAV(d.ctx, entry)
	}()
	if result.err != nil {
		return davCloudError(d.ctx, result.err)
	}
	if inputErr != nil {
		return inputErr
	}
	return d.finishMemoryCheckpoint(result.entry)
}

func (d *wopanDAVDrive) UploadStream(ctx context.Context, parent, name string, size int64, source *memoryUploadSource) (davEntry, error) {
	u, err := d.prepareUpload(ctx, parent, name, size)
	if err != nil {
		return davEntry{}, &davNotExecuted{err}
	}
	fid, err := u.run(ctx, source)
	if err != nil {
		return davEntry{}, err
	}
	return d.verifyUploaded(ctx, parent, name, size, fid)
}
func (b *backupDAVDrive) UploadStream(ctx context.Context, parent, name string, size int64, source *memoryUploadSource) (davEntry, error) {
	d, ok := b.primary.(streamingDAVDrive)
	if !ok || strings.HasPrefix(parent, "openlist:") {
		return davEntry{}, &davNotExecuted{errors.New("此 OpenList 路径缺少纯内存上传能力")}
	}
	entry, err := d.UploadStream(ctx, parent, name, size, source)
	if err == nil {
		b.remember(parent, []davEntry{entry})
	}
	return entry, err // Never replay released parts through an unrelated upload session.
}

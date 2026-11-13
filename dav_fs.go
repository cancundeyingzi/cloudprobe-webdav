package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/net/webdav"
)

type davMount struct {
	UploadStorageMode string `json:"upload_storage_mode,omitempty"`
	Alias             string `json:"alias"`
	Root              string `json:"root"`
	Provider          string `json:"provider"`
	Writable          bool   `json:"writable"`
	WriteStatus       string `json:"write_status"`
	Identity          string `json:"-"`
	drive             davDrive
	health            *routeHealth
	writeMu           sync.RWMutex
	writeWarning      *davWriteWarning
}
type davFS struct {
	mountMu      sync.RWMutex
	mounts       []*davMount
	spool        string
	maxUpload    int64
	cacheMu      sync.Mutex
	dirCache     davDirCache
	quotas       map[string]*davQuotaCache
	reservations davReservations
	transfers    *transferRuntime
}
type davNode struct {
	entry   davEntry
	mount   *davMount
	parent  string
	virtual bool
}
type davInfo struct{ entry davEntry }

func (i davInfo) Name() string { return i.entry.Name }
func (i davInfo) Size() int64  { return i.entry.Size }
func (i davInfo) Mode() os.FileMode {
	if i.IsDir() {
		return os.ModeDir | 0755
	}
	return 0644
}
func (i davInfo) ModTime() time.Time {
	if i.entry.Modified.IsZero() {
		return time.Unix(946684800, 0).UTC()
	}
	return i.entry.Modified
}
func (i davInfo) IsDir() bool { return i.entry.Directory }
func (i davInfo) Sys() any    { return nil }
func (i davInfo) ETag(context.Context) (string, error) {
	modified := ""
	if i.entry.ContentVersion == "" {
		modified = i.ModTime().UTC().Format(time.RFC3339Nano)
	}
	h := sha256.Sum256([]byte(strings.Join([]string{i.entry.ID, i.entry.FID, i.entry.ContentVersion, fmt.Sprint(i.Size()), modified}, "\x00")))
	value := fmt.Sprintf(`"%x"`, h[:16])
	if i.entry.ContentVersion == "" && i.entry.FID == "" {
		return "W/" + value, nil
	}
	return value, nil
}

type davRequestKey struct{}
type davRequestState struct {
	err           error
	aborted       bool
	bodyBytes     int64
	contentLength int64
	method        string
	destination   string
	backups       []davBackup
	created       map[string]string
	finish        func(bool) error
	externalRange bool
}

type davBackup struct {
	mount                      *davMount
	entry                      davEntry
	parent, target, backupName string
}

func davState(ctx context.Context) *davRequestState {
	s, _ := ctx.Value(davRequestKey{}).(*davRequestState)
	return s
}
func davCloudError(ctx context.Context, e error) error {
	if e == nil {
		return nil
	}
	v := &davUpstreamError{Message: e.Error(), Err: e}
	if s := davState(ctx); s != nil {
		s.err = v
	}
	return v
}
func davPath(name string) (string, error) {
	if !utf8.ValidString(name) || strings.ContainsAny(name, "\\\x00\r\n") || len(name) > 8192 {
		return "", os.ErrInvalid
	}
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) > 100 {
		return "", os.ErrInvalid
	}
	for _, p := range parts {
		if p == "." || p == ".." {
			return "", os.ErrPermission
		}
	}
	return path.Clean("/" + name), nil
}
func (f *davFS) lookup(ctx context.Context, name string) (davNode, error) {
	clean, e := davPath(name)
	if e != nil {
		return davNode{}, e
	}
	if clean == "/" {
		return davNode{entry: davEntry{Item: Item{ID: "virtual-root", Name: "/", Directory: true}}, virtual: true}, nil
	}
	// Keep only verified path nodes for a conditional request. Retaining entire
	// ancestor listings here would pin large snapshots for the whole download.
	snapshot := f.readSnapshot(ctx)
	if snapshot != nil {
		snapshot.mu.Lock()
		defer snapshot.mu.Unlock()
		if node, ok := snapshot.nodes[clean]; ok {
			return node, nil
		}
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	var m *davMount
	mounts := f.mountSnapshot()
	if snapshot != nil {
		mounts = snapshot.mounts
	}
	for _, candidate := range mounts {
		if candidate.Alias == parts[0] {
			m = candidate
			break
		}
	}
	if m == nil {
		return davNode{}, os.ErrNotExist
	}
	n := davNode{entry: davEntry{Item: Item{ID: m.Root, Name: m.Alias, Directory: true}}, mount: m, virtual: true}
	currentPath := "/" + m.Alias
	for _, part := range parts[1:] {
		if !n.entry.Directory {
			return davNode{}, os.ErrNotExist
		}
		nextPath := path.Join(currentPath, part)
		if snapshot != nil {
			if node, ok := snapshot.nodes[nextPath]; ok {
				n, currentPath = node, nextPath
				continue
			}
		}
		listContext := scopedDAV(ctx, m, currentPath)
		if backup, ok := m.drive.(*backupDAVDrive); ok {
			listContext = backup.parentContext(listContext, n.entry)
		}
		entries, e := f.listing(listContext, m, n.entry.ID)
		if e != nil {
			return davNode{}, e
		}
		var entry davEntry
		for attempt := 0; ; attempt++ {
			found, e := uniqueNamedEntry(entries, part)
			if e != nil {
				return davNode{}, davCloudError(ctx, e)
			}
			if found == nil {
				return davNode{}, os.ErrNotExist
			}
			entry = *found
			backup, ok := m.drive.(*backupDAVDrive)
			if !ok {
				break
			}
			entry, e = backup.bindLocation(ctx, n.entry, entry)
			if e == nil {
				break
			}
			if attempt != 0 || !errors.Is(e, errBackupPathUnavailable) {
				return davNode{}, davCloudError(ctx, e)
			}
			// A detached cache entry can outlive a newer directory publication.
			// Refresh this one directory before selecting a current native ID.
			epoch := f.dirCache.invalidateDirectory(m.Alias, n.entry.ID)
			entries, e = f.queryDirectory(listContext, m, n.entry.ID)
			if e != nil {
				return davNode{}, e
			}
			f.dirCache.put(m, n.entry.ID, entries, epoch)
		}
		n = davNode{entry: entry, mount: m, parent: n.entry.ID}
		currentPath = nextPath
		if snapshot != nil {
			snapshot.nodes[currentPath] = n
		}
	}
	return n, nil
}
func (f *davFS) parent(ctx context.Context, name string) (davNode, string, error) {
	clean, e := davPath(name)
	if e != nil {
		return davNode{}, "", e
	}
	n, e := f.lookup(ctx, path.Dir(clean))
	if e != nil {
		return davNode{}, "", e
	}
	base := path.Base(clean)
	if n.mount == nil || !n.entry.Directory || base == "/" {
		return davNode{}, "", os.ErrPermission
	}
	if e = safeFilename(base); e != nil {
		return davNode{}, "", e
	}
	if !n.mount.Writable {
		return davNode{}, "", os.ErrPermission
	}
	return n, base, nil
}
func (f *davFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	n, e := f.lookup(ctx, name)
	if e != nil {
		return nil, e
	}
	return davInfo{n.entry}, nil
}
func (f *davFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	release, e := f.reserve(ctx, name)
	if e != nil {
		return e
	}
	defer release()
	p, base, e := f.parent(ctx, name)
	if e != nil {
		return e
	}
	ctx = scopedDAV(ctx, p.mount, name)
	f.invalidate(p.mount)
	if _, e = f.lookup(ctx, name); e == nil {
		return os.ErrExist
	} else if !os.IsNotExist(e) {
		return e
	}
	created, e := p.mount.drive.Mkdir(ctx, p.entry.ID, base)
	f.invalidate(p.mount)
	if e != nil {
		return davCloudError(ctx, e)
	}
	if s := davState(ctx); s != nil {
		if s.created == nil {
			s.created = map[string]string{}
		}
		clean, _ := davPath(name)
		s.created[clean] = created.ID
	}
	return f.verify(ctx, p.mount, p.entry.ID, base, -1, created)
}
func (f *davFS) RemoveAll(ctx context.Context, name string) error {
	release, e := f.reserve(ctx, name)
	if e != nil {
		return e
	}
	defer release()
	n, e := f.lookup(ctx, name)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if n.virtual || !n.mount.Writable {
		return os.ErrPermission
	}
	ctx = scopedDAV(ctx, n.mount, name)
	if s := davState(ctx); s != nil && (s.method == "COPY" || s.method == "MOVE") {
		clean, _ := davPath(name)
		if clean == s.destination {
			backupName := "CloudProbe-backup-" + randomKey()[:12] + path.Ext(n.entry.Name)
			b := davBackup{mount: n.mount, entry: n.entry, parent: n.parent, target: clean, backupName: backupName}
			// Register before dispatch: an error may be a lost acknowledgement.
			s.backups = append(s.backups, b)
			defer f.invalidate(n.mount)
			if e = n.mount.drive.Move(ctx, n.entry, "", backupName); e != nil {
				checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
				target, backup, checkErr := b.snapshot(checkCtx)
				cancel()
				if checkErr != nil || target != nil || backup == nil || !sameCheckpointObject(*backup, b.entry) || ctx.Err() != nil {
					return davCloudError(ctx, fmt.Errorf("目标备份结果未确认；原路径 %s，待核验备份 %s：%w", clean, backupName, errors.Join(e, checkErr)))
				}
			}
			return nil
		}
	}
	e = n.mount.drive.Remove(ctx, n.entry)
	f.invalidate(n.mount)
	return davCloudError(ctx, e)
}
func (f *davFS) Rename(ctx context.Context, oldName, newName string) error {
	release, e := f.reserve(ctx, oldName, newName)
	if e != nil {
		return e
	}
	defer release()
	src, e := f.lookup(ctx, oldName)
	if e != nil {
		return e
	}
	dst, base, e := f.parent(ctx, newName)
	if e != nil {
		return e
	}
	if src.virtual || src.mount != dst.mount || !src.mount.Writable {
		return os.ErrPermission
	}
	oldClean, _ := davPath(oldName)
	newClean, _ := davPath(newName)
	if oldClean == newClean {
		return nil
	}
	if src.entry.Directory && strings.HasPrefix(newClean, oldClean+"/") {
		return os.ErrPermission
	}
	m := src.mount
	ctx = scopedDAV(ctx, m, oldName)
	parent := dst.entry.ID
	if parent == src.parent {
		parent = ""
	}
	e = m.drive.Move(ctx, src.entry, parent, base)
	f.invalidate(m)
	if e != nil {
		return davCloudError(ctx, e)
	}
	if e = f.verify(ctx, m, dst.entry.ID, base, -1, src.entry); e != nil {
		return e
	}
	if s := davState(ctx); s != nil {
		if s.created == nil {
			s.created = map[string]string{}
		}
		s.created[newClean] = src.entry.ID
	}
	return nil
}
func (f *davFS) verify(ctx context.Context, m *davMount, parent, name string, size int64, expected davEntry) (err error) {
	finish := logPhase(ctx, "提交目录核验", "云盘目录接口", "检查目标名称、大小："+name)
	defer func() { finish(err) }()
	for attempt := 0; attempt < 5; attempt++ {
		f.invalidate(m)
		a, e := f.listing(directoryLogContext(ctx), m, parent)
		if e != nil {
			return e
		}
		it, e := uniqueNamedEntry(a, name)
		if e != nil {
			return davCloudError(ctx, e)
		}
		if it != nil && (size < 0 || it.Size == size) {
			if !sameCloudObject(*it, expected) {
				return davCloudError(ctx, errors.New("目标名称已出现但对象身份不符，未确认操作成功"))
			}
			return nil
		}
		if e = davWait(ctx, 300*time.Millisecond); e != nil {
			return e
		}
	}
	return davCloudError(ctx, errors.New("云盘操作返回后未在目标目录确认到预期条目，未报告成功"))
}
func (f *davFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	ctx = withTransfers(ctx, f.transfers)
	writing := flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0
	if writing {
		if flag&os.O_APPEND != 0 {
			return nil, os.ErrInvalid
		}
		p, base, e := f.parent(ctx, name)
		if e != nil {
			return nil, e
		}
		n, e := f.lookup(ctx, name)
		if e == nil && n.entry.Directory {
			return nil, os.ErrPermission
		}
		if e != nil && !os.IsNotExist(e) {
			return nil, e
		}
		if flag&os.O_EXCL != 0 && e == nil {
			return nil, os.ErrExist
		}
		if flag&os.O_CREATE == 0 && os.IsNotExist(e) {
			return nil, e
		}
		var old *davEntry
		if e == nil {
			entry := n.entry
			old = &entry
		}
		mode := f.uploadStorage(p.mount)
		if f.transfers != nil && (mode == "memory" || mode == "auto") {
			if state := davState(ctx); state != nil && state.method == "PUT" {
				if mode == "auto" && state.contentLength < 0 {
					logDAV(ctx, runLogEntry{Operation: "上传缓存选择", Source: "本机", Result: "磁盘兼容", Detail: "内存优先：客户端未声明总长度，使用已选择的磁盘兼容方案"})
				} else {
					file, err := f.openMemoryFile(ctx, name, p, base, old, state.contentLength)
					if err == nil {
						return file, nil
					}
					var unavailable *memoryUnavailable
					if !errors.As(err, &unavailable) {
						return nil, davCloudError(ctx, err)
					}
					if mode == "memory" {
						return nil, davLocalError(ctx, http.StatusNotImplemented, unavailable.Error())
					}
					logDAV(ctx, runLogEntry{Operation: "上传缓存选择", Source: "本机", Result: "磁盘兼容", Detail: "内存优先：正文尚未接收，按配置切换磁盘兼容；" + unavailable.Error()})
				}
			}
		}
		if e = os.MkdirAll(f.spool, 0700); e != nil {
			return nil, e
		}
		tmp, owner, e := newOwnedSpool(ctx, f.spool, name)
		if e != nil {
			return nil, e
		}
		clean, _ := davPath(name)
		return &davFile{fs: f, ctx: scopedDAV(ctx, p.mount, clean), name: clean, node: davNode{mount: p.mount, parent: p.entry.ID, entry: davEntry{Item: Item{Name: base}, Modified: time.Now()}}, original: old, tmp: tmp, spoolOwner: owner, writing: true, spoolStarted: time.Now()}, nil
	}
	n, e := f.lookup(ctx, name)
	if e != nil {
		return nil, e
	}
	clean, _ := davPath(name)
	ctx = scopedDAV(ctx, n.mount, clean)
	df := &davFile{fs: f, ctx: ctx, name: clean, node: n}
	if !n.entry.Directory {
		df.remote = newTransferReader(ctx, n.mount, n.entry, f.transfers)
	}
	return df, nil
}

type davFile struct {
	name                    string
	fs                      *davFS
	ctx                     context.Context
	node                    davNode
	tmp                     *os.File
	remote                  transferReadCloser
	writing, closed, failed bool
	readPos                 int
	spoolOwner              *spoolOwner
	spoolStarted            time.Time
	spoolBytes              int64
	spoolWriteTime          time.Duration
	memory                  *memoryUploadSource
	uploadDone              chan memoryUploadResult
	releaseCheckpoint       func()
	original                *davEntry
	committed               *davEntry
}

func (d *davFile) Read(p []byte) (int, error) {
	if d.closed {
		return 0, os.ErrClosed
	}
	if d.tmp != nil {
		return d.tmp.Read(p)
	}
	if d.remote == nil {
		return 0, os.ErrInvalid
	}
	n, e := d.remote.Read(p)
	if e != nil && e != io.EOF {
		return n, davCloudError(d.ctx, e)
	}
	return n, e
}
func (d *davFile) Seek(off int64, whence int) (int64, error) {
	if d.closed {
		return 0, os.ErrClosed
	}
	if d.tmp != nil {
		return d.tmp.Seek(off, whence)
	}
	if d.remote == nil {
		return 0, os.ErrInvalid
	}
	return d.remote.Seek(off, whence)
}
func (d *davFile) Write(p []byte) (int, error) {
	if d.closed {
		return 0, os.ErrClosed
	}
	if !d.writing {
		return 0, os.ErrPermission
	}
	if d.memory != nil {
		n, e := d.memory.Write(p)
		if e != nil {
			d.failed = true
			e = davCloudError(d.ctx, e)
		}
		return n, e
	}
	pos, e := d.tmp.Seek(0, io.SeekCurrent)
	if e != nil {
		d.failed = true
		return 0, e
	}
	if int64(len(p)) > d.fs.maxUpload-pos {
		d.failed = true
		return 0, errors.New("上传超过配置的单文件上限")
	}
	started := time.Now()
	n, e := d.tmp.Write(p)
	d.spoolWriteTime += time.Since(started)
	d.spoolBytes += int64(n)
	if e != nil {
		d.failed = true
	}
	return n, e
}
func (d *davFile) Stat() (os.FileInfo, error) {
	if d.closed {
		return nil, os.ErrClosed
	}
	entry := d.node.entry
	if d.memory != nil {
		entry.Size = d.memory.Received()
	}
	if d.tmp != nil {
		fi, e := d.tmp.Stat()
		if e != nil {
			return nil, e
		}
		entry.Size = fi.Size()
	}
	return davInfo{entry}, nil
}
func (d *davFile) Readdir(count int) ([]os.FileInfo, error) {
	if !d.node.entry.Directory {
		return nil, os.ErrInvalid
	}
	var entries []davEntry
	if d.node.mount == nil {
		for _, m := range d.fs.mountSnapshot() {
			entries = append(entries, davEntry{Item: Item{ID: m.Root, Name: m.Alias, Directory: true}})
		}
	} else {
		listContext := scopedDAV(d.ctx, d.node.mount, d.name)
		if backup, ok := d.node.mount.drive.(*backupDAVDrive); ok {
			listContext = backup.parentContext(listContext, d.node.entry)
		}
		a, e := d.fs.listing(listContext, d.node.mount, d.node.entry.ID)
		if e != nil {
			return nil, e
		}
		entries = a
	}
	if count > 0 && d.readPos >= len(entries) {
		return nil, io.EOF
	}
	end := len(entries)
	if count > 0 && d.readPos+count < end {
		end = d.readPos + count
	}
	out := []os.FileInfo{}
	for _, it := range entries[d.readPos:end] {
		out = append(out, davInfo{it})
	}
	d.readPos = end
	return out, nil
}
func (d *davFile) Close() (err error) {
	if d.closed {
		return nil
	}
	d.closed = true
	if d.remote != nil {
		d.remote.Close()
	}
	if d.memory != nil {
		return d.closeMemory()
	}
	if d.tmp == nil {
		return nil
	}
	var cp *uploadCheckpoint
	var releaseCheckpoint func()
	committed := false
	defer func() {
		cleanupErr := d.finishSpool()
		if cleanupErr != nil {
			logDAV(d.ctx, runLogEntry{Operation: "本地正文清理", Source: "本机", Result: "待清理", Error: cleanupErr.Error(), Detail: "独立正文清理记录已保留，可在管理界面重试"})
			if err != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
		if committed && cp != nil {
			if cleanup := cp.complete(); cleanup != nil {
				logDAV(d.ctx, runLogEntry{Operation: "清理续传记录", Source: "本机", Result: "待清理", Error: cleanup.Error()})
			}
		}
		if releaseCheckpoint != nil {
			releaseCheckpoint()
		}
	}()
	logDAV(d.ctx, runLogEntry{Operation: "本地暂存", Source: "本机磁盘", Result: "写入结束", Bytes: d.spoolBytes, DurationMs: time.Since(d.spoolStarted).Milliseconds(), DiskWriteMs: d.spoolWriteTime.Milliseconds(), Detail: "阶段耗时含客户端收包；磁盘实际 Write 累计耗时单独记录"})
	s := davState(d.ctx)
	if d.failed || d.ctx.Err() != nil || (s != nil && (s.aborted || s.err != nil || (s.method == "PUT" && s.contentLength >= 0 && s.bodyBytes != s.contentLength))) {
		return errors.New("上传未完整接收，未提交云端目标；暂存正文交由终结清理")
	}
	flush := logPhase(d.ctx, "暂存落盘", "本机磁盘", "Sync 暂存文件")
	if e := d.tmp.Sync(); e != nil {
		flush(e)
		return e
	}
	flush(nil)
	m := d.node.mount
	release, e := d.fs.reserve(d.ctx, d.name)
	if e != nil {
		return e
	}
	defer release()
	f := d.fs
	if e := d.checkUploadTarget(d.original); e != nil {
		return e
	}
	cp, releaseCheckpoint, e = d.openCheckpoint()
	if e != nil {
		return davCloudError(d.ctx, e)
	}
	created, e := d.uploadWithCheckpoint(cp)
	f.invalidate(m)
	if e != nil {
		return davCloudError(d.ctx, e)
	}
	if d.committed == nil {
		err = d.commitUpload(created, fiSize(d.tmp), d.original)
	}
	committed = err == nil
	return err
}

func fiSize(f *os.File) int64 {
	info, err := f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (d *davFile) commitUpload(created davEntry, size int64, old *davEntry) (err error) {
	f, m := d.fs, d.node.mount
	if cp := checkpointFrom(d.ctx); cp != nil {
		if e := cp.ensureUsable(); e != nil {
			return davCloudError(d.ctx, e)
		}
	}
	defer f.invalidate(m) // Failed or uncertain renames also invalidate old names.
	if size < 0 {
		return davCloudError(d.ctx, errors.New("无法读取上传文件长度"))
	}
	if e := d.checkUploadTarget(old); e != nil {
		return e
	}
	var e error
	backupName := ""
	commit := logPhase(d.ctx, "提交上传", "云盘写入接口", "备份旧文件、改名并核验后清理备份")
	defer func() { commit(err) }()
	if old != nil {
		backupName = "CloudProbe-backup-" + randomKey()[:12] + path.Ext(old.Name)
		if cp := checkpointFrom(d.ctx); cp != nil {
			cp.mu.Lock()
			cp.data.Backup = backupName
			e = cp.saveLocked()
			cp.mu.Unlock()
			if e != nil {
				return e
			}
		}
		if e = m.drive.Move(d.ctx, *old, "", backupName); e != nil {
			if checkErr := d.confirmBackupRename(*old, backupName); checkErr != nil {
				return davCloudError(d.ctx, fmt.Errorf("原文件备份响应失败；保留现有名称和内容以待核对：%v；%w", checkErr, e))
			}
		}
	}
	if e = m.drive.Move(d.ctx, created, "", d.node.entry.Name); e != nil {
		if committed, checkErr := d.reconcileCommit(created, size, old, backupName, e); !committed {
			return davCloudError(d.ctx, checkErr)
		}
	}
	if e = d.confirmCommittedObject(created, size); e != nil {
		if old != nil {
			return davCloudError(d.ctx, fmt.Errorf("写入确认失败；旧内容保留于 %s：%w", backupName, e))
		}
		return e
	}
	if cp := checkpointFrom(d.ctx); cp != nil {
		cp.mu.Lock()
		cp.data.Uploaded = d.committed
		cp.data.UploadedVersion = d.committed.ContentVersion
		e = cp.saveLocked()
		cp.mu.Unlock()
		if e != nil {
			return davCloudError(d.ctx, e)
		}
	}
	if old != nil {
		if e = m.drive.Remove(d.ctx, *old); e != nil {
			return davCloudError(d.ctx, fmt.Errorf("新内容已提交，但清理旧内容备份 %s 失败：%w", backupName, e))
		}
	}
	f.invalidate(m)
	if s := davState(d.ctx); s != nil {
		if s.created == nil {
			s.created = map[string]string{}
		}
		s.created[d.name] = created.ID
	}
	return nil
}

func (f *davFS) finishBackups(ctx context.Context, s *davRequestState, success bool) error {
	if len(s.backups) == 0 {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	for i := len(s.backups) - 1; i >= 0; i-- {
		b := s.backups[i]
		m := b.mount
		release, reserveErr := f.reserve(cleanup, b.target)
		if reserveErr != nil {
			return reserveErr
		}
		e := func() error {
			target, backup, e := b.snapshot(cleanup)
			if e != nil {
				return fmt.Errorf("目标及备份无法核验；原路径 %s，待核验备份 %s：%w", b.target, b.backupName, e)
			}
			if backup == nil && !success && target != nil && sameCheckpointObject(*target, b.entry) {
				return nil // Backup rename was not executed, or restoration already succeeded.
			}
			if backup == nil || !sameCheckpointObject(*backup, b.entry) {
				return fmt.Errorf("备份身份或版本无法确认，未执行清理或恢复；原路径 %s，待核验备份 %s", b.target, b.backupName)
			}
			if success {
				if target == nil || s.created[b.target] == "" || s.created[b.target] != target.ID {
					return fmt.Errorf("提交目标身份无法确认；旧内容保留于 %s", b.backupName)
				}
				if e := b.nativeDrive().Remove(cleanup, *backup); e != nil {
					return fmt.Errorf("目标已提交，但旧内容备份 %s 未清理：%w", b.backupName, e)
				}
				return nil
			}
			if target != nil {
				if s.created[b.target] == "" || s.created[b.target] != target.ID {
					return fmt.Errorf("操作失败，目标路径已被其他操作占用；旧内容保留于 %s", b.backupName)
				}
				partial := "CloudProbe-partial-" + randomKey()[:12] + path.Ext(target.Name)
				if e = b.nativeDrive().Move(cleanup, *target, "", partial); e != nil {
					return fmt.Errorf("保留未完成目标失败；原内容仍在 %s：%w", b.backupName, e)
				}
			}
			moveErr := b.nativeDrive().Move(cleanup, *backup, "", b.entry.Name)
			target, backup, e = b.snapshot(cleanup)
			if e != nil || target == nil || !sameCheckpointObject(*target, b.entry) || backup != nil {
				return fmt.Errorf("恢复原目标名称未确认；原路径 %s，待核验备份 %s：%w", b.target, b.backupName, errors.Join(moveErr, e, errors.New("恢复后的对象关系未确认")))
			}
			return nil
		}()
		f.invalidate(m)
		release()
		if e != nil {
			return e
		}
	}
	return nil
}

func validDAVURL(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("网盘返回的传输 URL 无效")
	}
	return nil
}

// Each mount can explicitly select a compatible upload cache. Legacy mounts
// inherit the service setting; no body silently changes from memory to disk.
func (f *davFS) uploadStorage(m *davMount) string {
	if m != nil && m.UploadStorageMode != "" {
		return m.UploadStorageMode
	}
	if f.transfers != nil {
		return f.transfers.options.UploadStorageMode
	}
	return "disk"
}
func (f *davFS) memoryUploadAt(name string) bool {
	if f.transfers == nil {
		return false
	}
	clean, err := davPath(name)
	if err != nil {
		return false
	}
	alias := strings.SplitN(strings.TrimPrefix(clean, "/"), "/", 2)[0]
	for _, m := range f.mountSnapshot() {
		if m.Alias == alias {
			return f.uploadStorage(m) == "memory"
		}
	}
	return false
}

func (f *davFS) mountSnapshot() []*davMount {
	f.mountMu.RLock()
	defer f.mountMu.RUnlock()
	return append([]*davMount{}, f.mounts...)
}

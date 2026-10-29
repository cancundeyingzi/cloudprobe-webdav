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
}
type davCache struct {
	entries []davEntry
	until   time.Time
}
type davFS struct {
	mounts       []*davMount
	spool        string
	maxUpload    int64
	cacheMu      sync.Mutex
	cache        map[string]davCache
	quotas       map[string]*davQuotaCache
	cacheVersion uint64
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
func (f *davFS) listing(ctx context.Context, m *davMount, dir string) ([]davEntry, error) {
	k := m.Alias + "\x00" + dir
	f.cacheMu.Lock()
	if c, ok := f.cache[k]; ok && time.Now().Before(c.until) {
		f.cacheMu.Unlock()
		logDAV(ctx, runLogEntry{Operation: "读取目录", Source: "本机目录缓存", Result: "命中", Count: len(c.entries), Detail: "缓存有效期 3 秒；本次未请求网盘接口"})
		return c.entries, nil
	}
	// Do not reinsert a listing that raced a completed mutation.
	version := f.cacheVersion
	f.cacheMu.Unlock()
	a, e := m.drive.List(ctx, dir)
	if e != nil {
		return nil, davCloudError(ctx, e)
	}
	for _, it := range a {
		if it.Name == "" || it.Name == "." || it.Name == ".." || strings.ContainsAny(it.Name, "/\\\x00\r\n") || it.ID == "" {
			return nil, davCloudError(ctx, errors.New("云盘返回了缺少名称、ID 或含路径分隔符的条目，停止映射目录"))
		}
	}
	f.cacheMu.Lock()
	if f.cache == nil {
		f.cache = map[string]davCache{}
	}
	if f.cacheVersion == version {
		f.cache[k] = davCache{entries: a, until: time.Now().Add(3 * time.Second)}
	}
	f.cacheMu.Unlock()
	return a, nil
}
func (f *davFS) invalidate(m *davMount) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.cacheVersion++
	quotaKey, _ := quotaAccountKey(m)
	if q := f.quotas[quotaKey]; q != nil {
		q.epoch.Add(1)
	}
	for k := range f.cache {
		if strings.HasPrefix(k, m.Alias+"\x00") {
			delete(f.cache, k)
		}
	}
}
func (f *davFS) lookup(ctx context.Context, name string) (davNode, error) {
	clean, e := davPath(name)
	if e != nil {
		return davNode{}, e
	}
	if clean == "/" {
		return davNode{entry: davEntry{Item: Item{ID: "virtual-root", Name: "/", Directory: true}}, virtual: true}, nil
	}
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	var m *davMount
	for _, candidate := range f.mounts {
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
		entries, e := f.listing(scopedDAV(ctx, m, currentPath), m, n.entry.ID)
		if e != nil {
			return davNode{}, e
		}
		var found *davEntry
		for i := range entries {
			if entries[i].Name == part {
				if found != nil {
					return davNode{}, davCloudError(ctx, errors.New("云盘同一目录存在同名条目，无法唯一定位 WebDAV 路径"))
				}
				found = &entries[i]
			}
		}
		if found == nil {
			return davNode{}, os.ErrNotExist
		}
		n = davNode{entry: *found, mount: m, parent: n.entry.ID}
		currentPath = path.Join(currentPath, part)
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
			if e = n.mount.drive.Move(ctx, n.entry, "", backupName); e != nil {
				return davCloudError(ctx, e)
			}
			s.backups = append(s.backups, davBackup{mount: n.mount, entry: n.entry, parent: n.parent, target: clean, backupName: backupName})
			f.invalidate(n.mount)
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
		for _, it := range a {
			if it.Name == name && (size < 0 || it.Size == size) {
				if !sameCloudObject(it, expected) {
					return davCloudError(ctx, errors.New("目标名称已出现但对象身份不符，未确认操作成功"))
				}
				return nil
			}
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
		tmp, e := os.CreateTemp(f.spool, "upload-*")
		if e != nil {
			return nil, e
		}
		clean, _ := davPath(name)
		return &davFile{fs: f, ctx: scopedDAV(ctx, p.mount, clean), name: clean, node: davNode{mount: p.mount, parent: p.entry.ID, entry: davEntry{Item: Item{Name: base}, Modified: time.Now()}}, original: old, tmp: tmp, writing: true, spoolStarted: time.Now()}, nil
	}
	n, e := f.lookup(ctx, name)
	if e != nil {
		return nil, e
	}
	clean, _ := davPath(name)
	ctx = scopedDAV(ctx, n.mount, clean)
	df := &davFile{fs: f, ctx: ctx, name: clean, node: n}
	if !n.entry.Directory {
		if f.transfers != nil {
			df.remote = newTransferReader(ctx, n.mount, n.entry, f.transfers)
		} else {
			df.remote = &davReader{ctx: ctx, size: n.entry.Size, url: func() (string, error) {
				u, e := n.mount.drive.URL(ctx, n.entry)
				if e != nil {
					return "", davCloudError(ctx, e)
				}
				return u, nil
			}}
		}
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
		for _, m := range d.fs.mounts {
			entries = append(entries, davEntry{Item: Item{ID: m.Root, Name: m.Alias, Directory: true}})
		}
	} else {
		a, e := d.fs.listing(scopedDAV(d.ctx, d.node.mount, d.name), d.node.mount, d.node.entry.ID)
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
	defer os.Remove(d.tmp.Name())
	defer d.tmp.Close()
	logDAV(d.ctx, runLogEntry{Operation: "本地暂存", Source: "本机磁盘", Result: "写入结束", Bytes: d.spoolBytes, DurationMs: time.Since(d.spoolStarted).Milliseconds(), DiskWriteMs: d.spoolWriteTime.Milliseconds(), Detail: "阶段耗时含客户端收包；磁盘实际 Write 累计耗时单独记录"})
	s := davState(d.ctx)
	if d.failed || d.ctx.Err() != nil || (s != nil && (s.aborted || s.err != nil || (s.method == "PUT" && s.contentLength >= 0 && s.bodyBytes != s.contentLength))) {
		return errors.New("上传未完整接收，已丢弃本次暂存内容")
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
	cp, releaseCheckpoint, e := d.openCheckpoint()
	if e != nil {
		return davCloudError(d.ctx, e)
	}
	defer releaseCheckpoint()
	created, e := d.uploadWithCheckpoint(cp)
	f.invalidate(m)
	if e != nil {
		return davCloudError(d.ctx, e)
	}
	if d.committed == nil {
		err = d.commitUpload(created, fiSize(d.tmp), d.original)
	}
	if err == nil && cp != nil {
		if cleanup := cp.complete(); cleanup != nil {
			logDAV(d.ctx, runLogEntry{Operation: "清理续传记录", Source: "本机", Result: "待清理", Error: cleanup.Error()})
		}
	}
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
			if success {
				if e := m.drive.Remove(cleanup, b.entry); e != nil {
					return fmt.Errorf("目标已提交，但旧内容备份 %s 未清理：%w", b.backupName, e)
				}
				return nil
			}
			f.invalidate(m)
			current, e := f.lookup(cleanup, b.target)
			if e == nil {
				if s.created[b.target] != current.entry.ID {
					return fmt.Errorf("操作失败，目标路径已被其他操作占用；旧内容保留于 %s", b.backupName)
				}
				partial := "CloudProbe-partial-" + randomKey()[:12] + path.Ext(current.entry.Name)
				if e = m.drive.Move(cleanup, current.entry, "", partial); e != nil {
					return fmt.Errorf("保留未完成目标失败；原内容仍在 %s：%w", b.backupName, e)
				}
			} else if !os.IsNotExist(e) {
				return fmt.Errorf("目标状态无法确认；原内容保留于 %s：%w", b.backupName, e)
			}
			restore := b.entry
			restore.Name = b.backupName
			if e = m.drive.Move(cleanup, restore, "", b.entry.Name); e != nil {
				return fmt.Errorf("恢复原目标名称失败；旧内容保留于 %s：%w", b.backupName, e)
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

type davReader struct {
	ctx       context.Context
	size, pos int64
	body      io.ReadCloser
	url       func() (string, error)
	started   time.Time
	readBytes int64
	host      string
	readErr   error
}

func (d *davReader) Close() {
	if d.body != nil {
		d.body.Close()
		d.body = nil
		e := runLogEntry{Operation: "下载正文", Source: "云盘节点", Endpoint: d.host, Result: "读取结束", Bytes: d.readBytes, DurationMs: time.Since(d.started).Milliseconds(), BytesPerSecond: rateFor(d.readBytes, time.Since(d.started)), Detail: "实际从节点读取的字节数；可能包含客户端探测或分段读取，向客户端发送量另计"}
		if d.readErr != nil {
			e.Result, e.Error = "失败", d.readErr.Error()
		}
		logDAV(d.ctx, e)
	}
}
func (d *davReader) Seek(off int64, whence int) (int64, error) {
	var n int64
	switch whence {
	case io.SeekStart:
		n = off
	case io.SeekCurrent:
		n = d.pos + off
	case io.SeekEnd:
		n = d.size + off
	default:
		return 0, os.ErrInvalid
	}
	if n < 0 {
		return 0, os.ErrInvalid
	}
	if n != d.pos {
		d.Close()
		d.pos = n
	}
	return d.pos, nil
}
func (d *davReader) Read(p []byte) (n int, err error) {
	defer func() {
		if err != nil && err != io.EOF {
			d.readErr = err
			if d.body == nil {
				logDAV(d.ctx, runLogEntry{Operation: "下载正文", Source: "云盘节点", Endpoint: d.host, Result: "失败", Error: err.Error()})
			}
		}
	}()
	if len(p) == 0 {
		return 0, nil
	}
	if d.pos >= d.size {
		return 0, io.EOF
	}
	if d.body == nil {
		d.started, d.readBytes, d.readErr = time.Now(), 0, nil
		for attempt := 0; attempt < 2; attempt++ {
			u, e := d.url()
			if e != nil {
				return 0, e
			}
			if e = validDAVURL(u); e != nil {
				return 0, e
			}
			d.host = nodeHost(u)
			req, e := http.NewRequestWithContext(d.ctx, "GET", u, nil)
			if e != nil {
				return 0, e
			}
			req.Header.Set("User-Agent", "CloudProbe/"+version)
			if d.pos > 0 {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", d.pos))
			}
			hc := &http.Client{Transport: davDownloadTransport, CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 8 {
					return errors.New("下载重定向过多")
				}
				return validDAVURL(req.URL.String())
			}}
			resp, e := hc.Do(req)
			if e != nil {
				return 0, e
			}
			if (resp.StatusCode == 401 || resp.StatusCode == 403) && attempt == 0 {
				resp.Body.Close()
				logDAV(d.ctx, runLogEntry{Operation: "刷新下载地址", Source: "云盘节点", Result: "重试", Endpoint: resp.Request.URL.Host, Detail: fmt.Sprintf("节点 HTTP %d；重新获取下载地址", resp.StatusCode)})
				continue
			}
			if resp.StatusCode != 200 && resp.StatusCode != 206 {
				resp.Body.Close()
				return 0, fmt.Errorf("上游下载返回 HTTP %d", resp.StatusCode)
			}
			if d.pos > 0 {
				var start, end, total int64
				_, e = fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total)
				if resp.StatusCode != 206 || e != nil || start != d.pos || total != d.size || end < start {
					resp.Body.Close()
					return 0, errors.New("上游未按请求返回正确分段，不能伪报断点续传成功")
				}
			}
			d.body = resp.Body
			d.host = resp.Request.URL.Host
			logDAV(d.ctx, runLogEntry{Operation: "下载正文", Source: "云盘节点", Endpoint: d.host, Result: "开始读取", Detail: fmt.Sprintf("起始偏移 %d；HTTP %d", d.pos, resp.StatusCode)})
			break
		}
	}
	if d.body == nil {
		return 0, errors.New("下载地址刷新后仍不可访问")
	}
	if int64(len(p)) > d.size-d.pos {
		p = p[:d.size-d.pos]
	}
	n, e := d.body.Read(p)
	d.pos += int64(n)
	d.readBytes += int64(n)
	if tr, _ := d.ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
		tr.downloaded.Add(int64(n))
	}
	if e == io.EOF && d.pos < d.size {
		d.readErr = io.ErrUnexpectedEOF
		return n, io.ErrUnexpectedEOF
	}
	if e != nil && e != io.EOF {
		d.readErr = e
	}
	return n, e
}

var davDownloadTransport = &http.Transport{Proxy: http.ProxyFromEnvironment, ResponseHeaderTimeout: 35 * time.Second, TLSHandshakeTimeout: 15 * time.Second, IdleConnTimeout: 60 * time.Second, MaxIdleConnsPerHost: 8}

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
	for _, m := range f.mounts {
		if m.Alias == alias {
			return f.uploadStorage(m) == "memory"
		}
	}
	return false
}

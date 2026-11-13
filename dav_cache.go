package main

import (
	cachelist "container/list"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	davDirectoryTTL        = 3 * time.Second
	davCacheMaxDirectories = 256
	davCacheMaxEntries     = 50000
	davCacheMaxBytes       = 32 << 20
)

type davCache struct {
	mount   *davMount
	entries []davEntry
	until   time.Time
	bytes   int
	order   *cachelist.Element
}

type davDirCache struct {
	mu           sync.Mutex
	entries      map[string]davCache
	epochs       map[string]uint64
	order        cachelist.List // Insertion order also orders expiration; reads do not extend TTL.
	items, bytes int
	now          func() time.Time
}

func (c *davDirCache) timeNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// Caller owns c.mu. All removals update the capacity accounting together.
func (c *davDirCache) remove(key string) {
	if entry, ok := c.entries[key]; ok {
		c.order.Remove(entry.order)
		c.items -= len(entry.entries)
		c.bytes -= entry.bytes
		delete(c.entries, key)
	}
}

func (c *davDirCache) expire(now time.Time) {
	for oldest := c.order.Front(); oldest != nil; oldest = c.order.Front() {
		key := oldest.Value.(string)
		if now.Before(c.entries[key].until) {
			break
		}
		c.remove(key)
	}
}

func directoryCacheBytes(entries []davEntry) int {
	size := len(entries) * int(unsafe.Sizeof(davEntry{}))
	for _, e := range entries {
		size += len(e.ID) + len(e.FID) + len(e.Name) + len(e.Suffix) + len(e.Kind) + len(e.SampleParserDrop) + len(e.FilterReason) + len(e.Parent) + len(e.ContentVersion)
		if e.backupPath != nil {
			size += backupPathNodeBytes(e.backupPath)
		}
	}
	return size
}

func (c *davDirCache) get(mount *davMount, dir string) ([]davEntry, uint64, bool) {
	alias := mount.Alias
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire(c.timeNow())
	entry, ok := c.entries[alias+"\x00"+dir]
	if !ok || entry.mount != mount {
		return nil, c.epochs[alias], false
	}
	return entry.entries, c.epochs[alias], true
}

func (c *davDirCache) put(mount *davMount, dir string, entries []davEntry, epoch uint64) {
	alias := mount.Alias
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.timeNow()
	c.expire(now)
	if c.epochs[alias] != epoch {
		return
	}
	key := alias + "\x00" + dir
	c.remove(key)
	bytes := directoryCacheBytes(entries)
	if len(entries) > davCacheMaxEntries || bytes > davCacheMaxBytes {
		return
	}
	for len(c.entries) >= davCacheMaxDirectories || c.items+len(entries) > davCacheMaxEntries || c.bytes+bytes > davCacheMaxBytes {
		c.remove(c.order.Front().Value.(string))
	}
	if c.entries == nil {
		c.entries = map[string]davCache{}
	}
	c.entries[key] = davCache{mount: mount, entries: slices.Clone(entries), until: now.Add(davDirectoryTTL), bytes: bytes, order: c.order.PushBack(key)}
	c.items += len(entries)
	c.bytes += bytes
}

func (c *davDirCache) invalidate(alias string) {
	c.invalidateDirectory(alias, "")
}

func (c *davDirCache) invalidateDirectory(alias, dir string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expire(c.timeNow())
	if c.epochs == nil {
		c.epochs = map[string]uint64{}
	}
	c.epochs[alias]++
	for key := range c.entries {
		if dir == "" && strings.HasPrefix(key, alias+"\x00") || key == alias+"\x00"+dir {
			c.remove(key)
		}
	}
	return c.epochs[alias]
}

func (c *davDirCache) invalidatePath(name string) {
	clean, err := davPath(name)
	if err != nil {
		return
	}
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return
	}
	c.invalidate(parts[0])
}

func (f *davFS) listing(ctx context.Context, m *davMount, dir string) ([]davEntry, error) {
	if f.readSnapshot(ctx) != nil {
		epoch := f.dirCache.invalidateDirectory(m.Alias, dir)
		entries, err := f.queryDirectory(ctx, m, dir)
		if err == nil {
			f.dirCache.put(m, dir, entries, epoch)
		}
		return entries, err
	}
	entries, epoch, ok := f.dirCache.get(m, dir)
	if ok {
		logDAV(ctx, runLogEntry{Operation: "读取目录", Source: "本机目录缓存", Result: "命中", Count: len(entries), Detail: "缓存有效期 3 秒；本次未请求网盘接口"})
		return entries, nil
	}
	entries, err := f.queryDirectory(ctx, m, dir)
	if err == nil {
		f.dirCache.put(m, dir, entries, epoch)
	}
	return entries, err
}

func (f *davFS) queryDirectory(ctx context.Context, m *davMount, dir string) ([]davEntry, error) {
	entries, err := m.drive.List(ctx, dir)
	if err != nil {
		return nil, davCloudError(ctx, err)
	}
	for _, entry := range entries {
		if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\\\x00\r\n") || entry.ID == "" {
			return nil, davCloudError(ctx, errors.New("云盘返回了缺少名称、ID 或含路径分隔符的条目，停止映射目录"))
		}
	}
	return entries, nil
}

type davReadSnapshotKey struct{}
type davReadSnapshot struct {
	mu     sync.Mutex
	fs     *davFS
	mounts []*davMount
	nodes  map[string]davNode
}

func (f *davFS) freshReadContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, davReadSnapshotKey{}, &davReadSnapshot{fs: f, mounts: f.mountSnapshot(), nodes: map[string]davNode{}})
}

func (f *davFS) readSnapshot(ctx context.Context) *davReadSnapshot {
	snapshot, _ := ctx.Value(davReadSnapshotKey{}).(*davReadSnapshot)
	if snapshot != nil && snapshot.fs == f {
		return snapshot
	}
	return nil
}

func (f *davFS) invalidate(m *davMount) {
	f.dirCache.invalidate(m.Alias)
	f.cacheMu.Lock()
	quotaKey, _ := quotaAccountKey(m)
	if q := f.quotas[quotaKey]; q != nil {
		q.epoch.Add(1)
	}
	f.cacheMu.Unlock()
}

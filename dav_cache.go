package main

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"time"
)

type davDirCache struct {
	mu      sync.Mutex
	entries map[string]davCache
	epochs  map[string]uint64
}

func (c *davDirCache) get(alias, dir string) ([]davEntry, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[alias+"\x00"+dir]
	if !ok || !time.Now().Before(entry.until) {
		return nil, c.epochs[alias], false
	}
	return entry.entries, c.epochs[alias], true
}

func (c *davDirCache) put(alias, dir string, entries []davEntry, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]davCache{}
	}
	if c.epochs[alias] == epoch {
		c.entries[alias+"\x00"+dir] = davCache{entries: entries, until: time.Now().Add(3 * time.Second)}
	}
}

func (c *davDirCache) invalidate(alias string) {
	c.invalidateDirectory(alias, "")
}

func (c *davDirCache) invalidateDirectory(alias, dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epochs == nil {
		c.epochs = map[string]uint64{}
	}
	c.epochs[alias]++
	for key := range c.entries {
		if dir == "" && strings.HasPrefix(key, alias+"\x00") || key == alias+"\x00"+dir {
			delete(c.entries, key)
		}
	}
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
	entries, epoch, ok := f.dirCache.get(m.Alias, dir)
	if ok {
		logDAV(ctx, runLogEntry{Operation: "读取目录", Source: "本机目录缓存", Result: "命中", Count: len(entries), Detail: "缓存有效期 3 秒；本次未请求网盘接口"})
		return entries, nil
	}
	entries, err := m.drive.List(ctx, dir)
	if err != nil {
		return nil, davCloudError(ctx, err)
	}
	for _, entry := range entries {
		if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\\\x00\r\n") || entry.ID == "" {
			return nil, davCloudError(ctx, errors.New("云盘返回了缺少名称、ID 或含路径分隔符的条目，停止映射目录"))
		}
	}
	f.dirCache.put(m.Alias, dir, entries, epoch)
	return entries, nil
}

func (f *davFS) refreshReadPath(ctx context.Context, name string) error {
	clean, err := davPath(name)
	if err != nil {
		return err
	}
	parent, err := f.lookup(ctx, path.Dir(clean))
	if err != nil {
		return err
	}
	if parent.mount != nil {
		f.dirCache.invalidateDirectory(parent.mount.Alias, parent.entry.ID)
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

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// Quotas describe the account's storage pool, including data outside a mounted
// subdirectory. Never compute them by summing visible files or local disk space.
type davQuota struct {
	Total int64
	Used  int64
}

func (q davQuota) available() int64 { return max(0, q.Total-q.Used) }
func (q davQuota) validate() error {
	if q.Total < 0 || q.Used < 0 {
		return errors.New("容量接口返回负数或超出整数范围")
	}
	return nil
}

type davQuotaReader interface {
	Quota(context.Context) (davQuota, error)
}

type davQuotaCache struct {
	mu      contextMutex
	epoch   atomic.Uint64
	version uint64
	value   davQuota
	err     error
	until   time.Time
}

func quotaAccountKey(m *davMount) (string, bool) {
	// Older profiles may lack a stable account identifier. Do not deduplicate
	// those mounts using the hash of an empty account or a mount root ID.
	if m.Identity != "" && m.Identity != davIdentityHash(m.Provider, "") {
		return m.Provider + ":" + m.Identity, true
	}
	return "mount:" + m.Alias, false
}

func quotaDriveSupported(d davDrive) bool {
	if b, ok := d.(*backupDAVDrive); ok {
		return quotaDriveSupported(b.primary)
	}
	_, ok := d.(davQuotaReader)
	return ok
}

func (f *davFS) quotaMounts(name string) ([]*davMount, error) {
	clean, err := davPath(name)
	if err != nil {
		return nil, err
	}
	if clean != "/" {
		alias := strings.SplitN(strings.TrimPrefix(clean, "/"), "/", 2)[0]
		for _, m := range f.mounts {
			if m.Alias == alias && quotaDriveSupported(m.drive) {
				return []*davMount{m}, nil
			}
		}
		return nil, errors.ErrUnsupported
	}
	if len(f.mounts) == 0 {
		return nil, errors.ErrUnsupported
	}
	seen := map[string]bool{}
	var mounts []*davMount
	for _, m := range f.mounts {
		key, known := quotaAccountKey(m)
		if !quotaDriveSupported(m.drive) || !known && len(f.mounts) > 1 {
			return nil, errors.ErrUnsupported
		}
		if !seen[key] {
			mounts = append(mounts, m)
			seen[key] = true
		}
	}
	return mounts, nil
}

// QuotaSupported and Quota implement the optional RFC 4331 extension to the
// vendored webdav package. Capability discovery makes no upstream requests.
func (f *davFS) QuotaSupported(name string) bool {
	_, err := f.quotaMounts(name)
	return err == nil
}

func (f *davFS) quotaEntry(m *davMount) *davQuotaCache {
	key, _ := quotaAccountKey(m)
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if f.quotas == nil {
		f.quotas = map[string]*davQuotaCache{}
	}
	if f.quotas[key] == nil {
		f.quotas[key] = &davQuotaCache{}
	}
	return f.quotas[key]
}

func (f *davFS) mountQuota(ctx context.Context, m *davMount) (davQuota, error) {
	c := f.quotaEntry(m)
	if err := c.mu.LockContext(ctx); err != nil {
		return davQuota{}, err
	}
	defer c.mu.Unlock()
	epoch := c.epoch.Load()
	if c.version == epoch && time.Now().Before(c.until) {
		logDAV(scopedDAV(ctx, m, ""), runLogEntry{Operation: "查询网盘容量", Source: "本机容量缓存", Result: "命中", Detail: "成功结果缓存 30 秒；失败短暂缓存 5 秒；网盘写入后失效"})
		return c.value, c.err
	}
	started := time.Now()
	queryCtx, cancel := context.WithTimeout(scopedDAV(ctx, m, ""), 10*time.Second)
	defer cancel()
	q, err := m.drive.(davQuotaReader).Quota(queryCtx)
	if err == nil {
		err = q.validate()
	}
	entry := runLogEntry{Operation: "查询网盘容量", Source: "网盘容量接口", Result: "成功", DurationMs: time.Since(started).Milliseconds()}
	if err != nil {
		entry.Result, entry.Error = "失败", err.Error()
	} else {
		entry.Detail = fmt.Sprintf("账号总空间 %d 字节；已用 %d 字节；剩余 %d 字节；子目录共享账号配额", q.Total, q.Used, q.available())
	}
	logDAV(queryCtx, entry)
	// A cancellation belongs to this request, not to later callers. In-flight
	// queries racing a write retain their old epoch and cannot repopulate cache.
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.value, c.err, c.version = q, err, epoch
		ttl := 30 * time.Second
		if err != nil {
			ttl = 5 * time.Second
		}
		c.until = time.Now().Add(ttl)
	}
	return q, err
}

func (f *davFS) Quota(ctx context.Context, name string) (usedBytes, availableBytes int64, err error) {
	mounts, err := f.quotaMounts(name)
	if err != nil {
		return 0, 0, err
	}
	// Use a single deadline for virtual-root aggregation as well.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, m := range mounts {
		q, err := f.mountQuota(ctx, m)
		if err != nil {
			return 0, 0, err // Never present a partial aggregate as the total.
		}
		available := q.available()
		if q.Used > math.MaxInt64-usedBytes || available > math.MaxInt64-availableBytes {
			return 0, 0, errors.New("多个网盘容量合计超出整数范围")
		}
		usedBytes += q.Used
		availableBytes += available
	}
	return usedBytes, availableBytes, nil
}

func (b *backupDAVDrive) Quota(ctx context.Context) (davQuota, error) {
	if q, ok := b.primary.(davQuotaReader); ok {
		return q.Quota(ctx)
	}
	return davQuota{}, errors.ErrUnsupported
}

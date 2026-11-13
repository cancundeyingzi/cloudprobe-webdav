package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type readTarget struct {
	URL     string
	Headers http.Header
}
type readRoute struct {
	Name, Version string
	Resolve       func(context.Context) (readTarget, error)
}
type readRouteDrive interface {
	ReadRoutes(context.Context, davEntry) []readRoute
}
type routeFailure struct {
	count int
	until time.Time
}
type routeHealth struct {
	mu   sync.Mutex
	fail map[string]routeFailure
}

func (h *routeHealth) bad(name string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail == nil {
		h.fail = map[string]routeFailure{}
	}
	s := h.fail[name]
	s.count++
	if s.count >= 2 {
		s.until = time.Now().Add(15 * time.Second)
	}
	h.fail[name] = s
}
func (h *routeHealth) good(name string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	delete(h.fail, name)
	h.mu.Unlock()
}
func (h *routeHealth) cool(name string) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Now().Before(h.fail[name].until)
}
func routesFor(ctx context.Context, d davDrive, it davEntry) []readRoute {
	if driver, ok := d.(readRouteDrive); ok {
		return driver.ReadRoutes(ctx, it)
	}
	return []readRoute{{Name: "云盘下载地址", Resolve: func(c context.Context) (readTarget, error) { u, e := d.URL(c, it); return readTarget{URL: u}, e }}}
}
func (d *wopanDAVDrive) ReadRoutes(ctx context.Context, it davEntry) []readRoute {
	version := ""
	if it.FID != "" {
		version = "wopan-fid:" + it.FID
	}
	routes := []readRoute{
		{Name: "网易代理", Version: version, Resolve: func(c context.Context) (readTarget, error) {
			u, e := d.originalDAVDrive.URL(c, it)
			return readTarget{URL: u}, e
		}},
		{Name: "联通直连 V2", Version: version, Resolve: func(c context.Context) (readTarget, error) { return d.readDirectURL(c, it, true) }},
		{Name: "联通直连 V1", Version: version, Resolve: func(c context.Context) (readTarget, error) { return d.readDirectURL(c, it, false) }},
	}
	if d.client.bridge == nil && d.client.cfg.Transport == "bridge" {
		return routes[1:]
	}
	return routes

}
func (d *wopanDAVDrive) readDirectURL(ctx context.Context, it davEntry, v2 bool) (target readTarget, err error) {
	if e := d.directMu.LockContext(ctx); e != nil {
		return target, e
	}
	defer d.directMu.Unlock()
	d.syncAccountCredential()
	started := time.Now()
	endpoint := "GetDownloadUrl"
	if v2 {
		endpoint = "GetDownloadUrlV2"
	}
	defer func() { logURL(ctx, d.source(), endpoint, started, target.URL, err) }()
	if it.FID == "" {
		return target, errors.New("缺少 FID，不能定位直连下载对象")
	}
	for attempt := 0; attempt < 2; attempt++ {
		if v2 {
			data, e := d.w.GetDownloadUrlV2([]string{it.FID}, woContext(ctx))
			err = e
			if e == nil {
				for _, x := range data.List {
					if x.Fid == it.FID && validDAVURL(x.DownloadUrl) == nil {
						target.URL = x.DownloadUrl
						return target, nil
					}
				}
			}
		}
		if !v2 {
			data, e := d.w.GetDownloadUrl("0", []string{it.FID}, woContext(ctx))
			err = e
			if e == nil {
				for _, x := range data {
					if x.Fid == it.FID && validDAVURL(x.DownloadUrl) == nil {
						target.URL = x.DownloadUrl
						return target, nil
					}
				}
			}
		}
		if attempt == 0 && err != nil {
			if d.otherAuthorization() {
				continue
			}
			if !wopanAuthError(err) {
				break
			}
			if e := d.refreshNative(ctx); e == nil {
				continue
			}
		}
		break
	}
	if err == nil {
		err = errors.New("未返回指定 FID 的可用下载地址")
	}
	return target, err
}
func (d *mobileDAVDrive) ReadRoutes(ctx context.Context, it davEntry) []readRoute {
	return []readRoute{
		{Name: "网易代理", Resolve: func(c context.Context) (readTarget, error) {
			u, e := d.originalDAVDrive.URL(c, it)
			return readTarget{URL: u}, e
		}},
		{Name: "移动官方开放接口", Resolve: func(c context.Context) (readTarget, error) {
			if e := d.mu.LockContext(c); e != nil {
				return readTarget{}, e
			}
			defer d.mu.Unlock()
			cl, e := d.directClient()
			if e != nil {
				return readTarget{}, e
			}
			u, e := cl.downloadURL(c, it.ID, it.FID)
			if mobileAuthError(e) {
				if d.refresh(c) == nil {
					u, e = cl.downloadURL(c, it.ID, it.FID)
				}
			}
			return readTarget{URL: u}, e
		}},
	}
}

type resolvedReadRoute struct {
	denials          forbiddenRetries
	route            readRoute
	mu               contextMutex
	target           readTarget
	until            time.Time
	generation       uint64
	failedGeneration uint64
}

func (r *resolvedReadRoute) get(ctx context.Context) (readTarget, uint64, error) {
	if err := r.mu.LockContext(ctx); err != nil {
		return readTarget{}, 0, err
	}
	defer r.mu.Unlock()
	if err := r.denials.stopped(); err != nil {
		return readTarget{}, r.generation, err
	}
	if r.target.URL != "" && time.Now().Before(r.until) {
		return r.target, r.generation, nil
	}
	r.generation++
	t, e := r.route.Resolve(ctx)
	if e == nil {
		e = validDAVURL(t.URL)
	}
	if e == nil {
		r.target, r.until = t, time.Now().Add(30*time.Second)
	}
	return t, r.generation, e
}
func (r *resolvedReadRoute) invalidate(generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if generation != r.generation {
		return
	}
	r.target = readTarget{}
	r.until = time.Time{}
}

// A batch of requests sharing one address counts as one failed refresh round.
// Late responses may not invalidate or penalize a newer address.
func (r *resolvedReadRoute) failure(generation uint64, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if terminal := r.denials.stopped(); terminal != nil {
		return terminal
	}
	if generation != r.generation || r.failedGeneration == generation {
		return err
	}
	r.failedGeneration = generation
	r.target, r.until = readTarget{}, time.Time{}
	return r.denials.failure(err)
}

func (r *resolvedReadRoute) succeeded(generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if generation == r.generation {
		r.denials.failure(nil)
	}
}

type downloadSignature struct {
	mu                              sync.Mutex
	set                             bool
	etag, modified, version, source string
}

func (s *downloadSignature) accept(route readRoute, h http.Header) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	etag := h.Get("ETag")
	parsed, parseErr := parseDAVTags(etag)
	if parseErr != nil || parsed.any || len(parsed.tags) != 1 || len(etag) >= 2 && etag[:2] == "W/" {
		etag = ""
	}
	modified := h.Get("Last-Modified")
	changedAt, modifiedErr := http.ParseTime(modified)
	responseAt, dateErr := http.ParseTime(h.Get("Date"))
	if modifiedErr != nil || dateErr != nil || responseAt.Sub(changedAt) < time.Minute {
		modified = ""
	}
	if !s.set {
		s.set, s.etag, s.modified, s.version, s.source = true, etag, modified, route.Version, route.Name
		return nil
	}
	if s.version != "" && s.version == route.Version {
		if route.Name == s.source && etag != "" && s.etag != "" && etag != s.etag {
			return errors.New("同一下载来源的 ETag 已变化")
		}
		return nil
	}
	if s.etag != "" {
		if etag == s.etag {
			return nil
		}
		return errors.New("下载对象强 ETag 已变化或缺失，停止混合不同版本")
	}
	if route.Name == s.source && modified != "" && modified == s.modified {
		return nil
	}
	return fmt.Errorf("无法确认 %s 与已读取对象为同一版本，停止续传", route.Name)
}

func (s *downloadSignature) condition(route readRoute) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set && s.source == route.Name {
		if s.etag != "" {
			return s.etag
		}
		return s.modified
	}
	return ""
}

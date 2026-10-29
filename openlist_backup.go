package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type openListInput struct {
	Enabled      bool   `json:"enabled"`
	Address      string `json:"address"`
	Root         string `json:"root"`
	Token        string `json:"token,omitempty"`
	MetaPassword string `json:"meta_password,omitempty"`
	VerifyWrite  bool   `json:"verify_write"`
}
type openListSaved struct {
	openListInput
	Identity string `json:"identity"`
	Verified bool   `json:"verified"`
}
type openListClient struct {
	cfg    openListInput
	client *http.Client
	budget *fairLimiter
}

func nonnegativeInteger(v any) (int64, error) {
	switch x := v.(type) {
	case float64:
		if x >= 0 && x <= 9007199254740991 && float64(int64(x)) == x {
			return int64(x), nil
		}
	case json.Number:
		n, e := strconv.ParseInt(string(x), 10, 64)
		if e == nil && n >= 0 {
			return n, nil
		}
	case string:
		n, e := strconv.ParseInt(x, 10, 64)
		if e == nil && n >= 0 {
			return n, nil
		}
	}
	return 0, errors.New("缺少有效的非负整数字段")
}

func newOpenListClient(cfg openListInput) (*openListClient, error) {
	u, e := url.Parse(cfg.Address)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("OpenList 地址无效")
	}
	if e = validDownloadURL(cfg.Address); e != nil {
		return nil, errors.New("OpenList 地址需为 HTTPS 或本机 HTTP")
	}
	clean, e := davPath(cfg.Root)
	if e != nil {
		return nil, e
	}
	cfg.Root = clean
	cfg.Address = strings.TrimRight(cfg.Address, "/")
	return &openListClient{cfg: cfg, client: providerMetadataClient(2 * time.Minute)}, nil
}
func (c *openListClient) call(ctx context.Context, method, endpoint string, input any, stream io.Reader, length int64, headers http.Header) (map[string]any, error) {
	if endpoint == "/fs/put" && c.budget != nil {
		release, e := c.budget.acquire(ctx)
		if e != nil {
			return nil, e
		}
		defer release()
	}
	if stream == nil {
		b, e := json.Marshal(input)
		if e != nil {
			return nil, e
		}
		stream = bytes.NewReader(b)
		length = int64(len(b))
	}
	req, e := http.NewRequestWithContext(ctx, method, c.cfg.Address+"/api"+endpoint, stream)
	if e != nil {
		return nil, e
	}
	req.ContentLength = length
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("Authorization", c.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "CloudProbe/"+version)
	resp, e := c.client.Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	raw, e := readBounded(resp.Body, 16<<20)
	if e != nil {
		return nil, e
	}
	var result map[string]any
	if e = json.Unmarshal(raw, &result); e != nil {
		return nil, errors.New("OpenList 未返回有效 JSON")
	}
	code := number(result["code"])
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || code != 200 {
		return nil, fmt.Errorf("OpenList HTTP %d / 业务码 %d：%s", resp.StatusCode, code, valueString(result["message"]))
	}
	return object(result["data"]), nil
}
func (c *openListClient) listing(ctx context.Context, rel string) ([]davEntry, error) {
	var entries []davEntry
	seen := map[string]bool{}
	total := int64(-1)
	for page := 1; page <= 10000; page++ {
		data, e := c.call(ctx, "POST", "/fs/list", map[string]any{"path": path.Join(c.cfg.Root, rel), "password": c.cfg.MetaPassword, "page": page, "per_page": 100, "refresh": true}, nil, 0, nil)
		if e != nil {
			return nil, e
		}
		raw, has := data["content"]
		declared, hasTotal := data["total"]
		if !has || !hasTotal {
			return nil, errors.New("OpenList 目录缺少 content/total，不能视为空目录")
		}
		declaredCount, e := nonnegativeInteger(declared)
		if e != nil {
			return nil, errors.New("OpenList total 类型无效")
		}
		if raw == nil && declaredCount == 0 {
			return entries, nil
		}
		items, ok := raw.([]any)
		if !ok {
			return nil, errors.New("OpenList content 类型错误")
		}
		if total < 0 {
			total = declaredCount
		}
		if total != declaredCount {
			return nil, errors.New("OpenList 分页期间总数变化，需重新查询")
		}
		for _, raw := range items {
			x := object(raw)
			name := valueString(x["name"])
			if safeFilename(name) != nil {
				return nil, errors.New("OpenList 返回无效文件名")
			}
			dir, ok := x["is_dir"].(bool)
			if !ok {
				return nil, errors.New("OpenList 条目缺少目录标记")
			}
			size, e := nonnegativeInteger(x["size"])
			if e != nil {
				return nil, errors.New("OpenList 条目缺少有效大小")
			}
			if seen[name] {
				return nil, errors.New("OpenList 分页重复或存在同名条目")
			}
			seen[name] = true
			entry := davEntry{Item: Item{ID: "openlist:" + path.Join(rel, name), Name: name, Directory: dir, Size: size}, Modified: parseDAVTime(valueString(x["modified"]))}
			entries = append(entries, entry)
		}
		if int64(len(entries)) == total {
			return entries, nil
		}
		if len(items) == 0 || len(items) < 100 {
			return nil, errors.New("OpenList 分页未达到声明总数")
		}
	}
	return nil, errors.New("OpenList 分页达到上限")
}
func (c *openListClient) link(ctx context.Context, rel string, size int64) (readTarget, error) {
	x, e := c.call(ctx, "POST", "/fs/get", map[string]any{"path": path.Join(c.cfg.Root, rel), "password": c.cfg.MetaPassword}, nil, 0, nil)
	if e != nil {
		return readTarget{}, e
	}
	if valueString(x["name"]) != path.Base(rel) || number(x["size"]) != size || x["is_dir"] != false {
		return readTarget{}, errors.New("OpenList 返回对象与请求名称、大小、类型不符")
	}
	u := valueString(x["raw_url"])
	if e = validDAVURL(u); e != nil {
		return readTarget{}, e
	}
	return readTarget{URL: u}, nil
}
func (c *openListClient) proxyLink(ctx context.Context, rel string, size int64) (readTarget, error) {
	x, e := c.call(ctx, "POST", "/fs/get", map[string]any{"path": path.Join(c.cfg.Root, rel), "password": c.cfg.MetaPassword}, nil, 0, nil)
	if e != nil {
		return readTarget{}, e
	}
	if valueString(x["name"]) != path.Base(rel) || number(x["size"]) != size || x["is_dir"] != false {
		return readTarget{}, errors.New("OpenList 代理对象元数据不符")
	}
	u, e := url.Parse(c.cfg.Address)
	if e != nil {
		return readTarget{}, e
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/p" + path.Join("/", c.cfg.Root, rel)
	q := u.Query()
	if sign := valueString(x["sign"]); sign != "" {
		q.Set("sign", sign)
	}
	u.RawQuery = q.Encode()
	return readTarget{URL: u.String(), Headers: http.Header{"Authorization": []string{c.cfg.Token}}}, nil
}

type backupDAVDrive struct {
	primary davDrive
	backup  *openListClient
	root    string
	mu      sync.Mutex
	paths   map[string]string
	entries map[string]davEntry
}

func (b *backupDAVDrive) relative(id string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p, ok := b.paths[id]
	if !ok {
		return "", errors.New("OpenList 备用路径尚未建立，不能猜测云盘 ID 对应目录")
	}
	return p, nil
}
func (b *backupDAVDrive) remember(parent string, entries []davEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rel, ok := b.paths[parent]
	if !ok {
		return
	}
	for _, it := range entries {
		b.paths[it.ID] = path.Join(rel, it.Name)
		b.entries[it.ID] = it
	}
}
func (b *backupDAVDrive) moved(it davEntry, parent, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	old, ok := b.paths[it.ID]
	if !ok {
		return
	}
	base := path.Dir(old)
	if parent != "" {
		if dst, ok := b.paths[parent]; ok {
			base = dst
		}
	}
	next := path.Join(base, name)
	if next == "." {
		next = ""
	}
	for id, rel := range b.paths {
		if rel == old {
			b.paths[id] = next
		} else if strings.HasPrefix(rel, old+"/") {
			b.paths[id] = next + strings.TrimPrefix(rel, old)
		}
	}
}
func (b *backupDAVDrive) listBackup(ctx context.Context, parent string) ([]davEntry, error) {
	rel, e := b.relative(parent)
	if e != nil {
		return nil, e
	}
	started := time.Now()
	items, e := b.backup.listing(ctx, rel)
	logList(ctx, "OpenList 服务", "/api/fs/list", started, items, e)
	if e == nil {
		b.remember(parent, items)
	}
	return items, e
}
func (b *backupDAVDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	if strings.HasPrefix(parent, "openlist:") {
		return b.listBackup(ctx, parent)
	}
	items, e := b.primary.List(ctx, parent)
	if e == nil {
		b.remember(parent, items)
		return items, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	logFallback(ctx, "内置目录接口", "OpenList 服务", e)
	fallback, other := b.listBackup(ctx, parent)
	if other != nil {
		return nil, fmt.Errorf("内置：%v；OpenList：%w", e, other)
	}
	return fallback, nil
}
func (b *backupDAVDrive) ReadRoutes(ctx context.Context, it davEntry) []readRoute {
	var routes []readRoute
	if !strings.HasPrefix(it.ID, "openlist:") {
		routes = routesFor(ctx, b.primary, it)
	}
	routes = append(routes, readRoute{Name: "OpenList 服务", Resolve: func(c context.Context) (readTarget, error) {
		rel, e := b.relative(it.ID)
		if e != nil {
			return readTarget{}, e
		}
		return b.backup.link(c, rel, it.Size)
	}})
	routes = append(routes, readRoute{Name: "OpenList 代理正文", Resolve: func(c context.Context) (readTarget, error) {
		rel, e := b.relative(it.ID)
		if e != nil {
			return readTarget{}, e
		}
		return b.backup.proxyLink(c, rel, it.Size)
	}})
	return routes
}
func (b *backupDAVDrive) URL(ctx context.Context, it davEntry) (string, error) {
	var last error
	for _, r := range b.ReadRoutes(ctx, it) {
		t, e := r.Resolve(ctx)
		if e == nil {
			return t.URL, nil
		}
		last = e
	}
	return "", last
}
func safePreexecutionFailure(e error) bool {
	if e == nil {
		return false
	}
	var outcome *davMutationError
	if errors.As(e, &outcome) {
		return false
	}
	var notExecuted *davNotExecuted
	if errors.As(e, &notExecuted) {
		return true
	}
	return false
}
func (b *backupDAVDrive) findBackup(ctx context.Context, parent, name string) (davEntry, error) {
	items, e := b.listBackup(ctx, parent)
	if e != nil {
		return davEntry{}, e
	}
	for _, it := range items {
		if it.Name == name {
			return it, nil
		}
	}
	return davEntry{}, os.ErrNotExist
}
func (b *backupDAVDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	var err error
	if !strings.HasPrefix(parent, "openlist:") {
		it, e := b.primary.Mkdir(ctx, parent, name)
		if e == nil {
			b.remember(parent, []davEntry{it})
			return it, nil
		}
		err = e
		if !safePreexecutionFailure(e) {
			return davEntry{}, e
		}
	}
	rel, e := b.relative(parent)
	if e != nil {
		return davEntry{}, e
	}
	logFallback(ctx, "内置新建目录", "OpenList 服务", err)
	_, e = b.backup.call(ctx, "POST", "/fs/mkdir", map[string]any{"path": path.Join(b.backup.cfg.Root, rel, name)}, nil, 0, nil)
	if e != nil {
		return davEntry{}, e
	}
	return b.findBackup(ctx, parent, name)
}
func (b *backupDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	var primary error
	if !strings.HasPrefix(parent, "openlist:") {
		it, e := b.primary.Upload(ctx, parent, name, f)
		if e == nil {
			b.remember(parent, []davEntry{it})
			return it, nil
		}
		primary = e
		if ctx.Err() != nil {
			return davEntry{}, ctx.Err()
		}
		if !safePreexecutionFailure(e) {
			if found, check := b.findBackup(ctx, parent, name); check == nil {
				fi, _ := f.Stat()
				if fi != nil && found.Size == fi.Size() {
					return found, nil
				}
			}
			return davEntry{}, fmt.Errorf("内置上传结果不明确，未重放到 OpenList：%w", e)
		}
	}
	rel, e := b.relative(parent)
	if e != nil {
		return davEntry{}, e
	}
	fi, e := f.Stat()
	if e != nil {
		return davEntry{}, e
	}
	logFallback(ctx, "内置上传", "OpenList 服务", primary)
	headers := http.Header{"File-Path": []string{path.Join(b.backup.cfg.Root, rel, name)}, "Password": []string{b.backup.cfg.MetaPassword}}
	_, e = b.backup.call(ctx, "PUT", "/fs/put", nil, io.NewSectionReader(f, 0, fi.Size()), fi.Size(), headers)
	if e != nil {
		return davEntry{}, e
	}
	return b.findBackup(ctx, parent, name)
}
func (b *backupDAVDrive) Remove(ctx context.Context, it davEntry) error {
	var primary error
	if !strings.HasPrefix(it.ID, "openlist:") {
		primary = b.primary.Remove(ctx, it)
		if primary == nil {
			return nil
		}
		if !safePreexecutionFailure(primary) {
			return fmt.Errorf("删除结果不明确，未重放：%w", primary)
		}
	}
	rel, e := b.relative(it.ID)
	if e != nil {
		return e
	}
	logFallback(ctx, "内置删除", "OpenList 服务", primary)
	_, e = b.backup.call(ctx, "POST", "/fs/remove", map[string]any{"dir": path.Join(b.backup.cfg.Root, path.Dir(rel)), "names": []string{path.Base(rel)}}, nil, 0, nil)
	return e
}
func (b *backupDAVDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	var primary error
	if !strings.HasPrefix(it.ID, "openlist:") && !strings.HasPrefix(parent, "openlist:") {
		primary = b.primary.Move(ctx, it, parent, name)
		if primary == nil {
			b.moved(it, parent, name)
			return nil
		}
		if !safePreexecutionFailure(primary) {
			return fmt.Errorf("改名/移动结果不明确，未重放：%w", primary)
		}
	}
	rel, e := b.relative(it.ID)
	if e != nil {
		return e
	}
	logFallback(ctx, "内置改名/移动", "OpenList 服务", primary)
	if name != it.Name {
		_, e = b.backup.call(ctx, "POST", "/fs/rename", map[string]any{"path": path.Join(b.backup.cfg.Root, rel), "name": name}, nil, 0, nil)
		if e != nil {
			return e
		}
		b.moved(it, "", name)
		rel, _ = b.relative(it.ID)
	}
	if parent == "" {
		return nil
	}
	dst, e := b.relative(parent)
	if e != nil {
		return e
	}
	_, e = b.backup.call(ctx, "POST", "/fs/move", map[string]any{"src_dir": path.Join(b.backup.cfg.Root, path.Dir(rel)), "dst_dir": path.Join(b.backup.cfg.Root, dst), "names": []string{name}}, nil, 0, nil)
	if e == nil {
		b.moved(it, parent, name)
	}
	return e
}
func (g *guiApp) openListConfig(key string, in *openListInput, identity string) (*openListSaved, error) {
	file := g.data + string(os.PathSeparator) + "openlist-config.bin"
	all := map[string]openListSaved{}
	if raw, e := os.ReadFile(file); e == nil {
		plain, e := unprotectLocal(raw)
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(plain, &all); e != nil {
			return nil, e
		}
	}
	previous := all[key]
	if in == nil {
		if !previous.Enabled {
			return nil, nil
		}
		if previous.Identity != identity {
			previous.Verified = false
			previous.Identity = identity
		}
		return &previous, nil
	}
	if !in.Enabled {
		return nil, nil
	}
	copy := *in
	if copy.Token == "" && copy.Address == previous.Address {
		copy.Token = previous.Token
		copy.MetaPassword = previous.MetaPassword
	}
	verified := previous.Verified && previous.Identity == identity && previous.Address == copy.Address && previous.Root == copy.Root && previous.Token == copy.Token && previous.MetaPassword == copy.MetaPassword
	return &openListSaved{openListInput: copy, Identity: identity, Verified: verified}, nil
}
func (g *guiApp) saveOpenList(key string, cfg openListSaved) error {
	file := g.data + string(os.PathSeparator) + "openlist-config.bin"
	all := map[string]openListSaved{}
	if raw, e := os.ReadFile(file); e == nil {
		plain, e := unprotectLocal(raw)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(plain, &all); e != nil {
			return e
		}
	}
	all[key] = cfg
	plain, e := json.Marshal(all)
	if e != nil {
		return e
	}
	protected, e := protectLocal(plain)
	if e != nil {
		return e
	}
	return writePrivateBytes(file, protected)
}
func verifyOpenList(ctx context.Context, primary davDrive, root string, backup *openListClient, profile string) (err error) {
	if !backup.cfg.VerifyWrite {
		return errors.New("首次启用 OpenList 需勾选允许创建并清理专用验证文件，以确认同一云盘目录")
	}
	f, e := os.CreateTemp(profile, "openlist-proof-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	content := []byte("CloudProbe mapping proof " + randomKey())
	if _, e = f.Write(content); e != nil {
		return e
	}
	name := "CloudProbe-verify-" + randomKey()[:12] + ".txt"
	entry, e := primary.Upload(ctx, root, name, f)
	if e != nil {
		return fmt.Errorf("创建映射验证文件失败：%w", e)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
		defer cancel()
		if cleanupErr := primary.Remove(cleanup, entry); cleanupErr != nil {
			logDAV(ctx, runLogEntry{Operation: "OpenList 验证清理", Source: "云盘接口", Result: "失败", Error: cleanupErr.Error(), Detail: name})
			if err == nil {
				err = fmt.Errorf("验证成功但清理专用文件失败 %s：%w", name, cleanupErr)
			}
		}
	}()
	items, e := backup.listing(ctx, "")
	if e != nil {
		return e
	}
	found := false
	for _, it := range items {
		if it.Name == name && it.Size == int64(len(content)) && !it.Directory {
			found = true
		}
	}
	if !found {
		return errors.New("OpenList 挂载中没有找到唯一验证文件，未启用备用")
	}
	target, e := backup.link(ctx, name, int64(len(content)))
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, "GET", target.URL, nil)
	if e != nil {
		return e
	}
	client := &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) > 5 {
			return errors.New("验证下载重定向过多")
		}
		return validDAVURL(r.URL.String())
	}}
	resp, e := client.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	raw, e := readBounded(resp.Body, int64(len(content))+1)
	if e != nil {
		return e
	}
	if resp.StatusCode != 200 || !bytes.Equal(raw, content) {
		return errors.New("OpenList 验证文件正文不一致，未启用备用")
	}
	sum := sha256.Sum256(content)
	logDAV(ctx, runLogEntry{Operation: "OpenList 映射验证", Source: "OpenList 服务", Result: "成功", Detail: "已通过唯一临时文件和正文核验，验证摘要 " + hex.EncodeToString(sum[:8])})
	return nil
}

package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type quarkDAVDrive struct {
	uploadIdle     time.Duration
	client         *quarkClient
	bodyHTTP       *http.Client
	budget         *fairLimiter
	concurrency    int
	mode           string
	recovery       *uploadRecovery
	metadataHealth routeHealth
	// Tests can adjust the backoff without weakening production retries.
	retryWait func(context.Context, time.Duration) error
}

func newQuarkDrive(c *quarkClient, t *transferRuntime, workers int, mode string) *quarkDAVDrive {
	d := &quarkDAVDrive{client: c, concurrency: uploadConcurrency(workers), mode: uploadMode(mode), retryWait: davWait, uploadIdle: 90 * time.Second}
	body := *c.bodies
	if transport, ok := c.bodies.Transport.(*http.Transport); ok {
		tr := transport.Clone()
		if d.mode == "http1" {
			tr.ForceAttemptHTTP2 = false
			tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
			if tr.TLSClientConfig != nil {
				tr.TLSClientConfig = tr.TLSClientConfig.Clone()
				tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
			}
		}
		body.Transport = tr
	}
	d.bodyHTTP = &body
	if t != nil {
		d.budget = t.connections
		d.recovery = t.recovery
	}
	return d
}
func quarkStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := valueString(m[k]); s != "" {
			return s
		}
	}
	return ""
}
func quarkBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	default:
		s := valueString(v)
		if s == "1" || s == "true" {
			return true, true
		}
		if s == "0" || s == "false" {
			return false, true
		}
	}
	return false, false
}
func parseQuarkEntry(v any, parent string) (davEntry, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return davEntry{}, errors.New("夸克文件列表包含非对象条目")
	}
	id := quarkStr(m, "fid", "file_id")
	name := html.UnescapeString(quarkStr(m, "file_name", "filename"))
	if id == "" || name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return davEntry{}, errors.New("夸克文件条目缺少有效 fid/文件名")
	}
	directory, known := quarkBool(m["dir"])
	if !known {
		if typ, exists := m["file_type"]; exists {
			n, e := nonnegativeInteger(typ)
			if e != nil || n > 1 {
				return davEntry{}, errors.New("夸克文件类型无效")
			}
			directory = n == 0
			known = true
		}
	}
	if !known {
		return davEntry{}, errors.New("夸克文件条目缺少类型，不能猜测为文件或目录")
	}
	var size int64
	if !directory {
		var e error
		size, e = nonnegativeInteger(m["size"])
		if e != nil {
			return davEntry{}, errors.New("夸克文件条目缺少有效大小")
		}
	}
	modified := time.Time{}
	for _, key := range []string{"updated_at", "l_updated_at", "created_at"} {
		if n, e := nonnegativeInteger(m[key]); e == nil && n > 0 {
			if n < 1e11 {
				modified = time.Unix(n, 0)
			} else {
				modified = time.UnixMilli(n)
			}
			break
		}
	}
	it := davEntry{Item: classify("quark", Item{ID: id, Name: name, Size: size, Directory: directory}), Modified: modified, Parent: parent}
	// A Quark fid identifies an object, not a content version. Only hashes or
	// an explicit version may permit switching routes after delivery begins.
	if hash := quarkStr(m, "sha1", "sha1_hash"); quarkHashValid(hash, 20) {
		it.ContentVersion = "sha1:" + strings.ToLower(hash) + ":" + strconv.FormatInt(size, 10)
	} else if hash := quarkStr(m, "md5", "md5_hash"); quarkHashValid(hash, 16) {
		it.ContentVersion = "md5:" + strings.ToLower(hash) + ":" + strconv.FormatInt(size, 10)
	}
	return it, nil
}
func (d *quarkDAVDrive) List(ctx context.Context, parent string) (items []davEntry, err error) {
	if parent == "" || parent == "root" {
		parent = "0"
	}
	routes := d.metadataRoutes("list")
	for i, route := range routes {
		started := time.Now()
		items, err = d.listRoute(ctx, route, parent)
		logList(ctx, route.label(), "目录 "+parent, started, items, err)
		if err == nil {
			d.metadataHealth.good(string(route) + ":list")
			return items, nil
		}
		d.metadataHealth.bad(string(route) + ":list")
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if i+1 < len(routes) {
			logFallback(ctx, route.label(), routes[i+1].label(), err)
		}
	}
	if err == nil {
		err = errors.New("夸克账号没有可用授权")
	}
	return nil, err
}
func (d *quarkDAVDrive) listRoute(ctx context.Context, route quarkRoute, parent string) ([]davEntry, error) {
	out := []davEntry{}
	seen := map[string]bool{}
	cursors := map[string]bool{}
	var cursor any
	const pageSize = 100
	for page := 1; page <= 10000; page++ {
		var reply map[string]any
		var err error
		if route == quarkOpen {
			body := map[string]any{"parent_fid": parent, "size": pageSize, "sort": "file_name:asc"}
			if cursor != nil {
				body["query_cursor"] = cursor
			}
			reply, err = d.client.call(ctx, route, "POST", "/open/v1/file/list", nil, body)
		} else {
			reply, err = d.client.call(ctx, route, "GET", "/file/sort", url.Values{"pdir_fid": {parent}, "_page": {strconv.Itoa(page)}, "_size": {strconv.Itoa(pageSize)}, "_fetch_total": {"1"}, "_fetch_sub_dirs": {"0"}, "fetch_all_file": {"1"}, "fetch_risk_file_name": {"1"}, "_sort": {"file_type:asc,file_name:asc"}}, nil)
		}
		if err != nil {
			return nil, err
		}
		data, err := quarkObject(reply["data"])
		if err != nil {
			return nil, err
		}
		key := "list"
		if route == quarkOpen {
			key = "file_list"
		}
		raw, ok := data[key]
		if !ok {
			return nil, fmt.Errorf("夸克目录响应缺少 %s，不能视为空目录", key)
		}
		rows, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("夸克 %s 不是数组，不能视为空目录", key)
		}
		for _, row := range rows {
			it, e := parseQuarkEntry(row, parent)
			if e != nil {
				return nil, e
			}
			if seen[it.ID] {
				return nil, errors.New("夸克分页重复返回文件 ID，目录完整性无法确认")
			}
			seen[it.ID] = true
			out = append(out, it)
		}
		if route == quarkOpen {
			last, known := quarkBool(data["last_page"])
			if !known {
				return nil, errors.New("夸克开放目录缺少有效 last_page")
			}
			if last {
				return out, nil
			}
			cursor = data["next_query_cursor"]
			cm, e := quarkObject(cursor)
			if e != nil || quarkStr(cm, "token") == "" {
				return nil, errors.New("夸克开放目录缺少下一页游标")
			}
			token := quarkStr(cm, "token")
			if cursors[token] || len(rows) == 0 {
				return nil, errors.New("夸克开放目录游标重复或分页为空")
			}
			cursors[token] = true
		} else {
			meta, _ := reply["metadata"].(map[string]any)
			total, e := nonnegativeInteger(meta["_total"])
			if e != nil {
				total, e = nonnegativeInteger(meta["total"])
			}
			if e != nil {
				return nil, errors.New("夸克目录响应缺少有效总数，无法确认分页完整")
			}
			if int64(len(out)) > total {
				return nil, errors.New("夸克目录条目数超过返回总数")
			}
			if int64(len(out)) == total {
				return out, nil
			}
			if len(rows) == 0 {
				return nil, errors.New("夸克分页提前结束，不能当作完整目录")
			}
		}
	}
	return nil, errors.New("夸克目录超过分页保护上限")
}
func (d *quarkDAVDrive) Quota(ctx context.Context) (q davQuota, err error) {
	for _, route := range d.metadataRoutes("quota") {
		path := "/member"
		if route == quarkOpen {
			path = "/open/v1/user/get_vip_info"
		}
		started := time.Now()
		reply, e := d.client.call(ctx, route, "GET", path, url.Values{"fetch_subscribe": {"true"}, "fetch_identity": {"true"}}, nil)
		if e == nil {
			var m map[string]any
			m, e = quarkObject(reply["data"])
			if e == nil {
				a, b := "total_capacity", "use_capacity"
				if route == quarkOpen {
					a, b = "capacity", "used"
				}
				q.Total, e = nonnegativeInteger(m[a])
				if e == nil {
					q.Used, e = nonnegativeInteger(m[b])
				}
				if e != nil {
					e = errors.New("夸克容量响应缺少有效总空间或已用空间，未以零值代替")
				}
			}
		}
		logDAV(ctx, runLogEntry{Operation: "查询网盘容量", Source: route.label(), Endpoint: path, Result: map[bool]string{true: "成功", false: "失败"}[e == nil], DurationMs: time.Since(started).Milliseconds()})
		if e == nil {
			d.metadataHealth.good(string(route) + ":quota")
			return q, q.validate()
		}
		d.metadataHealth.bad(string(route) + ":quota")
		if ctx.Err() != nil {
			return davQuota{}, ctx.Err()
		}
		err = e
	}
	if err == nil {
		err = errors.New("夸克容量查询没有可用授权")
	}
	return davQuota{}, err
}
func (d *quarkDAVDrive) ReadRoutes(ctx context.Context, it davEntry) []readRoute {
	var routes []readRoute
	for _, route := range d.client.routes() {
		// A directory hash is an expectation, not a binding between a fresh
		// download URL and immutable bytes. The reader must use HTTP validators.
		routes = append(routes, readRoute{Name: route.label(), Resolve: func(ctx context.Context) (readTarget, error) { return d.downloadTarget(ctx, route, it) }})
	}
	return routes
}
func (d *quarkDAVDrive) URL(ctx context.Context, it davEntry) (string, error) {
	var err error
	for _, route := range d.client.routes() {
		target, e := d.downloadTarget(ctx, route, it)
		if e == nil {
			return target.URL, nil
		}
		err = e
	}
	if err == nil {
		err = errors.New("夸克下载缺少授权")
	}
	return "", err
}
func (d *quarkDAVDrive) downloadTarget(ctx context.Context, route quarkRoute, it davEntry) (target readTarget, err error) {
	started := time.Now()
	defer func() { logURL(ctx, route.label(), "下载地址", started, target.URL, err) }()
	path := "/file/download"
	body := map[string]any{"fids": []string{it.ID}}
	if route == quarkOpen {
		path = "/open/v1/file/get_download_url"
		body = map[string]any{"fid": it.ID}
	}
	out, err := d.client.call(ctx, route, "POST", path, nil, body)
	if err != nil {
		return target, err
	}
	var m map[string]any
	switch data := out["data"].(type) {
	case []any:
		if len(data) == 1 {
			m, _ = data[0].(map[string]any)
		}
	case map[string]any:
		m = data
		if rows, ok := data["list"].([]any); ok && len(rows) == 1 {
			m, _ = rows[0].(map[string]any)
		}
	}
	if m == nil {
		return target, errors.New("夸克下载响应缺少唯一文件对象")
	}
	if fid := quarkStr(m, "fid"); fid != "" && fid != it.ID {
		return target, errors.New("夸克下载地址对应了其他文件")
	}
	if n, e := nonnegativeInteger(m["size"]); e == nil && n != it.Size {
		return target, errors.New("夸克下载对象大小已改变，请重新读取目录")
	}
	target.URL = quarkStr(m, "download_url", "url")
	if err = d.client.validBodyURL(target.URL); err != nil {
		return readTarget{}, err
	}
	c := d.client.snapshot()
	target.Headers = http.Header{"User-Agent": {quarkUserAgent}, "Referer": {"https://pan.quark.cn/"}}
	if route == quarkOpen {
		target.Headers.Set("Cookie", "x_pan_client_id="+quarkOpenClient+"; x_pan_access_token="+c.AccessToken)
	} else {
		target.Headers.Set("Cookie", quarkCookieHeader(c.Cookies))
	}
	return target, nil
}
func (d *quarkDAVDrive) task(ctx context.Context, route quarkRoute, data map[string]any) error {
	id := quarkStr(data, "task_id")
	if id == "" {
		return nil
	}
	if done, ok := quarkBool(data["finish"]); ok && done {
		return nil
	}
	for {
		if err := d.retryWait(ctx, 500*time.Millisecond); err != nil {
			return err
		}
		var out map[string]any
		var err error
		if route == quarkOpen {
			out, err = d.client.call(ctx, route, "POST", "/open/v1/task/query", nil, map[string]any{"task_id": id})
		} else {
			out, err = d.client.call(ctx, route, "GET", "/task", url.Values{"task_id": {id}, "retry_index": {"0"}}, nil)
		}
		if err != nil {
			return err
		}
		m, err := quarkObject(out["data"])
		if err != nil {
			return err
		}
		status := quarkStr(m, "status")
		switch status {
		case "2":
			return nil
		case "0", "1":
			continue
		case "3", "4":
			return fmt.Errorf("夸克任务 %s 未完成：%s", status, quarkStr(m, "message", "error_info"))
		default:
			return errors.New("夸克任务返回未知状态，未视为完成")
		}
	}
}
func (d *quarkDAVDrive) mutate(ctx context.Context, operation string, webBody, openBody map[string]any, webPath, openPath string) (map[string]any, error) {
	var last error
	for _, route := range d.metadataRoutes(operation) {
		path, body := webPath, webBody
		if route == quarkOpen {
			path, body = openPath, openBody
		}
		finish := logPhase(ctx, operation, route.label(), path)
		out, err := d.client.call(ctx, route, "POST", path, nil, body)
		if err == nil {
			m, ok := out["data"].(map[string]any)
			if !ok {
				if out["data"] != nil {
					err = errors.New("夸克写入响应 data 类型未知")
				} else {
					m = map[string]any{}
				}
			}
			if err == nil {
				err = d.task(ctx, route, m)
			}
			finish(err)
			if err == nil {
				d.metadataHealth.good(string(route) + ":" + operation)
				return m, nil
			}
			return nil, &davMutationError{Phase: operation, Err: err}
		}
		finish(err)
		last = err
		d.metadataHealth.bad(string(route) + ":" + operation)
		if !quarkRejected(err) {
			return nil, &davMutationError{Phase: operation, Err: err}
		}
		logFallback(ctx, route.label(), "其他已授权夸克通道", err)
	}
	if last == nil {
		last = errors.New("夸克账号没有可用授权")
	}
	return nil, &davNotExecuted{last}
}
func (d *quarkDAVDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	if err := safeFilename(name); err != nil {
		return davEntry{}, err
	}
	out, err := d.mutate(ctx, "新建目录", map[string]any{"pdir_fid": parent, "file_name": name, "dir_path": "", "dir_init_lock": false}, map[string]any{"pdir_fid": parent, "dir_path": name}, "/file", "/open/v1/dir")
	if err != nil {
		return davEntry{}, err
	}
	id := quarkStr(out, "fid")
	if m, ok := out["file"].(map[string]any); ok {
		id = quarkStr(m, "fid")
	}
	for attempt := 0; attempt < 6; attempt++ {
		entries, err := d.List(ctx, parent)
		if err != nil {
			return davEntry{}, &davMutationError{Phase: "新建目录核验", Err: err}
		}
		for _, it := range entries {
			if it.Name == name && it.Directory && (id == "" || it.ID == id) {
				return it, nil
			}
		}
		if attempt < 5 {
			if err = d.retryWait(ctx, time.Second); err != nil {
				return davEntry{}, &davMutationError{Phase: "新建目录核验", Err: err}
			}
		}
	}
	return davEntry{}, &davMutationError{Phase: "新建目录核验", Err: errors.New("云端目录在核验窗口内尚不可见，未重复创建")}

}
func (d *quarkDAVDrive) Remove(ctx context.Context, it davEntry) error {
	_, err := d.mutate(ctx, "删除到回收站", map[string]any{"action_type": 1, "filelist": []string{it.ID}, "exclude_fids": []string{}}, map[string]any{"action_type": 1, "fid_list": []string{it.ID}}, "/file/delete", "/open/v1/file/delete")
	if err != nil {
		return err
	}
	if it.Parent != "" {
		return d.verifyMutation(ctx, it.Parent, it.ID, "", true)
	}
	return nil
}
func (d *quarkDAVDrive) verifyMutation(ctx context.Context, parent, id, name string, removed bool) error {
	for attempt := 0; attempt < 6; attempt++ {
		entries, err := d.List(ctx, parent)
		if err != nil {
			return &davMutationError{Phase: "变更后目录核验", Partial: true, Err: err}
		}
		found := false
		for _, it := range entries {
			if it.ID == id {
				found = true
				if !removed && it.Name == name {
					return nil
				}
			}
		}
		if removed && !found {
			return nil
		}
		if attempt < 5 {
			if err = d.retryWait(ctx, time.Second); err != nil {
				return &davMutationError{Phase: "变更后目录核验", Partial: true, Err: err}
			}
		}
	}
	return &davMutationError{Phase: "变更后目录核验", Partial: true, Err: errors.New("夸克目录索引在核验窗口内尚未更新，未重放写操作")}
}
func (d *quarkDAVDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	if err := safeFilename(name); err != nil {
		return err
	}
	changed := false
	if name != it.Name {
		_, err := d.mutate(ctx, "重命名", map[string]any{"fid": it.ID, "file_name": name}, map[string]any{"fid": it.ID, "file_name": name, "conflict_mode": "FAIL"}, "/file/rename", "/open/v1/file/rename")
		if err != nil {
			return err
		}
		changed = true
	}
	if parent != "" && parent != it.Parent {
		_, err := d.mutate(ctx, "移动", map[string]any{"action_type": 1, "filelist": []string{it.ID}, "exclude_fids": []string{}, "to_pdir_fid": parent}, map[string]any{"action_type": 1, "fid_list": []string{it.ID}, "to_pdir_fid": parent}, "/file/move", "/open/v1/file/move")
		if err != nil {
			if changed {
				return &davMutationError{Phase: "改名后的移动", Partial: true, Err: err}
			}
			return err
		}
	}
	expectedParent := parent
	if expectedParent == "" {
		expectedParent = it.Parent
	}
	if expectedParent != "" {
		return d.verifyMutation(ctx, expectedParent, it.ID, name, false)
	}
	return nil
}
func (d *quarkDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	return d.uploadFile(ctx, parent, name, f)
}

func quarkHashValid(value string, n int) bool {
	if len(value) != n*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (d *quarkDAVDrive) metadataRoutes(operation string) []quarkRoute {
	var good, cool []quarkRoute
	for _, route := range d.client.routes() {
		if d.metadataHealth.cool(string(route) + ":" + operation) {
			cool = append(cool, route)
		} else {
			good = append(good, route)
		}
	}
	return append(good, cool...)
}

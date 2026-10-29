package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"resty.dev/v3"
)

type davEntry struct {
	ContentVersion string `json:"-"`
	Parent         string `json:"-"`
	Item
	Modified time.Time
}

// All methods operate on cloud IDs. Local temporary files are only upload spools.
type davDrive interface {
	List(context.Context, string) ([]davEntry, error)
	URL(context.Context, davEntry) (string, error)
	Mkdir(context.Context, string, string) (davEntry, error)
	Upload(context.Context, string, string, *os.File) (davEntry, error)
	Remove(context.Context, davEntry) error
	Move(context.Context, davEntry, string, string) error
}

type davUpstreamError struct{ Message string }

func (e *davUpstreamError) Error() string { return e.Message }

type davNotExecuted struct{ error }

func (e *davNotExecuted) Unwrap() error { return e.error }

type originalDAVDrive struct {
	client *Client
	mu     contextMutex
	health routeHealth
}

func (d *originalDAVDrive) List(ctx context.Context, dir string) (entries []davEntry, err error) {
	started := time.Now()
	defer func() {
		logList(ctx, "网易代理", "/v1/api/"+d.client.cfg.Provider+" 目录查询", started, entries, err)
	}()
	if e := d.mu.LockContext(ctx); e != nil {
		return nil, e
	}
	defer d.mu.Unlock()
	r, e := d.client.List(ctx, dir, "webdav", nil)
	if e != nil {
		return nil, e
	}
	if !r.Complete {
		return nil, fmt.Errorf("目录查询未完成：%s", r.StopReason)
	}
	if len(r.Items) == 0 {
		logDAV(ctx, runLogEntry{Operation: "目录查询说明", Source: "网易代理", Result: "提示", Detail: r.StopReason})
		for _, note := range r.Notes {
			if strings.Contains(note, "省略了 fileInfoList") {
				return nil, errors.New("代理省略文件列表，需通过直连核验，不能当作已确认空目录")
			}
		}
	}
	entries = make([]davEntry, 0, len(r.Items))
	for _, it := range r.Items {
		entries = append(entries, davEntry{Item: it})
	}
	return entries, nil
}
func (d *originalDAVDrive) URL(ctx context.Context, it davEntry) (raw string, err error) {
	started := time.Now()
	defer func() {
		logURL(ctx, "网易代理", "/v1/api/"+d.client.cfg.Provider+" 下载地址", started, raw, err)
	}()
	if e := d.mu.LockContext(ctx); e != nil {
		return "", e
	}
	defer d.mu.Unlock()
	raw, err = d.client.downloadURL(ctx, it.ID, it.FID)
	if err == nil {
		err = validDAVURL(raw)
	}
	return raw, err
}

type wopanDAVDrive struct {
	originalDAVDrive
	w                 *wopan.WoClient
	directMu          contextMutex
	root              string
	family            string
	directErr         string
	redactor          *Redactor
	onRefresh         func(string, string) error
	serverAccess      string
	serverRefresh     string
	usingWeb          bool
	uploadConcurrency int
	uploadMode        string
	uploadSlots       chan struct{}
	uploadPartMiB     int
	uploadBudget      *fairLimiter
}

func newWopanDAVDrive(cl *Client, credentials map[string]any, root string, refresh func(string, string) error) *wopanDAVDrive {
	w := wopan.DefaultWithAccessToken(cl.cfg.Wopan.AccessToken)
	w.SetRefreshToken(valueString(credentials["refresh_token"]))
	jar, _ := cookiejar.New(nil)
	metadataClient := providerMetadataClient(3 * time.Minute)
	metadataClient.Jar = jar
	w.SetHttpClient(metadataClient)
	w.SetUserAgent(wopan.DefaultUA)
	d := &wopanDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}, w: w, root: root, family: "0", redactor: cl.redact, onRefresh: refresh}
	d.serverAccess = valueString(credentials["wopan_server_token"])
	d.serverRefresh = valueString(credentials["wopan_server_refresh_token"])
	cl.redact.Add(d.serverAccess)
	cl.redact.Add(d.serverRefresh)
	if web := valueString(credentials["wopan_web_token"]); web != "" {
		w.SetAccessToken(web)
		w.SetRefreshToken("")
		d.usingWeb = true
		cl.redact.Add(web)
	}
	cl.redact.Add(valueString(credentials["refresh_token"]))
	w.OnRefreshToken(func(access, refresh string) {
		d.mu.Lock()
		cl.redact.Add(access)
		cl.redact.Add(refresh)
		cl.cfg.Wopan.AccessToken = access
		d.mu.Unlock()
		if d.onRefresh != nil {
			if e := d.onRefresh(access, refresh); e != nil {
				d.directErr = "令牌已刷新，但持久化失败：" + cl.redact.Text(e.Error())
			}
		}
	})
	return d
}
func (d *wopanDAVDrive) refreshNative(ctx context.Context) (err error) {
	finish := logPhase(ctx, "刷新绑定授权", "网易代理", "/v1/api/wopan/refresh_token")
	defer func() { finish(err) }()
	refresh := d.serverRefresh
	if !d.usingWeb {
		_, refresh = d.w.GetToken()
	}
	if refresh == "" {
		return errors.New("联通刷新令牌缺失，请在爆米花重新绑定云盘")
	}
	if e := d.mu.LockContext(ctx); e != nil {
		return e
	}
	defer d.mu.Unlock()
	raw, e := d.client.wopan(ctx, "POST", "/refresh_token", map[string]any{"refresh_token": refresh})
	if e != nil {
		return e
	}
	m, e := decodeDocument(raw)
	if e != nil {
		return e
	}
	m, e = unwrap(m)
	if e != nil {
		return e
	}
	access, nextRefresh := valueString(m["access_token"]), valueString(m["refresh_token"])
	if len(access) < 16 {
		return errors.New("联通刷新响应未返回有效 access_token")
	}
	if nextRefresh == "" {
		nextRefresh = refresh
	}
	d.client.redact.Add(access)
	d.client.redact.Add(nextRefresh)
	d.client.cfg.Wopan.AccessToken = access
	if !d.usingWeb {
		d.w.SetAccessToken(access)
		d.w.SetRefreshToken(nextRefresh)
	}
	d.serverRefresh = nextRefresh
	if d.onRefresh != nil {
		return d.onRefresh(access, nextRefresh)
	}
	return nil
}
func woContext(ctx context.Context) wopan.RestyOption {
	return func(r *resty.Request) { r.SetContext(ctx) }
}
func (d *wopanDAVDrive) probe(ctx context.Context) error {
	if e := d.directMu.LockContext(ctx); e != nil {
		return e
	}
	defer d.directMu.Unlock()
	_, e := d.w.QueryAllFilesPersonal(d.root, 0, 100, 0, woContext(ctx))
	if wopanAuthError(e) && d.usingWeb {
		logFallback(ctx, "联通网页授权", "联通绑定授权直连", e)
		d.usingWeb = false
		d.w.SetAccessToken(d.client.cfg.Wopan.AccessToken)
		d.w.SetRefreshToken(d.serverRefresh)
		_, e = d.w.QueryAllFilesPersonal(d.root, 0, 100, 0, woContext(ctx))
	}
	if wopanAuthError(e) {
		if re := d.refreshNative(ctx); re == nil {
			_, e = d.w.QueryAllFilesPersonal(d.root, 0, 100, 0, woContext(ctx))
		}
	}
	if wopanAuthError(e) && d.serverAccess != "" && d.serverAccess != d.client.cfg.Wopan.AccessToken {
		d.w.SetAccessToken(d.serverAccess)
		d.w.SetRefreshToken(d.serverRefresh)
		d.client.cfg.Wopan.AccessToken = d.serverAccess
		_, e = d.w.QueryAllFilesPersonal(d.root, 0, 100, 0, woContext(ctx))
		if wopanAuthError(e) {
			if re := d.refreshNative(ctx); re == nil {
				_, e = d.w.QueryAllFilesPersonal(d.root, 0, 100, 0, woContext(ctx))
			}
		}
	}
	if e != nil {
		d.directErr = "联通直连写接口的凭据验证未通过：" + d.redactor.Text(e.Error())
		return errors.New(d.directErr)
	}
	d.directErr = ""
	if e := d.w.InitData(); e != nil {
		return fmt.Errorf("联通上传初始化失败：%s", d.redactor.Text(e.Error()))
	}
	logDAV(ctx, runLogEntry{Operation: "上传节点初始化", Source: d.source(), Endpoint: nodeHost(d.w.ZoneURL), Result: "成功"})
	return nil
}

func wopanAuthError(e error) bool {
	if e == nil {
		return false
	}
	s := e.Error()
	return strings.Contains(s, "1001") || strings.Contains(s, "9999") || strings.Contains(s, "无效登录") || strings.Contains(s, "授权/会话失效") || strings.Contains(s, "token expired")
}
func (d *wopanDAVDrive) List(ctx context.Context, dir string) ([]davEntry, error) {
	var entries []davEntry
	var primary error
	cooled := d.health.cool("list-proxy")
	if cooled {
		primary = errors.New("网易目录接口短暂冷却，尝试直连")
	} else {
		entries, primary = d.originalDAVDrive.List(ctx, dir)
	}
	if primary == nil {
		d.health.good("list-proxy")
		return entries, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !cooled {
		d.health.bad("list-proxy")
	}
	logFallback(ctx, "网易代理", d.source(), primary)
	if e := d.directMu.LockContext(ctx); e != nil {
		return nil, e
	}
	defer d.directMu.Unlock()
	if wopanAuthError(primary) {
		if e := d.refreshNative(ctx); e == nil {
			if entries, e = d.originalDAVDrive.List(ctx, dir); e == nil {
				return entries, nil
			}
		}
	}
	entries, backup := d.directList(ctx, dir)
	if wopanAuthError(backup) {
		if e := d.refreshNative(ctx); e == nil {
			entries, backup = d.directList(ctx, dir)
		}
	}
	if backup != nil {
		if cooled && ctx.Err() == nil {
			if retry, re := d.originalDAVDrive.List(ctx, dir); re == nil {
				d.health.good("list-proxy")
				return retry, nil
			}
		}
		return nil, fmt.Errorf("网易代理目录请求失败：%v；联通直连也失败：%w", primary, backup)
	}
	return entries, nil
}
func (d *wopanDAVDrive) directList(ctx context.Context, dir string) (entries []davEntry, err error) {
	started := time.Now()
	defer func() { logList(ctx, d.source(), "QueryAllFiles", started, entries, err) }()
	entries = []davEntry{}
	seen := map[string]bool{}
	previous := ""
	for page := 0; page < 10000; page++ {
		var data map[string]any
		_, e := d.w.RequestWoHome(wopan.KeyQueryAllFiles, wopan.Json{"spaceType": "0", "parentDirectoryId": dir, "pageNum": page, "pageSize": 100, "sortRule": 6, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, &data, woContext(ctx))
		if e != nil {
			return nil, e
		}
		_, hasFiles := data["files"]
		_, hasDirs := data["systemDirs"]
		if !hasFiles && !hasDirs {
			return nil, errors.New("联通直连响应缺少 files/systemDirs，不能视为空目录")
		}
		pageItems := []davEntry{}
		for _, field := range []string{"systemDirs", "files"} {
			v := data[field]
			if v == nil {
				continue
			}
			items, ok := v.([]any)
			if !ok {
				return nil, errors.New("联通直连文件列表类型无效")
			}
			for _, raw := range items {
				x := object(raw)
				if field == "systemDirs" && wopanSystemNavigation(x) {
					logDAV(ctx, runLogEntry{Operation: "目录系统入口", Source: d.source(), Result: "已识别", Detail: "systemDirs 返回无文件 ID 的系统导航元数据，单独记录，不映射成可读写文件", Count: 1})
					continue
				}
				typ := valueString(x["type"])
				if typ != "0" && typ != "1" {
					return nil, errors.New("联通直连返回未知目录条目类型，未当作空目录")
				}
				it := davEntry{Item: Item{ID: valueString(x["id"]), FID: valueString(x["fid"]), Name: valueString(x["name"]), Size: number(x["size"]), Directory: typ == "0"}}
				if it.ID == "" || it.Name == "" || strings.ContainsAny(it.Name, "/\\\x00\r\n") {
					return nil, errors.New("联通直连条目缺少 ID 或有效名称")
				}
				it.Item = classify("wopan", it.Item)
				it.Modified = parseDAVTime(valueString(x["updateTime"]))
				if it.Modified.IsZero() {
					it.Modified = parseDAVTime(valueString(x["modifyTime"]))
				}
				if it.Modified.IsZero() {
					it.Modified = parseDAVTime(valueString(x["createTime"]))
				}
				pageItems = append(pageItems, it)
			}
		}
		if !hasFiles && len(pageItems) == 0 {
			return nil, errors.New("联通只返回系统导航而没有 files 文件列表，不能视为空目录")
		}
		fingerprintBytes, _ := json.Marshal(pageItems)
		fp := fmt.Sprintf("%x", md5.Sum(fingerprintBytes))
		if len(pageItems) > 0 && fp == previous {
			return nil, errors.New("联通直连分页重复，目录未读取完整")
		}
		previous = fp
		for _, it := range pageItems {
			key := it.ID + "\x00" + it.FID
			if !seen[key] {
				seen[key] = true
				entries = append(entries, it)
			}
		}
		if len(list(data["files"])) < 100 {
			if total, ok := data["total"]; ok && number(total) > int64(len(entries)) {
				return nil, errors.New("联通目录返回条目少于声明总数，未报告完整")
			}
			return entries, nil
		}
	}
	return nil, errors.New("联通直连目录达到分页上限，查询未完成")
}
func wopanSystemNavigation(x map[string]any) bool {
	if valueString(x["id"]) != "" || valueString(x["fid"]) != "" || valueString(x["type"]) != "" || valueString(x["dirName"]) == "" || valueString(x["dirType"]) == "" {
		return false
	}
	allowed := map[string]bool{"createTime": true, "dirName": true, "dirType": true, "icon": true, "lowestAppVersion": true, "lowestSupportAppVersion": true, "note": true}
	for key := range x {
		if !allowed[key] {
			return false
		}
	}
	return true
}
func parseDAVTime(value string) time.Time {
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "20060102150405"} {
		if t, e := time.ParseInLocation(layout, value, time.Local); e == nil {
			return t
		}
	}
	return time.Time{}
}
func (d *wopanDAVDrive) URL(ctx context.Context, it davEntry) (string, error) {
	u, primary := d.originalDAVDrive.URL(ctx, it)
	if primary == nil && validDAVURL(u) == nil {
		return u, nil
	}
	if primary == nil {
		primary = validDAVURL(u)
	}
	logFallback(ctx, "网易代理", d.source()+" GetDownloadUrlV2", primary)
	if e := d.directMu.LockContext(ctx); e != nil {
		return "", e
	}
	defer d.directMu.Unlock()
	if wopanAuthError(primary) {
		if e := d.refreshNative(ctx); e == nil {
			u, e = d.originalDAVDrive.URL(ctx, it)
			if e == nil && validDAVURL(u) == nil {
				return u, nil
			}
		}
	}
	started := time.Now()
	data, e := d.w.GetDownloadUrlV2([]string{it.FID}, woContext(ctx))
	if e == nil {
		for _, entry := range data.List {
			if entry.Fid == it.FID && validDAVURL(entry.DownloadUrl) == nil {
				logURL(ctx, d.source(), "GetDownloadUrlV2", started, entry.DownloadUrl, nil)
				return entry.DownloadUrl, nil
			}
		}
	}
	if e == nil {
		e = errors.New("响应未返回匹配文件的可用下载地址")
	}
	logURL(ctx, d.source(), "GetDownloadUrlV2", started, "", e)
	logFallback(ctx, "GetDownloadUrlV2", "GetDownloadUrl", e)
	started = time.Now()
	last, e := d.w.GetDownloadUrl(wopan.SpaceTypePersonal, []string{it.FID}, woContext(ctx))
	if e == nil {
		for _, entry := range last {
			if entry.Fid == it.FID && validDAVURL(entry.DownloadUrl) == nil {
				logURL(ctx, d.source(), "GetDownloadUrl", started, entry.DownloadUrl, nil)
				return entry.DownloadUrl, nil
			}
		}
	}
	if e == nil {
		e = errors.New("响应未返回匹配文件的可用下载地址")
	}
	logURL(ctx, d.source(), "GetDownloadUrl", started, "", e)
	return "", fmt.Errorf("网易代理与联通两种下载地址接口均未返回可用结果：%v；%v", primary, e)
}
func (d *wopanDAVDrive) ready() error {
	if d.directErr != "" {
		return errors.New(d.directErr)
	}
	return nil
}
func (d *wopanDAVDrive) Mkdir(ctx context.Context, parent, name string) (entry davEntry, err error) {
	if e := d.directMu.LockContext(ctx); e != nil {
		return davEntry{}, e
	}
	defer d.directMu.Unlock()
	finish := logPhase(ctx, "新建目录", d.source(), name+"；CreateDirectory")
	defer func() { finish(err) }()
	if e := d.ready(); e != nil {
		return davEntry{}, &davNotExecuted{e}
	}
	r, e := d.w.CreateDirectory(wopan.SpaceTypePersonal, parent, name, d.family, woContext(ctx))
	if e != nil {
		return davEntry{}, mutationFailure(e, "CreateDirectory", false)
	}
	if r.Id == "" {
		return davEntry{}, errors.New("联通新建目录未返回目录 ID")
	}
	return davEntry{Item: Item{ID: r.Id, Name: name, Directory: true}, Modified: time.Now()}, nil
}
func (d *wopanDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (entry davEntry, err error) {
	fi, e := f.Stat()
	if e != nil {
		return davEntry{}, e
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return davEntry{}, e
	}
	fid, e := d.uploadParts(ctx, parent, name, f, fi.Size())
	if e != nil {
		return davEntry{}, e
	}
	return d.verifyUploaded(ctx, parent, name, fi.Size(), fid)
}

func (d *wopanDAVDrive) verifyUploaded(ctx context.Context, parent, name string, size int64, fid string) (entry davEntry, err error) {
	// Upload completion must be visible in the real directory, with the expected size.
	finish := logPhase(ctx, "上传目录核验", d.source(), "核对临时文件名称、大小和 FID")
	defer func() { finish(err) }()
	for attempt := 0; attempt < 5; attempt++ {
		entries, e := d.List(directoryLogContext(ctx), parent)
		if e != nil {
			return davEntry{}, e
		}
		for _, it := range entries {
			if it.Name == name && !it.Directory && it.Size == size && (fid == "" || it.FID == fid) {
				return it, nil
			}
		}
		if e = davWait(ctx, 400*time.Millisecond); e != nil {
			return davEntry{}, e
		}
	}
	return davEntry{}, errors.New("联通上传接口已返回，但未在目标目录确认到名称、大小一致的文件；未报告上传成功")
}

type wopanPartError struct {
	Message         string
	Retry           bool
	ResetConnection bool
	RetryAfter      time.Duration
	Auth            bool
}

func (e *wopanPartError) Error() string { return e.Message }
func sendWopanPart(client *http.Client, req *http.Request) (wopan.Upload2CResp, error) {
	return sendWopanPartActive(client, req, nil)
}
func sendWopanPartActive(client *http.Client, req *http.Request, guard *uploadIdleGuard) (wopan.Upload2CResp, error) {
	var result wopan.Upload2CResp
	response, e := client.Do(req)
	if e != nil {
		var ne net.Error
		retry := (req.Context().Err() == nil || errors.Is(req.Context().Err(), context.DeadlineExceeded)) && (errors.As(e, &ne) || errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF))
		return result, &wopanPartError{Message: e.Error(), Retry: retry, ResetConnection: true}
	}
	if trace, _ := req.Context().Value(uploadConnectionKey{}).(*uploadConnectionTrace); trace != nil {
		trace.response(response.Proto, response.StatusCode)
	}
	defer response.Body.Close()
	if guard != nil {
		response.Body = guard.body(response.Body)
	}
	raw, e := readBounded(response.Body, 1<<20)
	if e != nil {
		return result, &wopanPartError{Message: e.Error(), Retry: req.Context().Err() == nil, ResetConnection: true}
	}
	if response.StatusCode != 200 {
		return result, &wopanPartError{Message: fmt.Sprintf("联通上传节点 HTTP %d", response.StatusCode), Retry: retryableHTTP(response.StatusCode), RetryAfter: retryAfterHeader(response.Header), Auth: response.StatusCode == 401 || response.StatusCode == 403}
	}
	if e = json.Unmarshal(raw, &result); e != nil {
		return result, errors.New("联通上传响应不是有效 JSON；本次结果需要核对目录")
	}
	if result.Code != "0000" {
		message := fmt.Sprintf("联通上传节点返回 %s：%s", result.Code, result.Msg)
		if result.Code == "500020001" || result.Code == "1001" {
			message += "；可在官网备用登录中更新授权"
		}
		if result.Code == "1001" || result.Code == "9999" {
			return result, &wopanPartError{Message: message, Retry: true, Auth: true, RetryAfter: 30 * time.Second}
		}
		return result, errors.New(message)
	}
	return result, nil
}

func providerMetadataClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 5 {
			return errors.New("网盘元数据请求重定向过多")
		}
		if len(via) > 0 && (req.URL.Scheme != via[0].URL.Scheme || req.URL.Host != via[0].URL.Host) {
			return errors.New("网盘元数据跨域重定向被拒绝，未转发账号凭据")
		}
		return nil
	}}
}
func (d *wopanDAVDrive) Remove(ctx context.Context, it davEntry) (err error) {
	if e := d.directMu.LockContext(ctx); e != nil {
		return e
	}
	defer d.directMu.Unlock()
	finish := logPhase(ctx, "删除条目", d.source(), it.Name+"；DeleteFile")
	defer func() { finish(err) }()
	if e := d.ready(); e != nil {
		return &davNotExecuted{e}
	}
	dirs, files := []string{}, []string{}
	if it.Directory {
		dirs = append(dirs, it.ID)
	} else {
		files = append(files, it.ID)
	}
	_, e := d.w.RequestWoHome(wopan.KeyDeleteFile, wopan.Json{"spaceType": wopan.SpaceTypePersonal, "familyId": d.family, "vipLevel": "0", "dirList": dirs, "fileList": files, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, nil, woContext(ctx))
	return mutationFailure(e, "DeleteFile", false)
}
func (d *wopanDAVDrive) Move(ctx context.Context, it davEntry, parent, name string) (err error) {
	if e := d.directMu.LockContext(ctx); e != nil {
		return e
	}
	defer d.directMu.Unlock()
	finish := logPhase(ctx, "重命名或移动", d.source(), it.Name+" → "+name+"；RenameFileOrDirectory / MoveFile")
	defer func() { finish(err) }()
	if e := d.ready(); e != nil {
		return &davNotExecuted{e}
	}
	renamed := false
	if name != it.Name {
		t := 1
		if it.Directory {
			t = 0
		}
		fileType := "0"
		if !it.Directory {
			fileType = d.w.GetFileType(name)
		}
		if _, e := d.w.RequestWoHome(wopan.KeyRenameFileOrDirectory, wopan.Json{"spaceType": wopan.SpaceTypePersonal, "familyId": d.family, "type": t, "fileType": fileType, "id": it.ID, "name": name, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, nil, woContext(ctx)); e != nil {
			return mutationFailure(e, "RenameFileOrDirectory", false)
		}
		renamed = true
	}
	if parent == "" {
		return nil
	}
	dirs, files := []string{}, []string{}
	if it.Directory {
		dirs = append(dirs, it.ID)
	} else {
		files = append(files, it.ID)
	}
	_, e := d.w.RequestWoHome(wopan.KeyMoveFile, wopan.Json{"targetDirId": parent, "sourceType": wopan.SpaceTypePersonal, "targetType": wopan.SpaceTypePersonal, "dirList": dirs, "fileList": files, "familyId": d.family, "fromFamilyId": d.family, "secret": false, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, nil, woContext(ctx))
	return mutationFailure(e, "MoveFile", renamed)
}

type mobileDAVDrive struct {
	originalDAVDrive
	direct    *Client
	onRefresh func(string, string) error
	budget    *fairLimiter
	partMiB   int
}

func (d *mobileDAVDrive) directClient() (*Client, error) {
	if d.direct != nil {
		return d.direct, nil
	}
	cfg := d.client.cfg
	cfg.Transport = "direct"
	cfg.BaseURL = "https://miniapp.yun.139.com"
	cl, e := newClient(cfg, "")
	if e != nil {
		return nil, e
	}
	d.direct = cl
	return cl, nil
}
func (d *mobileDAVDrive) List(ctx context.Context, dir string) ([]davEntry, error) {
	if e := d.mu.LockContext(ctx); e != nil {
		return nil, e
	}
	defer d.mu.Unlock()
	started := time.Now()
	r, e := d.client.List(ctx, dir, "webdav", nil)
	if e == nil && !r.Complete {
		e = errors.New("移动代理目录分页未完成")
	}
	logList(ctx, "网易代理", "/v1/api/m139 getdisk", started, mobileDAVEntries(r.Items), e)
	if e == nil && r.Complete {
		return mobileDAVEntries(r.Items), nil
	}
	primary := e
	logFallback(ctx, "网易代理", "移动官方接口", e)
	cl, e := d.directClient()
	if e != nil {
		return nil, e
	}
	started = time.Now()
	r, e = cl.List(ctx, dir, "webdav-direct", nil)
	if mobileAuthError(e) {
		if re := d.refresh(ctx); re == nil {
			r, e = cl.List(ctx, dir, "webdav-direct", nil)
		}
	}
	if e == nil && !r.Complete {
		e = errors.New("移动直连目录分页未完成")
	}
	logList(ctx, "移动官方接口", "getdisk", started, mobileDAVEntries(r.Items), e)
	if e != nil {
		return nil, fmt.Errorf("移动代理请求失败：%v；官方直连接口也失败：%w", primary, e)
	}
	if !r.Complete {
		return nil, errors.New("移动直连目录分页未完成，不能当作空目录")
	}
	return mobileDAVEntries(r.Items), nil
}
func mobileDAVEntries(items []Item) []davEntry {
	out := make([]davEntry, 0, len(items))
	for _, it := range items {
		out = append(out, davEntry{Item: it})
	}
	return out
}
func (d *mobileDAVDrive) URL(ctx context.Context, it davEntry) (string, error) {
	if e := d.mu.LockContext(ctx); e != nil {
		return "", e
	}
	defer d.mu.Unlock()
	started := time.Now()
	u, primary := d.client.downloadURL(ctx, it.ID, it.FID)
	if primary == nil {
		primary = validDAVURL(u)
	}
	logURL(ctx, "网易代理", "/v1/api/m139 下载地址", started, u, primary)
	if primary == nil && validDAVURL(u) == nil {
		return u, nil
	}
	logFallback(ctx, "网易代理", "移动官方接口", primary)
	cl, e := d.directClient()
	if e != nil {
		return "", e
	}
	started = time.Now()
	u, e = cl.downloadURL(ctx, it.ID, it.FID)
	if mobileAuthError(e) {
		if re := d.refresh(ctx); re == nil {
			u, e = cl.downloadURL(ctx, it.ID, it.FID)
		}
	}
	if e == nil {
		e = validDAVURL(u)
	}
	logURL(ctx, "移动官方接口", "下载地址", started, u, e)
	if e != nil {
		return "", fmt.Errorf("移动代理下载地址失败：%v；官方直连接口也失败：%w", primary, e)
	}
	return u, nil
}
func mobileAuthError(e error) bool {
	if e == nil {
		return false
	}
	s := e.Error()
	return strings.Contains(s, "授权/会话失效") || strings.Contains(s, "业务码 20011") || strings.Contains(s, "业务码 9999")
}
func (d *mobileDAVDrive) refresh(ctx context.Context) error {
	cl, e := d.directClient()
	if e != nil {
		return e
	}
	m := cl.cfg.Mobile
	body, e := compactJSON(map[string]any{"appId": m.AppID, "appSecret": m.AppSecret, "refreshToken": m.AccessToken})
	if e != nil {
		return e
	}
	raw, e := cl.request(ctx, "POST", "open-mpplatform/oauth2/refreshToken", nil, body, map[string]string{"Content-Type": "application/json;charset=UTF-8"})
	if e != nil {
		return e
	}
	result, e := decodeDocument(raw)
	if e != nil {
		return e
	}
	result, e = unwrap(result)
	if e != nil {
		return e
	}
	access := valueString(result["accessToken"])
	if access == "" {
		return errors.New("移动刷新接口未返回 accessToken")
	}
	cl.redact.Add(access)
	d.client.redact.Add(access)
	cl.cfg.Mobile.AccessToken = access
	d.client.cfg.Mobile.AccessToken = access
	if d.onRefresh != nil {
		return d.onRefresh(access, "")
	}
	return nil
}

func mobileResult(raw []byte) (map[string]any, error) {
	m, e := decodeDocument(raw)
	if e != nil {
		return nil, e
	}
	m, e = unwrap(m)
	if e != nil {
		return nil, e
	}
	if result := object(m["result"]); result != nil {
		m = result
	}
	if e = checkCode(m); e != nil {
		return nil, e
	}
	if _, ok := m["@resultCode"]; !ok {
		if _, ok = m["resultCode"]; !ok {
			return nil, errors.New("移动响应缺少明确的 resultCode，不能视为操作成功")
		}
	}
	return m, nil
}
func (d *mobileDAVDrive) command(ctx context.Context, method, body string) (result map[string]any, err error) {
	if e := d.mu.LockContext(ctx); e != nil {
		return nil, e
	}
	defer d.mu.Unlock()
	started := time.Now()
	source := "网易代理"
	defer func() {
		entry := runLogEntry{Operation: "移动写入接口", Source: source, Endpoint: method, Result: "成功", DurationMs: time.Since(started).Milliseconds()}
		if err != nil {
			entry.Result, entry.Error = "失败", err.Error()
		}
		logDAV(ctx, entry)
	}()
	b, e := d.client.mobile(ctx, "richlifeApp/devapp/"+method, `<?xml version="1.0" encoding="UTF-8"?>`+body)
	if e == nil {
		var result map[string]any
		result, e = mobileResult(b)
		if e == nil {
			return result, nil
		}
	}
	// Authentication rejection happens before execution. Unknown write outcomes
	// are returned for inspection instead of replaying the whole operation.
	if mobileAuthError(e) {
		if re := d.refresh(ctx); re == nil {
			b, e = d.client.mobile(ctx, "richlifeApp/devapp/"+method, `<?xml version="1.0" encoding="UTF-8"?>`+body)
			if e == nil {
				result, e := mobileResult(b)
				return result, mutationFailure(e, method, false)
			}
		}
	}
	if code := businessErrorCode(e); code == "10012" || code == "10011" {
		logFallback(ctx, "网易代理", "移动官方接口", e)
		source = "移动官方接口"
		cl, de := d.directClient()
		if de != nil {
			return nil, de
		}
		b, de = cl.mobile(ctx, "richlifeApp/devapp/"+method, `<?xml version="1.0" encoding="UTF-8"?>`+body)
		if de != nil {
			return nil, mutationFailure(de, method, false)
		}
		result, de := mobileResult(b)
		return result, mutationFailure(de, method, false)
	}
	return nil, mutationFailure(e, method, false)
}
func (d *mobileDAVDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	m, e := d.command(ctx, "createCatalogExt", "<createCatalogExt><createCatalogExtReq><parentCatalogID>"+xmlEscape(parent)+"</parentCatalogID><newCatalogName>"+xmlEscape(name)+"</newCatalogName></createCatalogExtReq></createCatalogExt>")
	if e != nil {
		return davEntry{}, e
	}
	x := object(m["catalogInfo"])
	id := valueString(x["catalogID"])
	if id == "" {
		return davEntry{}, errors.New("移动新建目录未返回 catalogInfo.catalogID；开放接口可能只允许应用目录内写入")
	}
	return davEntry{Item: Item{ID: id, Name: name, Directory: true}, Modified: time.Now()}, nil
}
func (d *mobileDAVDrive) Remove(ctx context.Context, it davEntry) error {
	catalog, content := `<catalogIDs length="0"/>`, `<contentIDs length="0"/>`
	if it.Directory {
		catalog = `<catalogIDs length="1"><ID>` + xmlEscape(it.ID) + `</ID></catalogIDs>`
	} else {
		content = `<contentIDs length="1"><ID>` + xmlEscape(it.ID) + `</ID></contentIDs>`
	}
	_, e := d.command(ctx, "delCatalogContent", "<delCatalogContent>"+catalog+content+`<contentURLs length="0"/><oprReason>0</oprReason></delCatalogContent>`)
	return e
}
func (d *mobileDAVDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	renamed := false
	if name != it.Name {
		method, body := "updateContentInfo", "<updateContentInfo><contentID>"+xmlEscape(it.ID)+"</contentID><contentName>"+xmlEscape(name)+"</contentName></updateContentInfo>"
		if it.Directory {
			method, body = "updateCatalogInfo", "<updateCatalogInfo><catalogID>"+xmlEscape(it.ID)+"</catalogID><catalogName>"+xmlEscape(name)+"</catalogName><manualRename>1</manualRename></updateCatalogInfo>"
		} else if !strings.EqualFold(path.Ext(name), path.Ext(it.Name)) {
			return errors.New("移动开放接口不支持修改文件后缀；请使用复制到新名称后删除原文件")
		}
		if _, e := d.command(ctx, method, body); e != nil {
			return e
		}
		renamed = true
	}
	if parent == "" {
		return nil
	}
	catalog, content := `<catalogInfoList length="0"/>`, `<contentInfoList length="0"/>`
	if it.Directory {
		catalog = `<catalogInfoList length="1"><ID>` + xmlEscape(it.ID) + `</ID></catalogInfoList>`
	} else {
		content = `<contentInfoList length="1"><ID>` + xmlEscape(it.ID) + `</ID></contentInfoList>`
	}
	_, e := d.command(ctx, "moveContentCatalog", "<moveContentCatalog>"+content+catalog+"<newCatalogID>"+xmlEscape(parent)+"</newCatalogID></moveContentCatalog>")
	if e != nil && renamed {
		return &davMutationError{Phase: "moveContentCatalog", Partial: true, Err: e}
	}
	return e
}
func (d *mobileDAVDrive) Upload(ctx context.Context, parent, name string, f *os.File) (entry davEntry, err error) {
	finish := logPhase(ctx, "移动云端上传", "移动上传节点", name)
	defer func() { finish(err) }()
	fi, e := f.Stat()
	if e != nil {
		return davEntry{}, e
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return davEntry{}, e
	}
	h := md5.New()
	if _, e = io.Copy(h, f); e != nil {
		return davEntry{}, e
	}
	size := strconv.FormatInt(fi.Size(), 10)
	body := `<pcUploadFileRequest><totalSize>` + size + `</totalSize><uploadContentList length="1"><uploadContentInfo><contentName>` + xmlEscape(name) + `</contentName><contentSize>` + size + `</contentSize><digest>` + strings.ToUpper(fmt.Sprintf("%x", h.Sum(nil))) + `</digest></uploadContentInfo></uploadContentList><parentCatalogID>` + xmlEscape(parent) + `</parentCatalogID><operation>0</operation></pcUploadFileRequest>`
	m, e := d.command(ctx, "pcUploadFileRequest", body)
	if e != nil {
		return davEntry{}, e
	}
	r := object(m["uploadResult"])
	entries := list(object(r["newContentIDList"])["newContent"])
	if len(entries) != 1 {
		return davEntry{}, errors.New("移动上传第一阶段未返回唯一的文件信息")
	}
	x := object(entries[0])
	id, actualName := valueString(x["contentID"]), valueString(x["contentName"])
	if id == "" || actualName != name {
		return davEntry{}, errors.New("移动上传返回的文件名称或 ID 与请求不符，已停止写入")
	}
	need := valueString(x["isNeedUpload"])
	if need != "0" && need != "1" && need != "2" {
		return davEntry{}, errors.New("移动上传未返回有效 isNeedUpload 标记")
	}
	if need == "1" {
		u, task := valueString(r["redirectionUrl"]), valueString(r["uploadTaskID"])
		if e = validDownloadURL(u); e != nil || task == "" {
			return davEntry{}, errors.New("移动上传地址或任务 ID 无效")
		}
		hc := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
		var sent, confirmed atomic.Int64
		partSize := int64(5 << 20)
		if d.partMiB >= 5 && d.partMiB <= 16 {
			partSize = int64(d.partMiB) << 20
		}
		for off := int64(0); off < fi.Size() || off == 0; {
			n := min(partSize, fi.Size()-off)
			last := off+n == fi.Size()
			if e = d.sendUploadPart(ctx, hc, u, task, name, fi.Size(), off, n, max(int64(1), (fi.Size()+partSize-1)/partSize), off/partSize+1, last, f, &sent, &confirmed); e != nil {
				return davEntry{}, e
			}
			off += n
			if last {
				break
			}
		}
	}
	for attempt := 0; attempt < 5; attempt++ {
		entries, e := d.List(directoryLogContext(ctx), parent)
		if e != nil {
			return davEntry{}, e
		}
		for _, it := range entries {
			if it.ID == id && it.Name == name && it.Size == fi.Size() {
				return it, nil
			}
		}
		if e = davWait(ctx, 400*time.Millisecond); e != nil {
			return davEntry{}, e
		}
	}
	return davEntry{}, errors.New("移动上传后未在指定目录确认到名称、大小一致的文件；开放接口可能将上传限制在应用目录")
}

func contentType(name string) string {
	if v := mime.TypeByExtension(path.Ext(name)); v != "" {
		return v
	}
	return "application/octet-stream"
}
func davWait(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

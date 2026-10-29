package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	bridge interface {
		Request(context.Context, string, string, []byte, map[string]string) ([]byte, error)
	}
	cfg    Config
	http   *http.Client
	redact *Redactor
	folder string
	seq    int
	key    []byte
}

func newClient(c Config, folder string) (*Client, error) {
	if e := c.validate(); e != nil {
		return nil, e
	}
	cl := &Client{cfg: c, folder: folder, redact: newRedactor(c), http: &http.Client{Timeout: time.Duration(c.TimeoutSeconds) * time.Second}}
	cl.http.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && req.URL.Scheme+"://"+req.URL.Host != via[0].URL.Scheme+"://"+via[0].URL.Host {
			return errors.New("API 跨域重定向被拒绝，避免向其他服务转发凭据")
		}
		if len(via) > 5 {
			return errors.New("API 重定向次数过多")
		}
		return nil
	}
	if c.Transport == "netease" {
		var e error
		cl.key, e = nativeKey(c.Netease)
		if e != nil {
			return nil, stage("网易传输配置", e)
		}
		cl.redact.Add(string(cl.key))
	}
	return cl, nil
}
func (c *Client) trace(method, endpoint string, h http.Header, body []byte, status int, rh http.Header, response []byte, err error) error {
	if c.folder == "" {
		return nil
	}
	c.seq++
	record := map[string]any{"method": method, "url": c.redact.Text(endpoint), "request_headers": c.redact.Headers(h), "request_logical_body": c.redact.Text(string(body)), "http_status": status, "response_headers": c.redact.Headers(rh), "response_logical_body": c.redact.Text(string(response))}
	if err != nil {
		record["error"] = c.redact.Text(err.Error())
	}
	b, e := json.MarshalIndent(record, "", "  ")
	if e != nil {
		return e
	}
	return writeNew(filepath.Join(c.folder, fmt.Sprintf("http-%03d.json", c.seq)), b)
}
func (c *Client) request(ctx context.Context, method, path string, params map[string]any, body []byte, headers map[string]string) (data []byte, err error) {
	if c.cfg.Transport == "bridge" && c.bridge == nil {
		return nil, errors.New("原版桥接尚未连接，请通过界面启动")
	}
	if c.bridge != nil {
		logical := body
		if method == "GET" {
			logical, _ = compactJSON(params)
		}
		data, err = c.bridge.Request(ctx, method, path, logical, headers)
		h := http.Header{}
		for k, v := range headers {
			h.Set(k, v)
		}
		if e := c.trace(method, "original-sdk:"+path, h, logical, 0, nil, data, err); e != nil {
			return nil, e
		}
		return data, err
	}
	if c.cfg.Transport == "netease" {
		path = strings.TrimRight(c.cfg.Netease.PathPrefix, "/") + "/" + strings.TrimLeft(path, "/")
	}
	u, e := url.Parse(strings.TrimRight(c.cfg.BaseURL, "/") + "/" + strings.TrimLeft(path, "/"))
	if e != nil {
		return nil, e
	}
	if method == "GET" {
		u.RawQuery = queryParameters(params)
	}
	logical := append([]byte(nil), body...)
	if method == "GET" {
		logical, _ = compactJSON(params)
	}
	payload := body
	h := http.Header{}
	for k, v := range c.cfg.Headers {
		h.Set(k, v)
	}
	for k, v := range headers {
		h.Set(k, v)
	}
	if c.cfg.Transport == "netease" {
		n := c.cfg.Netease
		seed := strconv.FormatInt(time.Now().Unix(), 10)
		tag := makeSecTag(n)
		fixed := map[string]string{"SystemType": "win", "SystemVersion": "Windows 10", "Locale": "zh-CN", "AppVersionCode": n.AppVersionCode, "AppVersion": n.AppVersion, "DeviceId": n.DeviceID, "Resolution": "1920x1080", "Seed": seed, "SecTag": tag, "Sign": nativeSign(method, params, body, tag, seed, c.key), "NetworkType": "1", "TimeZone": "Asia/Shanghai", "TimeZoneOffset": "480"}
		for k, v := range fixed {
			if old := h.Get(k); old != "" && k != "Seed" && k != "SecTag" && k != "Sign" {
				v = old
			}
			h.Del(k)
			h[k] = []string{v}
		}
		if n.Token != "" {
			h.Set("Token", n.Token)
		}
		if n.SID != "" {
			h["sid"] = []string{n.SID}
		}
		if method == "POST" && len(body) > 0 {
			payload, e = encryptNative(c.key, body)
			if e != nil {
				return nil, stage("请求加密", e)
			}
			h.Set("Content-Type", "application/octet-stream")
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(payload))
	if e != nil {
		return nil, e
	}
	req.Header = h
	resp, e := c.http.Do(req)
	if e != nil {
		_ = c.trace(method, u.String(), h, logical, 0, nil, nil, e)
		return nil, stage("HTTP 请求", e)
	}
	defer resp.Body.Close()
	raw, e := readBounded(resp.Body, c.cfg.MaxResponseBytes)
	if e == nil && strings.EqualFold(resp.Header.Get("encryption"), "True") {
		if len(c.key) == 0 {
			e = errors.New("服务端返回加密响应，当前传输未配置网易解密")
		} else {
			raw, e = decryptNative(c.key, raw, c.cfg.MaxResponseBytes)
		}
	}
	if te := c.trace(method, u.String(), h, logical, resp.StatusCode, resp.Header, raw, e); te != nil {
		return nil, stage("写入诊断记录", te)
	}
	if e != nil {
		return nil, stage("读取或解密响应", e)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if doc, de := decodeDocument(raw); de == nil {
			if _, be := unwrap(doc); be != nil {
				return nil, &apiHTTPError{Code: resp.StatusCode, Err: fmt.Errorf("HTTP 状态 %d；%w", resp.StatusCode, be)}
			}
		}
		return nil, &apiHTTPError{Code: resp.StatusCode, Err: fmt.Errorf("HTTP 状态 %d；响应已脱敏保存", resp.StatusCode)}
	}
	return raw, nil
}
func (c *Client) ensureSID(ctx context.Context) error {
	if c.cfg.Transport != "netease" || c.cfg.Netease.SID != "" {
		return nil
	}
	b, e := c.request(ctx, "POST", "/v1/auth", nil, nil, nil)
	if e != nil {
		return stage("获取网易匿名 sid", e)
	}
	m, e := decodeDocument(b)
	if e != nil {
		return e
	}
	m, e = unwrap(m)
	if e != nil {
		return e
	}
	sid := valueString(m["sid"])
	if sid == "" {
		return errors.New("/v1/auth 未返回 sid")
	}
	c.cfg.Netease.SID = sid
	c.redact.Add(sid)
	return nil
}
func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
func (c *Client) mobile(ctx context.Context, path, body string) ([]byte, error) {
	m := c.cfg.Mobile
	for k, v := range map[string]string{"app_id": m.AppID, "app_secret": m.AppSecret, "access_token": m.AccessToken, "device_id": m.DeviceID} {
		if v == "" {
			return nil, fmt.Errorf("mobile.%s 不能为空", k)
		}
	}
	h := map[string]string{"Content-Type": "text/xml;charset=UTF-8", "appid": m.AppID, "secretkey": m.AppSecret, "channelid": m.ChannelID, "accesstoken": m.AccessToken, "deviceid": m.DeviceID}
	if c.cfg.Transport == "direct" {
		return c.request(ctx, "POST", path, nil, []byte(body), h)
	}
	if e := c.ensureSID(ctx); e != nil {
		return nil, e
	}
	wrapped, e := compactJSON(map[string]any{"method": "POST", "path": path, "headers": h, "body": body})
	if e != nil {
		return nil, e
	}
	return c.request(ctx, "POST", "/v1/api/m139", nil, wrapped, map[string]string{"Content-Type": "application/json"})
}
func (c *Client) wopan(ctx context.Context, method, path string, params map[string]any) ([]byte, error) {
	if c.cfg.Wopan.AccessToken == "" {
		return nil, errors.New("wopan.access_token 不能为空")
	}
	if e := c.ensureSID(ctx); e != nil {
		return nil, e
	}
	if method == "GET" {
		return c.request(ctx, method, "/v1/api/wopan"+path, params, nil, nil)
	}
	b, e := compactJSON(params)
	if e != nil {
		return nil, e
	}
	return c.request(ctx, method, "/v1/api/wopan"+path, nil, b, map[string]string{"Content-Type": "application/json"})
}
func (c *Client) page(ctx context.Context, dir string, page int) (Page, error) {
	var b []byte
	var e error
	if c.cfg.Provider == "mobile" {
		start := (page-1)*c.cfg.PageSize + 1
		end := page * c.cfg.PageSize
		filter := 2
		if c.cfg.IncludeDirectories {
			filter = 0
		}
		body := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><getDisk><catalogID>%s</catalogID><filterType>%d</filterType><catalogSortType>0</catalogSortType><contentType>0</contentType><contentSortType>0</contentSortType><sortDirection>1</sortDirection><startNumber>%d</startNumber><endNumber>%d</endNumber><catalogType>0</catalogType></getDisk>`, xmlEscape(dir), filter, start, end)
		b, e = c.mobile(ctx, "richlifeApp/devapp/getdisk", body)
	} else {
		b, e = c.wopan(ctx, "GET", "/query_files", map[string]any{"directory_id": uriEncode(dir), "page": page, "page_size": c.cfg.PageSize, "space_type": c.cfg.Wopan.SpaceType, "access_token": uriEncode(c.cfg.Wopan.AccessToken)})
	}
	if e != nil {
		return Page{}, e
	}
	return parsePage(c.cfg.Provider, b)
}
func (c *Client) List(ctx context.Context, dir, mode string, names []string) (Report, error) {
	inputDir := dir
	if c.cfg.Provider == "wopan" && strings.EqualFold(strings.TrimSpace(dir), "root") {
		dir = "0"
	}
	r := newReport(c.cfg.Provider, mode, dir)
	if inputDir != dir {
		r.Notes = append(r.Notes, "联通根目录别名 root 已转换为该接口使用的目录 ID 0。")
	}
	seen := map[string]bool{}
	pages := map[string]bool{}
	for p := 1; p <= c.cfg.MaxPages; p++ {
		page, e := c.page(ctx, dir, p)
		if e != nil {
			r.Error = c.redact.Text(e.Error())
			r.StopReason = "请求或解析失败"
			applyExpected(&r, names)
			return r, e
		}
		r.Pages = p
		r.Notes = append(r.Notes, page.Notes...)
		if page.ListOmitted {
			r.Complete = true
			if len(r.Items) == 0 {
				r.StopReason = "本次请求未返回文件条目，接口省略了列表；请核对目录 ID（不代表整个网盘为空）"
			} else {
				r.StopReason = "结束页省略了文件列表，已按分页标记结束，保留前面各页条目"
			}
			break
		}
		if len(page.Items) > 0 {
			fp := fingerprint(page.Items)
			if pages[fp] {
				r.StopReason = "返回重复页，服务可能忽略了分页参数"
				break
			}
			pages[fp] = true
		}
		added := 0
		for _, it := range page.Items {
			k := it.ID + "\x00" + it.FID
			if k == "\x00" {
				k = it.Name + "\x00" + it.Suffix
			}
			if !seen[k] {
				r.Items = append(r.Items, it)
				seen[k] = true
				added++
			}
		}
		if page.More != nil && !*page.More {
			r.Complete = true
			r.StopReason = "接口分页标记表示完成"
			break
		}
		if len(page.Items) == 0 {
			if page.More != nil && *page.More {
				r.StopReason = "空页却声明尚有下一页"
			} else {
				r.Complete = true
				r.StopReason = "未提供分页标记，继续请求至空页"
			}
			break
		}
		if added == 0 {
			r.StopReason = "当前页没有新增条目，停止避免循环"
			break
		}
	}
	if r.StopReason == "" {
		r.StopReason = "达到 max_pages 上限，查询未完成"
	}
	applyExpected(&r, names)
	return r, nil
}
func (c *Client) downloadURL(ctx context.Context, id, fid string) (string, error) {
	if c.cfg.Provider == "mobile" {
		if id == "" {
			return "", errors.New("移动下载需要 -id contentID")
		}
		body := `<?xml version="1.0" encoding="UTF-8"?><downloadRequest><contentID>` + xmlEscape(id) + `</contentID></downloadRequest>`
		b, e := c.mobile(ctx, "richlifeApp/devapp/downloadRequest", body)
		if e != nil {
			return "", e
		}
		return parseDownload("mobile", id, b)
	}
	if fid == "" {
		if id == "" {
			return "", errors.New("联通下载需 -id 或 -fid")
		}
		b, e := c.wopan(ctx, "POST", "/query_file_info", map[string]any{"access_token": c.cfg.Wopan.AccessToken, "file_id_list": []string{id}})
		if e != nil {
			return "", e
		}
		m, e := decodeDocument(b)
		if e != nil {
			return "", e
		}
		m, e = unwrap(m)
		if e != nil {
			return "", e
		}
		for _, v := range list(m["fileInfoList"]) {
			x := object(v)
			if valueString(x["id"]) == id {
				fid = valueString(x["fid"])
				break
			}
		}
		if fid == "" {
			return "", errors.New("文件信息中未找到指定 id 对应的 fid")
		}
	}
	b, e := c.wopan(ctx, "POST", "/get_download_url", map[string]any{"access_token": c.cfg.Wopan.AccessToken, "fid_list": []string{fid}})
	if e != nil {
		return "", e
	}
	return parseDownload("wopan", fid, b)
}
func writeNew(path string, data []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(data); e != nil {
		f.Close()
		return e
	}
	return f.Close()
}

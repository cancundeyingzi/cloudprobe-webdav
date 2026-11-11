package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// The website authorization and the mini-app authorization are separate
// channels. Website file IDs must never be sent to the mini-app API.
type mobileWebDrive struct {
	token, account, host string
	http                 *http.Client
	credentials          func() map[string]any
}

func mobileWebAccount(token string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(strings.TrimPrefix(token, "Basic ")))
	if err != nil {
		return "", errors.New("移动官网 Authorization 格式无效")
	}
	parts := strings.SplitN(string(raw), ":", 3)
	if len(parts) != 3 || parts[0] == "" || cloudPhone(parts[1]) == "" || parts[2] == "" {
		return "", errors.New("移动官网授权缺少完整账号或会话")
	}
	return cloudPhone(parts[1]), nil
}

func mobileWebSign(body []byte, stamp, nonce string) string {
	encoded := url.QueryEscape(string(body))
	for _, r := range [][2]string{{"+", "%20"}, {"%21", "!"}, {"%27", "'"}, {"%28", "("}, {"%29", ")"}, {"%2A", "*"}} {
		encoded = strings.ReplaceAll(encoded, r[0], r[1])
	}
	chars := strings.Split(encoded, "")
	sort.Strings(chars)
	digest := func(s string) string { h := md5.Sum([]byte(s)); return hex.EncodeToString(h[:]) }
	return strings.ToUpper(digest(digest(base64.StdEncoding.EncodeToString([]byte(strings.Join(chars, "")))) + digest(stamp+":"+nonce)))
}

func mobileWebHost(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && (u.Hostname() == "yun.139.com" || strings.HasSuffix(u.Hostname(), ".yun.139.com")) && (u.Port() == "" || u.Port() == "443")
}

func (d *mobileWebDrive) request(ctx context.Context, endpoint string, body map[string]any) (map[string]any, error) {
	if d.credentials != nil {
		c := d.credentials()
		if t := valueString(c["mobile_web_token"]); t != "" {
			copy := *d
			copy.token = t
			copy.credentials = nil
			return copy.request(ctx, endpoint, body)
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	stamp := time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05")
	nonce := randomKey()[:16]
	for k, v := range map[string]string{
		"Content-Type": "application/json", "Authorization": "Basic " + d.token,
		"Origin": "https://yun.139.com", "Referer": "https://yun.139.com/w/",
		"Caller": "web", "Cms-Device": "default", "Mcloud-Channel": "1000101", "Mcloud-Client": "10701", "Mcloud-Route": "001",
		"Mcloud-Sign": stamp + "," + nonce + "," + mobileWebSign(raw, stamp, nonce), "Mcloud-Version": "7.14.0",
		"x-DeviceInfo": "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||", "x-huawei-channelSrc": "10000034", "x-inner-ntwk": "2", "x-m4c-caller": "PC", "x-m4c-src": "10002", "x-SvcType": "1",
		"X-Yun-Api-Version": "v1", "X-Yun-App-Channel": "10000034", "X-Yun-Channel-Source": "10000034", "X-Yun-Client-Info": "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||dW5kZWZpbmVk||", "X-Yun-Module-Type": "100", "X-Yun-Svc-Type": "1",
	} {
		req.Header.Set(k, v)
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, &apiHTTPError{Code: resp.StatusCode, Err: fmt.Errorf("移动官网接口 HTTP %d", resp.StatusCode)}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 8<<20 {
		return nil, errors.New("移动官网响应超出上限")
	}
	var envelope struct {
		Success *bool          `json:"success"`
		Code    string         `json:"code"`
		Data    map[string]any `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err = dec.Decode(&envelope); err != nil {
		return nil, errors.New("移动官网未返回有效 JSON")
	}
	if envelope.Success == nil || !*envelope.Success {
		return nil, &apiBusinessError{Code: envelope.Code, Message: "移动官网未确认请求成功，请检查授权状态"}
	}
	if envelope.Data == nil {
		switch req.URL.Path {
		case "/file/update", "/file/batchMove", "/recyclebin/batchTrash", "/file/complete":
			return map[string]any{}, nil
		}
		return nil, errors.New("移动官网响应缺少 data")
	}
	return envelope.Data, nil
}
func (d *mobileWebDrive) post(ctx context.Context, path string, body map[string]any) (map[string]any, error) {
	return d.request(ctx, strings.TrimRight(d.host, "/")+path, body)
}

func (g *guiApp) acceptMobileWebToken(ctx context.Context, token string) error {
	token = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(token), "Basic "))
	if len(token) > 16384 || strings.ContainsAny(token, "\r\n") {
		return errors.New("移动官网授权格式无效")
	}
	account, err := mobileWebAccount(token)
	if err != nil {
		return err
	}
	d := &mobileWebDrive{token: token, account: account, http: providerMetadataClient(25 * time.Second)}
	route, err := d.request(ctx, "https://user-njs.yun.139.com/user/route/qryRoutePolicy", map[string]any{"userInfo": map[string]any{"userType": 1, "accountType": 1, "accountName": account}, "modAddrType": 1})
	if err != nil {
		return err
	}
	for _, v := range list(route["routePolicyList"]) {
		r := object(v)
		if valueString(r["modName"]) == "personal" && mobileWebHost(valueString(r["httpsUrl"])) {
			d.host = valueString(r["httpsUrl"])
			break
		}
	}
	if d.host == "" {
		return errors.New("移动官网未返回有效的个人云盘接口")
	}
	if _, err = d.List(ctx, "/"); err != nil {
		return fmt.Errorf("移动官网授权未通过目录验证：%w", err)
	}
	return g.acceptCloudAccount(ctx, guiAccount{Provider: "mobile", Name: "移动云盘 · " + maskAccount(account), Root: "/", credentials: map[string]any{"user_id": account, "mobile": account, "mobile_web_token": token, "mobile_web_host": d.host}})
}

func newMobileWebDrive(a guiAccount) *mobileWebDrive {
	return &mobileWebDrive{token: valueString(a.credentials["mobile_web_token"]), account: valueString(a.credentials["mobile"]), host: valueString(a.credentials["mobile_web_host"]), http: providerMetadataClient(30 * time.Second)}
}
func mobileWebEntry(m map[string]any, parent string) (davEntry, error) {
	e := davEntry{Item: Item{ID: valueString(m["fileId"]), Name: valueString(m["name"]), Directory: valueString(m["type"]) == "folder"}, Parent: parent}
	if e.ID == "" || e.Name == "" {
		return e, errors.New("移动官网目录条目缺少身份或名称")
	}
	if !e.Directory {
		n, err := nonnegativeInteger(m["size"])
		if err != nil {
			return e, err
		}
		e.Size = n
	}
	e.Modified, _ = time.Parse(time.RFC3339Nano, valueString(m["updatedAt"]))
	if h := valueString(m["contentHash"]); len(h) == 64 {
		if _, err := hex.DecodeString(h); err == nil {
			e.ContentVersion = fmt.Sprintf("sha256:%s:%d", strings.ToLower(h), e.Size)
		}
	}
	return e, nil
}
func (d *mobileWebDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	if parent == "root" {
		parent = "/"
	}
	cursor := ""
	seen := map[string]bool{}
	ids := map[string]bool{}
	entries := []davEntry{}
	for page := 0; page < 10000; page++ {
		data, err := d.post(ctx, "/file/list", map[string]any{"parentFileId": parent, "orderBy": "updated_at", "orderDirection": "DESC", "pageInfo": map[string]any{"pageSize": 100, "pageCursor": cursor}})
		if err != nil {
			return nil, err
		}
		items, ok := data["items"].([]any)
		if !ok {
			return nil, errors.New("移动官网目录响应缺少 items")
		}
		for _, v := range items {
			e, err := mobileWebEntry(object(v), parent)
			if err != nil {
				return nil, err
			}
			if ids[e.ID] {
				return nil, errors.New("移动官网分页返回重复对象")
			}
			ids[e.ID] = true
			entries = append(entries, e)
		}
		cursor = valueString(data["nextPageCursor"])
		if cursor == "" {
			return entries, nil
		}
		if seen[cursor] {
			return nil, errors.New("移动官网分页游标重复")
		}
		seen[cursor] = true
	}
	return nil, errors.New("移动官网目录超过分页上限")
}
func (d *mobileWebDrive) URL(ctx context.Context, e davEntry) (string, error) {
	data, err := d.post(ctx, "/file/getDownloadUrl", map[string]any{"fileId": e.ID})
	if err != nil {
		return "", err
	}
	u := valueString(data["url"])
	if data["cdnSwitch"] == true && valueString(data["cdnUrl"]) != "" {
		u = valueString(data["cdnUrl"])
	}
	return u, validDAVURL(u)
}
func (d *mobileWebDrive) verify(ctx context.Context, parent, id, name string, removed bool) (davEntry, error) {
	entries, err := d.List(ctx, parent)
	if err != nil {
		return davEntry{}, err
	}
	for _, e := range entries {
		if e.ID == id {
			if !removed && e.Name == name {
				return e, nil
			}
			return davEntry{}, errors.New("移动官网对象状态与操作不符")
		}
	}
	if removed {
		return davEntry{}, nil
	}
	return davEntry{}, errors.New("移动官网未返回操作后的对象")
}
func (d *mobileWebDrive) Mkdir(ctx context.Context, parent, name string) (davEntry, error) {
	data, err := d.post(ctx, "/file/create", map[string]any{"parentFileId": parent, "name": name, "description": "", "type": "folder", "fileRenameMode": "force_rename"})
	if err != nil {
		return davEntry{}, mutationFailure(err, "创建目录", false)
	}
	e, err := d.verify(ctx, parent, valueString(data["fileId"]), name, false)
	return e, mutationFailure(err, "核验新目录", true)
}
func (d *mobileWebDrive) Remove(ctx context.Context, e davEntry) error {
	_, err := d.post(ctx, "/recyclebin/batchTrash", map[string]any{"fileIds": []string{e.ID}})
	if err != nil {
		return mutationFailure(err, "移入回收站", false)
	}
	_, err = d.verify(ctx, e.Parent, e.ID, e.Name, true)
	return mutationFailure(err, "核验回收站操作", true)
}
func (d *mobileWebDrive) Move(ctx context.Context, e davEntry, parent, name string) error {
	changed := false
	if parent != e.Parent {
		_, err := d.post(ctx, "/file/batchMove", map[string]any{"fileIds": []string{e.ID}, "toParentFileId": parent})
		if err != nil {
			return mutationFailure(err, "移动", false)
		}
		changed = true
	}
	if name != e.Name {
		_, err := d.post(ctx, "/file/update", map[string]any{"fileId": e.ID, "name": name, "description": ""})
		if err != nil {
			return mutationFailure(err, "重命名", changed)
		}
	}
	_, err := d.verify(ctx, parent, e.ID, name, false)
	return mutationFailure(err, "核验移动", true)
}

func (d *mobileWebDrive) Upload(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	info, err := f.Stat()
	if err != nil {
		return davEntry{}, err
	}
	h := sha256.New()
	if _, err = io.Copy(h, io.NewSectionReader(f, 0, info.Size())); err != nil {
		return davEntry{}, err
	}
	hash := hex.EncodeToString(h.Sum(nil))
	partSize := int64(100 << 20)
	if info.Size() > 30<<30 {
		partSize = 512 << 20
	}
	count := max(int64(1), (info.Size()+partSize-1)/partSize)
	if count > 10000 {
		return davEntry{}, errors.New("移动上传分片数超过上游上限")
	}
	parts := make([]map[string]any, 0, count)
	for i := int64(0); i < count; i++ {
		parts = append(parts, map[string]any{"partNumber": i + 1, "partSize": min(partSize, info.Size()-i*partSize), "parallelHashCtx": map[string]any{"partOffset": i * partSize}})
	}
	data, err := d.post(ctx, "/file/create", map[string]any{"parentFileId": parent, "name": name, "type": "file", "fileRenameMode": "auto_rename", "size": info.Size(), "contentHash": hash, "contentHashAlgorithm": "SHA256", "contentType": "application/octet-stream", "parallelUpload": false, "partInfos": parts[:min(100, len(parts))]})
	if err != nil {
		return davEntry{}, mutationFailure(err, "创建上传", false)
	}
	id, upload := valueString(data["fileId"]), valueString(data["uploadId"])
	if id == "" {
		return davEntry{}, mutationFailure(errors.New("移动上传响应缺少 fileId"), "创建上传", true)
	}
	if data["exist"] != true && (data["rapidUpload"] != true || len(list(data["partInfos"])) > 0) {
		urls := list(data["partInfos"])
		for batch := 0; batch < len(parts); batch += 100 {
			if batch > 0 {
				data, err = d.post(ctx, "/file/getUploadUrl", map[string]any{"fileId": id, "uploadId": upload, "partInfos": parts[batch:min(batch+100, len(parts))], "commonAccountInfo": map[string]any{"account": d.account, "accountType": 1}})
				if err != nil {
					return davEntry{}, mutationFailure(err, "获取分片地址", true)
				}
				urls = list(data["partInfos"])
			}
			if len(urls) != min(100, len(parts)-batch) {
				return davEntry{}, mutationFailure(errors.New("上传分片地址数量不符"), "上传分片", true)
			}
			seen := map[int64]bool{}
			for _, v := range urls {
				p := object(v)
				n, err := nonnegativeInteger(p["partNumber"])
				if err != nil || n < int64(batch+1) || n > int64(min(batch+100, len(parts))) || seen[n] {
					return davEntry{}, mutationFailure(errors.New("上传分片编号无效"), "上传分片", true)
				}
				seen[n] = true
				u := valueString(p["uploadUrl"])
				if err = validDAVURL(u); err != nil {
					return davEntry{}, mutationFailure(err, "上传分片", true)
				}
				size := min(partSize, info.Size()-(n-1)*partSize)
				req, err := http.NewRequestWithContext(ctx, "PUT", u, io.NewSectionReader(f, (n-1)*partSize, size))
				if err != nil {
					return davEntry{}, err
				}
				req.ContentLength = size
				req.Header.Set("Content-Type", "application/octet-stream")
				req.Header.Set("Origin", "https://yun.139.com")
				req.Header.Set("Referer", "https://yun.139.com/")
				bodyClient := *d.http
				bodyClient.Timeout = 0
				resp, err := bodyClient.Do(req)
				if err != nil {
					return davEntry{}, mutationFailure(err, "上传分片", true)
				}
				io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
				resp.Body.Close()
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					return davEntry{}, mutationFailure(fmt.Errorf("上传分片 HTTP %d", resp.StatusCode), "上传分片", true)
				}
			}
		}
		_, err = d.post(ctx, "/file/complete", map[string]any{"fileId": id, "uploadId": upload, "contentHash": hash, "contentHashAlgorithm": "SHA256"})
		if err != nil {
			return davEntry{}, mutationFailure(err, "完成上传", true)
		}
	}
	e, err := d.verify(ctx, parent, id, name, false)
	if err == nil && e.Size != info.Size() {
		err = errors.New("移动上传后文件大小不符")
	}
	return e, mutationFailure(err, "核验上传", true)
}

func (g *guiApp) mobileAccountDrive(a guiAccount) davDrive {
	d := newMobileWebDrive(a)
	d.credentials = func() map[string]any {
		g.mu.Lock()
		defer g.mu.Unlock()
		return cloneGUIAccount(g.accounts[a.Key]).credentials
	}
	if valueString(a.credentials["token"]) == "" {
		return d
	}
	g.mu.Lock()
	identity, bridge, logged := g.identity, g.bridge, g.logged
	g.mu.Unlock()
	cl, err := g.cloudAccountClient(a, "", identity, bridge, logged)
	if err != nil || cl.cfg.Mobile.AppID == "" || cl.cfg.Mobile.AppSecret == "" {
		return d
	}
	backup := &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}, onRefresh: func(access, refresh string) error { return g.saveDAVCredential(a, access, refresh) }}
	return &cloudChannelDrive{channels: []cloudChannel{{"移动官网", "/", d}, {"移动绑定授权", "root", backup}}, nodes: map[string]cloudChannelNode{}}
}

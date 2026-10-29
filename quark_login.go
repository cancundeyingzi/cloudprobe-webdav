package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

type quarkLoginFlow struct {
	completed                               bool
	method, stage, err, image, authorizeURL string
	token, pageCode, wxURL, wxUUID, target  string
	credentials                             quarkCredentials
	expires                                 time.Time
	http                                    *http.Client
	api                                     *quarkClient
}

func (m *quarkManager) Start(method, target string) error {
	if method == "" {
		method = "quark"
	}
	if method != "quark" && method != "wechat" && method != "open" && method != "phone" {
		return errors.New("夸克登录方式无效")
	}
	m.Stop()
	m.mu.Lock()
	if m.loadError != "" {
		m.mu.Unlock()
		return errors.New(m.loadError)
	}
	generation := m.generation
	m.generation++
	generation = m.generation
	lifetime := 5 * time.Minute
	if method == "phone" {
		lifetime = 15 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), lifetime)
	ctx = context.WithValue(ctx, quarkLoginGenerationKey{}, generation)
	jar, _ := cookiejar.New(nil)
	flow := &quarkLoginFlow{method: method, target: target, stage: "正在准备夸克登录", expires: time.Now().Add(lifetime), credentials: quarkCredentials{DeviceID: m.deviceID, DeviceName: m.deviceName}, http: &http.Client{Timeout: 40 * time.Second, Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	flow.api = newQuarkClient(flow.credentials, nil)
	m.flow = flow
	m.cancel = cancel
	m.mu.Unlock()
	go func() {
		defer cancel()
		err := m.runLogin(ctx, generation, flow)
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.generation != generation {
			return
		}
		m.cancel = nil
		if err != nil {
			flow.stage = "夸克登录未完成"
			flow.err = m.app.redactDAV(err.Error())
			flow.image = ""
			flow.authorizeURL = ""
		} else {
			flow.stage = "夸克登录成功，授权已保存"
			flow.image = ""
			flow.authorizeURL = ""
		}
	}()
	return nil
}
func (m *quarkManager) runLogin(ctx context.Context, generation uint64, f *quarkLoginFlow) error {
	if f.method == "phone" {
		return m.runPhoneLogin(ctx, generation, f)
	}
	var scan string
	var err error
	switch f.method {
	case "open":
		scan, err = f.startOpen(ctx)
	case "wechat":
		scan, err = f.startWeChat(ctx)
	default:
		var data map[string]any
		data, err = f.uop(ctx, m.authBase+"/getTokenForQrcodeLogin", nil)
		if err == nil {
			members, _ := data["members"].(map[string]any)
			f.token = quarkStr(members, "token")
			if f.token == "" {
				err = errors.New("夸克扫码响应缺少 token")
			} else {
				v := url.Values{"token": {f.token}, "client_id": {"386"}, "sch": {"pckk@information_ch"}, "sve": {"3.24.0"}, "ssb": {"weblogin"}, "uc_param_str": {""}, "uc_biz_str": {"S:custom|OPT:SAREA@0|OPT:IMMERSIVE@1|OPT:BACK_BTN_STYLE@0"}}
				v.Set("uc_param_str", "")
				scan = "https://su.quark.cn/4_eMHBJ?" + v.Encode()
			}
		}
	}
	if err != nil {
		return err
	}
	png, err := qrcode.Encode(scan, qrcode.Medium, 256)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if m.generation != generation {
		m.mu.Unlock()
		return context.Canceled
	}
	f.image = "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
	f.stage = "等待扫码并在手机确认"
	if f.method == "open" {
		f.authorizeURL = scan
	}
	m.mu.Unlock()
	m.app.event("夸克登录二维码已在本机生成")
	if f.method == "open" {
		if err = f.pollOpen(ctx); err != nil {
			return err
		}
	} else {
		var ticket string
		if f.method == "wechat" {
			ticket, err = f.pollWeChat(ctx, func(s string) { m.update(generation, s, "") })
		} else {
			for {
				if err = davWait(ctx, 2*time.Second); err != nil {
					return err
				}
				var data map[string]any
				data, err = f.uop(ctx, m.authBase+"/getServiceTicketByQrcodeToken", url.Values{"token": {f.token}})
				if err != nil {
					if retryableTransfer(err) {
						m.update(generation, "登录请求暂时失败，继续等待扫码", "")
						continue
					}
					return err
				}
				status := quarkStr(data, "_login_status")
				members, _ := data["members"].(map[string]any)
				switch status {
				case "2000000":
					ticket = quarkStr(members, "service_ticket")
					if ticket == "" {
						return errors.New("夸克确认响应缺少 service_ticket")
					}
				case "50004002":
					return errors.New("夸克二维码已过期，请刷新")
				case "50004001":
					continue
				default:
					m.update(generation, "等待手机确认登录", "")
				}
				if ticket != "" {
					break
				}
			}
		}
		if err != nil {
			return err
		}
		if err = f.finishPC(ctx, ticket); err != nil {
			return err
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return m.accept(ctx, f.credentials, f.target)
}
func (f *quarkLoginFlow) loginHeaders(req *http.Request) {
	req.Header.Set("User-Agent", quarkUserAgent)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Referer", "https://pan.quark.cn/")
	req.Header.Set("Origin", "https://pan.quark.cn")
}
func (f *quarkLoginFlow) get(ctx context.Context, raw, referer string) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", raw, nil)
	if err != nil {
		return nil, nil, err
	}
	f.loginHeaders(req)
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, nil, &quarkTransportError{err, "夸克登录连接失败"}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20+1))
	if err != nil {
		return nil, nil, err
	}
	if len(body) > 2<<20 {
		return nil, nil, errors.New("登录响应过大")
	}
	if resp.StatusCode >= 400 {
		return resp, body, &upstreamHTTPError{code: resp.StatusCode, source: "夸克登录", after: retryAfterHeader(resp.Header)}
	}
	return resp, body, nil
}
func (f *quarkLoginFlow) uop(ctx context.Context, path string, query url.Values) (map[string]any, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	v := url.Values{"pr": {"ucpro"}, "fr": {"pc"}, "sys": {"darwin"}, "client_id": {"386"}, "v": {"1.2"}, "request_id": {strconv.FormatInt(time.Now().UnixNano(), 10)}, "uc_param_str": {"utprfr"}, "ut": {f.credentials.DeviceID}, "mi": {f.credentials.DeviceName}}
	for k, values := range query {
		v[k] = values
	}
	u.RawQuery = v.Encode()
	_, data, err := f.get(ctx, u.String(), "")
	if err != nil {
		return nil, err
	}
	reply, err := quarkJSON(data)
	if err != nil {
		return nil, err
	}
	status := quarkStr(reply, "status")
	m, ok := reply["data"].(map[string]any)
	if !ok || m == nil {
		if status == "50004001" || status == "50004002" {
			m = map[string]any{}
		} else {
			return nil, fmt.Errorf("夸克扫码状态 %s 缺少有效 data", status)
		}
	}
	m["_login_status"] = status
	if strings.HasSuffix(path, "getTokenForQrcodeLogin") && status != "2000000" {
		return nil, fmt.Errorf("夸克二维码请求失败，状态 %s", status)
	}
	return m, nil
}
func (f *quarkLoginFlow) finishPC(ctx context.Context, ticket string) error {
	u, err := url.Parse(f.api.pan + "/desktop/account/login")
	if err != nil {
		return err
	}
	u.RawQuery = url.Values{"st": {ticket}, "pr": {"ucpro"}, "fr": {"pc"}, "mi": {f.credentials.DeviceName}, "ut": {f.credentials.DeviceID}}.Encode()
	resp, _, err := f.get(ctx, u.String(), "")
	if err != nil {
		return err
	}
	c := map[string]string{}
	for _, host := range []string{"https://pan.quark.cn", "https://drive-pc.quark.cn", "https://drive.quark.cn", "https://quark.cn"} {
		base, _ := url.Parse(host)
		for _, cookie := range f.http.Jar.Cookies(base) {
			c[cookie.Name] = cookie.Value
		}
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Valid() == nil && cookie.Value != "" {
			c[cookie.Name] = cookie.Value
		}
	}
	if len(c) == 0 {
		return errors.New("夸克登录未返回有效 Cookie，会话未保存")
	}
	f.credentials.Cookies = c
	return nil
}
func (f *quarkLoginFlow) startOpen(ctx context.Context) (string, error) {
	out, err := f.api.request(ctx, quarkOpen, "POST", "/agent/v1/get_authorize_page_url", nil, map[string]any{"client_device_id": f.credentials.DeviceID, "device_name": f.credentials.DeviceName, "agent_id": "cloudprobe", "client_id": quarkOpenClient, "work_dir": "CloudProbe", "is_cloud_agent": "false", "is_unsure_agent": "false"}, true)
	if err != nil {
		return "", err
	}
	m, err := quarkObject(out["data"])
	if err != nil {
		return "", err
	}
	page := quarkStr(m, "authorize_page_url")
	u, e := url.Parse(page)
	if e != nil || u.Scheme != "https" || u.Hostname() != "pan.quark.cn" || u.User != nil {
		return "", errors.New("夸克授权页不是预期官网地址")
	}
	f.pageCode = quarkStr(m, "page_code")
	f.credentials.OpenDeviceID = quarkStr(m, "device_id")
	if f.pageCode == "" {
		return "", errors.New("夸克授权响应缺少 page_code")
	}
	return page, nil
}
func (f *quarkLoginFlow) pollOpen(ctx context.Context) error {
	for {
		if err := davWait(ctx, 2*time.Second); err != nil {
			return err
		}
		out, err := f.api.request(ctx, quarkOpen, "GET", "/agent/v1/oauth/get_aac_by_pagecode", url.Values{"page_code": {f.pageCode}}, nil, true)
		if err != nil {
			return err
		}
		m, err := quarkObject(out["data"])
		if err != nil {
			return err
		}
		code := quarkStr(m, "agent_auth_code")
		if code == "" {
			continue
		}
		out, err = f.api.request(ctx, quarkOpen, "GET", "/agent/v1/oauth/agent_auth_code", url.Values{"agent_auth_code": {code}}, nil, true)
		if err != nil {
			return err
		}
		m, err = quarkObject(out["data"])
		if err != nil {
			return err
		}
		f.credentials.AccessToken = quarkStr(m, "access_token")
		f.credentials.RefreshToken = quarkStr(m, "refresh_token")
		if id := quarkStr(m, "device_id"); id != "" {
			f.credentials.OpenDeviceID = id
		}
		if f.credentials.AccessToken == "" || f.credentials.RefreshToken == "" {
			return errors.New("夸克授权响应缺少令牌")
		}
		return nil
	}
}

var quarkWXUUID = regexp.MustCompile(`/connect/qrcode/([A-Za-z0-9_-]+)`)
var quarkWXStatus = regexp.MustCompile(`wx_errcode\s*=\s*(\d+)`)
var quarkWXCode = regexp.MustCompile(`wx_code\s*=\s*'([^']+)'`)

func (f *quarkLoginFlow) startWeChat(ctx context.Context) (string, error) {
	page := "https://pan.quark.cn/wechat_qrcode?desktop=1"
	if _, _, err := f.get(ctx, page, ""); err != nil {
		return "", err
	}
	f.wxURL = "https://uop.quark.cn/cas/thirdparty/authorize?" + url.Values{"client_id": {"386"}, "display": {"pc"}, "change_uid": {"0"}, "t": {strconv.FormatInt(time.Now().UnixMilli(), 10)}, "third_party_name": {"wechat"}, "la": {"zh"}, "redirect_uri": {page}}.Encode()
	qr := "https://open.weixin.qq.com/connect/qrconnect?" + url.Values{"login_type": {"jssdk"}, "self_redirect": {"true"}, "appid": {"wxcde89ee65c11b66a"}, "scope": {"snsapi_login"}, "fast_login": {"0"}, "redirect_uri": {f.wxURL}}.Encode()
	_, data, err := f.get(ctx, qr, page)
	if err != nil {
		return "", err
	}
	match := quarkWXUUID.FindSubmatch(data)
	if len(match) < 2 {
		return "", errors.New("微信登录响应缺少二维码 UUID")
	}
	f.wxUUID = string(match[1])
	if _, _, err = f.get(ctx, "https://open.weixin.qq.com/connect/qrcode/"+f.wxUUID, qr); err != nil {
		return "", err
	}
	return "https://open.weixin.qq.com/connect/confirm?uuid=" + url.QueryEscape(f.wxUUID), nil
}
func (f *quarkLoginFlow) pollWeChat(ctx context.Context, stage func(string)) (string, error) {
	last := ""
	for {
		raw := "https://lp.open.weixin.qq.com/connect/l/qrconnect?" + url.Values{"uuid": {f.wxUUID}, "last": {last}, "_": {strconv.FormatInt(time.Now().UnixMilli(), 10)}}.Encode()
		_, body, err := f.get(ctx, raw, "https://open.weixin.qq.com/")
		if err != nil {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			if err = davWait(ctx, 2*time.Second); err != nil {
				return "", err
			}
			continue
		}
		m := quarkWXStatus.FindSubmatch(body)
		if len(m) < 2 {
			return "", errors.New("微信轮询响应格式未知")
		}
		switch string(m[1]) {
		case "405":
			code := quarkWXCode.FindSubmatch(body)
			if len(code) < 2 {
				return "", errors.New("微信已确认但未返回授权码")
			}
			return f.wechatTicket(ctx, string(code[1]))
		case "404":
			stage("已扫码，请在微信中确认")
			last = "404"
		case "403":
			stage("微信登录已取消，可重新扫码")
			last = "403"
		case "402", "500":
			return "", errors.New("微信二维码已过期")
		default:
			stage("等待微信扫码")
		}
		if err = davWait(ctx, 500*time.Millisecond); err != nil {
			return "", err
		}
	}
}
func (f *quarkLoginFlow) wechatTicket(ctx context.Context, code string) (string, error) {
	next := f.wxURL + "&code=" + url.QueryEscape(code) + "&state="
	for hop := 0; hop < 6; hop++ {
		resp, _, err := f.get(ctx, next, "https://open.weixin.qq.com/")
		if err != nil {
			return "", err
		}
		base, _ := url.Parse(next)
		target, err := base.Parse(resp.Header.Get("Location"))
		if err != nil || resp.Header.Get("Location") == "" {
			return "", errors.New("微信授权未返回登录票据，可能尚未关联夸克账号")
		}
		if ticket := target.Query().Get("service_ticket"); ticket != "" {
			return ticket, nil
		}
		host := target.Hostname()
		if target.Scheme != "https" || !(host == "quark.cn" || strings.HasSuffix(host, ".quark.cn") || host == "open.weixin.qq.com") {
			return "", errors.New("微信授权重定向域名不受信任")
		}
		next = target.String()
	}
	return "", errors.New("微信授权重定向次数过多")
}

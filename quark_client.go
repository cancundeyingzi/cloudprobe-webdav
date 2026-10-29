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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Protocol references and revision pins are documented in third_party/quark.md.
// Cookies, OAuth credentials and device identifiers never appear in publicStatus.
type quarkCredentials struct {
	UID          string            `json:"uid"`
	Name         string            `json:"name"`
	DeviceID     string            `json:"device_id"`
	DeviceName   string            `json:"device_name"`
	Cookies      map[string]string `json:"cookies,omitempty"`
	AccessToken  string            `json:"access_token,omitempty"`
	RefreshToken string            `json:"refresh_token,omitempty"`
	OpenDeviceID string            `json:"open_device_id,omitempty"`
	OpenUID      string            `json:"open_uid,omitempty"`
	OpenVerified bool              `json:"open_verified,omitempty"`
}

const quarkUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/3.24.0 Chrome/112.0.5615.165 Electron/24.0.0 Safari/537.36"
const quarkOpenClient = "third_party_agent"

// Published default protocol credential, not a user account credential.
const quarkOpenSigningKey = "cf134812e2de4032bd1cb7c3727e84b3"

type quarkRoute string

const (
	quarkDesktop quarkRoute = "desktop"
	quarkWeb     quarkRoute = "web"
	quarkOpen    quarkRoute = "open"
)

func (r quarkRoute) label() string {
	switch r {
	case quarkDesktop:
		return "夸克桌面接口"
	case quarkWeb:
		return "夸克网页接口"
	default:
		return "夸克开放平台"
	}
}

type quarkClient struct {
	mu          sync.Mutex
	refresh     contextMutex
	credentials quarkCredentials
	metadata    *http.Client
	bodies      *http.Client
	endpoints   map[quarkRoute]string
	pan         string
	save        func(quarkCredentials) error
	redact      func(string) string
	testHosts   bool // Set only by local protocol fixtures, never a user preference.
}

func newQuarkClient(c quarkCredentials, save func(quarkCredentials) error) *quarkClient {
	q := &quarkClient{credentials: c, save: save, endpoints: map[quarkRoute]string{
		quarkDesktop: "https://drive-pc.quark.cn/1/clouddrive", quarkWeb: "https://drive.quark.cn/1/clouddrive", quarkOpen: "https://open-api-drive.quark.cn"}, pan: "https://pan.quark.cn"}
	q.metadata = providerMetadataClient(30 * time.Second)
	q.metadata.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 5 || len(via) > 0 && req.URL.Scheme+"://"+req.URL.Host != via[0].URL.Scheme+"://"+via[0].URL.Host {
			return errors.New("夸克 API 跨域重定向被拒绝")
		}
		return nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ForceAttemptHTTP2 = true
	tr.MaxIdleConns = 128
	tr.MaxIdleConnsPerHost = 32
	tr.ResponseHeaderTimeout = 60 * time.Second
	tr.DisableCompression = true
	q.bodies = &http.Client{Transport: tr, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	return q
}
func (q *quarkClient) snapshot() quarkCredentials {
	q.mu.Lock()
	defer q.mu.Unlock()
	c := q.credentials
	c.Cookies = map[string]string{}
	for k, v := range q.credentials.Cookies {
		c.Cookies[k] = v
	}
	return c
}
func quarkCookieHeader(c map[string]string) string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if k != "" && c[k] != "" {
			out = append(out, k+"="+c[k])
		}
	}
	return strings.Join(out, "; ")
}
func (q *quarkClient) scrub(s string) string {
	c := q.snapshot()
	for _, v := range append([]string{c.AccessToken, c.RefreshToken}, mapValues(c.Cookies)...) {
		if v != "" {
			s = strings.ReplaceAll(s, v, "[已脱敏]")
		}
	}
	if q.redact != nil {
		s = q.redact(s)
	}
	return s
}
func mapValues(m map[string]string) []string {
	v := make([]string, 0, len(m))
	for _, s := range m {
		v = append(v, s)
	}
	return v
}
func (q *quarkClient) ingest(cookies []*http.Cookie) error {
	if len(cookies) == 0 {
		return nil
	}
	q.mu.Lock()
	changed := false
	if q.credentials.Cookies == nil {
		q.credentials.Cookies = map[string]string{}
	}
	for _, c := range cookies {
		// Only account API responses call this method. CDN/OSS cookies do not
		// become account credentials, and cookie names/values are validated.
		if c.Name == "" || c.Valid() != nil {
			continue
		}
		if c.MaxAge < 0 || !c.Expires.IsZero() && c.Expires.Before(time.Now()) {
			if _, ok := q.credentials.Cookies[c.Name]; ok {
				delete(q.credentials.Cookies, c.Name)
				changed = true
			}
		} else if q.credentials.Cookies[c.Name] != c.Value {
			q.credentials.Cookies[c.Name] = c.Value
			changed = true
		}
	}
	q.mu.Unlock()
	if changed && q.save != nil {
		return q.save(q.snapshot())
	}
	return nil
}
func (q *quarkClient) routes() []quarkRoute {
	c := q.snapshot()
	var out []quarkRoute
	if len(c.Cookies) > 0 {
		out = append(out, quarkDesktop, quarkWeb)
	}
	if c.AccessToken != "" && c.OpenVerified {
		out = append(out, quarkOpen)
	}
	return out
}

type quarkTransportError struct {
	cause   error
	message string
}

func (e *quarkTransportError) Error() string { return e.message }
func (e *quarkTransportError) Unwrap() error { return e.cause }

type quarkAPIError struct {
	HTTP          int
	Code, Message string
	After         time.Duration
	Rejected      bool
	Auth          bool
}

func (e *quarkAPIError) Error() string {
	return fmt.Sprintf("夸克接口拒绝（状态 %d，业务码 %s）：%s", e.HTTP, e.Code, e.Message)
}
func (e *quarkAPIError) Unwrap() error {
	if e.HTTP == 429 || e.HTTP == 408 || e.HTTP >= 500 {
		return &upstreamHTTPError{code: e.HTTP, after: e.After, source: "夸克"}
	}
	return nil
}
func quarkRejected(err error) bool  { var e *quarkAPIError; return errors.As(err, &e) && e.Rejected }
func quarkAuthError(err error) bool { var e *quarkAPIError; return errors.As(err, &e) && e.Auth }
func quarkAuthCode(code string) bool {
	return code == "31001" || code == "31004" || code == "NO_AUTH" || strings.HasPrefix(code, "AUTH_ERROR") || code == "401"
}
func quarkJSON(data []byte) (map[string]any, error) {
	var out map[string]any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&out); err != nil || out == nil {
		return nil, errors.New("夸克返回了无效 JSON，不能解释为成功或空目录")
	}
	if err := ensureEOF(d); err != nil {
		return nil, err
	}
	return out, nil
}
func quarkObject(v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return nil, errors.New("夸克响应缺少有效 data 对象")
	}
	return m, nil
}
func quarkUUID() string {
	s := randomKey()[:32]
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func quarkSign(method, path, stamp string) string {
	h := sha256.Sum256([]byte(method + "&" + path + "&" + stamp + "&" + quarkOpenSigningKey))
	return hex.EncodeToString(h[:])
}
func (q *quarkClient) call(ctx context.Context, route quarkRoute, method, path string, query url.Values, body any) (map[string]any, error) {
	rejectedToken := q.snapshot().AccessToken
	out, err := q.request(ctx, route, method, path, query, body, false)
	if route == quarkOpen && quarkAuthError(err) && q.snapshot().RefreshToken != "" {
		if re := q.rotate(ctx, rejectedToken); re == nil {
			return q.request(ctx, route, method, path, query, body, false)
		}
	}
	return out, err
}
func (q *quarkClient) request(ctx context.Context, route quarkRoute, method, path string, query url.Values, body any, noAccess bool) (map[string]any, error) {
	c := q.snapshot()
	endpoint := q.endpoints[route] + path
	if strings.HasPrefix(path, "/desktop/") || strings.HasPrefix(path, "/account/") {
		endpoint = q.pan + path
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	v := u.Query()
	if route == quarkOpen {
		v.Set("req_id", quarkUUID())
		if !noAccess && c.AccessToken != "" {
			v.Set("access_token", c.AccessToken)
		}
		if c.OpenDeviceID != "" {
			v.Set("device_id", c.OpenDeviceID)
		}
	} else {
		v.Set("pr", "ucpro")
		v.Set("fr", "pc")
		if route == quarkDesktop {
			v.Set("sys", "darwin")
			v.Set("mi", c.DeviceName)
		}
	}
	for k, values := range query {
		for _, value := range values {
			v.Add(k, value)
		}
	}
	u.RawQuery = v.Encode()
	var b []byte
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("User-Agent", quarkUserAgent)
	req.Header.Set("Referer", "https://pan.quark.cn/")
	req.Header.Set("Origin", "https://pan.quark.cn")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if route == quarkOpen {
		stamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		req.Header.Set("x-pan-client-id", quarkOpenClient)
		req.Header.Set("x-pan-tm", stamp)
		req.Header.Set("x-pan-token", quarkSign(method, u.Path, stamp))
	} else {
		req.Header.Set("Cookie", quarkCookieHeader(c.Cookies))
	}
	started := time.Now()
	resp, err := q.metadata.Do(req)
	if err != nil {
		safe := err
		var requestError *url.Error
		if errors.As(err, &requestError) {
			safe = requestError.Err
		}
		return nil, &quarkTransportError{err, q.scrub(safe.Error())}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 8<<20 {
		return nil, errors.New("夸克响应超过 8 MiB")
	}
	if route != quarkOpen {
		if e := q.ingest(resp.Cookies()); e != nil {
			return nil, e
		}
	}
	doc, parseErr := quarkJSON(data)
	code, message := "", "响应结构未知"
	auth := resp.StatusCode == 401
	if doc != nil {
		code = valueString(doc["code"])
		message = valueString(doc["message"])
		if message == "" {
			message = valueString(doc["error_info"])
		}
		auth = auth || quarkAuthCode(code)
	}
	rejection := resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 || resp.StatusCode == 405 || resp.StatusCode == 501
	failed := resp.StatusCode < 200 || resp.StatusCode >= 300
	if doc != nil {
		if x, ok := doc["success"].(bool); ok && !x {
			failed = true
		}
		if route == quarkOpen {
			status, ok := doc["status"]
			if !ok || valueString(status) != "0" {
				failed = true
			}
			if n, ok := doc["errno"]; ok && valueString(n) != "0" {
				failed = true
				code = valueString(n)
			}
		} else {
			panSuccess, _ := doc["success"].(bool)
			panOK := (strings.HasPrefix(path, "/desktop/") || strings.HasPrefix(path, "/account/")) && panSuccess && code == "OK"
			if code != "" && code != "0" && code != "200" && !panOK {
				failed = true
			}
			if s, ok := doc["status"]; ok && valueString(s) != "200" && valueString(s) != "0" {
				failed = true
			}
			if _, ok := doc["code"]; !ok {
				if success, ok := doc["success"].(bool); !ok || !success {
					failed = true
				}
			}
		}
	}
	logDAV(ctx, runLogEntry{Operation: "夸克接口请求", Source: route.label(), Endpoint: u.Host + u.Path, Result: strconv.Itoa(resp.StatusCode), DurationMs: time.Since(started).Milliseconds(), HTTPStatus: resp.StatusCode})
	auth = auth || quarkAuthCode(code)
	if failed {
		return nil, &quarkAPIError{HTTP: resp.StatusCode, Code: code, Message: q.scrub(message), After: retryAfterHeader(resp.Header), Auth: auth, Rejected: rejection || auth}
	}
	if parseErr != nil {
		return nil, parseErr
	}
	return doc, nil
}
func (q *quarkClient) rotate(ctx context.Context, rejectedToken string) error {
	if err := q.refresh.LockContext(ctx); err != nil {
		return err
	}
	defer q.refresh.Unlock()
	c := q.snapshot()
	if c.AccessToken != rejectedToken {
		return nil
	}
	out, err := q.request(ctx, quarkOpen, "POST", "/agent/v1/oauth/access_token/rotate", nil, map[string]any{"refresh_token": c.RefreshToken, "device_id": c.OpenDeviceID}, true)
	if err != nil {
		return err
	}
	m, err := quarkObject(out["data"])
	if err != nil {
		return err
	}
	access := valueString(m["access_token"])
	refresh := valueString(m["refresh_token"])
	if access == "" || refresh == "" {
		return errors.New("夸克开放平台刷新响应缺少令牌")
	}
	q.mu.Lock()
	q.credentials.AccessToken = access
	q.credentials.RefreshToken = refresh
	q.mu.Unlock()
	if q.save != nil {
		return q.save(q.snapshot())
	}
	return nil
}
func (q *quarkClient) validBodyURL(raw string) error {
	if err := validDAVURL(raw); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	if q.testHosts && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost") {
		return nil
	}
	if u.Scheme != "https" {
		return errors.New("夸克上传/下载必须使用 HTTPS，拒绝通过明文 HTTP 发送账号凭据")
	}
	host := strings.ToLower(u.Hostname())
	for _, suffix := range []string{"quark.cn", "quark.com", "aliyuncs.com", "uc.cn", "ucweb.com"} {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return nil
		}
	}
	return errors.New("夸克返回了非预期下载/上传域名，拒绝向其发送凭据")
}

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"golang.org/x/net/websocket"
)

// Inspect only the browser profile explicitly created for this tool's login.
type wopanWebLogin struct {
	mu         sync.Mutex
	app        *guiApp
	cancel     context.CancelFunc
	stage      string
	running    bool
	err        string
	connection *websocket.Conn
	done       chan struct{}
	validate   func(context.Context, guiAccount, string) error
}

func (l *wopanWebLogin) Status() map[string]any {
	if l == nil {
		return map[string]any{"running": false}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{"running": l.running, "stage": l.stage, "error": l.err}
}
func (l *wopanWebLogin) Stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	c := l.cancel
	connection, done := l.connection, l.done
	l.cancel = nil
	l.mu.Unlock()
	if c != nil {
		c()
	}
	if connection != nil {
		connection.SetDeadline(time.Now().Add(time.Second))
		_ = websocket.JSON.Send(connection, map[string]any{"id": 99998, "method": "Browser.close"})
		connection.Close()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}
func findLoginBrowser() (string, error) {
	for _, name := range []string{"msedge", "microsoft-edge", "google-chrome", "chrome", "chromium"} {
		if p, e := exec.LookPath(name); e == nil {
			return p, nil
		}
	}
	if runtime.GOOS == "windows" {
		for _, root := range []string{os.Getenv("ProgramFiles(x86)"), os.Getenv("ProgramFiles"), os.Getenv("LOCALAPPDATA")} {
			for _, sub := range []string{"Microsoft/Edge/Application/msedge.exe", "Google/Chrome/Application/chrome.exe"} {
				p := filepath.Join(root, filepath.FromSlash(sub))
				if fi, e := os.Stat(p); e == nil && !fi.IsDir() {
					return p, nil
				}
			}
		}
		for _, key := range []string{`HKCU\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\msedge.exe`, `HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\msedge.exe`, `HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe`} {
			cmd := exec.Command("reg.exe", "query", key, "/ve")
			hideChildWindow(cmd)
			b, _ := cmd.Output()
			for _, line := range strings.Split(string(b), "\n") {
				if pos := strings.Index(line, "REG_SZ"); pos >= 0 {
					p := strings.Trim(strings.TrimSpace(line[pos+6:]), `"`)
					if _, e := os.Stat(p); e == nil {
						return p, nil
					}
				}
			}
		}
	}
	return "", errors.New("未找到 Edge 或 Chrome，可使用本机 Cookie 导入方式")
}
func (l *wopanWebLogin) Start(key string) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return errors.New("官方登录窗口已经打开")
	}
	l.mu.Unlock()
	g := l.app
	g.mu.Lock()
	a, ok := g.accounts[key]
	a = cloneGUIAccount(a)
	g.mu.Unlock()
	if !ok || a.Provider != "wopan" {
		return errors.New("请选择已绑定的联通云盘账号")
	}
	if davIdentity(a) == "" {
		return errors.New("账号缺少稳定身份标识，无法安全关联官网授权")
	}
	if d := g.dav.Status(); d["running"] == true {
		return errors.New("请先停止 WebDAV，再添加或更新备用登录")
	}
	browser, e := findLoginBrowser()
	if e != nil {
		return e
	}
	profile := filepath.Join(g.data, "official-wopan-browser", davIdentity(a))
	if e = os.MkdirAll(profile, 0700); e != nil {
		return e
	}
	_ = os.Remove(filepath.Join(profile, "DevToolsActivePort"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	cmd := exec.Command(browser, "--user-data-dir="+profile, "--remote-debugging-port=0", "--remote-debugging-address=127.0.0.1", "--remote-allow-origins=http://127.0.0.1", "--no-first-run", "--no-default-browser-check", "--disable-sync", "--new-window", "https://pan.wo.cn/")
	// An interactive login requested from the UI must remain visible.
	if e = cmd.Start(); e != nil {
		cancel()
		return e
	}
	l.mu.Lock()
	l.running = true
	l.cancel = cancel
	l.err = ""
	l.stage = "请在打开的联通官网窗口登录同一账号"
	done := make(chan struct{})
	l.done = done
	l.mu.Unlock()
	g.event("已打开本工具专用的联通官网登录窗口")
	go func() { _ = cmd.Wait() }()
	go l.capture(ctx, cancel, a, profile, done)
	return nil
}
func (l *wopanWebLogin) capture(ctx context.Context, cancel context.CancelFunc, a guiAccount, profile string, done chan struct{}) {
	defer cancel()
	defer close(done)
	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			_ = websocket.JSON.Send(conn, map[string]any{"id": 99999, "method": "Browser.close"})
			conn.Close()
		}
		l.mu.Lock()
		l.running = false
		l.cancel = nil
		l.connection = nil
		l.mu.Unlock()
	}()
	seq := int64(0)
	lastToken := ""
	lastCheck := time.Time{}
	for {
		if e := davWait(ctx, time.Second); e != nil {
			l.mu.Lock()
			l.stage = "官方登录已结束"
			l.mu.Unlock()
			return
		}
		if conn == nil {
			raw, e := os.ReadFile(filepath.Join(profile, "DevToolsActivePort"))
			if e != nil {
				continue
			}
			scanner := bufio.NewScanner(strings.NewReader(string(raw)))
			if !scanner.Scan() {
				continue
			}
			port, e := strconv.Atoi(strings.TrimSpace(scanner.Text()))
			if e != nil || port < 1 || port > 65535 || !scanner.Scan() {
				continue
			}
			wsPath := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(wsPath, "/devtools/browser/") {
				continue
			}
			config, e := websocket.NewConfig(fmt.Sprintf("ws://127.0.0.1:%d%s", port, wsPath), "http://127.0.0.1")
			if e != nil {
				continue
			}
			config.Dialer = &net.Dialer{Timeout: 3 * time.Second}
			conn, e = websocket.DialConfig(config)
			if e != nil {
				conn = nil
				continue
			}
			l.mu.Lock()
			l.connection = conn
			l.mu.Unlock()
		}
		seq++
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if e := websocket.JSON.Send(conn, map[string]any{"id": seq, "method": "Storage.getCookies"}); e != nil {
			conn.Close()
			conn = nil
			continue
		}
		var response struct {
			ID     int64 `json:"id"`
			Result struct {
				Cookies []struct {
					Name   string `json:"name"`
					Value  string `json:"value"`
					Domain string `json:"domain"`
				} `json:"cookies"`
			} `json:"result"`
		}
		for {
			if e := websocket.JSON.Receive(conn, &response); e != nil {
				conn.Close()
				conn = nil
				break
			}
			if response.ID == seq {
				break
			}
		}
		if conn == nil {
			continue
		}
		if response.ID != seq {
			continue
		}
		for _, cookie := range response.Result.Cookies {
			if cookie.Name != "WoCloud-Web-Token" || !(cookie.Domain == "wo.cn" || strings.HasSuffix(cookie.Domain, ".wo.cn")) {
				continue
			}
			token, e := url.PathUnescape(cookie.Value)
			if e != nil {
				continue
			}
			token = strings.Trim(strings.TrimSpace(token), `"`)
			if token == "" || (token == lastToken && time.Since(lastCheck) < 5*time.Second) {
				continue
			}
			lastToken = token
			lastCheck = time.Now()
			validate := l.validate
			if validate == nil {
				validate = l.app.acceptWebToken
			}
			if e = validate(ctx, a, token); e != nil {
				l.mu.Lock()
				l.err = e.Error()
				l.stage = "登录信息尚未通过验证，请检查是否为同一账号"
				l.mu.Unlock()
				continue
			}
			l.mu.Lock()
			l.err = ""
			l.stage = "官网备用登录已保存，可以启动 WebDAV"
			l.mu.Unlock()
			l.app.event("联通官网备用登录验证通过，已由 Windows 保护保存")
			return
		}
	}
}
func (g *guiApp) acceptWebToken(ctx context.Context, a guiAccount, token string) error {
	if len(token) < 16 || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
		return errors.New("官网令牌格式无效")
	}
	w := wopan.DefaultWithAccessToken(token)
	w.SetHttpClient(providerMetadataClient(25 * time.Second))
	w.SetUserAgent(wopan.DefaultUA)
	w.OnRefreshToken(func(string, string) {})
	user, e := w.AppQueryUser(woContext(ctx))
	if e != nil {
		return errors.New("官网登录态验证失败，请重新在官网登录")
	}
	if !sameWopanAccount(a.credentials, user.UserId) {
		return errors.New("官网账号与所选爆米花绑定账号不一致，请使用同一账号登录")
	}
	return g.saveDAVWebCredential(a, token)
}
func sameWopanAccount(cred map[string]any, actual string) bool {
	normal := func(s string) string {
		s = strings.TrimSpace(s)
		s = strings.TrimPrefix(s, "+86")
		if len(s) == 13 && strings.HasPrefix(s, "86") {
			s = s[2:]
		}
		return s
	}
	actual = normal(actual)
	for _, key := range []string{"user_id", "mobile", "username"} {
		v := normal(valueString(cred[key]))
		if v != "" && v == actual {
			return true
		}
	}
	return false
}
func webTokenFromInput(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "=") {
		v, e := url.PathUnescape(strings.Trim(raw, `"`))
		return v, e
	}
	r := &http.Request{Header: http.Header{"Cookie": []string{raw}}}
	for _, c := range r.Cookies() {
		if c.Name == "WoCloud-Web-Token" {
			return url.PathUnescape(strings.Trim(c.Value, `"`))
		}
	}
	return "", errors.New("Cookie 中没有 WoCloud-Web-Token；请仅在本机界面填写")
}
func (g *guiApp) wopanLoginAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var in struct {
		Account string `json:"account"`
		Cookie  string `json:"cookie"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	if e := d.Decode(&in); e != nil {
		g.fail(w, errors.New("登录请求格式错误"))
		return
	}
	if e := ensureEOF(d); e != nil {
		g.fail(w, e)
		return
	}
	if g.webLogin == nil {
		g.fail(w, errors.New("官网登录管理器尚未初始化"))
		return
	}
	if strings.HasSuffix(r.URL.Path, "/stop") {
		g.webLogin.Stop()
		respondJSON(w, 200, g.webLogin.Status())
		return
	}
	if strings.HasSuffix(r.URL.Path, "/start") {
		if e := g.webLogin.Start(in.Account); e != nil {
			g.fail(w, e)
			return
		}
		respondJSON(w, 200, g.webLogin.Status())
		return
	}
	g.mu.Lock()
	a, ok := g.accounts[in.Account]
	a = cloneGUIAccount(a)
	g.mu.Unlock()
	if !ok || a.Provider != "wopan" {
		g.fail(w, errors.New("请选择联通云盘账号"))
		return
	}
	if g.dav.Status()["running"] == true {
		g.fail(w, errors.New("请先停止 WebDAV 后更新备用凭据"))
		return
	}
	if strings.HasSuffix(r.URL.Path, "/clear") {
		if e := g.saveDAVWebCredential(a, ""); e != nil {
			g.fail(w, e)
			return
		}
		respondJSON(w, 200, map[string]any{"ok": true})
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/import") {
		http.NotFound(w, r)
		return
	}
	token, e := webTokenFromInput(in.Cookie)
	if e == nil {
		e = g.acceptWebToken(r.Context(), a, token)
	}
	if e != nil {
		g.fail(w, e)
		return
	}
	g.event("联通官网备用登录已从本机输入验证保存")
	respondJSON(w, 200, map[string]any{"ok": true})
}

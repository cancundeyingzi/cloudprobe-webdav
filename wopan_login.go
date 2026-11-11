package main

import (
	"context"
	"encoding/json"
	"errors"
	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type wopanWebLogin = cloudWebLogin

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
func (g *guiApp) acceptWebToken(ctx context.Context, a guiAccount, token string) error {
	var err error
	token, err = normalizeWopanWebToken(token)
	if err != nil {
		return err
	}
	if len(token) < 16 || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
		return errors.New("官网令牌格式无效")
	}
	w := wopan.DefaultWithAccessToken(token)
	w.SetHttpClient(providerMetadataClient(25 * time.Second))
	w.SetUserAgent(wopan.DefaultUA)
	w.OnRefreshToken(func(string, string) {})
	user, err := w.AppQueryUser(woContext(ctx))
	if err != nil {
		return errors.New("联通官网登录态验证失败，请重新在官网登录")
	}
	if user.UserId == "" {
		return errors.New("联通官网未返回账号身份")
	}
	return g.acceptCloudAccount(ctx, guiAccount{Provider: "wopan", Name: "联通云盘 · " + maskAccount(user.UserId), Root: "0", credentials: map[string]any{"user_id": user.UserId, "wopan_web_token": token, "wopan_web_managed": "1"}})
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

// The website stores JSON.stringify(token) inside a URL-encoded cookie.
// Decode the URL layer first, then the JSON string; '+' is part of a token.
func decodeWopanWebCookie(raw string) (string, error) {
	decoded, err := url.PathUnescape(strings.TrimSpace(raw))
	if err != nil {
		return "", errors.New("联通 Cookie 编码无效")
	}
	return normalizeWopanWebToken(decoded)
}
func normalizeWopanWebToken(raw string) (string, error) {
	token := strings.TrimSpace(raw)
	if strings.HasPrefix(token, `"`) {
		if json.Unmarshal([]byte(token), &token) != nil {
			return "", errors.New("联通令牌 JSON 包装无效")
		}
	}
	if strings.ContainsAny(token, "\r\n\x00\"") {
		return "", errors.New("联通令牌包含无效字符")
	}
	return token, nil
}
func wopanWebToken(credentials map[string]any) string {
	raw := valueString(credentials["wopan_web_token"])
	token, err := normalizeWopanWebToken(raw)
	if err != nil {
		return raw
	}
	return token
}
func webTokenFromInput(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.Contains(raw, "=") {
		return decodeWopanWebCookie(raw)
	}
	r := &http.Request{Header: http.Header{"Cookie": []string{raw}}}
	for _, c := range r.Cookies() {
		if c.Name == "WoCloud-Web-Token" {
			return decodeWopanWebCookie(c.Value)
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
	if strings.HasSuffix(r.URL.Path, "/clear") && (!ok || a.Provider != "wopan") {
		g.fail(w, errors.New("请选择联通云盘账号"))
		return
	}
	if strings.HasSuffix(r.URL.Path, "/clear") {
		if e := g.clearCloudWebChannel(a); e != nil {
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
	g.event("联通官网登录已验证保存，同一账号的渠道自动合并")
	respondJSON(w, 200, map[string]any{"ok": true})
}

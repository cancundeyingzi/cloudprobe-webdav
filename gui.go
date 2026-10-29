package main

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

//go:embed ui/index.html
var guiHTML []byte

//go:embed ui/run-log.js
var guiRunLogJS []byte

//go:embed ui/transfers.js
var guiTransfersJS []byte

//go:embed ui/quark.js
var guiQuarkJS []byte

//go:embed ui/app.js
var guiAppJS []byte

//go:embed ui/styles.css
var guiCSS []byte

type guiAccount struct {
	Channels    []string `json:"channels,omitempty"`
	WebBound    bool     `json:"official_web_available"`
	Key         string   `json:"key"`
	Provider    string   `json:"provider"`
	Name        string   `json:"name"`
	Root        string   `json:"root"`
	credentials map[string]any
}
type guiDownload struct{ Path, Name string }

func cloneGUIAccount(a guiAccount) guiAccount {
	credentials := make(map[string]any, len(a.credentials))
	for k, v := range a.credentials {
		credentials[k] = v
	}
	a.credentials = credentials
	return a
}

type guiApp struct {
	quark                 *quarkManager
	extraSecrets          []string
	runLog                *runtimeLog
	webLogin              *wopanWebLogin
	dav                   *davManager
	mu                    sync.Mutex
	op                    sync.Mutex
	bridge                *OriginalBridge
	stage                 string
	engineError           string
	logged                bool
	user                  string
	qrToken               string
	qrExpires             time.Time
	pollAfter             int64
	accounts              map[string]guiAccount
	pendingDAVCredentials map[string]davCredential
	credentialSaveError   string
	credentialRetry       *time.Timer
	browseEntries         map[string]map[string]davEntry
	accountOrder          []string
	identity              MobileConfig
	data                  string
	lastResult            string
	downloads             map[string]guiDownload
	events                []string
	nonce                 string
	host                  string
	shutdown              context.CancelFunc
}

func randomKey() string { b := make([]byte, 20); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func (g *guiApp) event(s string) {
	g.mu.Lock()
	g.events = append(g.events, time.Now().Format("15:04:05")+" "+s)
	if len(g.events) > 40 {
		g.events = g.events[len(g.events)-40:]
	}
	g.mu.Unlock()
	g.runLog.add(runLogEntry{Operation: "运行阶段", Source: "本机", Result: "信息", Detail: s})
}
func (g *guiApp) setStage(s string) { g.mu.Lock(); g.stage = s; g.mu.Unlock() }
func (g *guiApp) getBridge() (*OriginalBridge, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bridge == nil {
		if g.engineError != "" {
			return nil, errors.New(g.engineError)
		}
		return nil, errors.New("原版引擎尚在初始化")
	}
	if !g.bridge.Alive() {
		return nil, g.bridge.exitError()
	}
	return g.bridge, nil
}
func (g *guiApp) call(ctx context.Context, method, path string, params map[string]any) (map[string]any, error) {
	b, e := g.getBridge()
	if e != nil {
		return nil, e
	}
	p, _ := compactJSON(params)
	raw, e := b.Request(ctx, method, path, p, nil)
	if e != nil {
		return nil, e
	}
	doc, e := decodeDocument(raw)
	if e != nil {
		return nil, e
	}
	return unwrap(doc)
}

var phonePattern = regexp.MustCompile(`\b(1\d{2})\d{4}(\d{4})\b`)

func maskAccount(s string) string { return phonePattern.ReplaceAllString(s, "${1}****${2}") }
func (g *guiApp) loadAccounts(ctx context.Context) error {
	data, e := g.call(ctx, "GET", "/v1/drive/alltokens", map[string]any{})
	if e != nil {
		return e
	}
	accounts := map[string]guiAccount{}
	var order []string
	for _, p := range []struct{ field, provider, label string }{{"m139_credentials", "mobile", "移动云盘"}, {"wopan_credentials", "wopan", "联通云盘"}} {
		for i, raw := range list(data[p.field]) {
			c := object(raw)
			if c == nil {
				continue
			}
			name := valueString(c["username"])
			if name == "" {
				name = valueString(c["mobile"])
			}
			if name == "" {
				name = fmt.Sprintf("账号 %d", i+1)
			}
			root := defaultDirectory(p.provider)
			for _, key := range []string{"root_directory_id", "root_id", "directory_id"} {
				if v := valueString(c[key]); v != "" {
					root = v
					break
				}
			}
			key := randomKey()
			accounts[key] = guiAccount{Key: key, Provider: p.provider, Name: p.label + " · " + maskAccount(name), Root: root, credentials: c}
			order = append(order, key)
		}
	}
	g.mu.Lock()
	g.applyDAVCredentialsLocked(accounts)
	for _, key := range g.accountOrder {
		if a, ok := g.accounts[key]; ok && a.Provider == "quark" {
			accounts[key] = a
			order = append(order, key)
		}
	}
	g.accounts = accounts
	g.accountOrder = order
	g.mu.Unlock()
	g.event(fmt.Sprintf("已读取 %d 个已绑定的移动/联通云盘账号", len(accounts)))
	return nil
}
func (g *guiApp) restore(ctx context.Context) {
	g.op.Lock()
	defer g.op.Unlock()
	data, e := g.call(ctx, "GET", "/v1/account/info", map[string]any{})
	if e != nil {
		g.setStage("等待扫码登录")
		return
	}
	user := object(data["user"])
	if user == nil {
		g.setStage("等待扫码登录")
		return
	}
	g.setUser(user)
	if e = g.loadAccounts(ctx); e != nil {
		g.event("登录态有效，但网盘列表读取失败：" + e.Error())
	}
}
func (g *guiApp) setUser(user map[string]any) {
	name := valueString(user["mobile"])
	if name == "" {
		name = valueString(user["username"])
	}
	if name == "" {
		name = "爆米花账号"
	}
	g.mu.Lock()
	g.logged = true
	g.user = maskAccount(name)
	g.stage = "已登录"
	g.qrToken = ""
	g.mu.Unlock()
}
func (g *guiApp) publicStatus() map[string]any {
	g.mu.Lock()
	accounts := []guiAccount{}
	for _, k := range g.accountOrder {
		accounts = append(accounts, g.accounts[k])
	}
	engineError := g.engineError
	if g.bridge != nil && !g.bridge.Alive() {
		engineError = "原版桥接进程已退出，请关闭工具后重新启动"
	}
	out := map[string]any{"version": version, "engine_ready": g.bridge.Alive(), "stage": g.stage, "engine_error": engineError, "logged_in": g.logged, "user": g.user, "accounts": accounts, "has_accounts": len(accounts) > 0, "events": append([]string(nil), g.events...), "qr_expires": g.qrExpires.Unix(), "poll_after_seconds": g.pollAfter, "has_result": g.lastResult != ""}
	out["credential_save_pending"] = len(g.pendingDAVCredentials)
	out["credential_save_error"] = g.credentialSaveError
	out["secret_storage_encrypted"] = localCredentialsEncrypted
	dav := g.dav
	g.mu.Unlock()
	if dav != nil {
		out["webdav"] = dav.Status()
	}
	if g.quark != nil {
		out["quark"] = g.quark.Status()
	}
	if g.webLogin != nil {
		out["wopan_web_login"] = g.webLogin.Status()
	}
	if g.runLog != nil {
		out["run_log"] = g.runLog.status()
	}
	return out
}
func (g *guiApp) createQR(ctx context.Context) (map[string]any, error) {
	data, e := g.call(ctx, "GET", "/v1/account/tv/qrcode", map[string]any{})
	if e != nil {
		return nil, e
	}
	text := valueString(data["qrcode_url"])
	token := valueString(data["token"])
	if text == "" || token == "" {
		return nil, errors.New("原版接口未返回有效二维码")
	}
	b, e := g.getBridge()
	if e != nil {
		return nil, e
	}
	r, e := b.Call(ctx, map[string]any{"kind": "qr_png", "text": text})
	if e != nil {
		return nil, e
	}
	ttl := number(data["expired_in_second"])
	if ttl <= 0 {
		ttl = 180
	}
	g.mu.Lock()
	g.qrToken = token
	g.qrExpires = time.Now().Add(time.Duration(ttl) * time.Second)
	g.stage = "等待手机扫码"
	g.mu.Unlock()
	g.event("真实登录二维码已生成")
	return map[string]any{"image": "data:image/png;base64," + r.PNG, "expires_in": ttl}, nil
}
func (g *guiApp) poll(ctx context.Context) error {
	g.mu.Lock()
	token := g.qrToken
	logged := g.logged
	g.mu.Unlock()
	if logged || token == "" {
		return nil
	}
	data, e := g.call(ctx, "GET", "/v1/account/tv/qrcode/state", map[string]any{"token": token})
	if e != nil {
		return e
	}
	state := valueString(data["state"])
	if wait := number(data["query_after_second"]); wait > 0 && wait <= 15 {
		g.mu.Lock()
		g.pollAfter = wait
		g.mu.Unlock()
	}
	switch state {
	case "init":
		g.setStage("等待手机扫码")
	case "scanned":
		g.setStage("已扫码，请在手机上确认")
	case "login":
		g.setUser(object(data["user"]))
		g.event("手机确认登录成功")
		return g.loadAccounts(ctx)
	default:
		g.setStage("二维码已失效，请刷新")
		g.mu.Lock()
		g.qrToken = ""
		g.mu.Unlock()
	}
	return nil
}
func (g *guiApp) accountClient(key string) (*Client, error) {
	g.mu.Lock()
	a, ok := g.accounts[key]
	a = cloneGUIAccount(a)
	b := g.bridge
	identity := g.identity
	g.mu.Unlock()
	if ok && a.Provider == "quark" {
		folder, err := runFolder(filepath.Join(g.data, "results"))
		if err != nil {
			return nil, err
		}
		c := defaultConfig("mobile")
		c.Provider = "quark"
		return &Client{cfg: c, folder: folder, redact: newRedactor(Config{})}, nil
	}
	if !ok || b == nil {
		return nil, errors.New("请选择已登录账号绑定的云盘")
	}
	c := defaultConfig(a.Provider)
	c.Transport = "bridge"
	c.BaseURL = "https://api.filmly.netease.com"
	c.TimeoutSeconds = 40
	c.IncludeDirectories = true
	c.Mobile = identity
	c.Mobile.AccessToken = valueString(a.credentials["token"])
	c.Mobile.DeviceID = valueString(a.credentials["m139_device_id"])
	c.Mobile.ChannelID = "10010"
	c.Wopan.AccessToken = valueString(a.credentials["token"])
	if a.Provider == "mobile" && (identity.AppID == "" || identity.AppSecret == "") {
		return nil, errors.New("移动云盘应用身份尚未从原版组件读取")
	}
	folder, e := runFolder(filepath.Join(g.data, "results"))
	if e != nil {
		return nil, e
	}
	client, e := newClient(c, folder)
	if e != nil {
		return nil, e
	}
	client.bridge = b
	return client, nil
}
func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (g *guiApp) fail(w http.ResponseWriter, e error) {
	respondJSON(w, 400, map[string]any{"ok": false, "error": e.Error()})
}
func (g *guiApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	if r.Host != g.host {
		http.Error(w, "Host rejected", 403)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+g.host {
		http.Error(w, "Origin rejected", 403)
		return
	}
	if r.URL.Path == "/" && r.Method == "GET" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
		_, _ = w.Write([]byte(strings.ReplaceAll(string(guiHTML), "__CLOUDPROBE_UI_NONCE__", g.nonce)))
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		if r.URL.Path == "/styles.css" && r.Method == "GET" {
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
			_, _ = w.Write(guiCSS)
			return
		}
		if r.URL.Path == "/quark.js" && r.Method == "GET" {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			_, _ = w.Write(guiQuarkJS)
			return
		}
		if r.URL.Path == "/app.js" && r.Method == "GET" {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			_, _ = w.Write(guiAppJS)
			return
		}
		if r.URL.Path == "/transfers.js" && r.Method == "GET" {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			_, _ = w.Write(guiTransfersJS)
			return
		}
		if r.URL.Path == "/run-log.js" && r.Method == "GET" {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			_, _ = w.Write(guiRunLogJS)
			return
		}
		http.NotFound(w, r)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+g.host {
		http.Error(w, "Origin rejected", 403)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CloudProbe-UI")), []byte(g.nonce)) != 1 {
		http.Error(w, "请从程序打开的界面访问", 403)
		return
	}
	if r.URL.Path == "/api/status" && r.Method == "GET" {
		respondJSON(w, 200, g.publicStatus())
		return
	}
	if r.URL.Path == "/api/upload-resume" {
		g.checkpointAPI(w, r)
		return
	}
	if (r.URL.Path == "/api/run-log" || r.URL.Path == "/api/run-log/export") && r.Method == "GET" {
		g.runLogAPI(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/quark/") {
		g.quarkAPI(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/webdav/") {
		g.davAPI(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/download/") && r.Method == "GET" {
		g.mu.Lock()
		file, ok := g.downloads[strings.TrimPrefix(r.URL.Path, "/api/download/")]
		g.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''`+uriEncode(file.Name))
		http.ServeFile(w, r, file.Path)
		return
	}
	if r.URL.Path == "/api/export" && r.Method == "GET" {
		g.export(w, r)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var input struct {
		Account   string `json:"account"`
		Directory string `json:"directory"`
		Expected  string `json:"expected"`
		ID        string `json:"id"`
		FID       string `json:"fid"`
		Name      string `json:"name"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	d := json.NewDecoder(r.Body)
	if e := d.Decode(&input); e != nil {
		g.fail(w, errors.New("请求 JSON 无效"))
		return
	}
	if e := ensureEOF(d); e != nil {
		g.fail(w, e)
		return
	}
	g.op.Lock()
	defer g.op.Unlock()
	switch r.URL.Path {
	case "/api/qr":
		data, e := g.createQR(r.Context())
		if e != nil {
			g.fail(w, e)
			return
		}
		respondJSON(w, 200, data)
	case "/api/poll":
		if e := g.poll(r.Context()); e != nil {
			g.fail(w, e)
			return
		}
		respondJSON(w, 200, g.publicStatus())
	case "/api/accounts":
		g.mu.Lock()
		neteaseLogged := g.logged
		g.mu.Unlock()
		if !neteaseLogged && g.quark != nil {
			g.quark.publishAccounts()
			respondJSON(w, 200, g.publicStatus())
			return
		}
		if e := g.loadAccounts(r.Context()); e != nil {
			g.fail(w, e)
			return
		}
		respondJSON(w, 200, g.publicStatus())
	case "/api/logout":
		g.webLogin.Stop()
		if g.dav != nil {
			g.dav.mu.Lock()
			bound := false
			for _, mount := range g.dav.mounts {
				if mount.Provider != "quark" {
					bound = true
				}
			}
			g.dav.mu.Unlock()
			if bound {
				g.dav.Stop()
			}
		}
		if _, e := g.call(r.Context(), "POST", "/v2/account/logout", map[string]any{}); e != nil {
			g.fail(w, e)
			return
		}
		g.mu.Lock()
		g.logged = false
		g.user = ""
		g.qrToken = ""
		remaining := map[string]guiAccount{}
		var order []string
		for _, key := range g.accountOrder {
			if a, ok := g.accounts[key]; ok && a.Provider == "quark" {
				remaining[key] = a
				order = append(order, key)
			}
		}
		g.accounts = remaining
		g.accountOrder = order
		g.stage = "已退出本次测试会话"
		g.mu.Unlock()
		g.event("已退出登录")
		respondJSON(w, 200, g.publicStatus())
	case "/api/list":
		if strings.TrimSpace(input.Directory) == "" {
			g.fail(w, errors.New("请输入目录 ID"))
			return
		}
		cl, e := g.accountClient(input.Account)
		if e != nil {
			g.fail(w, e)
			return
		}
		names := []string{}
		if input.Expected != "" {
			names = append(names, input.Expected)
		}
		g.mu.Lock()
		account := g.accounts[input.Account]
		g.mu.Unlock()
		logCtx := scopedDAV(newRunTrace(r.Context(), g.runLog, "界面查询", input.Directory), &davMount{Alias: account.Name, Provider: account.Provider}, "目录 ID="+input.Directory)
		started := time.Now()
		var report Report
		var nativeEntries []davEntry
		if account.Provider == "quark" {
			report = newReport("quark", "live-native", input.Directory)
			drive, _, driveErr := g.guiReadDrive(input.Account, cl)
			e = driveErr
			if e == nil {
				nativeEntries, e = drive.List(logCtx, input.Directory)
			}
			if e == nil {
				for _, it := range nativeEntries {
					report.Items = append(report.Items, it.Item)
				}
				report.Complete = true
				report.Pages = 1
				report.StopReason = "原生夸克驱动已按上游分页规则读取完成；分页及通道见运行记录"
				applyExpected(&report, names)
			} else {
				report.Error = g.redactDAV(e.Error())
				report.StopReason = "原生夸克目录查询失败，不能视为空目录"
			}
		} else {
			report, e = cl.List(logCtx, input.Directory, "live-original-sdk", names)
			logList(logCtx, "网易代理", "目录查询", started, mobileDAVEntries(report.Items), e)
		}
		suspect := e != nil || !report.Complete
		for _, note := range report.Notes {
			if strings.Contains(note, "省略了 fileInfoList") {
				suspect = true
			}
		}
		if suspect && account.Provider != "quark" {
			drive, _, de := g.guiReadDrive(input.Account, cl)
			if de == nil {
				var entries []davEntry
				entries, de = drive.List(logCtx, input.Directory)
				if de == nil {
					report.Items = nil
					for _, it := range entries {
						report.Items = append(report.Items, classify(account.Provider, it.Item))
					}
					report.Complete = true
					report.Error = ""
					report.StopReason = "已通过备用读取方案完成目录查询；接口来源见运行记录"
					applyExpected(&report, names)
				}
			}
			e = de
		}
		if e == nil && report.Complete {
			if drive, _, driveErr := g.guiReadDrive(input.Account, cl); driveErr == nil {
				if backup, ok := drive.(*backupDAVDrive); ok {
					backup.remember(report.Directory, mobileDAVEntries(report.Items))
				}
			}
			g.mu.Lock()
			if g.browseEntries == nil {
				g.browseEntries = map[string]map[string]davEntry{}
			}
			known := map[string]davEntry{}
			for _, it := range report.Items {
				known[it.ID] = davEntry{Item: it}
			}
			for _, it := range nativeEntries {
				known[it.ID] = it
			}
			g.browseEntries[input.Account] = known
			g.mu.Unlock()
		}
		if se := saveReport(cl.folder, report, cl.redact); se != nil {
			g.fail(w, se)
			return
		}
		g.mu.Lock()
		g.lastResult = cl.folder
		g.mu.Unlock()
		if e != nil {
			respondJSON(w, 400, map[string]any{"ok": false, "error": cl.redact.Text(e.Error()), "report": report})
			return
		}
		g.event(fmt.Sprintf("目录查询完成，返回 %d 条记录", len(report.Items)))
		respondJSON(w, 200, map[string]any{"ok": true, "report": report})
	case "/api/fetch":
		cl, e := g.accountClient(input.Account)
		if e != nil {
			g.fail(w, e)
			return
		}
		g.mu.Lock()
		account := g.accounts[input.Account]
		g.mu.Unlock()
		logCtx := scopedDAV(newRunTrace(r.Context(), g.runLog, "界面下载", input.Name), &davMount{Alias: account.Name, Provider: account.Provider}, input.Name)
		drive, runtime, e := g.guiReadDrive(input.Account, cl)
		if e != nil {
			g.fail(w, e)
			return
		}
		g.mu.Lock()
		entry, known := g.browseEntries[input.Account][input.ID]
		g.mu.Unlock()
		if !known {
			g.fail(w, errors.New("请重新查询目录后选择文件"))
			return
		}
		receipt, e := guiFetchRedundant(logCtx, drive, runtime, cl, entry, input.Name)
		if e != nil {
			g.fail(w, errors.New(cl.redact.Text(e.Error())))
			return
		}
		key := randomKey()
		g.mu.Lock()
		g.downloads[key] = guiDownload{Path: filepath.Join(cl.folder, input.Name), Name: input.Name}
		g.lastResult = cl.folder
		g.mu.Unlock()
		g.event("指定文件已下载到测试目录")
		respondJSON(w, 200, map[string]any{"ok": true, "download_key": key, "receipt": receipt})
	case "/api/quit":
		respondJSON(w, 200, map[string]any{"ok": true})
		go func() { time.Sleep(200 * time.Millisecond); g.shutdown() }()
	default:
		http.NotFound(w, r)
	}
}
func (g *guiApp) export(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	folder := g.lastResult
	g.mu.Unlock()
	if folder == "" {
		g.fail(w, errors.New("尚无可导出的测试结果"))
		return
	}
	entries, e := os.ReadDir(folder)
	if e != nil {
		g.fail(w, e)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="cloudprobe-diagnostics.zip"`)
	z := zip.NewWriter(w)
	defer z.Close()
	for _, f := range entries {
		n := f.Name()
		if f.IsDir() || !(n == "report.json" || n == "report.html" || n == "summary.txt" || strings.HasPrefix(n, "http-") && strings.HasSuffix(n, ".json")) {
			continue
		}
		b, e := os.ReadFile(filepath.Join(folder, n))
		if e != nil {
			continue
		}
		dst, e := z.Create(n)
		if e == nil {
			_, _ = dst.Write(b)
		}
	}
}
func guiCommand(parent context.Context, args []string, w io.Writer) (int, error) {
	exe, e := os.Executable()
	if e != nil {
		return 1, e
	}
	base := filepath.Dir(exe)
	local, e := os.UserCacheDir()
	if e != nil {
		local = base
	}
	f := flags("gui", w)
	sdk := f.String("sdk", filepath.Join(base, "sdk"), "原版组件目录")
	bridgePath := f.String("bridge", filepath.Join(base, "bridge", "CloudProbe.Bridge.dll"), "桥接组件")
	data := f.String("data", filepath.Join(local, "CloudProbeGUI"), "独立测试数据目录")
	noOpen := f.Bool("no-open", false, "不自动打开浏览器")
	port := f.Int("port", 0, "本机端口，0 自动分配")
	if e = parseFlags(f, args); e != nil {
		return 2, e
	}
	*sdk, _ = filepath.Abs(*sdk)
	*bridgePath, _ = filepath.Abs(*bridgePath)
	*data, _ = filepath.Abs(*data)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	listener, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if e != nil {
		return 1, e
	}
	app := &guiApp{stage: "正在启动原版引擎", accounts: map[string]guiAccount{}, downloads: map[string]guiDownload{}, data: *data, nonce: randomKey(), host: listener.Addr().String(), shutdown: cancel}
	app.runLog = newRuntimeLog(*data, app.redactDAV)
	defer app.runLog.close()
	app.quark = newQuarkManager(app)
	defer app.quark.Stop()
	app.dav = newDAVManager(app)
	app.webLogin = &wopanWebLogin{app: app, stage: "未开始"}
	defer app.dav.Stop()
	defer app.webLogin.Stop()
	server := &http.Server{Handler: app, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		b, e := startOriginalBridge(ctx, *sdk, *bridgePath, filepath.Join(*data, "original-sdk"))
		if e != nil {
			app.mu.Lock()
			app.engineError = e.Error()
			app.stage = "引擎启动失败"
			app.mu.Unlock()
			app.event("原版桥接启动失败")
			return
		}
		app.mu.Lock()
		app.bridge = b
		app.stage = "原版引擎已就绪"
		app.mu.Unlock()
		app.event("原版引擎初始化成功，使用独立测试数据目录")
		if identity, e := b.Call(ctx, map[string]any{"kind": "identity"}); e == nil {
			app.mu.Lock()
			app.identity = MobileConfig{AppID: identity.AppID, AppSecret: identity.AppSecret}
			app.mu.Unlock()
		}
		app.restore(ctx)
	}()
	address := "http://" + app.host + "/#" + app.nonce
	fmt.Fprintln(w, "GUI_URL="+address)
	if !*noOpen {
		if e = openLocalBrowser(address); e != nil {
			fmt.Fprintln(w, "请手动打开上面的本机地址")
		}
	}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
			return
		}
		shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = server.Shutdown(shutdownCtx)
	}()
	e = server.Serve(listener)
	close(stopped)
	app.webLogin.Stop()
	app.dav.Stop()
	app.mu.Lock()
	b := app.bridge
	app.mu.Unlock()
	if b != nil {
		b.Close()
	}
	if e == http.ErrServerClosed {
		return 0, nil
	}
	return 1, e
}

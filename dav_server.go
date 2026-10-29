package main

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/webdav"
)

type davMountInput struct {
	UploadStorageMode string         `json:"upload_storage_mode,omitempty"`
	OpenList          *openListInput `json:"openlist,omitempty"`
	Account           string         `json:"account"`
	Alias             string         `json:"alias"`
	Root              string         `json:"root"`
	Writable          bool           `json:"writable"`
}
type davStartInput struct {
	transferOptions
	Listen            string          `json:"listen"`
	Port              int             `json:"port"`
	Username          string          `json:"username"`
	Password          string          `json:"password"`
	MaxUploadMiB      int64           `json:"max_upload_mib"`
	UploadConcurrency int             `json:"upload_concurrency"`
	UploadMode        string          `json:"upload_mode"`
	Mounts            []davMountInput `json:"mounts"`
}
type davSavedMount struct {
	UploadStorageMode string `json:"upload_storage_mode,omitempty"`
	OpenListEnabled   bool   `json:"openlist_enabled,omitempty"`
	OpenListAddress   string `json:"openlist_address,omitempty"`
	OpenListRoot      string `json:"openlist_root,omitempty"`
	Identity          string `json:"identity"`
	Alias             string `json:"alias"`
	Root              string `json:"root"`
	Writable          bool   `json:"writable"`
}
type davSavedConfig struct {
	transferOptions
	Listen            string          `json:"listen"`
	Port              int             `json:"port"`
	Username          string          `json:"username"`
	Salt              string          `json:"salt"`
	PasswordHash      string          `json:"password_hash"`
	MaxUploadMiB      int64           `json:"max_upload_mib"`
	UploadConcurrency int             `json:"upload_concurrency"`
	UploadMode        string          `json:"upload_mode"`
	Mounts            []davSavedMount `json:"mounts"`
}
type davActivity struct {
	RequestID      string `json:"request_id,omitempty"`
	Time           string `json:"time"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	Status         int    `json:"status"`
	Bytes          int64  `json:"bytes"`
	ReceivedBytes  int64  `json:"received_bytes"`
	SentBytes      int64  `json:"sent_bytes"`
	ConfirmedBytes int64  `json:"confirmed_bytes"`
	DurationMs     int64  `json:"duration_ms"`
	Error          string `json:"error,omitempty"`
}
type davManager struct {
	controlMu sync.RWMutex
	mu        sync.Mutex
	app       *guiApp
	server    *http.Server
	cancel    context.CancelFunc
	config    davSavedConfig
	mounts    []*davMount
	urls      []string
	activity  []davActivity
	active    int
	lastError string
	transfers *transferRuntime
}

func newDAVManager(g *guiApp) *davManager {
	d := &davManager{app: g}
	b, e := os.ReadFile(filepath.Join(g.data, "webdav.json"))
	if e == nil {
		_ = json.Unmarshal(b, &d.config)
	} else if errors.Is(e, os.ErrNotExist) {
		d.config.TransferScheduling = "client"
	}
	return d
}
func davIdentity(a guiAccount) string {
	id := valueString(a.credentials["user_id"])
	if id == "" {
		id = valueString(a.credentials["username"])
	}
	if id == "" {
		id = valueString(a.credentials["mobile"])
	}
	if strings.TrimSpace(id) == "" {
		return ""
	}
	return davIdentityHash(a.Provider, id)
}
func davIdentityHash(provider, id string) string {
	h := sha256.Sum256([]byte(provider + "\x00" + id))
	return hex.EncodeToString(h[:16])
}
func (d *davManager) Status() map[string]any {
	if d == nil {
		return map[string]any{"running": false}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	var mounts []map[string]any
	for _, m := range d.mounts {
		mounts = append(mounts, map[string]any{"alias": m.Alias, "provider": m.Provider, "root": m.Root, "writable": m.Writable, "write_status": m.WriteStatus, "upload_storage_mode": m.UploadStorageMode})
	}
	var saved []map[string]any
	if d.app != nil {
		d.app.mu.Lock()
		for _, m := range d.config.Mounts {
			for key, a := range d.app.accounts {
				if m.Identity != "" && davIdentity(a) == m.Identity {
					saved = append(saved, map[string]any{"account": key, "alias": m.Alias, "root": m.Root, "writable": m.Writable, "openlist_enabled": m.OpenListEnabled, "openlist_address": m.OpenListAddress, "openlist_root": m.OpenListRoot, "upload_storage_mode": m.UploadStorageMode})
				}
			}
		}
		d.app.mu.Unlock()
	}
	return map[string]any{"running": d.server != nil, "listen": d.config.Listen, "port": d.config.Port, "username": d.config.Username, "has_password": d.config.PasswordHash != "", "max_upload_mib": d.config.MaxUploadMiB, "upload_concurrency": uploadConcurrency(d.config.UploadConcurrency), "upload_mode": uploadMode(d.config.UploadMode), "transfer_settings": d.config.transferOptions.defaults(), "transfer_queues": d.transfers.status(), "urls": append([]string{}, d.urls...), "mounts": mounts, "saved_mounts": saved, "activity": append([]davActivity{}, d.activity...), "active": d.active, "last_error": d.lastError}
}
func (d *davManager) Stop() {
	if d == nil {
		return
	}
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	d.mu.Lock()
	s, c := d.server, d.cancel
	d.server = nil
	d.cancel = nil
	d.mu.Unlock()
	if c != nil {
		c()
	}
	if s != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
		_ = s.Close()
	}
}
func (d *davManager) Start(ctx context.Context, in davStartInput) (map[string]any, error) {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	d.mu.Lock()
	previous := d.config
	running := d.server != nil
	d.mu.Unlock()
	if running {
		return nil, errors.New("WebDAV 已运行，请先停止后更改设置")
	}
	if in.Listen == "" {
		in.Listen = "127.0.0.1"
	}
	if in.Listen != "127.0.0.1" && in.Listen != "0.0.0.0" {
		return nil, errors.New("监听地址请选择 127.0.0.1 或 0.0.0.0")
	}
	if in.Port < 1 || in.Port > 65535 {
		return nil, errors.New("端口需为 1—65535")
	}
	if in.Username == "" {
		in.Username = "cloudprobe"
	}
	if len(in.Username) > 100 || strings.ContainsAny(in.Username, ":\r\n\x00") {
		return nil, errors.New("WebDAV 用户名无效")
	}
	if in.MaxUploadMiB == 0 {
		in.MaxUploadMiB = 10240
	}
	if in.MaxUploadMiB < 1 || in.MaxUploadMiB > 1048576 {
		return nil, errors.New("上传上限需为 1—1048576 MiB")
	}
	if in.UploadConcurrency < 0 || in.UploadConcurrency > 16 {
		return nil, errors.New("每文件上传并发需为 1—16，留空默认 4")
	}
	if in.UploadMode != "" && in.UploadMode != "auto" && in.UploadMode != "http1" && in.UploadMode != "http2" {
		return nil, errors.New("上传传输模式请选择 HTTP/1.1 独立连接或 HTTP/2 共享连接对照")
	}
	cfg := davSavedConfig{Listen: in.Listen, Port: in.Port, Username: in.Username, MaxUploadMiB: in.MaxUploadMiB, UploadConcurrency: uploadConcurrency(in.UploadConcurrency), UploadMode: uploadMode(in.UploadMode), PasswordHash: previous.PasswordHash, Salt: previous.Salt}
	cfg.transferOptions = in.transferOptions.defaults()
	if err := cfg.transferOptions.validate(); err != nil {
		return nil, err
	}
	transfers := newTransferRuntime(cfg.transferOptions)
	generated := ""
	if in.Password == "" && cfg.PasswordHash == "" {
		in.Password = randomKey()[:24]
		generated = in.Password
	}
	if in.Password != "" {
		if len(in.Password) < 12 || len(in.Password) > 512 {
			return nil, errors.New("密码长度需为 12—512 个字节；留空可保留已设密码或首次自动生成")
		}
		salt := make([]byte, 16)
		if _, e := rand.Read(salt); e != nil {
			return nil, e
		}
		h, _ := pbkdf2.Key(sha256.New, in.Password, salt, 100000, 32)
		cfg.Salt = hex.EncodeToString(salt)
		cfg.PasswordHash = hex.EncodeToString(h)
	}
	if len(in.Mounts) == 0 {
		return nil, errors.New("请至少选择一个云盘挂载")
	}
	g := d.app
	if g.quark != nil && g.quark.Status()["running"] == true {
		return nil, errors.New("请先完成或取消夸克登录，再启动 WebDAV")
	}
	if g.webLogin != nil && g.webLogin.Status()["running"] == true {
		return nil, errors.New("请先完成或取消官网备用登录，再启动 WebDAV")
	}
	g.mu.Lock()
	logged := g.logged
	resourceErr := validateDAVMountResources(in.Mounts, g.accounts)
	g.mu.Unlock()
	if resourceErr != nil {
		return nil, resourceErr
	}
	if !logged && !g.quark.loggedClients() {
		return nil, errors.New("请先登录爆米花并读取绑定云盘，或添加独立夸克账号")
	}
	var mounts []*davMount
	aliases := map[string]bool{}
	for _, sel := range in.Mounts {
		g.mu.Lock()
		a, ok := g.accounts[sel.Account]
		a = cloneGUIAccount(a)
		g.mu.Unlock()
		if !ok {
			return nil, errors.New("所选账号已变化，请刷新账号后重新选择")
		}
		if davIdentity(a) == "" {
			return nil, errors.New("账号响应缺少稳定身份标识，无法安全恢复挂载和凭据；请重新读取绑定账号")
		}
		if sel.UploadStorageMode != "" && sel.UploadStorageMode != "memory" && sel.UploadStorageMode != "disk" && sel.UploadStorageMode != "auto" {
			return nil, errors.New("挂载上传缓存方式请选择跟随全局、严格纯内存、内存优先或磁盘兼容")
		}
		if e := safeFilename(sel.Alias); e != nil || aliases[sel.Alias] {
			return nil, errors.New("挂载名必须是唯一的普通文件夹名称")
		}
		aliases[sel.Alias] = true
		if sel.Root == "" {
			sel.Root = a.Root
		}
		if (a.Provider == "wopan" || a.Provider == "quark") && sel.Root == "root" {
			sel.Root = "0"
		}
		if strings.ContainsAny(sel.Root, "\r\n\x00") {
			return nil, errors.New("根目录 ID 无效")
		}
		cl, e := g.accountClient(sel.Account)
		if e != nil {
			return nil, e
		}
		// WebDAV logs contain operation summaries, not raw directory responses.
		cl.folder = ""
		m := &davMount{UploadStorageMode: sel.UploadStorageMode, Alias: sel.Alias, Root: sel.Root, Provider: a.Provider, Writable: sel.Writable, Identity: davIdentity(a), WriteStatus: "只读挂载", health: &routeHealth{}}
		backupKey := m.Identity + ":" + m.Alias
		backupConfig, backupErr := g.openListConfig(backupKey, sel.OpenList, m.Identity+"\x00"+m.Root)
		if backupErr != nil {
			return nil, errors.New("读取 OpenList 备用配置失败：" + g.redactDAV(backupErr.Error()))
		}
		mountCtx := scopedDAV(newRunTrace(ctx, g.runLog, "挂载验证", "/"+m.Alias), m, "/"+m.Alias)
		if a.Provider == "quark" {
			drive, err := g.quarkDrive(sel.Account, transfers, cfg.UploadConcurrency, cfg.UploadMode)
			if err != nil {
				return nil, err
			}
			m.drive = drive
			if m.Writable {
				m.WriteStatus = "夸克原生读写；网页上传需磁盘兼容模式，开放平台按实际能力验证"
			}
		} else if a.Provider == "wopan" {
			drive := newWopanDAVDrive(cl, a.credentials, sel.Root, func(access, refresh string) error { return g.saveDAVCredential(a, access, refresh) })
			drive.uploadConcurrency, drive.uploadPartMiB, drive.uploadBudget = cfg.UploadConcurrency, cfg.UploadPartMiB, transfers.connections
			drive.uploadMode = cfg.UploadMode
			m.drive = drive
			if m.Writable {
				probeCtx, cancel := context.WithTimeout(mountCtx, 35*time.Second)
				e = drive.probe(probeCtx)
				cancel()
				if e != nil {
					if backupConfig == nil || !backupConfig.Verified {
						return nil, errors.New(cl.redact.Text(e.Error()))
					}
					logFallback(mountCtx, "内置写入凭据验证", "已核验的 OpenList 备用", e)
				}
				m.WriteStatus = "读写接口已接入；每次操作检查上游结果"
			}
		} else {
			m.drive = &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}, onRefresh: func(access, refresh string) error { return g.saveDAVCredential(a, access, refresh) }, budget: transfers.connections, partMiB: cfg.UploadPartMiB}
			if m.Writable {
				m.WriteStatus = "开放接口写入受应用目录及后缀限制；尚需此账号实测"
			}
		}
		// Fail before opening a port when the requested mount root cannot be listed.
		probeCtx, cancel := context.WithTimeout(mountCtx, 40*time.Second)
		_, e = m.drive.List(probeCtx, m.Root)
		cancel()
		if e != nil {
			if backupConfig == nil || !backupConfig.Verified {
				return nil, fmt.Errorf("挂载 %s 的目录验证失败：%s", m.Alias, cl.redact.Text(e.Error()))
			}
		}
		savedMount := davSavedMount{UploadStorageMode: m.UploadStorageMode, Identity: m.Identity, Alias: m.Alias, Root: m.Root, Writable: m.Writable}
		if backupConfig != nil {
			g.mu.Lock()
			g.extraSecrets = append(g.extraSecrets, backupConfig.Token, backupConfig.MetaPassword)
			g.mu.Unlock()
			backup, e := newOpenListClient(backupConfig.openListInput)
			if e != nil {
				return nil, e
			}
			backup.budget = transfers.connections
			backupConfig.Address, backupConfig.Root = backup.cfg.Address, backup.cfg.Root
			u, _ := url.Parse(backup.cfg.Address)
			if backup.cfg.Address == "http://"+g.host || u.Host == net.JoinHostPort("127.0.0.1", strconv.Itoa(in.Port)) || u.Host == net.JoinHostPort("localhost", strconv.Itoa(in.Port)) {
				return nil, errors.New("OpenList 地址不能指向本工具管理界面")
			}
			if !backupConfig.Verified {
				if e = verifyOpenList(mountCtx, m.drive, m.Root, backup, g.data); e != nil {
					return nil, errors.New(g.redactDAV(e.Error()))
				}
				backupConfig.Verified = true
			}
			if e = g.saveOpenList(backupKey, *backupConfig); e != nil {
				return nil, e
			}
			m.drive = &backupDAVDrive{primary: m.drive, backup: backup, root: m.Root, paths: map[string]string{m.Root: ""}, entries: map[string]davEntry{}}
			checkCtx, cancel := context.WithTimeout(mountCtx, 40*time.Second)
			_, checkErr := m.drive.List(checkCtx, m.Root)
			cancel()
			if checkErr != nil {
				return nil, errors.New(g.redactDAV(checkErr.Error()))
			}
			savedMount.OpenListEnabled, savedMount.OpenListAddress, savedMount.OpenListRoot = true, backup.cfg.Address, backup.cfg.Root
			m.WriteStatus += "；OpenList 仅作目录和下载备用，写入使用原生接口"
		} else if sel.OpenList != nil && !sel.OpenList.Enabled {
			if e = g.saveOpenList(backupKey, openListSaved{Identity: m.Identity}); e != nil {
				return nil, e
			}
		}
		mounts = append(mounts, m)
		cfg.Mounts = append(cfg.Mounts, savedMount)
	}
	listener, e := net.Listen("tcp", net.JoinHostPort(in.Listen, strconv.Itoa(in.Port)))
	if e != nil {
		return nil, e
	}
	fs := &davFS{mounts: mounts, spool: filepath.Join(g.data, "webdav-spool"), maxUpload: in.MaxUploadMiB << 20, cache: map[string]davCache{}, transfers: transfers}
	handler := &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS(), Logger: func(r *http.Request, err error) {
		if state := davState(r.Context()); state != nil && state.err == nil && err != nil {
			state.err = err
		}
	}}
	serviceCtx, cancel := context.WithCancel(context.Background())
	service := &davService{manager: d, fs: fs, handler: handler, username: cfg.Username, salt: cfg.Salt, passwordHash: cfg.PasswordHash}
	server := &http.Server{Handler: service, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10, BaseContext: func(net.Listener) context.Context { return serviceCtx }}
	server.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, davIncomingConnectionKey{}, "webdav-"+randomKey()[:10])
	}
	if e = writePrivateJSON(filepath.Join(g.data, "webdav.json"), cfg); e != nil {
		cancel()
		listener.Close()
		return nil, e
	}
	d.mu.Lock()
	d.config = cfg
	d.transfers = transfers
	d.mounts = mounts
	d.server = server
	d.cancel = cancel
	d.urls = davURLs(in.Listen, in.Port)
	d.lastError = ""
	d.mu.Unlock()
	go func() {
		e := server.Serve(listener)
		if e != nil && e != http.ErrServerClosed {
			d.mu.Lock()
			d.lastError = e.Error()
			d.mu.Unlock()
		}
	}()
	g.event(fmt.Sprintf("WebDAV 已启动，挂载 %d 个云盘", len(mounts)))
	out := d.Status()
	if generated != "" {
		out["generated_password"] = generated
	}
	return out, nil
}
func davURLs(listen string, port int) []string {
	result := []string{fmt.Sprintf("http://127.0.0.1:%d/dav/", port)}
	if listen == "0.0.0.0" {
		addrs, _ := net.InterfaceAddrs()
		for _, a := range addrs {
			ip, _, e := net.ParseCIDR(a.String())
			if e == nil && ip.To4() != nil && !ip.IsLoopback() && ip.IsPrivate() {
				result = append(result, fmt.Sprintf("http://%s:%d/dav/", ip.String(), port))
			}
		}
	}
	return result
}
func writePrivateJSON(file string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return writePrivateBytes(file, b)
}
func writePrivateBytes(file string, b []byte) error {
	if e := os.MkdirAll(filepath.Dir(file), 0700); e != nil {
		return e
	}
	tmp, e := os.CreateTemp(filepath.Dir(file), ".config-*")
	if e != nil {
		return e
	}
	defer os.Remove(tmp.Name())
	if e = tmp.Chmod(0600); e == nil {
		_, e = tmp.Write(b)
	}
	if e == nil {
		e = tmp.Sync()
	}
	ce := tmp.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(tmp.Name(), file)
}

type davService struct {
	manager                      *davManager
	fs                           *davFS
	handler                      *webdav.Handler
	username, salt, passwordHash string
	slots                        chan struct{}
	authOnce                     sync.Once
	auth                         *davAuthenticator
	requestOnce                  sync.Once
	requestSlots                 chan struct{}
}
type davIncomingConnectionKey struct{}

func (s *davService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requestOnce.Do(func() { s.requestSlots = make(chan struct{}, 256) })
	select {
	case s.requestSlots <- struct{}{}:
		defer func() { <-s.requestSlots }()
	default:
		http.Error(w, "请求队列已满，请稍后重试", http.StatusServiceUnavailable)
		return
	}
	started := time.Now()
	state := &davRequestState{method: r.Method, contentLength: r.ContentLength, externalRange: r.Header.Get("Range") != ""}
	r = r.WithContext(context.WithValue(r.Context(), davRequestKey{}, state))
	var runtime *runtimeLog
	if s.manager != nil && s.manager.app != nil {
		runtime = s.manager.app.runLog
	}
	logCtx := newRunTrace(r.Context(), runtime, r.Method, strings.TrimPrefix(r.URL.Path, "/dav"))
	for _, m := range s.fs.mounts {
		if r.URL.Path == "/dav/"+m.Alias || strings.HasPrefix(r.URL.Path, "/dav/"+m.Alias+"/") {
			logCtx = scopedDAV(logCtx, m, "")
			break
		}
	}
	logCtx = withTransfers(logCtx, s.fs.transfers)
	r = r.WithContext(logCtx)
	tr := logCtx.Value(runTraceKey{}).(*runTrace)
	connectionID, _ := r.Context().Value(davIncomingConnectionKey{}).(string)
	localAddress := r.Host
	if addr, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		localAddress = addr.String()
	}
	state.finish = func(success bool) error { return s.fs.finishBackups(r.Context(), state, success) }
	rw := &davResponse{ResponseWriter: w, state: state, head: r.Method == "HEAD", redact: s.redact}
	rw.Header().Set("X-Content-Type-Options", "nosniff")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("X-CloudProbe-Request-ID", tr.id)
	defer func() {
		status := rw.status
		if status == 0 {
			status = 200
		}
		entry := runLogEntry{Operation: "WebDAV 请求", Source: "第三方客户端", Result: strconv.Itoa(status), DurationMs: time.Since(started).Milliseconds(), ReceivedBytes: state.bodyBytes, SentBytes: tr.sent.Load(), ConfirmedBytes: tr.confirmed.Load(), ResponseBytes: rw.bytes, Error: s.safeError(state.err)}
		entry.ConnectionID, entry.Protocol, entry.LocalAddress, entry.RemoteAddress = connectionID, r.Proto, localAddress, r.RemoteAddr
		if r.Method == "PUT" {
			entry.Bytes = state.bodyBytes
			if status >= 200 && status < 300 {
				entry.BytesPerSecond = rateFor(state.bodyBytes, time.Since(started))
			}
			entry.Detail = "整体速度含目录解析、收包暂存、排队、云端传输及提交；响应体大小单独记录"
		}
		if r.Method == "GET" {
			entry.Bytes = tr.downloaded.Load()
			entry.BytesPerSecond = rateFor(rw.bytes, time.Since(started))
		}
		logDAV(logCtx, entry)
		if s.manager != nil {
			s.manager.mu.Lock()
			s.manager.activity = append(s.manager.activity, davActivity{RequestID: tr.id, Time: time.Now().Format("15:04:05"), Method: r.Method, Path: r.URL.Path, Status: status, Bytes: rw.bytes, ReceivedBytes: state.bodyBytes, SentBytes: tr.sent.Load(), ConfirmedBytes: tr.confirmed.Load(), DurationMs: time.Since(started).Milliseconds(), Error: entry.Error})
			if len(s.manager.activity) > 150 {
				s.manager.activity = s.manager.activity[len(s.manager.activity)-150:]
			}
			s.manager.mu.Unlock()
		}
	}()
	if code := s.authenticationStatus(r); code != 0 {
		if code == http.StatusUnauthorized {
			rw.Header().Set("WWW-Authenticate", `Basic realm="CloudProbe WebDAV", charset="UTF-8"`)
			http.Error(rw, "需要 WebDAV 用户名和密码", code)
		} else {
			rw.Header().Set("Retry-After", "1")
			http.Error(rw, "认证请求过多或暂不可用，请稍后重试", code)
		}
		return
	}
	logDAV(logCtx, runLogEntry{Operation: "WebDAV 请求", Source: "第三方客户端", Result: "开始", ConnectionID: connectionID, Protocol: r.Proto, LocalAddress: localAddress, RemoteAddress: r.RemoteAddr})
	if origin := r.Header.Get("Origin"); origin != "" {
		u, e := url.Parse(origin)
		if e != nil || u.Host != r.Host {
			http.Error(rw, "Origin rejected", 403)
			return
		}
	}
	if r.URL.Path != "/dav" && !strings.HasPrefix(r.URL.Path, "/dav/") {
		http.NotFound(rw, r)
		return
	}
	if _, e := davPath(strings.TrimPrefix(r.URL.Path, "/dav")); e != nil {
		http.Error(rw, "路径无效", 400)
		return
	}
	if strings.Contains(strings.ToLower(r.URL.RawPath), "%2f") || strings.Contains(strings.ToLower(r.URL.RawPath), "%5c") {
		http.Error(rw, "路径分隔符编码无效", 400)
		return
	}
	if s.slots != nil && (r.Method == "PUT" || r.Method == "GET" || r.Method == "COPY") {
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		case <-r.Context().Done():
			return
		default:
			http.Error(rw, "当前传输任务已满，请稍后重试", 503)
			return
		}
	}
	if s.fs.transfers != nil && (r.Method == "PUT" || r.Method == "GET" || r.Method == "COPY") {
		kind := "下载"
		if r.Method != "GET" {
			kind = "上传"
		}
		release, err := s.fs.transfers.file(r.Context(), kind, strings.TrimPrefix(r.URL.Path, "/dav"))
		if err != nil {
			http.Error(rw, "传输队列已取消", http.StatusRequestTimeout)
			return
		}
		defer release()
	}
	if s.manager != nil {
		s.manager.mu.Lock()
		s.manager.active++
		s.manager.mu.Unlock()
		defer func() { s.manager.mu.Lock(); s.manager.active--; s.manager.mu.Unlock() }()
	}
	if r.Method == "PROPFIND" && r.Header.Get("Depth") == "" {
		r.Header.Set("Depth", "1")
	}
	if r.Method == "PROPFIND" && r.Header.Get("Depth") == "infinity" {
		http.Error(rw, "请使用 Depth: 0 或 1 逐层浏览", 403)
		return
	}
	if r.Method == "COPY" || r.Method == "MOVE" {
		dest, e := url.Parse(r.Header.Get("Destination"))
		if e != nil || dest.Host != "" && dest.Host != r.Host || dest.RawQuery != "" || dest.Fragment != "" || !strings.HasPrefix(dest.Path, "/dav/") {
			http.Error(rw, "Destination 必须在本 WebDAV 服务内", 400)
			return
		}
		if _, e = davPath(strings.TrimPrefix(dest.Path, "/dav")); e != nil {
			http.Error(rw, "Destination 路径无效", 400)
			return
		}
		srcPath := strings.TrimPrefix(r.URL.Path, "/dav")
		dstPath := strings.TrimPrefix(dest.Path, "/dav")
		state.destination, _ = davPath(dstPath)
		if r.Header.Get("Overwrite") == "" {
			r.Header.Set("Overwrite", "T")
		}
		srcParts := strings.Split(strings.Trim(srcPath, "/"), "/")
		dstParts := strings.Split(strings.Trim(dstPath, "/"), "/")
		if srcParts[0] != dstParts[0] {
			http.Error(rw, "暂不支持跨挂载复制或移动", 501)
			return
		}
		if dstPath == srcPath || strings.HasPrefix(strings.TrimRight(dstPath, "/"), strings.TrimRight(srcPath, "/")+"/") {
			http.Error(rw, "目标不能是源路径或源目录内部", 403)
			return
		}
	}
	if r.Method == "PUT" || r.Method == "DELETE" || r.Method == "MOVE" || r.Method == "COPY" || r.Method == "MKCOL" || r.Method == "LOCK" || r.Method == "PROPPATCH" {
		target := strings.TrimPrefix(r.URL.Path, "/dav")
		if r.Method == "COPY" {
			u, _ := url.Parse(r.Header.Get("Destination"))
			target = strings.TrimPrefix(u.Path, "/dav")
		}
		parts := strings.Split(strings.Trim(target, "/"), "/")
		writable := false
		for _, m := range s.fs.mounts {
			if m.Alias == parts[0] {
				writable = m.Writable
			}
		}
		if !writable {
			http.Error(rw, "此挂载为只读或路径不存在", 403)
			return
		}
		targets := []string{target}
		if r.Method == "COPY" || r.Method == "MOVE" {
			targets = []string{strings.TrimPrefix(r.URL.Path, "/dav"), state.destination}
		}
		release, reserveErr := s.fs.reserve(r.Context(), targets...)
		if reserveErr != nil {
			code := http.StatusLocked
			var statusErr *davStatusError
			if errors.As(reserveErr, &statusErr) {
				code = statusErr.Code
			}
			http.Error(rw, s.safeError(reserveErr), code)
			return
		}
		defer release()
		source := strings.TrimPrefix(r.URL.Path, "/dav")
		s.fs.refreshPath(source)
		if state.destination != "" {
			s.fs.refreshPath(state.destination)
		}
		writeTargets := []string{target}
		if r.Method == "MOVE" {
			writeTargets = append(writeTargets, state.destination)
		}
		for _, name := range writeTargets {
			if err := s.fs.checkNativeWrite(r.Context(), name); err != nil {
				failure := davWriteCheckFailure(r.Context(), name, err)
				http.Error(rw, s.safeError(failure), failure.Code)
				return
			}
		}
		if !s.preconditions(rw, r, source) {
			return
		}
		if r.Method == "PUT" {
			if s.fs.memoryUploadAt(target) {
				if r.ContentLength < 0 {
					http.Error(rw, "纯内存上传需要客户端提供文件总长度 Content-Length", http.StatusLengthRequired)
					return
				}
				for _, m := range s.fs.mounts {
					if m.Alias == parts[0] && !streamDriveAvailable(m.drive) {
						http.Error(rw, "此挂载不支持纯内存上传，请明确选择磁盘兼容模式", http.StatusNotImplemented)
						return
					}
				}
			}
			if r.ContentLength > s.fs.maxUpload {
				http.Error(rw, "文件超过配置的上传上限", 413)
				return
			}
			if r.Header.Get("Content-Range") != "" {
				http.Error(rw, "不支持 WebDAV 局部 PUT；请提交完整文件", 501)
				return
			}
		}
	}
	if r.Method == "GET" || r.Method == "HEAD" {
		s.fs.refreshPath(strings.TrimPrefix(r.URL.Path, "/dav"))
	}
	if r.Method == "PUT" {
		r.Body = &davBody{ReadCloser: http.MaxBytesReader(rw, r.Body, s.fs.maxUpload), state: state, ctx: logCtx}
	} else {
		r.Body = http.MaxBytesReader(rw, r.Body, 1<<20)
	}
	s.handler.ServeHTTP(rw, r)
}
func (s *davService) redact(text string) string {
	if s.manager == nil {
		return text
	}
	return s.manager.app.redactDAV(text)
}
func (s *davService) safeError(e error) string {
	if e == nil {
		return ""
	}
	return s.redact(e.Error())
}

type davBody struct {
	io.ReadCloser
	state         *davRequestState
	ctx           context.Context
	started, last time.Time
}

func (b *davBody) Read(p []byte) (int, error) {
	if b.started.IsZero() {
		b.started, b.last = time.Now(), time.Now()
		logDAV(b.ctx, runLogEntry{Operation: "接收客户端正文", Source: "第三方客户端", Result: "开始", Bytes: max(int64(0), b.state.contentLength)})
	}
	n, e := b.ReadCloser.Read(p)
	b.state.bodyBytes += int64(n)
	if b.ctx != nil && (time.Since(b.last) >= time.Second || e != nil) {
		b.last = time.Now()
		entry := runLogEntry{Operation: "接收客户端正文", Source: "第三方客户端", Result: "接收中", ReceivedBytes: b.state.bodyBytes, DurationMs: time.Since(b.started).Milliseconds(), BytesPerSecond: rateFor(b.state.bodyBytes, time.Since(b.started))}
		if e == io.EOF {
			entry.Result = "已接收"
		} else if e != nil {
			entry.Result, entry.Error = "失败", e.Error()
		}
		logDAV(b.ctx, entry)
	}
	if e != nil && e != io.EOF {
		b.state.aborted = true
		b.state.err = e
	}
	return n, e
}

type davResponse struct {
	http.ResponseWriter
	state        *davRequestState
	status       int
	bytes        int64
	head, failed bool
	redact       func(string) string
}

func (w *davResponse) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	if w.state.finish != nil {
		finish := w.state.finish
		w.state.finish = nil
		if e := finish(code >= 200 && code < 300 && w.state.err == nil && !w.state.aborted); e != nil {
			w.state.err = &davUpstreamError{Message: e.Error(), Err: e}
		}
	}
	if w.state.err != nil {
		var local *davStatusError
		var upstream *davUpstreamError
		var max *http.MaxBytesError
		if errors.As(w.state.err, &local) {
			code = local.Code
			w.failed = true
		} else if errors.As(w.state.err, &upstream) {
			code = 502
			w.failed = true
		} else if errors.As(w.state.err, &max) {
			code = 413
			w.failed = true
		}
	}
	w.status = code
	if w.failed {
		w.Header().Del("Content-Length")
		w.Header().Del("ETag")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.ResponseWriter.WriteHeader(code)
	if w.failed && !w.head {
		message := w.state.err.Error()
		if w.redact != nil {
			message = w.redact(message)
		}
		_, _ = io.WriteString(w.ResponseWriter, message+"\n")
	}
}
func (w *davResponse) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if w.failed {
		return len(b), nil
	}
	n, e := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, e
}

func (g *guiApp) redactDAV(text string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	r := &Redactor{}
	for _, secret := range g.extraSecrets {
		r.Add(secret)
	}
	for _, a := range g.accounts {
		for k, v := range a.credentials {
			if sensitive(k) {
				r.Add(valueString(v))
			}
		}
	}
	r.Add(g.identity.AppSecret)
	return r.Text(text)
}
func (g *guiApp) davAPI(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/webdav/wopan-login/") {
		g.wopanLoginAPI(w, r)
		return
	}
	if g.dav == nil {
		g.fail(w, errors.New("WebDAV 管理器尚未初始化"))
		return
	}
	if r.Method == "GET" && r.URL.Path == "/api/webdav/status" {
		respondJSON(w, 200, g.dav.Status())
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	if r.URL.Path == "/api/webdav/stop" {
		g.dav.Stop()
		g.event("WebDAV 已停止")
		respondJSON(w, 200, g.dav.Status())
		return
	}
	if r.URL.Path != "/api/webdav/start" {
		http.NotFound(w, r)
		return
	}
	var in davStartInput
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if e := dec.Decode(&in); e != nil {
		g.fail(w, errors.New("WebDAV 设置 JSON 无效"))
		return
	}
	if e := ensureEOF(dec); e != nil {
		g.fail(w, e)
		return
	}
	g.op.Lock()
	defer g.op.Unlock()
	out, e := g.dav.Start(r.Context(), in)
	if e != nil {
		g.fail(w, errors.New(g.redactDAV(e.Error())))
		return
	}
	respondJSON(w, 200, out)
}

// Credentials refreshed by the direct driver stay in this tool's private profile.
type davCredential struct {
	Access   string `json:"access_token"`
	Refresh  string `json:"refresh_token"`
	BaseHash string `json:"base_hash,omitempty"`
	WebToken string `json:"web_token,omitempty"`
}

func (g *guiApp) saveDAVCredential(a guiAccount, access, refresh string) error {
	g.mu.Lock()
	warn := false
	defer func() {
		g.mu.Unlock()
		if warn {
			g.event("凭据保存失败：新令牌仍在内存中使用，原文件已保留，每 30 秒重试保存；退出前请确认告警消失")
		}
	}()
	identity := davIdentity(a)
	if identity == "" {
		return errors.New("账号缺少稳定身份标识，未修改或持久化凭据")
	}
	baseHash := valueString(a.credentials["wopan_server_token_hash"])
	if baseHash == "" {
		baseHash = credentialHash(valueString(a.credentials["token"]))
	}
	for key, current := range g.accounts {
		if davIdentity(current) == identity {
			current = cloneGUIAccount(current)
			current.credentials["token"] = access
			current.credentials["refresh_token"] = refresh
			g.accounts[key] = current
		}
	}
	if g.pendingDAVCredentials == nil {
		g.pendingDAVCredentials = map[string]davCredential{}
	}
	g.pendingDAVCredentials[identity] = davCredential{Access: access, Refresh: refresh, BaseHash: baseHash}
	if err := g.persistDAVCredentialsLocked("", ""); err != nil {
		warn = g.credentialSaveError == ""
		g.credentialSaveError = "新令牌已在内存生效但尚未保存；每 30 秒重试，原凭据文件保留"
		g.scheduleCredentialRetryLocked()
		return &davCredentialSavePendingError{Err: fmt.Errorf("%s：%w", g.credentialSaveError, err)}
	}
	return nil
}
func (g *guiApp) applyDAVCredentials(accounts map[string]guiAccount) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.applyDAVCredentialsLocked(accounts)
}
func (g *guiApp) applyDAVCredentialsLocked(accounts map[string]guiAccount) {
	for k, a := range accounts {
		a.credentials["wopan_server_token"] = valueString(a.credentials["token"])
		a.credentials["wopan_server_refresh_token"] = valueString(a.credentials["refresh_token"])
		a.credentials["wopan_server_token_hash"] = credentialHash(valueString(a.credentials["token"]))
		accounts[k] = a
	}
	all, err := readDAVCredentials(filepath.Join(g.data, "webdav-credentials.bin"))
	if err != nil {
		all = map[string]davCredential{}
	}
	for identity, pending := range g.pendingDAVCredentials {
		c := all[identity]
		c.Access, c.Refresh, c.BaseHash = pending.Access, pending.Refresh, pending.BaseHash
		all[identity] = c
	}
	for k, a := range accounts {
		identity := davIdentity(a)
		if identity == "" {
			continue
		}
		c, ok := all[identity]
		if ok && c.WebToken != "" {
			a.credentials["wopan_web_token"] = c.WebToken
			a.WebBound = true
			accounts[k] = a
		}
		if ok && c.Access != "" && (c.BaseHash == "" || c.BaseHash == valueString(a.credentials["wopan_server_token_hash"]) || credentialHash(c.Access) == valueString(a.credentials["wopan_server_token_hash"])) {
			a.credentials["token"] = c.Access
			a.credentials["refresh_token"] = c.Refresh
			accounts[k] = a
		}
	}
}

func credentialHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (g *guiApp) saveDAVWebCredential(a guiAccount, token string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	identity := davIdentity(a)
	if identity == "" {
		return errors.New("账号缺少稳定身份标识，未保存官网授权")
	}
	if err := g.persistDAVCredentialsLocked(identity, token); err != nil {
		return err
	}
	for key, current := range g.accounts {
		if davIdentity(current) == identity {
			current = cloneGUIAccount(current)
			current.credentials["wopan_web_token"] = token
			current.WebBound = token != ""
			g.accounts[key] = current
		}
	}
	return nil
}

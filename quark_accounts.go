package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type quarkSavedAccounts struct {
	DeviceID   string             `json:"device_id"`
	DeviceName string             `json:"device_name"`
	Accounts   []quarkCredentials `json:"accounts"`
}
type quarkLoginGenerationKey struct{}

type quarkManager struct {
	commitMu             sync.Mutex
	mu                   sync.Mutex
	saveMu               sync.Mutex
	app                  *guiApp
	clients              map[string]*quarkClient
	deviceID, deviceName string
	loadError            string
	flow                 *quarkLoginFlow
	generation           uint64
	cancel               context.CancelFunc
	// Login endpoints are replaceable only by package tests.
	authBase       string
	validateClient func(quarkCredentials) *quarkClient
	writeVault     func(string, []byte) error
	phoneWindow    func(context.Context, string) (quarkPhoneWindow, error)
}

func newQuarkManager(g *guiApp) *quarkManager {
	m := &quarkManager{app: g, clients: map[string]*quarkClient{}, deviceID: randomKey()[:26], deviceName: "CloudProbe-Windows", authBase: "https://uop.quark.cn/cas/ajax"}
	raw, err := os.ReadFile(filepath.Join(g.data, "quark-accounts.bin"))
	if err == nil {
		var saved quarkSavedAccounts
		var plain []byte
		plain, err = unprotectLocal(raw)
		if err == nil {
			err = json.Unmarshal(plain, &saved)
		}
		if err == nil {
			if saved.DeviceID != "" {
				m.deviceID = saved.DeviceID
			}
			if saved.DeviceName != "" {
				m.deviceName = saved.DeviceName
			}
			for _, c := range saved.Accounts {
				if c.UID != "" {
					m.addClient(c)
				}
			}
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		m.loadError = "夸克资料读取或解密失败；原文件保留，未视为账号为空"
	}
	m.publishAccounts()
	return m
}
func quarkAccountKey(uid string) string { return "quark-" + davIdentityHash("quark", uid) }
func (m *quarkManager) makeClient(c quarkCredentials) *quarkClient {
	q := newQuarkClient(c, func(c quarkCredentials) error { return m.persist(c) })
	q.redact = m.app.redactDAV
	return q
}
func (m *quarkManager) addClient(c quarkCredentials) *quarkClient {
	q := m.makeClient(c)
	m.clients[quarkAccountKey(c.UID)] = q
	return q
}
func (m *quarkManager) rememberSecrets(c quarkCredentials) {
	m.app.mu.Lock()
	for _, value := range append([]string{c.AccessToken, c.RefreshToken}, mapValues(c.Cookies)...) {
		if value == "" {
			continue
		}
		exists := false
		for _, old := range m.app.extraSecrets {
			if old == value {
				exists = true
				break
			}
		}
		if !exists {
			m.app.extraSecrets = append(m.app.extraSecrets, value)
		}
	}
	m.app.mu.Unlock()
}
func (m *quarkManager) persist(update quarkCredentials) error {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.rememberSecrets(update)
	m.mu.Lock()
	if m.loadError != "" {
		m.mu.Unlock()
		return errors.New(m.loadError)
	}
	saved := quarkSavedAccounts{DeviceID: m.deviceID, DeviceName: m.deviceName}
	for _, q := range m.clients {
		c := q.snapshot()
		saved.Accounts = append(saved.Accounts, c)
	}
	m.mu.Unlock()
	return m.writeSaved(saved)
}

// Callers hold saveMu; lifecycle writers also hold commitMu until publication.
func (m *quarkManager) writeSaved(saved quarkSavedAccounts) error {
	sort.Slice(saved.Accounts, func(i, j int) bool { return saved.Accounts[i].UID < saved.Accounts[j].UID })
	data, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	encrypted, err := protectLocal(data)
	if err != nil {
		return err
	}
	writer := m.writeVault
	if writer == nil {
		writer = writePrivateBytes
	}
	return writer(filepath.Join(m.app.data, "quark-accounts.bin"), encrypted)
}
func (m *quarkManager) appendAccounts(accounts map[string]guiAccount, order []string) (map[string]guiAccount, []string) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.clients))
	clients := map[string]*quarkClient{}
	for key, q := range m.clients {
		keys = append(keys, key)
		clients[key] = q
	}
	m.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		c := clients[key].snapshot()
		channels := []string{}
		if len(c.Cookies) > 0 {
			channels = append(channels, "桌面 / 网页会话已保存")
		}
		if c.AccessToken != "" && c.OpenVerified {
			channels = append(channels, "开放平台授权已核验")
		}
		name := c.Name
		if name == "" {
			name = "独立账号"
		}
		accounts[key] = guiAccount{Key: key, Provider: "quark", Name: "夸克网盘 · " + maskAccount(name), Root: "0", Channels: channels, credentials: map[string]any{"user_id": c.UID, "quark_key": key}}
		order = append(order, key)
	}
	return accounts, order
}
func (m *quarkManager) publishAccounts() {
	if m == nil {
		return
	}
	native, nativeOrder := m.appendAccounts(map[string]guiAccount{}, nil)
	m.app.mu.Lock()
	defer m.app.mu.Unlock()
	accounts := map[string]guiAccount{}
	var order []string
	for _, key := range m.app.accountOrder {
		if a, ok := m.app.accounts[key]; ok && a.Provider != "quark" {
			accounts[key] = a
			order = append(order, key)
		}
	}
	for _, key := range nativeOrder {
		accounts[key] = native[key]
		order = append(order, key)
	}
	m.app.accounts, m.app.accountOrder = accounts, order
}
func (m *quarkManager) client(key string) (*quarkClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.clients[key]
	if q == nil {
		return nil, errors.New("夸克账号不存在，请重新登录")
	}
	return q, nil
}
func (m *quarkManager) accept(ctx context.Context, c quarkCredentials, target string) error {
	// Validate with the account service before storing any credentials. A
	// matching display name or phone suffix is never accepted as identity.
	q := newQuarkClient(c, nil)
	if m.validateClient != nil {
		q = m.validateClient(c)
	}
	var info map[string]any
	var err error
	if len(c.Cookies) > 0 {
		info, err = q.call(ctx, quarkDesktop, "GET", "/desktop/account/userinfo", nil, nil)
		if err != nil {
			info, err = q.call(ctx, quarkWeb, "GET", "/account/info", url.Values{"platform": {"pc"}}, nil)
		}
	} else {
		info, err = q.call(ctx, quarkOpen, "GET", "/open/v1/user/info", nil, nil)
	}
	if err != nil {
		return err
	}
	data, err := quarkObject(info["data"])
	if err != nil {
		return err
	}
	uid := quarkStr(data, "uid", "user_id")
	if uid == "" {
		return errors.New("夸克身份响应缺少稳定 uid/user_id，未保存授权")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c = q.snapshot()
	c.UID = uid
	c.Name = quarkStr(data, "nickname", "nick_name", "nickName", "mobile", "username")
	if len(c.Cookies) == 0 {
		c.OpenUID = uid
		c.OpenVerified = true
	}
	return m.commitCredential(ctx, c, target)
}
func (m *quarkManager) commitCredential(ctx context.Context, c quarkCredentials, target string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	generation, tagged := ctx.Value(quarkLoginGenerationKey{}).(uint64)
	if tagged && generation != m.generation {
		m.mu.Unlock()
		return context.Canceled
	}
	if m.loadError != "" {
		err := errors.New(m.loadError)
		m.mu.Unlock()
		return err
	}
	key := quarkAccountKey(c.UID)
	if target != "" {
		old := m.clients[target]
		if old == nil || old.snapshot().UID != c.UID {
			m.mu.Unlock()
			return errors.New("登录账号与所选夸克账号身份不同，未合并凭据")
		}
		key = target
	}
	if old := m.clients[key]; old != nil {
		previous := old.snapshot()
		if len(c.Cookies) > 0 {
			previous.Cookies = c.Cookies
			previous.Name = c.Name
			if c.DeviceID != "" {
				previous.DeviceID = c.DeviceID
			}
			c = previous
		} else {
			previous.AccessToken = c.AccessToken
			previous.RefreshToken = c.RefreshToken
			previous.OpenUID = c.OpenUID
			previous.OpenDeviceID = c.OpenDeviceID
			previous.OpenVerified = c.OpenVerified
			c = previous
		}
	}
	saved := quarkSavedAccounts{DeviceID: m.deviceID, DeviceName: m.deviceName, Accounts: []quarkCredentials{c}}
	for id, client := range m.clients {
		if id != key {
			saved.Accounts = append(saved.Accounts, client.snapshot())
		}
	}
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Cancel/replace is serialized with this commit. Failed vault writes do
	// not make credentials visible to other persistence callbacks or the UI.
	if err := m.writeSaved(saved); err != nil {
		return err
	}
	m.mu.Lock()
	m.clients[key] = m.makeClient(c)
	if tagged && m.flow != nil {
		m.flow.completed = true
		m.flow.stage = "夸克登录成功，授权已保存"
		m.flow.image = ""
		m.flow.authorizeURL = ""
	}
	m.mu.Unlock()
	m.rememberSecrets(c)
	m.publishAccounts()
	m.app.event("夸克账号授权已核验并加密保存")
	return nil
}
func (m *quarkManager) removeAccount(key string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.mu.Lock()
	if m.clients[key] == nil {
		m.mu.Unlock()
		return errors.New("夸克账号不存在")
	}
	if m.loadError != "" {
		err := errors.New(m.loadError)
		m.mu.Unlock()
		return err
	}
	saved := quarkSavedAccounts{DeviceID: m.deviceID, DeviceName: m.deviceName}
	for id, q := range m.clients {
		if id != key {
			saved.Accounts = append(saved.Accounts, q.snapshot())
		}
	}
	m.mu.Unlock()
	if err := m.writeSaved(saved); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.clients, key)
	m.mu.Unlock()
	m.publishAccounts()
	return nil
}
func (m *quarkManager) Status() map[string]any {
	if m == nil {
		return map[string]any{"running": false}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{"running": m.cancel != nil, "error": m.loadError, "stage": "尚未发起夸克登录"}
	if m.flow != nil {
		out["stage"] = m.flow.stage
		out["error"] = m.flow.err
		out["image"] = m.flow.image
		out["authorize_url"] = m.flow.authorizeURL
		out["method"] = m.flow.method
		out["completed"] = m.flow.completed
		out["expires"] = m.flow.expires.Unix()
	}
	return out
}
func (m *quarkManager) Stop() {
	if m == nil {
		return
	}
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.mu.Lock()
	c := m.cancel
	m.cancel = nil
	m.generation++
	if m.flow != nil {
		if !m.flow.completed {
			m.flow.stage = "登录已取消"
		}
		m.flow.image = ""
		m.flow.authorizeURL = ""
	}
	m.mu.Unlock()
	if c != nil {
		c()
	}
}
func (m *quarkManager) update(generation uint64, stage, err string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation == generation && m.flow != nil {
		m.flow.stage = stage
		m.flow.err = err
	}
}
func (g *guiApp) quarkAPI(w http.ResponseWriter, r *http.Request) {
	if g.quark == nil {
		g.fail(w, errors.New("夸克管理器未就绪"))
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var input struct {
		Method  string `json:"method"`
		Cookie  string `json:"cookie"`
		Account string `json:"account"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&input); err != nil {
		g.fail(w, errors.New("夸克请求 JSON 无效"))
		return
	}
	if err := ensureEOF(dec); err != nil {
		g.fail(w, err)
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/api/quark/")
	if action != "quota" && action != "cancel" {
		g.op.Lock()
		defer g.op.Unlock()
	}
	if action != "quota" && action != "cancel" && g.dav.Status()["running"] == true {
		g.fail(w, errors.New("请先停止 WebDAV，再添加、更新或移除夸克授权"))
		return
	}
	var err error
	switch action {
	case "start":
		err = g.quark.Start(input.Method, input.Account)
	case "cancel":
		g.quark.Stop()
	case "import":
		raw := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(input.Cookie), "Cookie:"))
		if strings.ContainsAny(raw, "\r\n") {
			err = errors.New("Cookie 不允许换行")
			break
		}
		req := &http.Request{Header: http.Header{"Cookie": {raw}}}
		cookies := map[string]string{}
		for _, c := range req.Cookies() {
			if c.Valid() != nil {
				err = errors.New("Cookie 无效")
				break
			}
			cookies[c.Name] = c.Value
		}
		if err != nil {
			break
		}
		if len(cookies) == 0 {
			err = errors.New("请输入夸克官网完整 Cookie")
			break
		}
		g.quark.mu.Lock()
		device, name := g.quark.deviceID, g.quark.deviceName
		g.quark.mu.Unlock()
		err = g.quark.accept(r.Context(), quarkCredentials{Cookies: cookies, DeviceID: device, DeviceName: name}, input.Account)
	case "remove":
		g.quark.Stop()
		err = g.quark.removeAccount(input.Account)
	case "quota":
		var q *quarkClient
		q, err = g.quark.client(input.Account)
		if err != nil {
			break
		}
		drive := newQuarkDrive(q, nil, 4, "auto")
		ctx, cancel := context.WithTimeout(newRunTrace(r.Context(), g.runLog, "界面容量", "夸克容量"), 12*time.Second)
		defer cancel()
		var capacity davQuota
		capacity, err = drive.Quota(ctx)
		if err == nil {
			respondJSON(w, 200, map[string]any{"total": capacity.Total, "used": capacity.Used, "available": capacity.available()})
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		g.fail(w, errors.New(g.redactDAV(err.Error())))
		return
	}
	respondJSON(w, 200, g.publicStatus())
}
func (m *quarkManager) loggedClients() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.clients) > 0
}
func (g *guiApp) quarkDrive(key string, t *transferRuntime, workers int, mode string) (*quarkDAVDrive, error) {
	if g.quark == nil {
		return nil, errors.New("夸克管理器未就绪")
	}
	q, err := g.quark.client(key)
	if err != nil {
		return nil, err
	}
	return newQuarkDrive(q, t, workers, mode), nil
}

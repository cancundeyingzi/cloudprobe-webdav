package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type savedCloudAccount struct {
	Key         string         `json:"key"`
	Provider    string         `json:"provider"`
	Name        string         `json:"name"`
	Root        string         `json:"root"`
	Credentials map[string]any `json:"credentials"`
}

func cloudPhone(s string) string {
	s = strings.TrimPrefix(strings.TrimSpace(s), "+86")
	if len(s) == 13 && strings.HasPrefix(s, "86") {
		s = s[2:]
	}
	if len(s) != 11 || s[0] != '1' {
		return ""
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return s
}

func sameCloudAccount(a, b guiAccount) bool {
	if a.Provider != b.Provider {
		return false
	}
	au, bu := valueString(a.credentials["user_id"]), valueString(b.credentials["user_id"])
	if au != "" && au == bu {
		return true
	}
	for _, ak := range []string{"user_id", "mobile", "username"} {
		phone := cloudPhone(valueString(a.credentials[ak]))
		if phone == "" {
			continue
		}
		for _, bk := range []string{"user_id", "mobile", "username"} {
			if phone == cloudPhone(valueString(b.credentials[bk])) {
				return true
			}
		}
	}
	return false
}

func cloudAccountChannels(a guiAccount) guiAccount {
	if a.Provider == "quark" {
		return a
	}
	if raw := valueString(a.credentials["wopan_web_token"]); raw != "" {
		if token, err := normalizeWopanWebToken(raw); err == nil && token != raw {
			a = cloneGUIAccount(a)
			a.credentials["wopan_web_token"] = token
		}
	}
	a.Channels = nil
	if valueString(a.credentials["token"]) != "" {
		a.Channels = append(a.Channels, "绑定授权直连 / 网易代理")
	}
	a.WebBound = valueString(a.credentials["wopan_web_token"]) != "" || valueString(a.credentials["mobile_web_token"]) != ""
	if a.WebBound {
		a.Channels = append(a.Channels, "官网授权")
	}
	return a
}

// Preserve the existing key and identity so saved mounts and recovery records
// continue to refer to the same account when another channel is added.
func mergeCloudAccount(accounts map[string]guiAccount, incoming guiAccount) guiAccount {
	for key, old := range accounts {
		if !sameCloudAccount(old, incoming) {
			continue
		}
		merged := cloneGUIAccount(old)
		merged.credentials["account_identity"] = davIdentity(old)
		for field, v := range incoming.credentials {
			if field == "wopan_web_token" && valueString(old.credentials["wopan_web_managed"]) == "1" && valueString(incoming.credentials["wopan_web_managed"]) != "1" {
				continue
			}
			if (field == "user_id" || field == "account_identity") && valueString(merged.credentials[field]) != "" {
				continue
			}
			if valueString(v) != "" {
				merged.credentials[field] = v
			}
		}
		merged.Key = key
		if incoming.Name != "" {
			merged.Name = incoming.Name
		}
		// The new mobile web API uses a distinct ID namespace. Store both roots.
		if valueString(merged.credentials["mobile_web_token"]) != "" {
			merged.Root = "/"
		}
		merged = cloudAccountChannels(merged)
		accounts[key] = merged
		return merged
	}
	if incoming.Key == "" {
		incoming.Key = incoming.Provider + "-" + davIdentity(incoming)
	}
	incoming = cloudAccountChannels(incoming)
	accounts[incoming.Key] = incoming
	return incoming
}

func (g *guiApp) writeCloudAccountsLocked(accounts map[string]guiAccount) error {
	if g.cloudAccountError != "" {
		return errors.New(g.cloudAccountError)
	}
	var saved []savedCloudAccount
	for _, a := range accounts {
		if a.Provider != "mobile" && a.Provider != "wopan" {
			continue
		}
		saved = append(saved, savedCloudAccount{a.Key, a.Provider, a.Name, a.Root, a.credentials})
	}
	sort.Slice(saved, func(i, j int) bool { return saved[i].Key < saved[j].Key })
	raw, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	raw, err = protectLocal(raw)
	if err != nil {
		return err
	}
	return writePrivateBytes(filepath.Join(g.data, "cloud-accounts.bin"), raw)
}

func (g *guiApp) restoreCloudAccounts() {
	raw, err := os.ReadFile(filepath.Join(g.data, "cloud-accounts.bin"))
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	var saved []savedCloudAccount
	if err == nil {
		raw, err = unprotectLocal(raw)
	}
	if err == nil {
		err = json.Unmarshal(raw, &saved)
	}
	accounts := map[string]guiAccount{}
	if err == nil {
		for _, s := range saved {
			a := guiAccount{Key: s.Key, Provider: s.Provider, Name: s.Name, Root: s.Root, credentials: s.Credentials}
			if a.Key == "" || davIdentity(a) == "" || (a.Provider != "wopan" && a.Provider != "mobile") || accounts[a.Key].Key != "" {
				err = errors.New("账号记录无效或重复")
				break
			}
			accounts[a.Key] = cloudAccountChannels(a)
		}
	}
	g.mu.Lock()
	if err != nil {
		g.cloudAccountError = "云盘账号资料读取失败，原文件已保留：" + err.Error()
		g.mu.Unlock()
		return
	}
	g.applyDAVCredentialsLocked(accounts)
	for _, a := range accounts {
		mergeCloudAccount(g.accounts, a)
	}
	g.rebuildAccountOrderLocked()
	g.mu.Unlock()
}

func (g *guiApp) rebuildAccountOrderLocked() {
	seen := map[string]bool{}
	order := make([]string, 0, len(g.accounts))
	for _, key := range g.accountOrder {
		if _, ok := g.accounts[key]; ok && !seen[key] {
			order = append(order, key)
			seen[key] = true
		}
	}
	var added []string
	for key := range g.accounts {
		if !seen[key] {
			added = append(added, key)
		}
	}
	sort.Strings(added)
	g.accountOrder = append(order, added...)
}

func (g *guiApp) acceptCloudAccount(ctx context.Context, incoming guiAccount) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if davIdentity(incoming) == "" {
		return errors.New("官网未返回可验证的账号身份，未保存登录")
	}
	accounts := make(map[string]guiAccount, len(g.accounts)+1)
	for key, a := range g.accounts {
		accounts[key] = cloneGUIAccount(a)
	}
	a := mergeCloudAccount(accounts, incoming)
	if err := g.writeCloudAccountsLocked(accounts); err != nil {
		return fmt.Errorf("保存云盘账号失败：%w", err)
	}
	g.accounts = accounts
	g.rebuildAccountOrderLocked()
	for k, v := range a.credentials {
		if sensitive(k) {
			g.extraSecrets = append(g.extraSecrets, valueString(v))
		}
	}
	return nil
}

func (g *guiApp) clearCloudWebChannel(a guiAccount) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	accounts := make(map[string]guiAccount, len(g.accounts))
	for k, current := range g.accounts {
		accounts[k] = cloneGUIAccount(current)
	}
	current, ok := accounts[a.Key]
	if !ok {
		return errors.New("账号已变化，请刷新")
	}
	current.credentials["wopan_web_token"] = ""
	current.credentials["wopan_web_managed"] = "1"
	if valueString(current.credentials["token"]) == "" {
		delete(accounts, a.Key)
	} else {
		accounts[a.Key] = cloudAccountChannels(current)
	}
	if err := g.writeCloudAccountsLocked(accounts); err != nil {
		return err
	}
	// Remove legacy authorization too; a failed write remains visible and can
	// be retried instead of silently reappearing on the next binding refresh.
	if err := g.persistDAVCredentialsLocked(davIdentity(a), ""); err != nil {
		return err
	}
	g.accounts = accounts
	g.rebuildAccountOrderLocked()
	return nil
}

func (g *guiApp) cloudAccountClient(a guiAccount, folder string, identity MobileConfig, bridge *OriginalBridge, logged bool) (*Client, error) {
	c := defaultConfig(a.Provider)
	c.IncludeDirectories = true
	c.TimeoutSeconds = 40
	c.Transport = "bridge"
	c.BaseURL = "https://api.filmly.netease.com"
	c.Mobile = identity
	for _, field := range []struct {
		key   string
		value *string
	}{{"mobile_app_id", &c.Mobile.AppID}, {"mobile_app_secret", &c.Mobile.AppSecret}, {"m139_device_id", &c.Mobile.DeviceID}, {"token", &c.Mobile.AccessToken}} {
		if v := valueString(a.credentials[field.key]); v != "" {
			*field.value = v
		}
	}
	c.Wopan.AccessToken = valueString(a.credentials["token"])
	c.Wopan.UserID = valueString(a.credentials["user_id"])
	cl, err := newClient(c, folder)
	if err != nil {
		return nil, err
	}
	if logged && bridge != nil && bridge.Alive() && valueString(a.credentials["token"]) != "" {
		cl.bridge = bridge
	}
	if a.Provider == "mobile" && valueString(a.credentials["mobile_web_token"]) == "" && (c.Mobile.AppID == "" || c.Mobile.AppSecret == "") {
		return nil, errors.New("移动开放授权缺少应用身份，请使用移动官网登录或重新读取绑定渠道")
	}
	return cl, nil
}

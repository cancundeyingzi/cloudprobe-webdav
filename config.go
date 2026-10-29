package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Config struct {
	IncludeDirectories bool              `json:"include_directories,omitempty"`
	Provider           string            `json:"provider"`
	Transport          string            `json:"transport"`
	BaseURL            string            `json:"base_url"`
	Mobile             MobileConfig      `json:"mobile"`
	Wopan              WopanConfig       `json:"wopan"`
	Netease            NeteaseConfig     `json:"netease"`
	Headers            map[string]string `json:"headers,omitempty"`
	TimeoutSeconds     int               `json:"timeout_seconds"`
	PageSize           int               `json:"page_size"`
	MaxPages           int               `json:"max_pages"`
	MaxResponseBytes   int64             `json:"max_response_bytes"`
	DownloadMaxBytes   int64             `json:"download_max_bytes"`
}
type MobileConfig struct {
	AppID       string `json:"app_id"`
	AppSecret   string `json:"app_secret"`
	AccessToken string `json:"access_token"`
	DeviceID    string `json:"device_id"`
	ChannelID   string `json:"channel_id"`
}
type WopanConfig struct {
	AccessToken string `json:"access_token"`
	SpaceType   int    `json:"space_type"`
	UserID      string `json:"user_id,omitempty"`
}
type NeteaseConfig struct {
	PathPrefix     string `json:"path_prefix"`
	Token          string `json:"token"`
	SID            string `json:"sid,omitempty"`
	NativeDLL      string `json:"native_dll,omitempty"`
	RawKeyBase64   string `json:"raw_key_base64,omitempty"`
	DeviceID       string `json:"device_id"`
	SecTag         string `json:"sec_tag,omitempty"`
	AppVersion     string `json:"app_version"`
	AppVersionCode string `json:"app_version_code"`
}

func defaultConfig(provider string) Config {
	c := Config{Provider: provider, Transport: "direct", BaseURL: "https://miniapp.yun.139.com", TimeoutSeconds: 25, PageSize: 200, MaxPages: 100, MaxResponseBytes: 8 << 20, DownloadMaxBytes: 10 << 20}
	c.Mobile.ChannelID = "10010"
	c.Netease.AppVersion = "1.2.1"
	c.Netease.PathPrefix = "/a"
	c.Netease.AppVersionCode = "557"
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	c.Netease.DeviceID = "cloudprobe-" + hex.EncodeToString(b)
	if provider == "wopan" {
		c.Transport = "netease"
		c.BaseURL = "https://api.filmly.netease.com"
		c.PageSize = 100
	}
	return c
}

var envVariable = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandValue(s string) (string, error) {
	var missing string
	v := envVariable.ReplaceAllStringFunc(s, func(m string) string {
		n := envVariable.FindStringSubmatch(m)[1]
		v, ok := os.LookupEnv(n)
		if !ok || v == "" {
			missing = n
		}
		return v
	})
	if missing != "" {
		return "", fmt.Errorf("环境变量 %s 未设置", missing)
	}
	return v, nil
}
func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("读取配置失败：%w", err)
	}
	var c Config
	dec := json.NewDecoder(strings.NewReader(strings.TrimPrefix(string(data), "\ufeff")))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&c); err != nil {
		return c, fmt.Errorf("配置 JSON 无效：%w", err)
	}
	if err = ensureEOF(dec); err != nil {
		return c, err
	}
	for _, p := range []*string{&c.BaseURL, &c.Mobile.AppID, &c.Mobile.AppSecret, &c.Mobile.AccessToken, &c.Mobile.DeviceID, &c.Wopan.AccessToken, &c.Netease.Token, &c.Netease.SID, &c.Netease.NativeDLL, &c.Netease.RawKeyBase64, &c.Netease.DeviceID, &c.Netease.SecTag} {
		*p, err = expandValue(*p)
		if err != nil {
			return c, err
		}
	}
	for k, v := range c.Headers {
		c.Headers[k], err = expandValue(v)
		if err != nil {
			return c, err
		}
	}
	if c.Netease.NativeDLL != "" && !filepath.IsAbs(c.Netease.NativeDLL) {
		c.Netease.NativeDLL = filepath.Join(filepath.Dir(path), c.Netease.NativeDLL)
	}
	d := defaultConfig(c.Provider)
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = d.TimeoutSeconds
	}
	if c.PageSize == 0 {
		c.PageSize = d.PageSize
	}
	if c.MaxPages == 0 {
		c.MaxPages = d.MaxPages
	}
	if c.MaxResponseBytes == 0 {
		c.MaxResponseBytes = d.MaxResponseBytes
	}
	if c.DownloadMaxBytes == 0 {
		c.DownloadMaxBytes = d.DownloadMaxBytes
	}
	if c.Mobile.ChannelID == "" {
		c.Mobile.ChannelID = d.Mobile.ChannelID
	}
	if c.Netease.AppVersion == "" {
		c.Netease.AppVersion = d.Netease.AppVersion
	}
	if c.Netease.PathPrefix == "" {
		c.Netease.PathPrefix = d.Netease.PathPrefix
	}
	if c.Netease.AppVersionCode == "" {
		c.Netease.AppVersionCode = d.Netease.AppVersionCode
	}
	return c, c.validate()
}

func (c Config) credentialsReady() error {
	var missing []string
	if c.Provider == "mobile" {
		for k, v := range map[string]string{"mobile.app_id": c.Mobile.AppID, "mobile.app_secret": c.Mobile.AppSecret, "mobile.access_token": c.Mobile.AccessToken, "mobile.device_id": c.Mobile.DeviceID} {
			if v == "" {
				missing = append(missing, k)
			}
		}
	} else if c.Wopan.AccessToken == "" {
		missing = append(missing, "wopan.access_token")
	}
	if c.Transport == "netease" && c.Netease.Token == "" {
		missing = append(missing, "netease.token（爆米花已登录账号令牌）")
	}
	if len(missing) > 0 {
		return fmt.Errorf("缺少授权配置：%s", strings.Join(missing, "、"))
	}
	return nil
}
func (c Config) validate() error {
	if c.Provider != "mobile" && c.Provider != "wopan" {
		return errors.New("provider 必须是 mobile 或 wopan")
	}
	if c.Transport != "direct" && c.Transport != "netease" && c.Transport != "gateway" && c.Transport != "bridge" {
		return errors.New("transport 必须是 direct、netease、gateway 或 bridge")
	}
	if c.Provider == "wopan" && c.Transport == "direct" {
		return errors.New("联通真实上游协议尚未确认：请选 netease，或配置自行掌握的 gateway")
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("base_url 必须是无账号、查询参数和片段的 HTTP(S) 基地址")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return errors.New("实网 base_url 必须使用 HTTPS；仅本机演示支持 HTTP")
		}
	}
	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 600 || c.PageSize < 1 || c.PageSize > 200 || c.MaxPages < 1 || c.MaxPages > 10000 {
		return errors.New("超时需 1—600 秒，每页 1—200 条，最多页数 1—10000")
	}
	if c.MaxResponseBytes < 1024 || c.MaxResponseBytes > 128<<20 || c.DownloadMaxBytes < 1 {
		return errors.New("响应上限需 1 KiB—128 MiB，下载上限必须大于零")
	}
	if c.Transport == "netease" && c.Netease.DeviceID == "" {
		return errors.New("netease.device_id 不能为空，请使用 init 生成稳定的测试设备标识")
	}
	for k, v := range c.Headers {
		if strings.ContainsAny(k+v, "\r\n") {
			return errors.New("headers 不允许包含换行")
		}
	}
	return nil
}

package main

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/webdav"
)

func prepareDAVSettings(in *davStartInput, previous davSavedConfig) (*davSavedConfig, string, error) {
	if in.Listen == "" {
		in.Listen = "127.0.0.1"
	}
	if in.Listen != "127.0.0.1" && in.Listen != "0.0.0.0" {
		return nil, "", errors.New("监听地址请选择 127.0.0.1 或 0.0.0.0")
	}
	if in.Port < 1 || in.Port > 65535 {
		return nil, "", errors.New("端口需为 1—65535")
	}
	if in.Username == "" {
		in.Username = "cloudprobe"
	}
	if len(in.Username) > 100 || strings.ContainsAny(in.Username, ":\r\n\x00") {
		return nil, "", errors.New("WebDAV 用户名无效")
	}
	if in.MaxUploadMiB == 0 {
		in.MaxUploadMiB = 10240
	}
	if in.MaxUploadMiB < 1 || in.MaxUploadMiB > 1048576 {
		return nil, "", errors.New("上传上限需为 1—1048576 MiB")
	}
	if in.UploadConcurrency < 0 || in.UploadConcurrency > 16 {
		return nil, "", errors.New("每文件上传并发需为 1—16，留空默认 4")
	}
	if in.UploadMode != "" && in.UploadMode != "auto" && in.UploadMode != "http1" && in.UploadMode != "http2" {
		return nil, "", errors.New("上传传输模式请选择 HTTP/1.1 独立连接或 HTTP/2 共享连接对照")
	}
	cfg := davSavedConfig{Listen: in.Listen, Port: in.Port, Username: in.Username, MaxUploadMiB: in.MaxUploadMiB, UploadConcurrency: uploadConcurrency(in.UploadConcurrency), UploadMode: uploadMode(in.UploadMode), PasswordHash: previous.PasswordHash, Salt: previous.Salt, GeneratedPassword: previous.GeneratedPassword}
	cfg.transferOptions = in.transferOptions.defaults()
	if err := cfg.transferOptions.validate(); err != nil {
		return nil, "", err
	}
	generated := ""
	if in.Password == "" && cfg.PasswordHash == "" {
		in.Password = randomKey()[:24]
		generated = in.Password
	}
	if in.Password != "" {
		if len(in.Password) < 12 || len(in.Password) > 512 {
			return nil, "", errors.New("密码长度需为 12—512 个字节；留空可保留已设密码或首次自动生成")
		}
		salt := make([]byte, 16)
		if _, e := rand.Read(salt); e != nil {
			return nil, "", e
		}
		h, _ := pbkdf2.Key(sha256.New, in.Password, salt, 100000, 32)
		cfg.Salt = hex.EncodeToString(salt)
		cfg.PasswordHash = hex.EncodeToString(h)
		cfg.GeneratedPassword = ""
		if generated != "" {
			protected, err := protectLocal([]byte(generated))
			if err != nil {
				return nil, "", err
			}
			cfg.GeneratedPassword = base64.StdEncoding.EncodeToString(protected)
		}
	}
	return &cfg, generated, nil
}

func configureDAVMounts(in *davStartInput, cfg *davSavedConfig, previous davSavedConfig, accounts map[string]guiAccount) error {
	cfg.KnownAccounts = append([]string{}, previous.KnownAccounts...)
	if !in.restore || previous.KnownAccounts == nil && len(previous.Mounts) > 0 {
		for _, a := range accounts {
			cfg.KnownAccounts = appendUnique(cfg.KnownAccounts, davIdentity(a))
		}
	}
	if in.restore {
		cfg.Mounts = append([]davSavedMount{}, previous.Mounts...)
	}
	if !in.restore {
		for _, saved := range previous.Mounts {
			present := false
			for _, a := range accounts {
				if davIdentity(a) == saved.Identity {
					present = true
					break
				}
			}
			if !present {
				cfg.Mounts = append(cfg.Mounts, saved)
			}
		}
	}
	aliases := map[string]bool{}
	for i := range in.Mounts {
		sel := &in.Mounts[i]
		a, ok := accounts[sel.Account]
		if !ok || davIdentity(a) == "" {
			return errors.New("所选账号缺少稳定身份，请刷新账号列表")
		}
		if e := safeFilename(sel.Alias); e != nil || aliases[sel.Alias] {
			return errors.New("挂载名必须是唯一的普通文件夹名称")
		}
		aliases[sel.Alias] = true
		if sel.Root == "" {
			sel.Root = a.Root
		}
		if (a.Provider == "wopan" || a.Provider == "quark") && sel.Root == "root" {
			sel.Root = "0"
		}
		if strings.ContainsAny(sel.Root, "\r\n\x00") {
			return errors.New("根目录 ID 无效")
		}
		if sel.UploadStorageMode != "" && sel.UploadStorageMode != "memory" && sel.UploadStorageMode != "disk" && sel.UploadStorageMode != "auto" {
			return errors.New("挂载上传缓存方式无效")
		}
	}
	if err := validateDAVMountResources(in.Mounts, accounts); err != nil {
		return err
	}
	for _, sel := range in.Mounts {
		a := accounts[sel.Account]
		saved := savedMountInput(sel, a)
		if !in.restore {
			cfg.Mounts = append(cfg.Mounts, saved)
		}
	}
	return nil
}

func (d *davManager) startDAVListener(in davStartInput, cfg davSavedConfig) error {
	g := d.app
	transfers := newTransferRuntime(cfg.transferOptions)
	var mounts []*davMount
	loads := map[string]davLoadState{}
	listener, e := net.Listen("tcp", net.JoinHostPort(in.Listen, strconv.Itoa(in.Port)))
	if e != nil {
		return e
	}
	fs := &davFS{mounts: mounts, spool: filepath.Join(g.data, "webdav-spool"), maxUpload: in.MaxUploadMiB << 20, transfers: transfers}
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
		return e
	}
	d.mu.Lock()
	d.config = cfg
	d.fs = fs
	d.serviceContext = serviceCtx
	d.mountInputs = map[string]davMountInput{}
	for _, sel := range in.Mounts {
		d.mountInputs[sel.Alias] = sel
	}
	d.loads = loads
	d.jobs = map[string]davLoadState{}
	d.attempted = map[string]guiAccount{}
	d.retryAt = map[string]time.Time{}
	d.retryDelay = map[string]time.Duration{}
	d.generatedPassword = ""
	if raw, err := base64.StdEncoding.DecodeString(cfg.GeneratedPassword); err == nil && len(raw) > 0 {
		if plain, err := unprotectLocal(raw); err == nil {
			d.generatedPassword = string(plain)
		}
	}
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
	go d.watchMounts(serviceCtx)
	g.event(fmt.Sprintf("WebDAV 已启动，挂载 %d 个云盘", len(mounts)))
	return nil
}

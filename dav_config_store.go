package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func readDAVConfig(file string) (davSavedConfig, error) {
	var cfg davSavedConfig
	f, err := os.Open(file)
	if errors.Is(err, os.ErrNotExist) {
		cfg.TransferScheduling = "client"
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return cfg, err
	}
	if len(b) > 1<<20 {
		return cfg, errors.New("WebDAV 配置超过 1 MiB")
	}
	var shape map[string]json.RawMessage
	if err = json.Unmarshal(b, &shape); err != nil || shape == nil {
		return cfg, errors.New("WebDAV 配置必须是有效 JSON 对象")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	if err = dec.Decode(&cfg); err != nil {
		return davSavedConfig{}, err
	}
	return cfg, nil
}

func (d *davManager) loadConfig() error {
	cfg, err := readDAVConfig(filepath.Join(d.app.data, "webdav.json"))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		d.configError = "WebDAV 配置读取失败，原文件已保留；请重新读取修复后的配置，或先备份再重置：" + err.Error()
		return errors.New(d.configError)
	}
	d.config = cfg
	d.configError = ""
	return nil
}

func (d *davManager) recoverConfig(ctx context.Context, reset bool) (string, error) {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	d.mu.Lock()
	running := d.server != nil
	failed := d.configError != ""
	d.mu.Unlock()
	if running {
		return "", errors.New("请先停止 WebDAV 后恢复配置")
	}
	lease, err := lockFileWait(ctx, filepath.Join(d.app.data, "webdav-config.lock"))
	if err != nil {
		return "", err
	}
	defer lease.Close()
	backup := ""
	if reset {
		if !failed {
			return "", errors.New("配置没有读取错误，无需重置")
		}
		file := filepath.Join(d.app.data, "webdav.json")
		info, e := os.Lstat(file)
		if e != nil && !os.IsNotExist(e) {
			return "", e
		}
		if e == nil {
			if !info.Mode().IsRegular() {
				return "", errors.New("配置路径不是普通文件，请手动核验；未移动或删除")
			}
			backup = fmt.Sprintf("webdav-recovery-%s-%s.json", time.Now().Format("20060102-150405"), randomKey()[:8])
			if err = os.Rename(file, filepath.Join(d.app.data, backup)); err != nil {
				return "", err
			}
		}
	}
	return backup, d.loadConfig()
}

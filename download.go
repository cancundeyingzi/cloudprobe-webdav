package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func validDownloadURL(s string) error {
	u, e := url.Parse(s)
	if e != nil || u.Host == "" || u.User != nil {
		return errors.New("下载 URL 无效")
	}
	if u.Scheme == "https" {
		return nil
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && (u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())) {
		return nil
	}
	return errors.New("下载必须使用 HTTPS（本机演示除外）")
}
func safeFilename(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\:*?\"<>|") || strings.TrimRight(name, " .") != name {
		return errors.New("-name 必须是普通文件名，不能含路径或 Windows 非法字符")
	}
	for _, r := range name {
		if r < 32 {
			return errors.New("文件名不能含控制字符")
		}
	}
	stem := strings.ToUpper(strings.Split(name, ".")[0])
	if stem == "CON" || stem == "PRN" || stem == "AUX" || stem == "NUL" || (len(stem) == 4 && (strings.HasPrefix(stem, "COM") || strings.HasPrefix(stem, "LPT")) && stem[3] >= '1' && stem[3] <= '9') {
		return errors.New("文件名是 Windows 保留设备名")
	}
	return nil
}

type DownloadReceipt struct {
	Mode        string `json:"mode"`
	ID          string `json:"id"`
	FID         string `json:"fid,omitempty"`
	File        string `json:"file"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	URL         string `json:"url_redacted"`
	ContentType string `json:"content_type"`
}

func downloadTo(ctx context.Context, targetURL, path string, maxBytes int64, timeout time.Duration) (n int64, sum, contentType string, err error) {
	started := time.Now()
	host := nodeHost(targetURL)
	defer func() {
		e := runLogEntry{Operation: "下载正文", Source: "云盘节点", Endpoint: host, Result: "成功", Bytes: n, DurationMs: time.Since(started).Milliseconds(), BytesPerSecond: rateFor(n, time.Since(started))}
		if err != nil {
			e.Result, e.Error = "失败", err.Error()
		}
		logDAV(ctx, e)
	}()
	if e := validDownloadURL(targetURL); e != nil {
		return 0, "", "", e
	}
	hc := &http.Client{Timeout: timeout, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) > 10 {
			return errors.New("下载重定向过多")
		}
		return validDownloadURL(req.URL.String())
	}}
	req, e := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if e != nil {
		return 0, "", "", e
	}
	req.Header.Set("User-Agent", "CloudProbe/"+version)
	resp, e := hc.Do(req)
	if e != nil {
		return 0, "", "", e
	}
	defer resp.Body.Close()
	host = resp.Request.URL.Host
	if resp.StatusCode != http.StatusOK {
		return 0, "", "", fmt.Errorf("下载 HTTP 状态 %d", resp.StatusCode)
	}
	if resp.ContentLength > maxBytes {
		return 0, "", "", fmt.Errorf("文件声明长度 %d 超过上限 %d", resp.ContentLength, maxBytes)
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return 0, "", "", e
	}
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	h := sha256.New()
	n, e = io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, maxBytes+1))
	if e != nil {
		return n, "", "", e
	}
	if n > maxBytes {
		return n, "", "", fmt.Errorf("实际内容超过下载上限 %d 字节，已移除本次不完整文件", maxBytes)
	}
	if e = f.Sync(); e != nil {
		return n, "", "", e
	}
	if e = f.Close(); e != nil {
		return n, "", "", e
	}
	success = true
	return n, hex.EncodeToString(h.Sum(nil)), resp.Header.Get("Content-Type"), nil
}
func (c *Client) Fetch(ctx context.Context, id, fid, name, mode string) (DownloadReceipt, error) {
	r := DownloadReceipt{Mode: mode, ID: id, FID: fid, File: name}
	if e := safeFilename(name); e != nil {
		return r, e
	}
	started := time.Now()
	u, e := c.downloadURL(ctx, id, fid)
	if e == nil {
		e = validDownloadURL(u)
	}
	logURL(ctx, "网易代理", "获取下载地址", started, u, e)
	if e != nil {
		return r, stage("获取下载地址", e)
	}
	r.URL = c.redact.Text(u)
	r.Bytes, r.SHA256, r.ContentType, e = downloadTo(ctx, u, filepath.Join(c.folder, name), c.cfg.DownloadMaxBytes, time.Duration(c.cfg.TimeoutSeconds)*time.Second)
	return r, stage("下载指定文件", e)
}

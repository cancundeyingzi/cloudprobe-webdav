package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/websocket"
)

// A launcher can hand the window to another process. Only the dedicated
// browser endpoint and its page targets establish the lifetime of a login.
func openOfficialBrowser(ctx context.Context, data, provider, address string) (*quarkPhoneBrowser, error) {
	browser, err := findLoginBrowser()
	if err != nil {
		return nil, err
	}
	profile, cleanup, err := newBrowserProfile(data, provider)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = cleanup()
		}
	}()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	command := exec.Command(browser, "--user-data-dir="+profile, "--remote-debugging-port="+strconv.Itoa(port), "--remote-debugging-address=127.0.0.1", "--remote-allow-origins=http://127.0.0.1", "--no-first-run", "--no-default-browser-check", "--disable-sync", "--new-window", address)
	// The user explicitly requested this visible, interactive login window.
	if err = command.Start(); err != nil {
		return nil, errors.New("无法打开官网登录窗口，请检查 Edge / Chrome 是否可用")
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait(); close(exited) }()
	ready, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/json/version", port)
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	for {
		ws, e := officialBrowserEndpoint(ready, client, endpoint, port)
		if e == nil {
			cfg, _ := websocket.NewConfig(ws, "http://127.0.0.1")
			cfg.Dialer = &net.Dialer{Timeout: time.Second}
			connection, e := websocket.DialConfig(cfg)
			if e == nil {
				transferred = true
				return &quarkPhoneBrowser{rpc: newQuarkBrowserRPC(ctx, connection), command: command, exited: exited, cleanup: cleanup}, nil
			}
		}
		if e = davWait(ready, 100*time.Millisecond); e != nil {
			_ = command.Process.Kill()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("官网登录窗口连接超时；窗口可能仍在，请关闭本次专用窗口后重试，或改用另一浏览器")
		}
	}
}

func officialBrowserEndpoint(ctx context.Context, client *http.Client, endpoint string, port int) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var v struct {
		WebSocket string `json:"webSocketDebuggerUrl"`
	}
	if resp.StatusCode != 200 {
		return "", errors.New("浏览器尚未就绪")
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&v); err != nil {
		return "", err
	}
	u, err := url.Parse(v.WebSocket)
	if err != nil || u.Scheme != "ws" || u.Host != net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) || u.User != nil || !strings.HasPrefix(u.Path, "/devtools/browser/") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("浏览器返回了无效的本机连接地址")
	}
	return v.WebSocket, nil
}

func (b *quarkPhoneBrowser) cookies(ctx context.Context) ([]quarkBrowserCookie, error) {
	raw, err := b.rpc.call(ctx, "Storage.getCookies", nil, "")
	if err != nil {
		return nil, err
	}
	var v struct {
		Cookies []quarkBrowserCookie `json:"cookies"`
	}
	if err = json.Unmarshal(raw, &v); err != nil {
		return nil, errors.New("官网登录窗口返回了无效会话")
	}
	return v.Cookies, nil
}

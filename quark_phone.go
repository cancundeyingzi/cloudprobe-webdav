package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type quarkPhoneSnapshot struct {
	Cookies map[string]string
}
type quarkPhoneWindow interface {
	Poll(context.Context) (quarkPhoneSnapshot, error)
	Close()
}

type quarkPhoneBrowser struct {
	rpc          *quarkBrowserRPC
	command      *exec.Cmd
	exited       <-chan error
	cleanup      func() error
	cleanupError error
}
type quarkBrowserRPC struct {
	connection *websocket.Conn
	seq        int64
	stopWatch  chan struct{}
	watchDone  chan struct{}
	closeOnce  sync.Once
	onEvent    func(string, json.RawMessage)
}

func newQuarkBrowserRPC(ctx context.Context, connection *websocket.Conn) *quarkBrowserRPC {
	rpc := &quarkBrowserRPC{connection: connection, stopWatch: make(chan struct{}), watchDone: make(chan struct{})}
	go func() {
		defer close(rpc.watchDone)
		select {
		case <-ctx.Done():
			connection.SetDeadline(time.Now())
		case <-rpc.stopWatch:
		}
	}()
	return rpc
}
func (c *quarkBrowserRPC) call(ctx context.Context, method string, params any, session string) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.seq++
	id := c.seq
	request := map[string]any{"id": id, "method": method}
	if params != nil {
		request["params"] = params
	}
	if session != "" {
		request["sessionId"] = session
	}
	deadline := time.Now().Add(3 * time.Second)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	c.connection.SetDeadline(deadline)
	if err := websocket.JSON.Send(c.connection, request); err != nil {
		return nil, errors.New("官网登录窗口通信失败，请重新打开窗口")
	}
	for {
		var reply struct {
			ID     int64           `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := websocket.JSON.Receive(c.connection, &reply); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("官网登录窗口已关闭或未响应，请重新打开")
		}
		if reply.ID != id {
			if c.onEvent != nil && reply.Method != "" {
				c.onEvent(reply.Method, reply.Params)
			}
			continue
		}
		if reply.Error != nil {
			return nil, fmt.Errorf("官网登录窗口命令未完成（%s，%d）", method, reply.Error.Code)
		}
		return reply.Result, nil
	}
}
func (c *quarkBrowserRPC) Close() {
	c.closeOnce.Do(func() {
		close(c.stopWatch)
		<-c.watchDone
		c.connection.SetDeadline(time.Now().Add(500 * time.Millisecond))
		_ = websocket.JSON.Send(c.connection, map[string]any{"id": c.seq + 1, "method": "Browser.close"})
		c.connection.Close()
	})
}
func openQuarkPhoneWindow(ctx context.Context, data string) (quarkPhoneWindow, error) {
	return openOfficialBrowser(ctx, data, "quark", "https://pan.quark.cn/")
}

func (b *quarkPhoneBrowser) Poll(ctx context.Context) (quarkPhoneSnapshot, error) {
	cookies, err := b.cookies(ctx)
	if err != nil {
		return quarkPhoneSnapshot{}, err
	}
	return quarkPhoneSnapshot{Cookies: quarkPhoneCookies(cookies, time.Now())}, nil
}

type quarkBrowserCookie struct {
	Name    string  `json:"name"`
	Value   string  `json:"value"`
	Domain  string  `json:"domain"`
	Expires float64 `json:"expires"`
}

func quarkPhoneCookies(cookies []quarkBrowserCookie, now time.Time) map[string]string {
	out := map[string]string{}
	for _, c := range cookies {
		domain := strings.ToLower(strings.TrimPrefix(c.Domain, "."))
		if domain != "quark.cn" && !strings.HasSuffix(domain, ".quark.cn") {
			continue
		}
		if c.Expires > 0 && c.Expires <= float64(now.Unix()) {
			continue
		}
		cookie := http.Cookie{Name: c.Name, Value: c.Value}
		if c.Value == "" || cookie.Valid() != nil {
			continue
		}
		// Prefer the shared account cookie if a page also sets a host cookie.
		if _, set := out[c.Name]; !set || domain == "quark.cn" {
			out[c.Name] = c.Value
		}
	}
	if out["__pus"] == "" && out["__puus"] == "" {
		return nil
	}
	return out
}
func (b *quarkPhoneBrowser) Close() {
	b.rpc.Close()
	select {
	case <-b.exited:
	case <-time.After(2 * time.Second):
		_ = b.command.Process.Kill()
	}
	if b.cleanup != nil {
		b.cleanupError = b.cleanup()
	}
}
func (m *quarkManager) runPhoneLogin(ctx context.Context, generation uint64, f *quarkLoginFlow) error {
	open := m.phoneWindow
	if open == nil {
		open = openQuarkPhoneWindow
	}
	window, err := open(ctx, m.app.data)
	if err != nil {
		return err
	}
	defer func() {
		window.Close()
		if browser, ok := window.(*quarkPhoneBrowser); ok && browser.cleanupError != nil {
			m.app.event("登录窗口资料清理未完成，下次启动重试：" + browser.cleanupError.Error())
		}
	}()
	m.app.event("已打开夸克专用官网手机号登录窗口")
	lastCookie := ""
	lastAttempt := time.Time{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot, err := window.Poll(ctx)
		if err != nil {
			return err
		}
		stage := "请在专用官网窗口点击手机登录，并完成手机号与验证码登录"
		header := quarkCookieHeader(snapshot.Cookies)
		if header != "" && (header != lastCookie || time.Since(lastAttempt) > 5*time.Second) {
			lastCookie = header
			lastAttempt = time.Now()
			credentials := f.credentials
			credentials.Cookies = snapshot.Cookies
			if err = m.accept(ctx, credentials, f.target); err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.update(generation, "官网会话尚未通过账号核验，请完成登录或切换正确账号", m.app.redactDAV(err.Error()))
		} else {
			m.update(generation, stage, "")
		}
		if err = davWait(ctx, time.Second); err != nil {
			return err
		}
	}
}

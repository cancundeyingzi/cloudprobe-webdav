package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type quarkPhoneSnapshot struct {
	Cookies     map[string]string
	MobileReady bool
}
type quarkPhoneWindow interface {
	Poll(context.Context) (quarkPhoneSnapshot, error)
	Close()
}

type quarkPhoneBrowser struct {
	rpc             *quarkBrowserRPC
	command         *exec.Cmd
	exited          <-chan error
	target, session string
}
type quarkBrowserRPC struct {
	connection *websocket.Conn
	seq        int64
	stopWatch  chan struct{}
	watchDone  chan struct{}
	closeOnce  sync.Once
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
	browser, err := findLoginBrowser()
	if err != nil {
		return nil, err
	}
	// Never attach to a personal browser profile or a previous login attempt.
	profile := filepath.Join(data, "official-quark-browser", "phone-"+randomKey()[:16])
	if err = os.MkdirAll(profile, 0700); err != nil {
		return nil, err
	}
	command := exec.Command(browser, "--user-data-dir="+profile, "--remote-debugging-port=0", "--remote-debugging-address=127.0.0.1", "--remote-allow-origins=http://127.0.0.1", "--no-first-run", "--no-default-browser-check", "--disable-sync", "--new-window", "https://pan.quark.cn/")
	// This visible window is the phone login explicitly requested by the user.
	if err = command.Start(); err != nil {
		return nil, errors.New("无法打开夸克官网窗口，请检查 Edge / Chrome 是否可用")
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait(); close(exited) }()
	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	for {
		if err = readyCtx.Err(); err != nil {
			_ = command.Process.Kill()
			return nil, errors.New("官网登录窗口未能就绪，请重新打开")
		}
		select {
		case <-exited:
			return nil, errors.New("官网登录窗口已提前关闭，请重新打开")
		default:
		}
		raw, readErr := os.ReadFile(filepath.Join(profile, "DevToolsActivePort"))
		if readErr == nil {
			scanner := bufio.NewScanner(strings.NewReader(string(raw)))
			if scanner.Scan() {
				port, parseErr := strconv.Atoi(strings.TrimSpace(scanner.Text()))
				if parseErr == nil && port > 0 && port <= 65535 && scanner.Scan() {
					path := strings.TrimSpace(scanner.Text())
					if strings.HasPrefix(path, "/devtools/browser/") && !strings.ContainsAny(path, "?#\r\n") {
						cfg, configErr := websocket.NewConfig(fmt.Sprintf("ws://127.0.0.1:%d%s", port, path), "http://127.0.0.1")
						if configErr == nil {
							cfg.Dialer = &net.Dialer{Timeout: 2 * time.Second}
							connection, dialErr := websocket.DialConfig(cfg)
							if dialErr == nil {
								return &quarkPhoneBrowser{rpc: newQuarkBrowserRPC(ctx, connection), command: command, exited: exited}, nil
							}
						}
					}
				}
			}
		}
		if err = davWait(readyCtx, 100*time.Millisecond); err != nil {
			_ = command.Process.Kill()
			return nil, err
		}
	}
}

const quarkChooseMobileScript = `(() => {
  const frames=Array.from(document.querySelectorAll('iframe'));
  if(frames.some(frame=>{try{const u=new URL(frame.src);return u.origin==='https://uop.quark.cn'&&u.searchParams.get('custom_login_type')==='mobile'}catch{return false}}))return true;
  const choices=Array.from(document.querySelectorAll('button,a,span,div')).filter(node=>node.textContent.trim()==='手机登录'&&node.getClientRects().length);
  choices.sort((a,b)=>a.childElementCount-b.childElementCount);
  if(choices[0])choices[0].click();
  return false;
})()`

func (b *quarkPhoneBrowser) Poll(ctx context.Context) (quarkPhoneSnapshot, error) {
	var snapshot quarkPhoneSnapshot
	select {
	case <-b.exited:
		return snapshot, errors.New("官网登录窗口已关闭，请重新打开")
	default:
	}
	raw, err := b.rpc.call(ctx, "Target.getTargets", nil, "")
	if err != nil {
		return snapshot, err
	}
	var targets struct {
		Targets []struct {
			ID   string `json:"targetId"`
			Type string `json:"type"`
			URL  string `json:"url"`
		} `json:"targetInfos"`
	}
	if err = json.Unmarshal(raw, &targets); err != nil {
		return snapshot, errors.New("官网登录窗口返回了未知页面状态")
	}
	target := ""
	for _, page := range targets.Targets {
		u, e := url.Parse(page.URL)
		if e == nil && page.Type == "page" && u.Scheme == "https" && u.Host == "pan.quark.cn" {
			target = page.ID
			break
		}
	}
	if target == "" {
		return snapshot, nil
	}
	if b.target != target || b.session == "" {
		result, e := b.rpc.call(ctx, "Target.attachToTarget", map[string]any{"targetId": target, "flatten": true}, "")
		if e != nil {
			return snapshot, e
		}
		var attachment struct {
			Session string `json:"sessionId"`
		}
		if json.Unmarshal(result, &attachment) != nil || attachment.Session == "" {
			return snapshot, errors.New("无法连接专用官网登录页")
		}
		b.target = target
		b.session = attachment.Session
	}
	result, err := b.rpc.call(ctx, "Runtime.evaluate", map[string]any{"expression": quarkChooseMobileScript, "returnByValue": true}, b.session)
	if err != nil {
		return snapshot, err
	}
	var evaluation struct {
		Result struct {
			Value bool `json:"value"`
		} `json:"result"`
	}
	if json.Unmarshal(result, &evaluation) == nil {
		snapshot.MobileReady = evaluation.Result.Value
	}
	cookies, err := b.rpc.call(ctx, "Network.getCookies", map[string]any{"urls": []string{"https://pan.quark.cn/desktop/account/userinfo", "https://pan.quark.cn/account/info", "https://drive-pc.quark.cn/1/clouddrive/", "https://drive.quark.cn/1/clouddrive/"}}, b.session)
	if err != nil {
		return snapshot, err
	}
	var data struct {
		Cookies []quarkBrowserCookie `json:"cookies"`
	}
	if json.Unmarshal(cookies, &data) != nil {
		return snapshot, errors.New("官网登录窗口未返回有效会话状态")
	}
	snapshot.Cookies = quarkPhoneCookies(data.Cookies, time.Now())
	return snapshot, nil
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
	defer window.Close()
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
		if snapshot.MobileReady {
			stage = "请在专用官网窗口输入手机号、短信验证码并完成登录"
		}
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

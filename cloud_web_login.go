package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type cloudWebLogin struct {
	mu         sync.Mutex
	app        *guiApp
	provider   string
	cancel     context.CancelFunc
	done       chan struct{}
	running    bool
	stage, err string
	validate   func(context.Context, guiAccount, string) error
}

func (l *cloudWebLogin) Status() map[string]any {
	if l == nil {
		return map[string]any{"running": false}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{"running": l.running, "stage": l.stage, "error": l.err}
}
func (l *cloudWebLogin) Stop() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
func (l *cloudWebLogin) Start(_ string) error {
	l.mu.Lock()
	if l.running {
		l.mu.Unlock()
		return errors.New("官方登录窗口已经打开")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	l.running, l.cancel, l.err, l.stage = true, cancel, "", "正在打开官网，请在窗口内完成登录"
	done := make(chan struct{})
	l.done = done
	provider := l.provider
	if provider == "" {
		provider = "wopan"
	}
	l.mu.Unlock()
	go func() {
		defer close(done)
		defer cancel()
		address := "https://pan.wo.cn/"
		if provider == "mobile" {
			address = "about:blank"
		}
		browser, err := openOfficialBrowser(ctx, l.app.data, provider, address)
		if err == nil {
			err = l.captureBrowser(ctx, browser, provider)
			browser.Close()
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.running = false
		l.cancel = nil
		if errors.Is(err, context.Canceled) {
			l.stage = "登录已取消"
			l.err = ""
		} else if err != nil {
			l.stage = "官网登录未完成"
			l.err = l.app.redactDAV(err.Error())
		} else {
			l.stage = "官网登录成功，账号及渠道已保存"
			l.err = ""
		}
	}()
	return nil
}

func (l *cloudWebLogin) captureBrowser(ctx context.Context, b *quarkPhoneBrowser, provider string) error {
	mobileToken := ""
	if provider == "mobile" {
		b.rpc.onEvent = func(method string, raw json.RawMessage) {
			if method != "Network.requestWillBeSent" {
				return
			}
			var e struct {
				Request struct {
					URL     string            `json:"url"`
					Headers map[string]string `json:"headers"`
				} `json:"request"`
			}
			if json.Unmarshal(raw, &e) != nil {
				return
			}
			u, err := url.Parse(e.Request.URL)
			if err != nil || u.Scheme != "https" || !(u.Hostname() == "yun.139.com" || strings.HasSuffix(u.Hostname(), ".yun.139.com")) {
				return
			}
			for k, v := range e.Request.Headers {
				if strings.EqualFold(k, "Authorization") && strings.HasPrefix(v, "Basic ") {
					mobileToken = strings.TrimSpace(strings.TrimPrefix(v, "Basic "))
				}
			}
		}
		raw, err := b.rpc.call(ctx, "Target.createTarget", map[string]any{"url": "about:blank"}, "")
		if err != nil {
			return err
		}
		var t struct {
			ID string `json:"targetId"`
		}
		if err = json.Unmarshal(raw, &t); err != nil {
			return err
		}
		raw, err = b.rpc.call(ctx, "Target.attachToTarget", map[string]any{"targetId": t.ID, "flatten": true}, "")
		if err != nil {
			return err
		}
		var a struct {
			Session string `json:"sessionId"`
		}
		if err = json.Unmarshal(raw, &a); err != nil || a.Session == "" {
			return errors.New("无法连接移动官网登录页")
		}
		if _, err = b.rpc.call(ctx, "Network.enable", nil, a.Session); err != nil {
			return err
		}
		if _, err = b.rpc.call(ctx, "Page.navigate", map[string]any{"url": "https://yun.139.com/w/"}, a.Session); err != nil {
			return err
		}
	}
	last := ""
	checked := time.Time{}
	for {
		cookies, err := b.cookies(ctx)
		if err != nil {
			return err
		}
		token := mobileToken
		if provider == "wopan" {
			for _, c := range cookies {
				domain := strings.TrimPrefix(strings.ToLower(c.Domain), ".")
				if c.Name == "WoCloud-Web-Token" && (domain == "wo.cn" || strings.HasSuffix(domain, ".wo.cn")) && (c.Expires <= 0 || c.Expires > float64(time.Now().Unix())) {
					token, _ = decodeWopanWebCookie(c.Value)
					break
				}
			}
		}
		if token != "" && (token != last || time.Since(checked) > 10*time.Second) {
			last, checked = token, time.Now()
			if l.validate != nil {
				err = l.validate(ctx, guiAccount{}, token)
			} else if provider == "mobile" {
				err = l.app.acceptMobileWebToken(ctx, token)
			} else {
				err = l.app.acceptWebToken(ctx, guiAccount{}, token)
			}
			if err == nil {
				return nil
			}
			l.mu.Lock()
			l.err = l.app.redactDAV(err.Error())
			l.stage = "请完成官网登录，正在等待有效授权"
			l.mu.Unlock()
		}
		if err = davWait(ctx, time.Second); err != nil {
			return err
		}
	}
}

func (g *guiApp) mobileLoginAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var in struct {
		Token string `json:"token"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(&in); err != nil {
		g.fail(w, errors.New("移动登录请求无效"))
		return
	}
	if err := ensureEOF(d); err != nil {
		g.fail(w, err)
		return
	}
	if g.mobileLogin == nil {
		g.fail(w, errors.New("移动登录管理器未初始化"))
		return
	}
	var err error
	switch strings.TrimPrefix(r.URL.Path, "/api/mobile-login/") {
	case "start":
		err = g.mobileLogin.Start("")
	case "stop":
		g.mobileLogin.Stop()
	case "import":
		err = g.acceptMobileWebToken(r.Context(), in.Token)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		g.fail(w, err)
		return
	}
	respondJSON(w, 200, g.publicStatus())
}

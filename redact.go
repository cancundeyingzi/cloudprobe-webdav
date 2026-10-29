package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const hidden = "[已隐藏]"

type Redactor struct {
	mu      sync.RWMutex
	secrets []string
}

func newRedactor(c Config) *Redactor {
	r := &Redactor{}
	for _, s := range []string{c.Mobile.AccessToken, c.Mobile.AppSecret, c.Wopan.AccessToken, c.Netease.Token, c.Netease.SID, c.Netease.RawKeyBase64} {
		r.Add(s)
	}
	for _, s := range c.Headers {
		r.Add(s)
	}
	return r
}
func (r *Redactor) Add(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s != "" {
		r.secrets = append(r.secrets, s)
		sort.SliceStable(r.secrets, func(i, j int) bool { return len(r.secrets[i]) > len(r.secrets[j]) })
	}
}
func sensitive(k string) bool {
	k = strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
	return strings.Contains(k, "token") || strings.Contains(k, "secret") || strings.Contains(k, "password") || strings.Contains(k, "cookie") || strings.Contains(k, "apikey") || k == "authorization" || k == "sid" || k == "sign" || k == "rawkeybase64" || k == "seed" || k == "sectag"
}

var urlPattern = regexp.MustCompile(`https?://[^\s<>"']+`)

func (r *Redactor) string(s string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, v := range r.secrets {
		s = strings.ReplaceAll(s, v, hidden)
		s = strings.ReplaceAll(s, url.QueryEscape(v), hidden)
		s = strings.ReplaceAll(s, uriEncode(v), hidden)
	}
	return urlPattern.ReplaceAllStringFunc(s, func(v string) string {
		u, e := url.Parse(v)
		if e != nil {
			return "[URL 已隐藏]"
		}
		if u.User != nil {
			u.User = url.User(hidden)
		}
		if u.RawQuery != "" {
			q := u.Query()
			for k := range q {
				q[k] = []string{hidden}
			}
			u.RawQuery = q.Encode()
		}
		u.Fragment = ""
		return u.String()
	})
}
func (r *Redactor) clean(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := map[string]any{}
		for k, v := range x {
			if sensitive(k) {
				m[k] = hidden
			} else {
				m[k] = r.clean(v)
			}
		}
		return m
	case []any:
		a := make([]any, len(x))
		for i, v := range x {
			a[i] = r.clean(v)
		}
		return a
	case string:
		return r.string(x)
	default:
		return v
	}
}
func (r *Redactor) Text(s string) string {
	var v any
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	if d.Decode(&v) == nil && ensureEOF(d) == nil {
		if b, e := compactJSON(r.clean(v)); e == nil {
			return string(b)
		}
	}
	return r.string(s)
}
func (r *Redactor) Headers(h http.Header) map[string]string {
	m := map[string]string{}
	for k, v := range h {
		switch strings.ToLower(k) {
		case "content-type", "content-length", "encryption", "filmly-trace-id", "date":
			m[k] = r.string(strings.Join(v, ", "))
		default:
			m[k] = hidden
		}
	}
	return m
}

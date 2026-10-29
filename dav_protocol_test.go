package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"golang.org/x/net/websocket"
)

func TestWopanMultipartHasFixedLengthAndExactPartBytes(t *testing.T) {
	data := bytes.Repeat([]byte("part-check-"), 1000000)
	parts := map[string][]byte{}
	firstUnique := ""
	firstRejected := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength <= 0 || len(r.TransferEncoding) != 0 {
			t.Error("multipart must send a fixed Content-Length")
		}
		if e := r.ParseMultipartForm(16 << 20); e != nil {
			t.Error(e)
			http.Error(w, "bad multipart", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		file, header, e := r.FormFile("file")
		if e != nil {
			t.Error(e)
			http.Error(w, "missing file", 400)
			return
		}
		defer file.Close()
		if header.Filename != "part.bin" {
			t.Error("multipart file name incorrect")
		}
		b, _ := io.ReadAll(file)
		if r.FormValue("partIndex") == "1" && !firstRejected {
			firstRejected = true
			firstUnique = r.FormValue("uniqueId")
			http.Error(w, "temporary gateway failure", 503)
			return
		}
		if r.FormValue("uniqueId") != firstUnique {
			t.Error("retry restarted the upload session")
		}
		parts[r.FormValue("partIndex")] = b
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":"0000","data":{"fid":"file-fid"}}`)
	}))
	defer server.Close()
	f, e := os.CreateTemp(t.TempDir(), "part-*")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	f.Write(data)
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fake-test-token-1234567890"
	cl, e := newClient(cfg, "")
	if e != nil {
		t.Fatal(e)
	}
	d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
	d.uploadConcurrency = 1
	d.uploadPartMiB = 8
	d.w.ZoneURL = server.URL
	d.w.ClassifyRuleData = &wopan.ClassifyRuleData{}
	fid, e := d.uploadParts(context.Background(), "parent", "part.bin", f, int64(len(data)))
	if e != nil || fid != "file-fid" {
		t.Fatal("multipart failed", e)
	}
	joined := append(append([]byte{}, parts["1"]...), parts["2"]...)
	if !bytes.Equal(joined, data) {
		t.Fatal("multipart offsets or last-part size incorrect")
	}
}

type protocolRoundTripper func(*http.Request) (*http.Response, error)

func (f protocolRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failedProtocolBridge struct{}

func (failedProtocolBridge) Request(context.Context, string, string, []byte, map[string]string) ([]byte, error) {
	return nil, errors.New("primary unavailable")
}
func TestWopanMetadataFallbackAndUnknownShape(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		cfg := defaultConfig("wopan")
		cfg.Transport = "bridge"
		cfg.Wopan.AccessToken = "fake-test-token-1234567890"
		cl, _ := newClient(cfg, "")
		cl.bridge = failedProtocolBridge{}
		d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
		d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
			data := `{"files":[{"id":"file-id","fid":"file-fid","name":"ordinary.zip","size":123,"type":1}]}`
			if unknown {
				data = `{"unexpected":true}`
			}
			body := `{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":` + data + `}}`
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
		})})
		a, e := d.List(context.Background(), "0")
		if unknown && e == nil {
			t.Fatal("unknown backup response treated as empty")
		}
		if !unknown && (e != nil || len(a) != 1 || a[0].Name != "ordinary.zip") {
			t.Fatal("fallback metadata failed", e)
		}
	}
}
func TestWopanDownloadLinkUsesSecondDirectVariant(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "fake-test-token-1234567890"
	cl, _ := newClient(cfg, "")
	cl.bridge = failedProtocolBridge{}
	d := newWopanDAVDrive(cl, map[string]any{}, "0", nil)
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		var request struct {
			Header struct {
				Key string `json:"key"`
			} `json:"header"`
		}
		json.NewDecoder(r.Body).Decode(&request)
		data := `{"list":[]}`
		if request.Header.Key == wopan.KeyGetDownloadUrl {
			data = `[{"fid":"file-fid","downloadUrl":"https://example.test/file"}]`
		}
		body := `{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":` + data + `}}`
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})})
	u, e := d.URL(context.Background(), davEntry{Item: Item{ID: "id", FID: "file-fid"}})
	if e != nil || u != "https://example.test/file" {
		t.Fatal("second direct download variant failed", e)
	}
}
func TestOfficialLoginCaptureIgnoresUnrelatedEvents(t *testing.T) {
	server := httptest.NewServer(websocket.Server{Handshake: func(*websocket.Config, *http.Request) error { return nil }, Handler: func(conn *websocket.Conn) {
		defer conn.Close()
		for {
			var request map[string]any
			if e := websocket.JSON.Receive(conn, &request); e != nil {
				return
			}
			if request["method"] == "Browser.close" {
				return
			}
			websocket.JSON.Send(conn, map[string]any{"method": "unrelated.event"})
			websocket.JSON.Send(conn, map[string]any{"id": request["id"], "result": map[string]any{"cookies": []map[string]string{{"name": "WoCloud-Web-Token", "domain": "pan.wo.cn", "value": "test%2Btoken"}}}})
		}
	}})
	defer server.Close()
	u, _ := url.Parse(server.URL)
	profile := t.TempDir()
	os.WriteFile(filepath.Join(profile, "DevToolsActivePort"), []byte(u.Port()+"\n/devtools/browser/test\n"), 0600)
	accepted := false
	l := &wopanWebLogin{app: &guiApp{}, validate: func(ctx context.Context, a guiAccount, token string) error {
		if token != "test+token" {
			return errors.New("wrong token normalization")
		}
		accepted = true
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go l.capture(ctx, cancel, guiAccount{}, profile, done)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("login capture did not complete")
	}
	if !accepted {
		t.Fatal("matching login result was lost behind an unrelated event")
	}
	status, _ := json.Marshal(l.Status())
	if bytes.Contains(status, []byte("test+token")) {
		t.Fatal("login status leaked token")
	}
}

func TestProviderMetadataRejectsCredentialRedirect(t *testing.T) {
	foreignHit := false
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignHit = true }))
	defer foreign.Close()
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, foreign.URL, 302) }))
	defer primary.Close()
	r, _ := http.NewRequest("POST", primary.URL, strings.NewReader("{}"))
	r.Header.Set("Accesstoken", "private-test-token")
	resp, e := providerMetadataClient(time.Second).Do(r)
	if resp != nil {
		resp.Body.Close()
	}
	if e == nil || foreignHit {
		t.Fatal("metadata credentials could cross a redirect")
	}
}

type mobileProtocolBridge struct {
	code      int
	uncertain bool
}

func (b mobileProtocolBridge) Request(context.Context, string, string, []byte, map[string]string) ([]byte, error) {
	if b.uncertain {
		return nil, errors.New("connection lost after request")
	}
	return []byte(fmt.Sprintf(`{"code":%d,"message":"rejected before forwarding"}`, b.code)), nil
}
func TestMobileReadAndPreexecutionWriteUseDirectFallback(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/xml")
		if strings.HasSuffix(r.URL.Path, "getdisk") {
			io.WriteString(w, `<result resultCode="0"><getDiskResult><isCompleted>1</isCompleted><contentList length="1"><contentInfo><contentID>txt</contentID><contentName>notes.txt</contentName><contentSuffix>txt</contentSuffix><contentSize>10</contentSize></contentInfo></contentList></getDiskResult></result>`)
		} else {
			io.WriteString(w, `<result resultCode="0"><catalogInfo><catalogID>new-dir</catalogID></catalogInfo></result>`)
		}
	}))
	defer server.Close()
	cfg := defaultConfig("mobile")
	cfg.Transport = "bridge"
	cfg.IncludeDirectories = true
	cfg.Mobile = MobileConfig{AppID: "app", AppSecret: "private-test-secret", AccessToken: "private-test-token", DeviceID: "device", ChannelID: "10010"}
	primary, _ := newClient(cfg, "")
	primary.bridge = mobileProtocolBridge{code: 10012}
	cfg.Transport = "direct"
	cfg.BaseURL = server.URL
	direct, _ := newClient(cfg, "")
	d := &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: primary}, direct: direct}
	items, e := d.List(context.Background(), "root")
	if e != nil || len(items) != 1 || items[0].Name != "notes.txt" {
		t.Fatal("mobile read fallback failed", e)
	}
	created, e := d.Mkdir(context.Background(), "root", "child")
	if e != nil || created.ID != "new-dir" {
		t.Fatal("known preexecution rejection did not use direct route", e)
	}
	before := requests
	primary.bridge = mobileProtocolBridge{uncertain: true}
	if _, e = d.Mkdir(context.Background(), "root", "uncertain"); e == nil || requests != before {
		t.Fatal("unknown write outcome was replayed")
	}
}

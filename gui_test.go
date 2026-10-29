package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGUIRejectsWrongHostOriginAndMissingNonce(t *testing.T) {
	app := &guiApp{host: "127.0.0.1:12345", nonce: "local-ui-nonce", accounts: map[string]guiAccount{}, downloads: map[string]guiDownload{}}
	for _, tc := range []struct {
		host, origin, nonce string
		status              int
	}{{"evil.example:12345", "", "local-ui-nonce", 403}, {app.host, "http://evil.example", "local-ui-nonce", 403}, {app.host, "", "", 403}, {app.host, "", "local-ui-nonce", 200}} {
		req := httptest.NewRequest("GET", "http://"+tc.host+"/api/status", nil)
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("X-CloudProbe-UI", tc.nonce)
		w := httptest.NewRecorder()
		app.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Errorf("guard got %d, want %d", w.Code, tc.status)
		}
	}
}

func TestGUIRootBootstrapsCurrentLocalSession(t *testing.T) {
	app := &guiApp{host: "127.0.0.1:12345", nonce: "current-local-session"}
	r := httptest.NewRequest("GET", "http://"+app.host+"/", nil)
	w := httptest.NewRecorder()
	app.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `name="cloudprobe-ui-session" content="current-local-session"`) {
		t.Fatal("bare local address did not bootstrap its current session")
	}
	if strings.Contains(w.Body.String(), "__CLOUDPROBE_UI_NONCE__") {
		t.Fatal("session marker was not replaced")
	}
	r = httptest.NewRequest("GET", "http://"+app.host+"/", nil)
	r.Header.Set("Origin", "https://untrusted.example")
	w = httptest.NewRecorder()
	app.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("cross-origin bootstrap accepted")
	}
}
func TestGUIStatusDoesNotExposeAccountCredentials(t *testing.T) {
	app := &guiApp{host: "127.0.0.1:12345", nonce: "nonce", logged: true, qrToken: "qr-session-secret", identity: MobileConfig{AppSecret: "application-secret"}, accountOrder: []string{"a"}, accounts: map[string]guiAccount{"a": {Key: "a", Provider: "mobile", Name: "移动云盘", credentials: map[string]any{"token": "user-token-secret", "refresh_token": "refresh-secret"}}}}
	b, e := json.Marshal(app.publicStatus())
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range []string{"qr-session-secret", "application-secret", "user-token-secret", "refresh-secret", "credentials"} {
		if strings.Contains(string(b), s) {
			t.Fatalf("status leaked %s", s)
		}
	}
}
func TestMobileBrowsingPreservesFoldersAndOrdinaryFiles(t *testing.T) {
	raw := []byte(`<result resultCode="0"><getDiskResult><isCompleted>1</isCompleted><catalogList length="1"><catalogInfo><catalogID>dir1</catalogID><catalogName>文档</catalogName></catalogInfo></catalogList><contentList length="1"><contentInfo><contentID>txt1</contentID><contentName>说明.txt</contentName><contentSuffix>txt</contentSuffix></contentInfo></contentList></getDiskResult></result>`)
	page, e := parsePage("mobile", raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Items) != 2 || !page.Items[0].Directory || page.Items[1].Name != "说明.txt" || page.Items[1].SubtitleCandidate {
		t.Fatalf("bad browse data: %+v", page.Items)
	}
	foldersOnly := []byte(`{"result":{"getDiskResult":{"isCompleted":"1","catalogList":{"@length":1,"catalogInfo":{"catalogID":"d","catalogName":"目录"}}}}}`)
	page, e = parsePage("mobile", foldersOnly)
	if e != nil || len(page.Items) != 1 || !page.Items[0].Directory {
		t.Fatalf("folders-only: %+v %v", page, e)
	}
}

type testOriginalRequester struct {
	method, path string
	params       []byte
}

func (b *testOriginalRequester) Request(ctx context.Context, method, path string, p []byte, h map[string]string) ([]byte, error) {
	b.method = method
	b.path = path
	b.params = append([]byte(nil), p...)
	return []byte(`{"code":0,"data":{"meta":{"code":"200"},"nextPage":false,"fileInfoList":[{"id":"t","fid":"f","name":"说明.txt"}]}}`), nil
}
func TestOriginalBridgeTransportUsesLogicalRequestAndRedactsOutput(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "bridge-private-token"
	folder := t.TempDir()
	cl, e := newClient(cfg, folder)
	if e != nil {
		t.Fatal(e)
	}
	spy := &testOriginalRequester{}
	cl.bridge = spy
	r, e := cl.List(context.Background(), "root", "live-original-sdk", nil)
	if e != nil || !r.Complete || len(r.Items) != 1 {
		t.Fatalf("original transport: %+v %v", r, e)
	}
	if spy.method != "GET" || spy.path != "/v1/api/wopan/query_files" {
		t.Fatal("原版桥接应收到逻辑路径")
	}
	if !strings.Contains(string(spy.params), "bridge-private-token") {
		t.Fatal("授权材料未到后台桥接")
	}
	b, _ := os.ReadFile(filepath.Join(folder, "http-001.json"))
	if strings.Contains(string(b), "bridge-private-token") {
		t.Fatal("诊断记录泄漏令牌")
	}
}
func TestGUIExportExcludesDownloadedContentAndSDKLogs(t *testing.T) {
	folder := t.TempDir()
	for _, f := range []string{"report.json", "http-001.json", "说明.txt", "private-token.log"} {
		_ = os.WriteFile(filepath.Join(folder, f), []byte(f), 0600)
	}
	app := &guiApp{host: "127.0.0.1:12345", nonce: "nonce", lastResult: folder}
	req := httptest.NewRequest("GET", "http://"+app.host+"/api/export", nil)
	req.Header.Set("X-CloudProbe-UI", "nonce")
	w := httptest.NewRecorder()
	app.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	data := w.Body.String()
	if strings.Contains(data, "说明.txt") || strings.Contains(data, "private-token.log") {
		t.Fatal("诊断导出不应包含下载正文或 SDK 私有日志")
	}
	if !strings.Contains(data, "report.json") || !strings.Contains(data, "http-001.json") {
		t.Fatal("诊断材料缺失")
	}
}

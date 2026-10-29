package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMobileFilesAndClientFilter(t *testing.T) {
	raw := []byte(`<result resultCode="0"><getDiskResult><isCompleted>1</isCompleted><contentList length="3"><contentInfo><contentID>1</contentID><contentName>说明.txt</contentName><contentSuffix>txt</contentSuffix></contentInfo><contentInfo><contentID>2</contentID><contentName>电影.srt</contentName><contentSuffix>srt</contentSuffix></contentInfo><contentInfo><contentID>3</contentID><contentName>无扩展文件</contentName><contentSuffix></contentSuffix></contentInfo></contentList></getDiskResult></result>`)
	page, e := parsePage("mobile", raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Items) != 3 || page.More == nil || *page.More {
		t.Fatalf("bad page: %+v", page)
	}
	if page.Items[0].SubtitleCandidate || !page.Items[1].SubtitleCandidate || page.Items[2].SubtitleCandidate {
		t.Fatalf("wrong filter: %+v", page.Items)
	}
	if !strings.Contains(page.Items[2].FilterReason, "后缀为空") {
		t.Fatal("未显示样本对无后缀文件的解析过滤")
	}
	single := []byte(`{"code":0,"data":{"code":0,"data":{"result":{"@resultCode":"0","getDiskResult":{"isCompleted":"1","contentList":{"@length":"1","contentInfo":{"contentID":"4","contentName":"文档.txt","contentSuffix":"txt"}}}}}}}`)
	p, e := parsePage("mobile", single)
	if e != nil || len(p.Items) != 1 || p.Items[0].Name != "文档.txt" {
		t.Fatalf("single map: %+v %v", p, e)
	}
}
func TestMalformedAndBusinessErrorsAreNotEmptyFolders(t *testing.T) {
	for _, tc := range []struct{ p, s string }{{"wopan", `{"code":10011,"message":"login required"}`}, {"wopan", `{"code":0,"data":{"meta":{"code":"9999"},"fileInfoList":[]}}`}, {"wopan", `{"code":0,"data":{}}`}, {"wopan", `{"fileInfoList":"not a list"}`}, {"mobile", `{"result":{"@resultCode":20011}}`}, {"mobile", `{"result":{"getDiskResult":{"isCompleted":"maybe","contentList":{}}}}`}, {"mobile", `<html>not an API</html>`}, {"mobile", `{"code":0} trailing`}} {
		t.Run(tc.p+tc.s[:min(len(tc.s), 30)], func(t *testing.T) {
			if _, e := parsePage(tc.p, []byte(tc.s)); e == nil {
				t.Fatal("错误响应被当成成功空目录")
			}
		})
	}
}
func demoConfig(provider, base string) Config {
	c := defaultConfig(provider)
	c.BaseURL = base
	c.PageSize = 2
	c.Transport = "gateway"
	if provider == "mobile" {
		c.Transport = "direct"
	}
	c.Mobile = MobileConfig{AppID: "fixture-app", AppSecret: "fixture-app-secret", AccessToken: "fixture-mobile-token", DeviceID: "fixture-device", ChannelID: "10010"}
	c.Wopan.AccessToken = "fixture-wopan-token"
	return c
}
func TestPaginationAndTextDownloadBothProviders(t *testing.T) {
	srv := demoServer()
	defer srv.Close()
	for _, p := range []string{"mobile", "wopan"} {
		t.Run(p, func(t *testing.T) {
			folder := t.TempDir()
			c, e := newClient(demoConfig(p, srv.URL), folder)
			if e != nil {
				t.Fatal(e)
			}
			r, e := c.List(context.Background(), "root", "test", []string{"说明.txt", "不存在.txt"})
			if e != nil {
				t.Fatal(e)
			}
			if !r.Complete || r.Pages != 2 || len(r.Items) != 4 {
				t.Fatalf("bad pagination: %+v", r)
			}
			if !r.Expected[0].Returned || r.Expected[1].Returned {
				t.Fatal("预期文件对照错误")
			}
			if strings.Contains(r.Expected[1].Conclusion, "无权限") {
				t.Fatal("不能凭未返回断言权限")
			}
			receipt, e := c.Fetch(context.Background(), "text-1", "", "说明.txt", "test")
			if e != nil {
				t.Fatal(e)
			}
			b, e := os.ReadFile(filepath.Join(folder, "说明.txt"))
			if e != nil || string(b) != demoText || receipt.Bytes != int64(len(demoText)) {
				t.Fatalf("bad text: %q %v", b, e)
			}
			if e = saveReport(folder, r, c.redact); e != nil {
				t.Fatal(e)
			}
			entries, _ := os.ReadDir(folder)
			for _, entry := range entries {
				if !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				b, _ := os.ReadFile(filepath.Join(folder, entry.Name()))
				for _, secret := range []string{"fixture-app-secret", "fixture-mobile-token", "fixture-wopan-token", "demo-download-secret"} {
					if bytes.Contains(b, []byte(secret)) {
						t.Errorf("%s 泄漏 %s", entry.Name(), secret)
					}
				}
			}
		})
	}
}
func TestRepeatedPageAndPartialFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if fail && r.URL.Query().Get("page") == "2" {
					http.Error(w, "failed", 503)
					return
				}
				fmt.Fprint(w, `{"code":0,"data":{"meta":{"code":"200"},"nextPage":true,"fileInfoList":[{"id":"1","fid":"f1","name":"说明.txt"}]}}`)
			}))
			defer srv.Close()
			c, e := newClient(demoConfig("wopan", srv.URL), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			r, e := c.List(context.Background(), "root", "test", []string{"另一个.txt"})
			if fail && e == nil {
				t.Fatal("503 必须失败")
			}
			if r.Complete || len(r.Items) != 1 || r.Expected[0].Returned {
				t.Fatalf("incomplete state lost: %+v", r)
			}
			if !strings.Contains(r.Expected[0].Conclusion, "未完成") {
				t.Fatal("未完成时不能给出确定否定结论")
			}
		})
	}
}
func TestDownloadBoundsAndNoCredentialForwarding(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("secretkey") != "" || r.Header.Get("Token") != "" {
			t.Error("下载端不应收到 API 凭据")
		}
		w.Header().Set("Content-Type", "text/plain")
		fl, _ := w.(http.Flusher)
		if fl != nil {
			fl.Flush()
		}
		fmt.Fprint(w, strings.Repeat("x", 80))
	}))
	defer srv.Close()
	file := filepath.Join(t.TempDir(), "test.txt")
	if _, _, _, e := downloadTo(context.Background(), srv.URL, file, 16, time.Second); e == nil || !strings.Contains(e.Error(), "超过下载上限") {
		t.Fatalf("应当实际接收流后命中字节上限，得到：%v", e)
	}
	if _, e := os.Stat(file); !os.IsNotExist(e) {
		t.Fatal("不完整文件应移除")
	}
	if e := os.WriteFile(file, []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e := downloadTo(context.Background(), srv.URL, file, 100, time.Second); !os.IsExist(e) {
		t.Fatalf("应当拒绝覆盖原有文件，得到：%v", e)
	}
	b, _ := os.ReadFile(file)
	if string(b) != "keep" {
		t.Fatal("覆盖了已有文件")
	}
}
func TestRedaction(t *testing.T) {
	c := defaultConfig("mobile")
	c.Mobile.AccessToken = "my-private-token"
	c.Mobile.AppSecret = "my-private-secret"
	r := newRedactor(c)
	raw := `{"sid":"new-server-sid","data":{"access_token":"another-token","downloadUrl":"https://files.example.com/a?sign=secret-value&contentID=123","body":"my-private-secret my-private-token"}}`
	safe := r.Text(raw)
	for _, s := range []string{"new-server-sid", "another-token", "secret-value", "my-private-secret", "my-private-token"} {
		if strings.Contains(safe, s) {
			t.Errorf("secret not removed: %s", s)
		}
	}
	h := r.Headers(http.Header{"Set-Cookie": []string{"opaque"}, "X-Custom-Key": []string{"custom-private"}})
	if h["Set-Cookie"] != hidden || h["X-Custom-Key"] != hidden {
		t.Fatal("未知响应头也不应泄漏")
	}
}
func TestNativeCryptoTamperingAndLimits(t *testing.T) {
	key := make([]byte, 32)
	frame, e := encryptNative(key, []byte(`{"code":0,"data":{"sid":"example"}}`))
	if e != nil {
		t.Fatal(e)
	}
	plain, e := decryptNative(key, frame, 1024)
	if e != nil || !json.Valid(plain) {
		t.Fatal(e)
	}
	frame[len(frame)-1] ^= 1
	if _, e = decryptNative(key, frame, 1024); e == nil {
		t.Fatal("未检测认证标签被篡改")
	}
	frame[len(frame)-1] ^= 1
	if _, e = decryptNative(key, frame, 2); e == nil {
		t.Fatal("未限制解压后大小")
	}
	for _, bad := range [][]byte{nil, {16}, {255, 1, 2}, bytes.Repeat([]byte{0}, 40)} {
		if _, e = decryptNative(key, bad, 1024); e == nil {
			t.Fatal("接受了坏帧")
		}
	}
}
func TestConfigEnvAndUnknownKey(t *testing.T) {
	c := defaultConfig("mobile")
	c.Mobile.AccessToken = "${CLOUDPROBE_TEST_TOKEN}"
	t.Setenv("CLOUDPROBE_TEST_TOKEN", "env-secret-token")
	b, _ := json.Marshal(c)
	p := filepath.Join(t.TempDir(), "config.json")
	_ = os.WriteFile(p, b, 0600)
	got, e := loadConfig(p)
	if e != nil || got.Mobile.AccessToken != "env-secret-token" {
		t.Fatal(e)
	}
	_ = os.WriteFile(p, []byte(`{"provider":"mobile","typo":true}`), 0600)
	if _, e = loadConfig(p); e == nil {
		t.Fatal("未知字段未报错")
	}
}
func TestFullDemoCommand(t *testing.T) {
	var out bytes.Buffer
	dir := t.TempDir()
	code, e := run(context.Background(), []string{"demo", "-out", dir}, &out)
	if e != nil || code != 0 {
		t.Fatalf("%d %v %s", code, e, out.String())
	}
	for _, p := range []string{"mobile", "wopan"} {
		matches, _ := filepath.Glob(filepath.Join(dir, p, "run-*", "report.html"))
		if len(matches) != 1 {
			t.Fatalf("%s HTML 报告缺失", p)
		}
	}
}
func TestFilenameRules(t *testing.T) {
	for _, n := range []string{"../x.txt", "CON", "a\\b", "LPT1.txt", "x.", ""} {
		if safeFilename(n) == nil {
			t.Errorf("接受了非法名字 %q", n)
		}
	}
	if e := safeFilename("说明.txt"); e != nil {
		t.Fatal(e)
	}
}
func TestRawKeyValidation(t *testing.T) {
	if _, e := nativeKey(NeteaseConfig{RawKeyBase64: base64.StdEncoding.EncodeToString(make([]byte, 31))}); e == nil {
		t.Fatal("接受非32字节密钥")
	}
}

func TestMobileEmptySuffixDoesNotGetReconstructedIntoCandidate(t *testing.T) {
	raw := []byte(`{"result":{"@resultCode":0,"getDiskResult":{"isCompleted":"1","contentList":{"@length":1,"contentInfo":[{"contentID":"s","contentName":"电影.srt","contentSuffix":""}]}}}}`)
	p, e := parsePage("mobile", raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Items) != 1 || p.Items[0].SubtitleCandidate || p.Items[0].SampleParserDrop == "" {
		t.Fatal("不能用文件名推导后缀来绕过样本解析层的空值过滤")
	}
	bad := []byte(`{"result":{"getDiskResult":{"isCompleted":"1","contentList":{"@length":1,"contentInfo":"invalid"}}}}`)
	if _, e = parsePage("mobile", bad); e == nil {
		t.Fatal("不能把错误的条目类型当成空目录")
	}
}

func TestNativeTransportPreservesPrefixAndDecryptsBusinessError(t *testing.T) {
	key := make([]byte, 32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/a/v1/auth" {
			t.Errorf("missing prefix: %s", r.URL.Path)
		}
		if r.Header.Get("Sign") == "" || r.Header.Get("SecTag") == "" {
			t.Error("签名头缺失")
		}
		b, e := encryptNative(key, []byte(`{"code":10012,"message":"签名错误"}`))
		if e != nil {
			t.Fatal(e)
		}
		w.Header().Set("Encryption", "True")
		w.WriteHeader(401)
		_, _ = w.Write(b)
	}))
	defer srv.Close()
	c := defaultConfig("wopan")
	c.BaseURL = srv.URL
	c.Netease.RawKeyBase64 = base64.StdEncoding.EncodeToString(key)
	cl, e := newClient(c, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	e = cl.ensureSID(context.Background())
	if e == nil || !strings.Contains(e.Error(), "10012") || !strings.Contains(e.Error(), "签名错误") {
		t.Fatalf("应解密并报告业务原因，得到：%v", e)
	}
}

func TestFilterReadsSavedHTTPTrace(t *testing.T) {
	folder := t.TempDir()
	input := filepath.Join(folder, "http.json")
	body := `{"fileInfoList":[{"id":"i","fid":"f","name":"说明.txt"}],"nextPage":false}`
	data, _ := json.Marshal(map[string]any{"response_logical_body": body})
	if e := os.WriteFile(input, data, 0600); e != nil {
		t.Fatal(e)
	}
	var text bytes.Buffer
	code, e := run(context.Background(), []string{"filter", "-provider", "wopan", "-input", input, "-out", filepath.Join(folder, "out")}, &text)
	if e != nil || code != 0 || !strings.Contains(text.String(), "说明.txt") {
		t.Fatalf("filter trace failed: %d %v", code, e)
	}
}

func TestNativeIndependentGoldenAndSigning(t *testing.T) {
	// 固定帧由 Node.js crypto/zlib 独立生成，使用 16 字节 IV。
	frame, e := base64.StdEncoding.DecodeString("EAABAgMEBQYHCAkKCwwNDg+1I5uemYLJw/GyvDjOsee2EVOOc5gThExudWJPs78lPB8z4otGd4hJa/aZI/+X6OCjzYIZ9povFxGFQ6KPiWJKXypTV5NjFCv883v8CcdL")
	if e != nil {
		t.Fatal(e)
	}
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	plain, e := decryptNative(key, frame, 1024)
	if e != nil {
		t.Fatal(e)
	}
	if string(plain) != `{"code":0,"data":{"sid":"fixture-session"}}` {
		t.Fatalf("golden mismatch: %q", plain)
	}
	got := nativeSign("POST", nil, []byte(`{"a":"a b"}`), "TAG", "1700000000", []byte("0123456789abcdef0123456789abcdef"))
	if got != "98da2c18cdd93e47c87f83ef592ffb88e52ce27a4a4b64758de8e9214eed987f" {
		t.Fatalf("sign mismatch: %s", got)
	}
	if nativeURLEncode("a/b +") != "a/b%20%2B" {
		t.Fatal("样本 URL 编码必须保留斜杠")
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

// Opt-in integration probe. Never includes account credentials in test output.
func TestLiveWopanDAVCredentialProbe(t *testing.T) {
	profile := os.Getenv("CLOUDPROBE_LIVE_PROFILE")
	if profile == "" {
		t.Skip("设置 CLOUDPROBE_LIVE_PROFILE 后才访问真实账号")
	}
	bundle := os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bridgeFile := os.Getenv("CLOUDPROBE_LIVE_BRIDGE")
	if bridgeFile == "" {
		bridgeFile = filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll")
	}
	b, e := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), bridgeFile, filepath.Join(profile, "original-sdk"))
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	raw, e := b.Request(ctx, "GET", "/v1/drive/alltokens", []byte("{}"), nil)
	if e != nil {
		t.Fatal(e)
	}
	m, e := decodeDocument(raw)
	if e != nil {
		t.Fatal("凭据响应格式错误")
	}
	m, e = unwrap(m)
	if e != nil {
		t.Fatal("读取绑定账号失败")
	}
	accounts := list(m["wopan_credentials"])
	if len(accounts) == 0 {
		t.Fatal("当前账号没有联通云盘绑定")
	}
	cred := object(accounts[0])
	liveAccount := guiAccount{Key: "live", Provider: "wopan", credentials: cred}
	liveApp := &guiApp{data: profile, accounts: map[string]guiAccount{"live": liveAccount}}
	liveApp.applyDAVCredentials(liveApp.accounts)
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = valueString(cred["token"])
	cl, e := newClient(cfg, "")
	if e != nil {
		t.Fatal("创建客户端失败")
	}
	cl.bridge = b
	d := newWopanDAVDrive(cl, cred, "0", func(access, refresh string) error { return liveApp.saveDAVCredential(liveAccount, access, refresh) })
	if os.Getenv("CLOUDPROBE_LIVE_REFRESH") == "1" {
		if refreshErr := d.refreshNative(ctx); refreshErr != nil {
			t.Log("令牌刷新请求未通过：" + cl.redact.Text(refreshErr.Error()))
		} else {
			t.Log("令牌刷新成功，已使用本机凭据保护持久化")
		}
	}
	e = d.probe(ctx)
	result := map[string]any{"time": time.Now().Format(time.RFC3339), "direct_credentials_accepted": e == nil}
	_, userErr := d.w.AppQueryUser(woContext(ctx))
	result["account_info_ok"] = userErr == nil
	if userErr != nil {
		result["account_info_error"] = cl.redact.Text(userErr.Error())
	}
	family, familyErr := d.w.FamilyUserCurrentEncode(woContext(ctx))
	result["family_info_ok"] = familyErr == nil
	if familyErr != nil {
		result["family_info_error"] = cl.redact.Text(familyErr.Error())
	} else {
		result["has_default_family"] = family.DefaultHomeId != 0
	}
	listed, listErr := d.w.QueryAllFilesPersonal("0", 0, 100, 0, woContext(ctx))
	if listErr == nil {
		result["direct_root_count"] = len(listed.Files)
		own := 0
		for _, it := range listed.Files {
			if strings.HasPrefix(it.Name, "CloudProbe-WebDAV-Test-") {
				own++
			}
		}
		result["direct_test_directory_count"] = own
	}
	original, originalErr := cl.List(ctx, "0", "live-webdav-probe", nil)
	if originalErr == nil {
		own := 0
		for _, it := range original.Items {
			if strings.HasPrefix(it.Name, "CloudProbe-WebDAV-Test-") {
				own++
			}
		}
		result["proxy_root_count"] = len(original.Items)
		result["proxy_test_directory_count"] = own
	}
	zone, zoneErr := d.w.GetZoneInfo(woContext(ctx))
	if zoneErr == nil {
		parsed, _ := url.Parse(zone.Url)
		if parsed != nil {
			result["upload_host"] = parsed.Host
		}
	} else {
		result["zone_error"] = cl.redact.Text(zoneErr.Error())
	}
	if e != nil {
		result["error"] = cl.redact.Text(e.Error())
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	_ = os.WriteFile(filepath.Join(profile, "webdav-credential-probe.json"), data, 0600)
	if e != nil {
		t.Fatal(cl.redact.Text(e.Error()))
	}
	t.Log("联通直连目录请求接受了已有账号凭据")
	t.Logf("账号信息请求成功：%v；家庭信息请求成功：%v", userErr == nil, familyErr == nil)
	if listErr == nil {
		t.Logf("直连根目录条目数：%d", len(listed.Files))
	}
	t.Logf("测试目录计数：直连 %v；原版代理 %v", result["direct_test_directory_count"], result["proxy_test_directory_count"])
	if zoneErr == nil {
		parsed, _ := url.Parse(zone.Url)
		if parsed != nil {
			t.Logf("上传节点：%s", parsed.Host)
		}
	}
	if userErr != nil {
		t.Log(cl.redact.Text(userErr.Error()))
	}
	if familyErr != nil {
		t.Log(cl.redact.Text(familyErr.Error()))
	}
}

func TestLiveWebDAVWopanCRUD(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_WRITE") != "1" {
		t.Skip("仅在显式批准并设置 CLOUDPROBE_LIVE_WRITE=1 后执行真实测试目录读写")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	if profile == "" || bundle == "" {
		t.Fatal("缺少私有测试目录配置")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	bridgeFile := os.Getenv("CLOUDPROBE_LIVE_BRIDGE")
	if bridgeFile == "" {
		bridgeFile = filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll")
	}
	b, e := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), bridgeFile, filepath.Join(profile, "original-sdk"))
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	raw, e := b.Request(ctx, "GET", "/v1/drive/alltokens", []byte("{}"), nil)
	if e != nil {
		t.Fatal(e)
	}
	m, e := decodeDocument(raw)
	if e != nil {
		t.Fatal("凭据格式错误")
	}
	m, e = unwrap(m)
	if e != nil {
		t.Fatal("读取绑定失败")
	}
	all := list(m["wopan_credentials"])
	if len(all) == 0 {
		t.Fatal("无联通绑定")
	}
	cred := object(all[0])
	liveAccount := guiAccount{Key: "live", Provider: "wopan", credentials: cred}
	liveApp := &guiApp{data: profile, accounts: map[string]guiAccount{"live": liveAccount}}
	liveApp.applyDAVCredentials(liveApp.accounts)
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = valueString(cred["token"])
	cl, e := newClient(cfg, "")
	if e != nil {
		t.Fatal(e)
	}
	cl.bridge = b
	drive := newWopanDAVDrive(cl, cred, "0", func(access, refresh string) error { return liveApp.saveDAVCredential(liveAccount, access, refresh) })
	if e = drive.probe(ctx); e != nil {
		t.Fatal(cl.redact.Text(e.Error()))
	}
	fs := &davFS{mounts: []*davMount{{Alias: "联通云盘-1", Root: "0", Writable: true, Provider: "wopan", drive: drive}}, spool: filepath.Join(profile, "live-webdav-spool"), maxUpload: 64 << 20, cache: map[string]davCache{}}
	fs.transfers = newTransferRuntime(transferOptions{})
	drive.uploadBudget = fs.transfers.connections
	drive.uploadPartMiB = 5
	salt := []byte("1234567890123456")
	hash, _ := pbkdf2.Key(sha256.New, "test-password-123", salt, 100000, 32)
	h := &davService{fs: fs, handler: &webdav.Handler{Prefix: "/dav", FileSystem: fs, LockSystem: webdav.NewMemLS()}, username: "tester", salt: hex.EncodeToString(salt), passwordHash: hex.EncodeToString(hash), slots: make(chan struct{}, 4)}
	server := httptest.NewServer(h)
	defer server.Close()
	checks := []string{}
	dir := "CloudProbe-WebDAV-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	root := "/联通云盘-1/" + dir
	defer func() {
		result := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "provider": "wopan", "test_directory": dir, "checks_passed": checks, "success": !t.Failed()}
		data, _ := json.MarshalIndent(result, "", "  ")
		_ = os.WriteFile(filepath.Join(profile, "webdav-live-verification.json"), data, 0600)
	}()
	step := func(method, target, body string, want int, headers map[string]string) []byte {
		t.Helper()
		code, b, _ := davRequest(t, server.URL, method, target, strings.NewReader(body), headers)
		if code != want {
			t.Fatalf("%s returned %d: %s", method, code, cl.redact.Text(string(b)))
		}
		return b
	}
	step("MKCOL", root, "", 201, nil)
	checks = append(checks, "MKCOL")
	t.Log("真实云盘测试目录已创建")
	if os.Getenv("CLOUDPROBE_LIVE_DIR_ONLY") == "1" {
		step("MKCOL", root+"/source", "", 201, nil)
		step("MKCOL", root+"/target", "", 201, nil)
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + root + "/target/renamed"
		step("MOVE", root+"/source", "", 201, map[string]string{"Destination": u.String()})
		checks = append(checks, "directory rename + move")
		step("DELETE", root, "", 204, nil)
		checks = append(checks, "delete directory")
		t.Log("真实目录的新建、重命名、移动、删除已通过")
		return
	}
	textPath := root + "/说明 & 100%.txt"
	step("PUT", textPath, "CloudProbe 真实 WebDAV 测试\n第一版。", 201, nil)
	checks = append(checks, "PUT text")
	got := step("GET", textPath, "", 200, nil)
	if string(got) != "CloudProbe 真实 WebDAV 测试\n第一版。" {
		t.Fatal("文本下载内容不一致")
	}
	checks = append(checks, "GET text hash")
	step("PUT", textPath, "修改后保存的第二版。", 201, nil)
	got = step("GET", textPath, "", 200, nil)
	if string(got) != "修改后保存的第二版。" {
		t.Fatal("覆盖保存后的内容不一致")
	}
	checks = append(checks, "PUT overwrite + GET verify")
	t.Log("文本上传、下载、覆盖保存已通过")
	step("MKCOL", root+"/子目录", "", 201, nil)
	destination := func(target string) string { u, _ := url.Parse(server.URL); u.Path = "/dav" + target; return u.String() }
	step("MOVE", textPath, "", 201, map[string]string{"Destination": destination(root + "/子目录/已改名.txt")})
	checks = append(checks, "MOVE rename + directory move")
	step("COPY", root+"/子目录/已改名.txt", "", 201, map[string]string{"Destination": destination(root + "/副本.txt")})
	checks = append(checks, "COPY")
	got = step("GET", root+"/副本.txt", "", 200, nil)
	if string(got) != "修改后保存的第二版。" {
		t.Fatal("副本内容不一致")
	}
	code, body, headers := davRequest(t, server.URL, "LOCK", root+"/副本.txt", strings.NewReader(`<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`), map[string]string{"Depth": "0"})
	if code != 200 {
		t.Fatalf("LOCK %d %s", code, cl.redact.Text(string(body)))
	}
	step("PUT", root+"/副本.txt", "锁内保存。", 201, map[string]string{"If": "(" + headers.Get("Lock-Token") + ")"})
	step("UNLOCK", root+"/副本.txt", "", 204, map[string]string{"Lock-Token": headers.Get("Lock-Token")})
	checks = append(checks, "LOCK + save + UNLOCK")
	data := bytes.Repeat([]byte("CloudProbe-range-check-"), 530000)
	code, body, _ = davRequest(t, server.URL, "PUT", root+"/binary.bin", bytes.NewReader(data), nil)
	if code != 201 {
		t.Fatalf("binary PUT %d %s", code, cl.redact.Text(string(body)))
	}
	code, body, _ = davRequest(t, server.URL, "GET", root+"/binary.bin", nil, map[string]string{"Range": "bytes=10485770-10485869"})
	if code != 206 || !bytes.Equal(body, data[10485770:10485870]) {
		t.Fatalf("range failed %d %s", code, cl.redact.Text(string(body)))
	}
	checks = append(checks, "upload >10MiB + Range exact bytes")
	code, body, headers = davRequest(t, server.URL, "HEAD", root+"/binary.bin", nil, nil)
	if code != 200 || len(body) != 0 || headers.Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatal("HEAD incorrect")
	}
	checks = append(checks, "HEAD length")
	got = step("GET", root+"/binary.bin", "", 200, nil)
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatal("二进制文件下载 SHA-256 不一致")
	}
	checks = append(checks, "binary full download SHA256")
	t.Log("大于 10 MiB 的二进制上传、完整下载与 Range 比对已通过")
	step("DELETE", root+"/副本.txt", "", 204, nil)
	checks = append(checks, "DELETE file")
	step("DELETE", root, "", 204, nil)
	checks = append(checks, "DELETE test directory")
	fs.invalidate(fs.mounts[0])
	if _, e = fs.lookup(ctx, root); !os.IsNotExist(e) {
		t.Fatal("删除测试目录后仍能找到条目")
	}
	t.Log("仅本次创建的测试文件和目录已删除，原有文件未操作")
}

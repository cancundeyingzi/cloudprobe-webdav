package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

const quotaPropfind = `<d:propfind xmlns:d="DAV:"><d:prop><d:quota-used-bytes/><d:quota-available-bytes/><d:displayname/></d:prop></d:propfind>`

type quotaTestDrive struct {
	*fakeDAVDrive
	calls atomic.Int64
	read  func(context.Context) (davQuota, error)
}

func (d *quotaTestDrive) Quota(ctx context.Context) (davQuota, error) {
	d.calls.Add(1)
	return d.read(ctx)
}

func installQuotaTestDrive(fs *davFS, fake *fakeDAVDrive, q davQuota) *quotaTestDrive {
	d := &quotaTestDrive{fakeDAVDrive: fake, read: func(context.Context) (davQuota, error) { return q, nil }}
	fs.mounts[0].drive = d
	fs.mounts[0].Identity = "test-account-a"
	return d
}

func parseQuotaProps(t *testing.T, body []byte) map[string]map[string]string {
	t.Helper()
	var doc struct {
		Responses []struct {
			Href  string `xml:"href"`
			Stats []struct {
				Status string `xml:"status"`
				Prop   struct {
					Values []struct {
						XMLName xml.Name
						Text    string `xml:",chardata"`
					} `xml:",any"`
				} `xml:"prop"`
			} `xml:"propstat"`
		} `xml:"response"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		t.Fatalf("invalid multistatus: %v; %s", err, body)
	}
	result := map[string]map[string]string{}
	for _, response := range doc.Responses {
		properties := map[string]string{}
		for _, stat := range response.Stats {
			for _, p := range stat.Prop.Values {
				properties[p.XMLName.Space+p.XMLName.Local] = stat.Status + "|" + strings.TrimSpace(p.Text)
			}
		}
		result[response.Href] = properties
	}
	return result
}

func onlyQuotaProps(t *testing.T, body []byte) map[string]string {
	t.Helper()
	props := parseQuotaProps(t, body)
	if len(props) != 1 {
		t.Fatalf("expected one resource, got %d", len(props))
	}
	for _, p := range props {
		return p
	}
	panic("unreachable")
}

func TestQuotaPROPFINDDiscoveryAndAllprop(t *testing.T) {
	s, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{Total: 1000, Used: 250})
	root := "/联通云盘-1"
	for _, body := range []string{"", `<x:propfind xmlns:x="DAV:"><x:allprop/></x:propfind>`, `<x:propfind xmlns:x="DAV:"><x:propname/></x:propfind>`} {
		p := onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", root, body, 207, map[string]string{"Depth": "0"}))
		_, has := p["DAV:quota-used-bytes"]
		if has != strings.Contains(body, "propname") {
			t.Fatal("allprop/propname quota discovery mismatch", p)
		}
	}
	if d.calls.Load() != 0 {
		t.Fatal("discovery or ordinary listing fetched remote quota")
	}
	for _, body := range []string{quotaPropfind, `<d:propfind xmlns:d="DAV:"><d:allprop/><d:include><d:quota-used-bytes/><d:quota-available-bytes/></d:include></d:propfind>`} {
		p := onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", root, body, 207, map[string]string{"Depth": "0"}))
		if p["DAV:quota-used-bytes"] != "HTTP/1.1 200 OK|250" || p["DAV:quota-available-bytes"] != "HTTP/1.1 200 OK|750" {
			t.Fatal("incorrect quota values", p)
		}
	}
	if d.calls.Load() != 1 {
		t.Fatal("quota cache not shared across explicit and include requests", d.calls.Load())
	}
}

func TestQuotaUnavailableDoesNotBecomeZeroOrBreakOtherProperties(t *testing.T) {
	s, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{})
	d.read = func(context.Context) (davQuota, error) { return davQuota{}, errors.New("provider temporarily failed") }
	p := onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1", quotaPropfind, 207, map[string]string{"Depth": "0"}))
	if p["DAV:quota-used-bytes"] != "HTTP/1.1 502 Bad Gateway|" || p["DAV:quota-available-bytes"] != "HTTP/1.1 502 Bad Gateway|" || !strings.Contains(p["DAV:displayname"], "200 OK") {
		t.Fatal("quota errors were swallowed or blocked normal properties", p)
	}
	// Failure cooldown prevents repeated requests hammering a failing provider.
	wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1", quotaPropfind, 207, map[string]string{"Depth": "0"})
	if d.calls.Load() != 1 {
		t.Fatal("failure cooldown missing")
	}
	fs.invalidate(fs.mounts[0])
	d.read = func(context.Context) (davQuota, error) { return davQuota{Total: 100, Used: 100}, nil }
	p = onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1", quotaPropfind, 207, map[string]string{"Depth": "0"}))
	if p["DAV:quota-available-bytes"] != "HTTP/1.1 200 OK|0" {
		t.Fatal("real zero remaining capacity must be distinguishable from failure", p)
	}
}

func TestQuotaUnsupportedFilesAndNamespaces(t *testing.T) {
	s, fake, fs := testDAVService(t, true)
	p := onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1", quotaPropfind, 207, map[string]string{"Depth": "0"}))
	if p["DAV:quota-used-bytes"] != "HTTP/1.1 404 Not Found|" {
		t.Fatal("unsupported provider did not return property-level 404", p)
	}
	d := installQuotaTestDrive(fs, fake, davQuota{Total: 100, Used: 20})
	wantDAV(t, s.URL, "PUT", "/联通云盘-1/file.txt", "sample", 201, nil)
	p = onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1/file.txt", quotaPropfind, 207, map[string]string{"Depth": "0"}))
	if p["DAV:quota-used-bytes"] != "HTTP/1.1 404 Not Found|" || d.calls.Load() != 0 {
		t.Fatal("ordinary files queried account quota", p)
	}
	p = onlyQuotaProps(t, wantDAV(t, s.URL, "PROPFIND", "/联通云盘-1", `<d:propfind xmlns:d="DAV:" xmlns:q="urn:other"><d:prop><q:quota-used-bytes/></d:prop></d:propfind>`, 207, map[string]string{"Depth": "0"}))
	if p["urn:otherquota-used-bytes"] != "HTTP/1.1 404 Not Found|" || d.calls.Load() != 0 {
		t.Fatal("quota names matched without checking DAV namespace", p)
	}
}

func TestQuotaProtectedPropertyRejectsAtomicPROPPATCH(t *testing.T) {
	s, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{Total: 100, Used: 20})
	body := `<d:propertyupdate xmlns:d="DAV:" xmlns:x="urn:test"><d:set><d:prop><d:quota-used-bytes>1</d:quota-used-bytes><x:other>value</x:other></d:prop></d:set></d:propertyupdate>`
	response := wantDAV(t, s.URL, "PROPPATCH", "/联通云盘-1", body, 207, nil)
	p := onlyQuotaProps(t, response)
	if p["DAV:quota-used-bytes"] != "HTTP/1.1 403 Forbidden|" || p["urn:testother"] != "HTTP/1.1 424 Failed Dependency|" || !strings.Contains(string(response), "cannot-modify-protected-property") {
		t.Fatal("protected properties were not rejected atomically", p)
	}
	if d.calls.Load() != 0 || len(fake.entries) != 0 {
		t.Fatal("PROPPATCH performed cloud operations")
	}
}

func TestQuotaRootDeduplicationAndNoPartialAggregate(t *testing.T) {
	_, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{Total: 1000, Used: 250})
	fs.mounts = append(fs.mounts, &davMount{Alias: "同账号子目录", Root: "subdir", Provider: "wopan", Identity: "test-account-a", drive: d})
	other := &quotaTestDrive{fakeDAVDrive: fake, read: func(context.Context) (davQuota, error) { return davQuota{Total: 2000, Used: 1500}, nil }}
	fs.mounts = append(fs.mounts, &davMount{Alias: "另一账号", Provider: "mobile", Identity: "test-account-b", drive: other})
	u, a, err := fs.Quota(context.Background(), "/")
	if err != nil || u != 1750 || a != 1250 || d.calls.Load() != 1 || other.calls.Load() != 1 {
		t.Fatal("virtual root duplicated an account or mixed scopes", u, a, err)
	}
	other.read = func(context.Context) (davQuota, error) { return davQuota{}, errors.New("upstream failed") }
	fs.invalidate(fs.mounts[2])
	u, a, err = fs.Quota(context.Background(), "/")
	if err == nil || u != 0 || a != 0 {
		t.Fatal("partial quota total reported after an account failed", u, a, err)
	}
	fs.mounts[2].Identity = ""
	if fs.QuotaSupported("/") || !fs.QuotaSupported("/另一账号") {
		t.Fatal("unknown identity aggregated or independent mount lost support")
	}
}

func TestQuotaCacheSharedAcrossAliasesAndInvalidatedByWrites(t *testing.T) {
	s, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{Total: 1000, Used: 100})
	fs.mounts = append(fs.mounts, &davMount{Alias: "alias", Provider: "wopan", Identity: "test-account-a", Root: "0", Writable: true, drive: d})
	for _, name := range []string{"/联通云盘-1", "/alias"} {
		if _, _, err := fs.Quota(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
	if d.calls.Load() != 1 {
		t.Fatal("aliases did not share quota cache")
	}
	wantDAV(t, s.URL, "PUT", "/alias/new.txt", "sample", 201, nil)
	if _, _, err := fs.Quota(context.Background(), "/联通云盘-1"); err != nil || d.calls.Load() != 2 {
		t.Fatal("write through another alias did not invalidate quota", d.calls.Load(), err)
	}
}

func TestQuotaConcurrentQueriesAndCancellation(t *testing.T) {
	_, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{})
	entered, release := make(chan struct{}), make(chan struct{})
	d.read = func(context.Context) (davQuota, error) {
		close(entered)
		<-release
		return davQuota{Total: 100, Used: 10}, nil
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, _, err := fs.Quota(context.Background(), "/联通云盘-1"); err != nil {
				t.Error(err)
			}
		})
	}
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := fs.Quota(ctx, "/联通云盘-1"); !errors.Is(err, context.Canceled) {
		t.Fatal("queued quota query failed to cancel", err)
	}
	close(release)
	wg.Wait()
	if d.calls.Load() != 1 {
		t.Fatal("concurrent quota queries did not coalesce", d.calls.Load())
	}
}

func TestQuotaInFlightInvalidationAndOverflow(t *testing.T) {
	_, fake, fs := testDAVService(t, true)
	d := installQuotaTestDrive(fs, fake, davQuota{})
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d.read = func(context.Context) (davQuota, error) {
		if d.calls.Load() == 1 {
			close(entered)
			<-release
		}
		return davQuota{Total: 100, Used: 10}, nil
	}
	go func() { defer close(done); fs.Quota(context.Background(), "/联通云盘-1") }()
	<-entered
	fs.invalidate(fs.mounts[0])
	close(release)
	<-done
	fs.Quota(context.Background(), "/联通云盘-1")
	if d.calls.Load() != 2 {
		t.Fatal("pre-write in-flight quota repopulated fresh cache")
	}
	d.read = func(context.Context) (davQuota, error) {
		return davQuota{Total: math.MaxInt64, Used: math.MaxInt64}, nil
	}
	fs.invalidate(fs.mounts[0])
	fs.mounts = append(fs.mounts, &davMount{Alias: "other", Identity: "test-account-b", drive: d})
	if _, _, err := fs.Quota(context.Background(), "/"); err == nil {
		t.Fatal("aggregate integer overflow was accepted")
	}
	if (davQuota{Total: 100, Used: 110}).available() != 0 {
		t.Fatal("over-quota account returned negative available bytes")
	}
}

func TestQuotaProviderParsingUnitsAndInvalidResponses(t *testing.T) {
	for _, tc := range []struct {
		provider, method, raw string
		total, used           int64
		valid                 bool
	}{
		{"wopan", "", `{"usageInfo":{"byteTotalSize":"1000","byteUsedSize":200}}`, 1000, 200, true},
		{"wopan", "", `{"code":"0000","usageInfo":{"byteTotalSize":"1000","byteUsedSize":200}}`, 1000, 200, true},
		{"wopan", "", `{"usageInfo":{"byteTotalSize":"0","byteUsedSize":0}}`, 0, 0, true},
		{"wopan", "", `{"usageInfo":{"byteTotalSize":"9007199254740993","byteUsedSize":9007199254740992}}`, 9007199254740993, 9007199254740992, true},
		{"wopan", "", `{"usageInfo":{"totalSize":"1TB","usedSize":0}}`, 0, 0, false},
		{"wopan", "", `{"usageInfo":{"byteTotalSize":"1000"}}`, 0, 0, false},
		{"wopan", "", `{"usageInfo":{"byteTotalSize":1000,"byteUsedSize":null}}`, 0, 0, false},
		{"wopan", "", `{"usageInfo":{"byteTotalSize":"9223372036854775808","byteUsedSize":0}}`, 0, 0, false},
		{"wopan", "", `{"code":9999,"usageInfo":{"byteTotalSize":1000,"byteUsedSize":0}}`, 0, 0, false},
		{"mobile", "getDiskInfo", `<result resultCode="0"><diskInfo><diskSize>3072</diskSize><freeDiskSize>3069</freeDiskSize></diskInfo></result>`, 3072 << 20, 3 << 20, true},
		{"mobile", "getDiskInfo", `<result resultCode="0"><diskInfo><diskSize>10</diskSize><freeDiskSize>0</freeDiskSize></diskInfo></result>`, 10 << 20, 10 << 20, true},
		{"mobile", "getServiceDiskInfoV2", `<result resultCode="0"><serviceDiskInfo><service>1</service><usedSize>800</usedSize><totalSize>1024</totalSize></serviceDiskInfo></result>`, 1024 << 20, 800 << 20, true},
		{"mobile", "getDiskInfo", `<result resultCode="0"><diskInfo><diskSize>10</diskSize></diskInfo></result>`, 0, 0, false},
		{"mobile", "getDiskInfo", `<result resultCode="0"><diskInfo><diskSize>10</diskSize><freeDiskSize>11</freeDiskSize></diskInfo></result>`, 0, 0, false},
		{"mobile", "getDiskInfo", `<result resultCode="0"><diskInfo><diskSize>9000000000000</diskSize><freeDiskSize>0</freeDiskSize></diskInfo></result>`, 0, 0, false},
		{"mobile", "getServiceDiskInfoV2", `<result resultCode="0"><serviceDiskInfo><service>2</service><usedSize>800</usedSize><totalSize>1024</totalSize></serviceDiskInfo></result>`, 0, 0, false},
		{"mobile", "getServiceDiskInfoV2", `<result resultCode="0"><serviceDiskInfo><service>1</service><usedSize>1.5</usedSize><totalSize>1024</totalSize></serviceDiskInfo></result>`, 0, 0, false},
	} {
		var q davQuota
		var err error
		if tc.provider == "wopan" {
			q, err = parseWopanQuota([]byte(tc.raw))
		} else {
			q, err = parseMobileQuota([]byte(tc.raw), tc.method)
		}
		if (err == nil) != tc.valid || tc.valid && (q.Total != tc.total || q.Used != tc.used) {
			t.Errorf("%s %s: q=%+v err=%v", tc.provider, tc.raw, q, err)
		}
	}
}

func TestWopanQuotaDispatcherAndContext(t *testing.T) {
	cfg := defaultConfig("wopan")
	cfg.Transport, cfg.Wopan.AccessToken = "bridge", "fixture-token-012345678901234567"
	cl, _ := newClient(cfg, "")
	d := newWopanDAVDrive(cl, nil, "0", nil)
	var keys []string
	d.w.SetHttpClient(&http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		var request struct {
			Header struct {
				Key string `json:"key"`
			} `json:"header"`
		}
		json.NewDecoder(r.Body).Decode(&request)
		keys = append(keys, request.Header.Key)
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("quota request lost deadline")
		}
		data := `{"userId":"fixture-account"}`
		if request.Header.Key == wopan.KeyQueryCloudUsageInfo {
			data = `{"code":"0000","usageInfo":{"byteTotalSize":"1000","byteUsedSize":200}}`
		}
		body := `{"STATUS":"200","RSP":{"RSP_CODE":"0000","DATA":` + data + `}}`
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q, err := d.Quota(ctx)
	if err != nil || q.Total != 1000 || q.Used != 200 || strings.Join(keys, ",") != "AppQueryUser,QueryCloudUsageInfo" {
		t.Fatal("dispatcher quota flow failed", q, keys, err)
	}
}

type quotaBridgeFunc func(context.Context, string, string, []byte, map[string]string) ([]byte, error)

func (f quotaBridgeFunc) Request(ctx context.Context, m, p string, b []byte, h map[string]string) ([]byte, error) {
	return f(ctx, m, p, b, h)
}

func TestMobileQuotaProxyDirectAndAlternateMethod(t *testing.T) {
	cfg := defaultConfig("mobile")
	cfg.Transport = "bridge"
	cfg.Mobile = MobileConfig{AppID: "fixture", AppSecret: "fixture-secret", AccessToken: "fixture-token", DeviceID: "device"}
	cl, _ := newClient(cfg, "")
	var proxyCalls, directCalls int
	cl.bridge = quotaBridgeFunc(func(context.Context, string, string, []byte, map[string]string) ([]byte, error) {
		proxyCalls++
		return nil, errors.New("proxy unavailable")
	})
	directConfig := cfg
	directConfig.Transport = "direct"
	direct, _ := newClient(directConfig, "")
	direct.http.Transport = protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		directCalls++
		body := `<result resultCode="0"><unknown/></result>`
		if strings.HasSuffix(r.URL.Path, "/getServiceDiskInfoV2") {
			body = `<result resultCode="0"><serviceDiskInfo><service>1</service><usedSize>2</usedSize><totalSize>10</totalSize></serviceDiskInfo></result>`
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})
	d := &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}, direct: direct}
	q, err := d.Quota(context.Background())
	if err != nil || q.Total != 10<<20 || q.Used != 2<<20 || directCalls != 2 || proxyCalls != 2 {
		t.Fatal("mobile fallback failed", q, proxyCalls, directCalls, err)
	}
	b := &backupDAVDrive{primary: d}
	if q, err = b.Quota(context.Background()); err != nil || q.Total != 10<<20 {
		t.Fatal("OpenList wrapper swallowed quota capability", q, err)
	}
}

func TestRcloneWebDAVQuotaAbout(t *testing.T) {
	bin := os.Getenv("CLOUDPROBE_RCLONE")
	if bin == "" {
		t.Skip("设置 CLOUDPROBE_RCLONE 后执行真实第三方客户端容量查询")
	}
	s, fake, fs := testDAVService(t, true)
	installQuotaTestDrive(fs, fake, davQuota{Total: 1000000, Used: 123456})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "obscure", "test-password-123")
	hideChildWindow(cmd)
	pass, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "rclone.conf")
	os.WriteFile(config, []byte("[cp]\ntype = webdav\nurl = "+s.URL+"/dav/联通云盘-1/\nvendor = other\nuser = tester\npass = "+strings.TrimSpace(string(pass))+"\n"), 0600)
	cmd = exec.CommandContext(ctx, bin, "about", "cp:", "--json", "--config", config, "--retries", "1", "--low-level-retries", "1")
	hideChildWindow(cmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rclone about failed: %v %s", err, out)
	}
	var usage struct{ Total, Used, Free int64 }
	if err = json.Unmarshal(out, &usage); err != nil || usage.Total != 1000000 || usage.Used != 123456 || usage.Free != 876544 {
		t.Fatalf("rclone quota mismatch: %+v %v %s", usage, err, out)
	}
	t.Log("rclone about: total / used / free matched upstream quota")
}

func TestLiveWebDAVQuotaReadOnly(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUOTA") != "1" {
		t.Skip("仅显式启用时访问真实账号容量；不会读写网盘文件")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b, err := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll"), filepath.Join(profile, "original-sdk"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	g := &guiApp{bridge: b, data: profile, accounts: map[string]guiAccount{}}
	if identity, e := b.Call(ctx, map[string]any{"kind": "identity"}); e == nil {
		g.identity = MobileConfig{AppID: identity.AppID, AppSecret: identity.AppSecret}
	}
	g.runLog = newRuntimeLog(profile, g.redactDAV)
	defer g.runLog.close()
	if err = g.loadAccounts(ctx); err != nil {
		t.Fatal("读取已登录账号失败")
	}
	var reports []map[string]any
	for _, key := range g.accountOrder {
		a := g.accounts[key]
		cl, err := g.accountClient(key)
		if err != nil {
			t.Fatal("初始化账号客户端失败")
		}
		cl.folder = ""
		var drive davDrive
		if a.Provider == "wopan" {
			wd := newWopanDAVDrive(cl, a.credentials, "0", nil)
			t.Logf("联通官网备用授权存在：%v", wd.usingWeb)
			drive = wd
		} else {
			drive = &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}}
		}
		server, _, fs := testDAVService(t, false)
		fs.mounts = []*davMount{{Alias: "quota-test", Provider: a.Provider, Identity: davIdentity(a), drive: drive}}
		used, available, err := fs.Quota(newRunTrace(ctx, g.runLog, "只读容量验证", "/quota-test"), "/quota-test")
		if err != nil {
			t.Fatalf("%s 只读容量验证失败：%s", a.Provider, cl.redact.Text(err.Error()))
		}
		p := onlyQuotaProps(t, wantDAV(t, server.URL, "PROPFIND", "/quota-test", quotaPropfind, 207, map[string]string{"Depth": "0"}))
		if p["DAV:quota-used-bytes"] != "HTTP/1.1 200 OK|"+strconv.FormatInt(used, 10) || p["DAV:quota-available-bytes"] != "HTTP/1.1 200 OK|"+strconv.FormatInt(available, 10) {
			t.Fatal("真实账号容量未通过 WebDAV 正确返回")
		}
		// Values stay in a private report. Public output contains only outcomes.
		reports = append(reports, map[string]any{"provider": a.Provider, "used_bytes": used, "available_bytes": available, "read_only": true, "webdav_properties_match": true, "result": "PASS"})
		t.Logf("%s 真实账号容量查询 PASS；未读取文件名或正文，未写入网盘", a.Provider)
	}
	if len(reports) == 0 {
		t.Fatal("没有已登录网盘账号")
	}
	if err = writePrivateJSON(filepath.Join(profile, "quota-verification-private.json"), reports); err != nil {
		t.Fatal(err)
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLiveQuarkPhoneAccountSmoke(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_QUARK_PHONE") != "1" {
		t.Skip("显式选择手机号账号后，才验证专用目录")
	}
	app := &guiApp{data: os.Getenv("CLOUDPROBE_LIVE_QUARK_PROFILE"), accounts: map[string]guiAccount{}}
	app.quark = newQuarkManager(app)
	var client *quarkClient
	var err error
	if before := os.Getenv("CLOUDPROBE_LIVE_QUARK_BEFORE_PROFILE"); before != "" {
		raw, e := os.ReadFile(filepath.Join(before, "quark-accounts.bin"))
		if e != nil {
			t.Fatal(e)
		}
		plain, e := unprotectLocal(raw)
		if e != nil {
			t.Fatal(e)
		}
		var accounts quarkSavedAccounts
		if e = json.Unmarshal(plain, &accounts); e != nil {
			t.Fatal(e)
		}
		previous := map[string]bool{}
		for _, account := range accounts.Accounts {
			previous[account.UID] = true
		}
		app.quark.mu.Lock()
		candidates := []*quarkClient{}
		for _, q := range app.quark.clients {
			if !previous[q.snapshot().UID] {
				candidates = append(candidates, q)
			}
		}
		app.quark.mu.Unlock()
		if len(candidates) != 1 {
			t.Fatal("无法唯一选择新增手机号账号，未写入任何目录")
		}
		client = candidates[0]
	} else {
		client, err = quarkLiveClient(app.quark)
		if err != nil {
			t.Fatal(err)
		}
	}
	runtime := newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "memory", UploadRecoveryMode: "off"})
	drive := newQuarkDrive(client, runtime, 4, "auto")
	completionShape := map[string]any{}
	baseTransport := drive.bodyHTTP.Transport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}
	drive.bodyHTTP.Transport = phoneCompletionShapeTransport{base: baseTransport, shape: completionShape}
	server, _, fs := testDAVService(t, true)
	fs.transfers = runtime
	fs.mounts[0].drive = drive
	fs.mounts[0].Alias = "夸克测试"
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].UploadStorageMode = "disk"
	fs.mounts[0].Identity = davIdentityHash("quark", client.snapshot().UID)
	root := "/夸克测试/CloudProbe-phone-test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:8]
	report := map[string]any{"version": version, "phone_account_explicitly_selected": true, "test_path": root, "cleanup_ok": false, "completion_shape": completionShape}
	request := func(ctx context.Context, method, path string, body []byte, headers map[string]string) (int, []byte, error) {
		u, _ := url.Parse(server.URL)
		u.Path = "/dav" + path
		req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		req.SetBasicAuth("tester", "test-password-123")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		response, err := (&http.Client{Timeout: time.Minute}).Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		return response.StatusCode, data, err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, _, err := request(ctx, "DELETE", root, nil, nil)
		report["cleanup_ok"] = err == nil && (status == 204 || status == 404)
		if report["cleanup_ok"] != true {
			t.Error("手机号专用测试目录清理失败")
		}
		if path := os.Getenv("CLOUDPROBE_LIVE_QUARK_PHONE_REPORT"); path != "" {
			b, _ := json.MarshalIndent(report, "", "  ")
			os.MkdirAll(filepath.Dir(path), 0700)
			os.WriteFile(path, b, 0600)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	expect := func(method, path string, data []byte, want int, headers map[string]string) []byte {
		t.Helper()
		status, body, err := request(ctx, method, path, data, headers)
		if err != nil || status != want {
			t.Fatalf("phone account %s status=%d want=%d error=%v detail=%s", method, status, want, err, client.scrub(string(body[:min(len(body), 500)])))
		}
		return body
	}
	quota, err := drive.Quota(ctx)
	if err != nil {
		t.Fatal(err)
	}
	report["quota_total"] = quota.Total
	expect("MKCOL", root, nil, 201, nil)
	payload := bytes.Repeat([]byte("CloudProbe phone login verified\n"), 128)
	file := root + "/phone.txt"
	expect("PUT", file, payload, 201, nil)
	readback := expect("GET", file, nil, 200, nil)
	if !bytes.Equal(readback, payload) {
		t.Fatal("phone account SHA256 mismatch")
	}
	rangeData := expect("GET", file, nil, 206, map[string]string{"Range": "bytes=3-128"})
	if !bytes.Equal(rangeData, payload[3:129]) {
		t.Fatal("phone account range mismatch")
	}
	target, _ := url.Parse(server.URL)
	target.Path = "/dav" + root + "/renamed.txt"
	expect("MOVE", file, nil, 201, map[string]string{"Destination": target.String()})
	props := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:quota-used-bytes/><D:quota-available-bytes/></D:prop></D:propfind>`
	body := expect("PROPFIND", root, []byte(props), 207, map[string]string{"Depth": "0"})
	if !strings.Contains(string(body), "200 OK") {
		t.Fatal("phone account quota extension failed")
	}
	hash := sha256.Sum256(readback)
	report["sha256"] = hex.EncodeToString(hash[:])
	report["bytes"] = len(payload)
	report["status"] = "PASS"
}

type phoneCompletionShapeTransport struct {
	base  http.RoundTripper
	shape map[string]any
}

func (t phoneCompletionShapeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if request.Method == "POST" {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(body))
		t.shape["http_status"] = response.StatusCode
		if readErr == nil {
			if doc, e := quarkJSON(body); e == nil {
				t.shape["json_shape"] = phoneSafeJSONShape(doc, 0)
			} else {
				t.shape["format"] = "non-JSON"
			}
		}
	}
	return response, nil
}
func phoneSafeJSONShape(value any, depth int) any {
	if depth > 3 {
		return "..."
	}
	switch v := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, child := range v {
			valid := len(key) <= 64 && len(key) > 0
			for _, r := range key {
				if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
					valid = false
				}
			}
			if !valid {
				continue
			}
			out[key] = phoneSafeJSONShape(child, depth+1)
			if key == "status" || key == "code" || key == "success" || key == "finish" {
				switch x := child.(type) {
				case bool:
					out[key] = x
				case json.Number:
					out[key] = x
				case string:
					if len(x) < 16 {
						out[key] = x
					}
				}
			}
		}
		return out
	case []any:
		if len(v) > 0 {
			return []any{phoneSafeJSONShape(v[0], depth+1)}
		}
		return []any{}
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "bool"
	default:
		return "number"
	}
}

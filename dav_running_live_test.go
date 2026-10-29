package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLiveWebDAVAgainstRunningGUI(t *testing.T) {
	sessionFile := os.Getenv("CLOUDPROBE_LIVE_DAV_SESSION")
	if sessionFile == "" {
		t.Skip("显式设置本机 WebDAV 测试会话后才执行真实云盘读写")
	}
	var session struct {
		Username string `json:"username"`
		Password string `json:"password"`
		URL      string `json:"url"`
	}
	raw, e := os.ReadFile(sessionFile)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &session); e != nil {
		t.Fatal(e)
	}
	dir := "CloudProbe-WebDAV-Test-" + time.Now().Format("20060102-150405") + "-" + randomKey()[:6]
	root := "联通云盘-1/" + dir
	checks := []string{}
	defer func() {
		result := map[string]any{"version": version, "time": time.Now().Format(time.RFC3339), "provider": "wopan", "test_directory": dir, "checks_passed": checks, "success": !t.Failed()}
		b, _ := json.MarshalIndent(result, "", "  ")
		os.WriteFile(filepath.Join(filepath.Dir(sessionFile), "real-webdav-verification.json"), b, 0600)
	}()
	client := &http.Client{Timeout: 3 * time.Minute}
	endpoint := func(p string) string {
		u, _ := url.Parse(session.URL)
		u.Path = strings.TrimRight(u.Path, "/") + "/" + p
		return u.String()
	}
	request := func(method, p string, body []byte, want int, headers map[string]string) ([]byte, http.Header) {
		t.Helper()
		r, e := http.NewRequest(method, endpoint(p), bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		r.SetBasicAuth(session.Username, session.Password)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		resp, e := client.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		data, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
		}
		if resp.StatusCode != want {
			t.Fatalf("%s returned %d: %s", method, resp.StatusCode, data)
		}
		return data, resp.Header
	}
	request("MKCOL", root, nil, 201, nil)
	checks = append(checks, "MKCOL")
	t.Log("真实目录已创建")
	request("PUT", root+"/说明 & 100%.txt", []byte("第一版：WebDAV 真实内容。"), 201, nil)
	checks = append(checks, "PUT text")
	b, _ := request("GET", root+"/说明 & 100%.txt", nil, 200, nil)
	if string(b) != "第一版：WebDAV 真实内容。" {
		t.Fatal("text differs")
	}
	checks = append(checks, "GET text")
	request("PUT", root+"/说明 & 100%.txt", []byte("第二版：修改后保存。"), 201, nil)
	b, _ = request("GET", root+"/说明 & 100%.txt", nil, 200, nil)
	if string(b) != "第二版：修改后保存。" {
		t.Fatal("overwrite differs")
	}
	checks = append(checks, "overwrite + read back")
	t.Log("文本上传、下载和覆盖保存通过")
	request("MKCOL", root+"/子目录", nil, 201, nil)
	request("MOVE", root+"/说明 & 100%.txt", nil, 201, map[string]string{"Destination": endpoint(root + "/子目录/重命名.txt")})
	checks = append(checks, "MOVE rename + directory move")
	request("COPY", root+"/子目录/重命名.txt", nil, 201, map[string]string{"Destination": endpoint(root + "/副本.txt")})
	checks = append(checks, "COPY")
	_, headers := request("LOCK", root+"/副本.txt", []byte(`<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`), 200, map[string]string{"Depth": "0"})
	request("PUT", root+"/副本.txt", []byte("锁内写入。"), 201, map[string]string{"If": "(" + headers.Get("Lock-Token") + ")"})
	request("UNLOCK", root+"/副本.txt", nil, 204, map[string]string{"Lock-Token": headers.Get("Lock-Token")})
	checks = append(checks, "LOCK + PUT + UNLOCK")
	data := bytes.Repeat([]byte("CloudProbe-verify-"), 690000)
	request("PUT", root+"/binary.bin", data, 201, nil)
	checks = append(checks, fmt.Sprintf("binary upload %d bytes", len(data)))
	b, h := request("GET", root+"/binary.bin", nil, 206, map[string]string{"Range": "bytes=10485770-10485869"})
	if !bytes.Equal(b, data[10485770:10485870]) || h.Get("Content-Range") == "" {
		t.Fatal("range differs")
	}
	checks = append(checks, "Range exact bytes")
	b, h = request("HEAD", root+"/binary.bin", nil, 200, nil)
	if len(b) != 0 || h.Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatal("HEAD length differs")
	}
	checks = append(checks, "HEAD")
	b, _ = request("GET", root+"/binary.bin", nil, 200, nil)
	if sha256.Sum256(b) != sha256.Sum256(data) {
		t.Fatal("binary SHA256 differs")
	}
	checks = append(checks, "full binary SHA256")
	t.Log("大于 10 MiB 的二进制上传、完整下载和分段读取通过")
	request("DELETE", root+"/副本.txt", nil, 204, nil)
	checks = append(checks, "DELETE file")
	request("DELETE", root, nil, 204, nil)
	checks = append(checks, "DELETE test directory")
	t.Log("本次创建的测试目录已清理")
}

func TestLiveCleanupOwnedEmptyDirectories(t *testing.T) {
	file := os.Getenv("CLOUDPROBE_LIVE_DAV_SESSION")
	if file == "" {
		t.Skip("只在明确指定真实测试会话后清理本次测试的空目录")
	}
	var session struct {
		Username string `json:"username"`
		Password string `json:"password"`
		URL      string `json:"url"`
	}
	raw, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &session); e != nil {
		t.Fatal(e)
	}
	base, _ := url.Parse(session.URL)
	base.Path = strings.TrimRight(base.Path, "/") + "/联通云盘-1/"
	type response struct {
		Href  string `xml:"href"`
		Props []struct {
			Prop struct {
				Type struct {
					Collection *struct{} `xml:"collection"`
				} `xml:"resourcetype"`
			} `xml:"prop"`
		} `xml:"propstat"`
	}
	list := func(u *url.URL) []response {
		t.Helper()
		r, _ := http.NewRequest("PROPFIND", u.String(), nil)
		r.SetBasicAuth(session.Username, session.Password)
		r.Header.Set("Depth", "1")
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 207 {
			t.Fatal("cleanup listing status", resp.StatusCode)
		}
		var result struct {
			Responses []response `xml:"response"`
		}
		if e = xml.NewDecoder(resp.Body).Decode(&result); e != nil {
			t.Fatal(e)
		}
		return result.Responses
	}
	pattern := regexp.MustCompile(`^CloudProbe-WebDAV-Test-20261025-[0-9]{6}-[0-9a-f]{6}$`)
	removed, retained := 0, 0
	for _, entry := range list(base) {
		u, e := url.Parse(entry.Href)
		if e != nil {
			t.Fatal(e)
		}
		clean := strings.TrimRight(u.Path, "/")
		if path.Dir(clean) != strings.TrimRight(base.Path, "/") || !pattern.MatchString(path.Base(clean)) {
			continue
		}
		u.Scheme = base.Scheme
		u.Host = base.Host
		if len(list(u)) != 1 {
			retained++
			continue
		}
		r, _ := http.NewRequest("DELETE", u.String(), nil)
		r.SetBasicAuth(session.Username, session.Password)
		resp, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		if resp.StatusCode != 204 {
			t.Fatal("cleanup delete status", resp.StatusCode)
		}
		removed++
	}
	b, _ := json.MarshalIndent(map[string]any{"removed_empty_test_directories": removed, "retained_nonempty_directories": retained, "only_this_task_prefix": true}, "", "  ")
	os.WriteFile(filepath.Join(filepath.Dir(file), "cleanup-verification.json"), b, 0600)
	t.Logf("清理本任务产生的空测试目录 %d 个；保留非空目录 %d 个", removed, retained)
}

func TestLiveRcloneAgainstRunningGUI(t *testing.T) {
	bin, file := os.Getenv("CLOUDPROBE_RCLONE"), os.Getenv("CLOUDPROBE_LIVE_DAV_SESSION")
	if bin == "" || file == "" {
		t.Skip("显式指定真实服务会话及 rclone 后才执行")
	}
	var session struct {
		Username string `json:"username"`
		Password string `json:"password"`
		URL      string `json:"url"`
	}
	raw, e := os.ReadFile(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &session); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	obscure := exec.Command(bin, "obscure", session.Password)
	hideChildWindow(obscure)
	pass, e := obscure.Output()
	if e != nil {
		t.Fatal(e)
	}
	config := filepath.Join(dir, "rclone.conf")
	os.WriteFile(config, []byte("[cp]\ntype = webdav\nurl = "+session.URL+"\nvendor = other\nuser = "+session.Username+"\npass = "+strings.TrimSpace(string(pass))+"\n"), 0600)
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(bin, append(args, "--config", config, "--retries", "1", "--low-level-retries", "1", "--log-level", "ERROR")...)
		hideChildWindow(cmd)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("rclone %s failed: %v\n%s", args[0], e, out)
		}
		return string(out)
	}
	root := "cp:联通云盘-1/CloudProbe-WebDAV-Rclone-Test-" + time.Now().Format("20060102-150405")
	run("mkdir", root)
	local := filepath.Join(dir, "local.txt")
	os.WriteFile(local, []byte("真实云盘 rclone 第一版。"), 0600)
	run("copyto", local, root+"/说明.txt")
	if got := run("cat", root+"/说明.txt"); got != "真实云盘 rclone 第一版。" {
		t.Fatal("real download differs")
	}
	os.WriteFile(local, []byte("真实云盘 rclone 第二版。"), 0600)
	run("copyto", local, root+"/说明.txt", "--ignore-times")
	run("moveto", root+"/说明.txt", root+"/重命名.txt")
	run("copyto", root+"/重命名.txt", root+"/副本.txt")
	if got := run("cat", root+"/副本.txt"); got != "真实云盘 rclone 第二版。" {
		t.Fatal("real copy differs")
	}
	run("deletefile", root+"/副本.txt")
	run("purge", root)
	result := map[string]any{"client": "rclone 1.75.2", "provider": "wopan", "time": time.Now().Format(time.RFC3339), "success": true, "checks": []string{"mkdir", "upload", "download", "overwrite", "rename", "copy", "delete", "purge"}}
	b, _ := json.MarshalIndent(result, "", "  ")
	os.WriteFile(filepath.Join(filepath.Dir(file), "real-rclone-verification.json"), b, 0600)
	t.Log("rclone 对真实联通云盘完成读写闭环，测试目录已清理")
}

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in interoperability test with an actual third-party WebDAV client. The
// upstream in this test is isolated test data, never the user's cloud account.
func TestRcloneWebDAVInteroperability(t *testing.T) {
	bin := os.Getenv("CLOUDPROBE_RCLONE")
	if bin == "" {
		t.Skip("设置 CLOUDPROBE_RCLONE 后运行第三方客户端互操作测试")
	}
	server, _, _ := testDAVService(t, true)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "obscure", "test-password-123")
	hideChildWindow(cmd)
	obscured, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	config := filepath.Join(dir, "rclone.conf")
	if e = os.WriteFile(config, []byte("[cp]\ntype = webdav\nurl = "+server.URL+"/dav/\nvendor = other\nuser = tester\npass = "+strings.TrimSpace(string(obscured))+"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	run := func(args ...string) string {
		t.Helper()
		args = append(args, "--config", config, "--retries", "1", "--low-level-retries", "1", "--log-level", "ERROR")
		c := exec.CommandContext(ctx, bin, args...)
		hideChildWindow(c)
		out, e := c.CombinedOutput()
		if e != nil {
			t.Fatalf("rclone %s failed: %v\n%s", args[0], e, out)
		}
		return string(out)
	}
	root := "cp:联通云盘-1/第三方测试"
	run("mkdir", root)
	local := filepath.Join(dir, "local.txt")
	os.WriteFile(local, []byte("第一版 rclone 内容 0123456789"), 0600)
	run("copyto", local, root+"/说明.txt")
	if got := run("cat", root+"/说明.txt"); got != "第一版 rclone 内容 0123456789" {
		t.Fatal("third-party download differs")
	}
	os.WriteFile(local, []byte("第二版：修改后保存。"), 0600)
	run("copyto", local, root+"/说明.txt", "--ignore-times")
	if got := run("cat", root+"/说明.txt"); got != "第二版：修改后保存。" {
		t.Fatal("third-party overwrite differs")
	}
	run("moveto", root+"/说明.txt", root+"/重命名.txt")
	run("copyto", root+"/重命名.txt", root+"/复制.txt")
	if got := run("lsf", root); !strings.Contains(got, "复制.txt") || !strings.Contains(got, "重命名.txt") {
		t.Fatal("third-party listing incomplete", got)
	}
	run("deletefile", root+"/复制.txt")
	run("purge", root)
	t.Log("rclone: mkdir / PUT / GET / overwrite / MOVE / COPY / DELETE / purge passed against isolated test upstream")
}

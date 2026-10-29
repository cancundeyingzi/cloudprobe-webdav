package main

import (
	"context"
	"fmt"
	wopan "github.com/OpenListTeam/wopan-sdk-go"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestLiveDirectoryShapeSummary(t *testing.T) {
	if os.Getenv("CLOUDPROBE_LIVE_SHAPE") != "1" {
		t.Skip("显式启用后仅统计目录条目类型和字段，不输出用户文件名")
	}
	profile, bundle := os.Getenv("CLOUDPROBE_LIVE_PROFILE"), os.Getenv("CLOUDPROBE_LIVE_BUNDLE")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	b, e := startOriginalBridge(ctx, filepath.Join(bundle, "sdk"), filepath.Join(bundle, "bridge", "CloudProbe.Bridge.dll"), filepath.Join(profile, "original-sdk"))
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	g := &guiApp{bridge: b, data: profile, accounts: map[string]guiAccount{}}
	raw, e := g.call(ctx, "GET", "/v1/drive/alltokens", map[string]any{})
	if e != nil {
		t.Fatal("绑定读取失败")
	}
	items := list(raw["wopan_credentials"])
	if len(items) == 0 {
		t.Fatal("无绑定")
	}
	a := guiAccount{Key: "shape", Provider: "wopan", credentials: object(items[0])}
	g.accounts[a.Key] = a
	g.applyDAVCredentials(g.accounts)
	a = g.accounts[a.Key]
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = valueString(a.credentials["token"])
	cl, _ := newClient(cfg, "")
	cl.bridge = b
	d := newWopanDAVDrive(cl, a.credentials, "0", nil)
	if e = d.probe(ctx); e != nil {
		t.Fatal(cl.redact.Text(e.Error()))
	}
	var data map[string]any
	_, e = d.w.RequestWoHome(wopan.KeyQueryAllFiles, wopan.Json{"spaceType": "0", "parentDirectoryId": "0", "pageNum": 0, "pageSize": 100, "sortRule": 6, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, &data, woContext(ctx))
	if e != nil {
		t.Fatal(cl.redact.Text(e.Error()))
	}
	for _, field := range []string{"files", "systemDirs"} {
		counts := map[string]int{}
		shapes := map[string][]string{}
		for _, entry := range list(data[field]) {
			x := object(entry)
			typ := fmt.Sprintf("type=%s fileType=%s", valueString(x["type"]), valueString(x["fileType"]))
			counts[typ]++
			keys := []string{}
			for key := range x {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			shapes[typ] = keys
		}
		t.Logf("%s 类型计数：%v；字段结构：%v", field, counts, shapes)
	}
}

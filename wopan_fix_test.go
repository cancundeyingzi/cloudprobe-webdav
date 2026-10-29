package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestWopanObservedOmittedListIsQualifiedNoItemsResponse(t *testing.T) {
	actualShape := []byte(`{"code":0,"data":{"meta":{"code":"200","message":"OK"},"nextPage":false},"message":"成功"}`)
	p, e := parsePage("wopan", actualShape)
	if e != nil {
		t.Fatal(e)
	}
	if !p.ListOmitted || p.More == nil || *p.More || len(p.Items) != 0 || len(p.Notes) == 0 {
		t.Fatalf("bad qualified omission: %+v", p)
	}
	for _, s := range []string{
		`{"code":0,"data":{"meta":{"code":"200"},"nextPage":true}}`,
		`{"code":0,"data":{"meta":{"code":"200"}}}`,
		`{"code":0,"data":{"nextPage":false}}`,
		`{"code":0,"data":{"meta":{"code":"9999"},"nextPage":false}}`,
		`{"code":0,"data":{"meta":{"code":"200"},"nextPage":false,"unknownFiles":[1]}}`,
	} {
		if _, e := parsePage("wopan", []byte(s)); e == nil {
			t.Fatalf("unknown/error response swallowed: %s", s)
		}
	}
}

type rootVerifyingBridge struct {
	t        *testing.T
	response string
	calls    int
}

func (b *rootVerifyingBridge) Request(ctx context.Context, m, path string, params []byte, h map[string]string) ([]byte, error) {
	b.calls++
	var p map[string]any
	if e := json.Unmarshal(params, &p); e != nil {
		b.t.Fatal(e)
	}
	if p["directory_id"] != "0" {
		b.t.Fatalf("root ID must be 0, got %v", p["directory_id"])
	}
	return []byte(b.response), nil
}
func TestWopanRootAliasQueriesZeroAndRetainsArchivesAndVideos(t *testing.T) {
	bridge := &rootVerifyingBridge{t: t, response: `{"code":0,"data":{"meta":{"code":"200"},"nextPage":false,"fileInfoList":[{"id":"d","fid":"0","name":"测试目录","type":"0","fileType":"0"},{"id":"a","fid":"fa","name":"测试.7z.001","type":"1","fileType":"5"},{"id":"v","fid":"fv","name":"测试.mkv","type":"1","fileType":"2"}]}}`}
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "test-authorized-token"
	cl, e := newClient(cfg, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	cl.bridge = bridge
	r, e := cl.List(context.Background(), "root", "test", nil)
	if e != nil {
		t.Fatal(e)
	}
	if r.Directory != "0" || !r.Complete || len(r.Items) != 3 {
		t.Fatalf("bad root result: %+v", r)
	}
	if !r.Items[0].Directory || r.Items[0].Kind != "文件夹" {
		t.Fatal("numeric type=0 was not recognized as folder")
	}
	if r.Items[1].Kind != "压缩包（分卷）" || r.Items[2].Kind != "视频" {
		t.Fatalf("archive/video lost: %+v", r.Items)
	}
	if defaultDirectory("wopan") != "0" || defaultDirectory("mobile") != "root" {
		t.Fatal("provider root defaults mixed")
	}
}

func TestWopanOmittedListReportDoesNotClaimEmptyCloud(t *testing.T) {
	bridge := &rootVerifyingBridge{t: t, response: `{"code":0,"data":{"meta":{"code":"200"},"nextPage":false}}`}
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cfg.Wopan.AccessToken = "test-token"
	cl, e := newClient(cfg, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	cl.bridge = bridge
	r, e := cl.List(context.Background(), "0", "test", []string{"应存在.zip"})
	if e != nil {
		t.Fatal(e)
	}
	if !r.Complete || len(r.Items) != 0 || !strings.Contains(r.StopReason, "核对目录 ID") || !strings.Contains(r.StopReason, "不代表整个网盘为空") {
		t.Fatalf("misleading empty result: %+v", r)
	}
}

func TestWopanNumericDirectoryTypeWorksForNumberAndString(t *testing.T) {
	for _, kind := range []string{`0`, `"0"`} {
		raw := `{"code":0,"data":{"meta":{"code":"200"},"nextPage":false,"fileInfoList":[{"id":"folder","fid":"0","name":"文件夹","type":` + kind + `,"fileType":"0"}]}}`
		p, e := parsePage("wopan", []byte(raw))
		if e != nil || len(p.Items) != 1 || !p.Items[0].Directory {
			t.Fatalf("numeric folder parse failed: %+v %v", p, e)
		}
	}
}

func TestArchiveVolumeKindsIncludeGzipAndUnfilteredFiles(t *testing.T) {
	for _, name := range []string{"示例.zip.001", "示例.7z.002", "示例.rar.003", "示例.tar.gz.006", "示例.tar.001", "示例.xz.009"} {
		it := classify("wopan", Item{ID: "i", FID: "f", Name: name})
		if it.Kind != "压缩包（分卷）" || it.SubtitleCandidate {
			t.Fatalf("bad archive kind: %+v", it)
		}
	}
	it := classify("wopan", Item{ID: "i", FID: "f", Name: "普通文件.001"})
	if it.Kind == "压缩包（分卷）" {
		t.Fatal("不能只凭数字后缀推断为压缩分卷")
	}
}

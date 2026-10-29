package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
)

const demoText = "这是云盘测试工具的本地模拟文本。\n此内容没有来自真实云盘。\n"

func demoServer() *httptest.Server {
	var srv *httptest.Server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/download/text" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			fmt.Fprint(w, demoText)
			return
		}
		if r.URL.Path == "/richlifeApp/devapp/getdisk" {
			b, e := readBounded(r.Body, 1<<20)
			if e != nil {
				http.Error(w, "bad request", 400)
				return
			}
			doc, e := decodeDocument(b)
			if e != nil {
				http.Error(w, "bad xml", 400)
				return
			}
			get := object(doc["getDisk"])
			if valueString(get["contentType"]) != "0" || valueString(get["filterType"]) != "2" {
				http.Error(w, "wrong filter", 400)
				return
			}
			entries := []map[string]any{{"contentID": "video-1", "contentName": "电影.mkv", "contentSuffix": "mkv", "contentSize": 9000}, {"contentID": "subtitle-1", "contentName": "电影.srt", "contentSuffix": "srt", "contentSize": 120}, {"contentID": "text-1", "contentName": "说明.txt", "contentSuffix": "txt", "contentSize": len(demoText)}, {"contentID": "no-ext", "contentName": "无扩展文件", "contentSuffix": "", "contentSize": 4}}
			start := int(number(get["startNumber"])) - 1
			end := int(number(get["endNumber"]))
			if start < 0 || start > len(entries) {
				start = len(entries)
			}
			if end > len(entries) {
				end = len(entries)
			}
			if end < start {
				end = start
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"@resultCode": 0, "getDiskResult": map[string]any{"isCompleted": map[bool]string{true: "1", false: "0"}[end == len(entries)], "contentList": map[string]any{"@length": end - start, "contentInfo": entries[start:end]}}}})
			return
		}
		if r.URL.Path == "/richlifeApp/devapp/downloadRequest" {
			fmt.Fprintf(w, `{"result":{"@resultCode":0,"String":%q}}`, srv.URL+"/download/text?access_token=demo-download-secret")
			return
		}
		if r.URL.Path == "/v1/api/wopan/query_files" {
			page, _ := strconv.Atoi(r.URL.Query().Get("page"))
			size, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
			if page < 1 || size < 1 {
				http.Error(w, "missing paging", 400)
				return
			}
			entries := []map[string]any{{"id": "video-1", "fid": "fid-video", "name": "电影.mkv", "fileSize": 9000}, {"id": "subtitle-1", "fid": "fid-subtitle", "name": "电影.srt", "fileSize": 120}, {"id": "text-1", "fid": "fid-text", "name": "说明.txt", "fileSize": len(demoText)}, {"id": "no-ext", "fid": "fid-noext", "name": "无扩展文件", "fileSize": 4}}
			start := (page - 1) * size
			if start > len(entries) {
				start = len(entries)
			}
			end := start + size
			if end > len(entries) {
				end = len(entries)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"meta": map[string]any{"code": "200"}, "nextPage": end < len(entries), "fileInfoList": entries[start:end]}})
			return
		}
		if r.URL.Path == "/v1/api/wopan/query_file_info" {
			fmt.Fprint(w, `{"code":0,"data":{"meta":{"code":"200"},"fileInfoList":[{"id":"text-1","fid":"fid-text","name":"说明.txt"}]}}`)
			return
		}
		if r.URL.Path == "/v1/api/wopan/get_download_url" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"meta": map[string]any{"code": "200"}, "downLoadInfos": []any{map[string]any{"fid": "fid-text", "downloadUrl": srv.URL + "/download/text?sign=demo-download-secret"}}}})
			return
		}
		http.Error(w, "unknown demo route "+strings.TrimSpace(r.URL.Path), 404)
	})
	srv = httptest.NewServer(h)
	return srv
}

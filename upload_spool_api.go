package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (g *guiApp) spoolCleanupAPI(w http.ResponseWriter, r *http.Request) {
	dir := filepath.Join(g.data, "webdav-spool")
	if r.Method == "GET" {
		entries, err := os.ReadDir(filepath.Join(dir, "owners"))
		if err != nil && !os.IsNotExist(err) {
			g.fail(w, err)
			return
		}
		rows := []map[string]any{}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			id := strings.TrimSuffix(entry.Name(), ".json")
			if !validOwnerID(id) {
				continue
			}
			lease, err := lockUploadCheckpoint(filepath.Join(dir, "owners", "lease-"+id[:2]))
			if err != nil {
				rows = append(rows, map[string]any{"id": id, "state": "接收或上传中", "busy": true})
				continue
			}
			o, readErr := readSpoolOwner(dir, id)
			if readErr != nil {
				lease.Close()
				rows = append(rows, map[string]any{"id": id, "state": "正文清理记录损坏，需人工核验", "busy": true})
				continue
			}
			var size int64
			if info, e := os.Lstat(filepath.Join(dir, o.data.Name)); e == nil {
				size = info.Size()
			}
			rows = append(rows, map[string]any{"id": id, "target": o.data.Target, "size": size, "state": "待清理本地正文", "busy": false})
			lease.Close()
		}
		respondJSON(w, 200, map[string]any{"bodies": rows})
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var in struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&in) != nil || !validOwnerID(in.ID) {
		http.Error(w, "invalid body ID", 400)
		return
	}
	lease, err := lockUploadCheckpoint(filepath.Join(dir, "owners", "lease-"+in.ID[:2]))
	if err != nil {
		respondJSON(w, 409, map[string]any{"error": "正文正在使用，暂不能清理"})
		return
	}
	defer lease.Close()
	o, err := readSpoolOwner(dir, in.ID)
	if err == nil {
		err = o.cleanup()
	}
	if err != nil {
		g.fail(w, err)
		return
	}
	respondJSON(w, 200, map[string]any{"ok": true})
}

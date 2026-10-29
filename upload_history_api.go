package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func historyKey(id string) (string, bool) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 || !validOwnerID(parts[0]) || !validOwnerID(parts[1]) {
		return "", false
	}
	return parts[0], true
}
func (g *guiApp) checkpointHistoryAPI(w http.ResponseWriter, r *http.Request) {
	resume := filepath.Join(g.data, "webdav-spool", "resume")
	dir := filepath.Join(resume, "history")
	if r.Method == "GET" {
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			g.fail(w, err)
			return
		}
		rows := []map[string]any{}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".bin") {
				continue
			}
			id := strings.TrimSuffix(entry.Name(), ".bin")
			key, ok := historyKey(id)
			if !ok {
				continue
			}
			lease, err := lockUploadCheckpoint(filepath.Join(resume, "lease-"+key[:2]))
			if err != nil {
				rows = append(rows, map[string]any{"id": id, "state": "关联任务正在使用", "busy": true})
				continue
			}
			h, err := readCheckpointHistory(filepath.Join(dir, entry.Name()))
			lease.Close()
			if err != nil || h.Data.Key != key {
				rows = append(rows, map[string]any{"id": id, "state": "历史记录无法读取，需人工核验", "busy": true})
				continue
			}
			rows = append(rows, map[string]any{"id": id, "target": h.Data.Target, "provider": h.Data.Provider, "state": h.State, "stage": h.Data.Stage, "backup": h.Data.Backup, "parent": h.Data.Parent})
		}
		respondJSON(w, 200, map[string]any{"history": rows})
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var in struct {
		ID       string `json:"id"`
		Reviewed bool   `json:"reviewed"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&in) != nil || !in.Reviewed {
		http.Error(w, "explicit review required", 400)
		return
	}
	key, ok := historyKey(in.ID)
	if !ok {
		http.Error(w, "invalid history ID", 400)
		return
	}
	lease, err := lockUploadCheckpoint(filepath.Join(resume, "lease-"+key[:2]))
	if err != nil {
		respondJSON(w, 409, map[string]any{"error": "关联任务正在使用，稍后再处理历史记录"})
		return
	}
	defer lease.Close()
	name := filepath.Join(dir, in.ID+".bin")
	h, err := readCheckpointHistory(name)
	if err != nil {
		g.fail(w, err)
		return
	}
	if h.Data.Key != key {
		http.Error(w, "history mismatch", 409)
		return
	}
	if err = os.Remove(name); err != nil {
		g.fail(w, err)
		return
	}
	respondJSON(w, 200, map[string]any{"ok": true})
}

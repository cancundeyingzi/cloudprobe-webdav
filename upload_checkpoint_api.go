package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type activeCheckpoint struct {
	checkpoint *uploadCheckpoint
	cancel     context.CancelFunc
	retired    bool
}

var activeCheckpoints sync.Map

func readUploadCheckpoint(file string) (*uploadCheckpoint, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (5<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 5<<20 {
		return nil, os.ErrInvalid
	}
	b, err = unprotectLocal(b)
	if err != nil {
		return nil, err
	}
	c := &uploadCheckpoint{file: file}
	if err = json.Unmarshal(b, &c.data); err != nil {
		return nil, err
	}
	if c.data.Version != 1 || c.data.Parts == nil || filepath.Base(file) != c.data.Key+".bin" {
		return nil, os.ErrInvalid
	}
	c.restoreVersions()
	return c, nil
}

func (g *guiApp) checkpointAPI(w http.ResponseWriter, r *http.Request) {
	dir := filepath.Join(g.data, "webdav-spool", "resume")
	if r.Method == "GET" {
		rows := []map[string]any{}
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			g.fail(w, err)
			return
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".bin") {
				continue
			}
			key := strings.TrimSuffix(entry.Name(), ".bin")
			if len(key) != 64 {
				continue
			}
			if _, err := hex.DecodeString(key); err != nil {
				continue
			}
			file := filepath.Join(dir, entry.Name())
			var c *uploadCheckpoint
			active, ok := activeCheckpoints.Load(file)
			if ok {
				a := active.(*activeCheckpoint)
				c = a.checkpoint
				c.mu.Lock()
				if a.retired || c.closed || c.activity != a {
					c.mu.Unlock()
					continue
				}
			} else {
				lease, lockErr := lockUploadCheckpoint(filepath.Join(dir, "lease-"+key[:2]))
				if lockErr != nil {
					rows = append(rows, map[string]any{"id": key, "state": "任务正在准备或提交"})
					continue
				}
				c, err = readUploadCheckpoint(file)
				_ = lease.Close()
				if err != nil {
					var pathError *os.PathError
					rows = append(rows, map[string]any{"id": key, "state": "记录无法读取，需手动处理", "corrupt": true, "can_forget": !errors.As(err, &pathError)})
					continue
				}
			}
			state := "等待客户端重传并续传云端"
			if ok {
				state = "云端上传中"
			}
			if c.data.Uploaded != nil {
				state = "正文已上传，待提交核验"
			}
			if c.data.Paused {
				state = "已暂停"
			}
			if c.data.Invalid && !c.data.Paused {
				state = "内容或会话已变化，等待完整重传重建"
			}
			var session struct{ PartSize int64 }
			_ = json.Unmarshal(c.data.Session, &session)
			var confirmed int64
			for index := range c.data.Parts {
				if index > 0 && session.PartSize > 0 {
					confirmed += max(int64(0), min(session.PartSize, c.data.Size-(index-1)*session.PartSize))
				}
			}
			rows = append(rows, map[string]any{"id": key, "target": c.data.Target, "provider": c.data.Provider, "size": c.data.Size, "confirmed": confirmed, "parts": len(c.data.Parts), "state": state, "updated": c.data.Updated, "paused": c.data.Paused, "mode": c.data.Mode})
			if ok {
				c.mu.Unlock()
			}
		}
		respondJSON(w, 200, map[string]any{"tasks": rows})
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var input struct {
		ID     string `json:"id"`
		Action string `json:"action"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&input) != nil || len(input.ID) != 64 {
		http.Error(w, "invalid task", 400)
		return
	}
	if _, err := hex.DecodeString(input.ID); err != nil {
		http.Error(w, "invalid task", 400)
		return
	}
	if input.Action != "pause" && input.Action != "resume" && input.Action != "reset" && input.Action != "forget" {
		http.Error(w, "invalid action", 400)
		return
	}
	file := filepath.Join(dir, input.ID+".bin")
	if value, ok := activeCheckpoints.Load(file); ok {
		a := value.(*activeCheckpoint)
		if input.Action != "pause" {
			respondJSON(w, 409, map[string]any{"error": "任务仍在上传，请先暂停并等待当前请求结束"})
			return
		}
		err := a.pause()
		if err != nil {
			if errors.Is(err, errCheckpointRetired) {
				respondJSON(w, 409, map[string]any{"error": err.Error()})
				return
			}
			g.fail(w, err)
			return
		}
		respondJSON(w, 200, map[string]any{"ok": true})
		return
	}
	lease, err := lockUploadCheckpoint(filepath.Join(dir, "lease-"+input.ID[:2]))
	if err != nil {
		respondJSON(w, 409, map[string]any{"error": "任务正在使用，请稍后重试"})
		return
	}
	defer lease.Close()
	if input.Action == "forget" {
		_, readErr := readUploadCheckpoint(file)
		var pathError *os.PathError
		if readErr == nil {
			respondJSON(w, 409, map[string]any{"error": "记录仍可读取，请使用放弃续传以清理关联正文"})
			return
		}
		if errors.As(readErr, &pathError) {
			g.fail(w, readErr)
			return
		}
		err = os.Remove(file)
	} else {
		var c *uploadCheckpoint
		c, err = readUploadCheckpoint(file)
		if err == nil {
			if input.Action == "reset" {
				err = c.discard()
			} else {
				c.data.Paused = input.Action == "pause"
				err = c.saveLocked()
			}
		}
	}
	if err != nil {
		g.fail(w, err)
		return
	}
	respondJSON(w, 200, map[string]any{"ok": true})
}

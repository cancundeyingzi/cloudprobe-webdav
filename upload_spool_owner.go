package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Body ownership is durable before any client bytes are accepted, independently
// of the resumable cloud task which may not exist until the body is complete.
type spoolOwnerData struct {
	Version int       `json:"version"`
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Target  string    `json:"target"`
	Created time.Time `json:"created"`
}
type spoolOwner struct {
	dir, file string
	data      spoolOwnerData
	lease     *os.File
}

func validOwnerID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}
func readSpoolOwner(dir, id string) (*spoolOwner, error) {
	if !validOwnerID(id) {
		return nil, os.ErrInvalid
	}
	name := filepath.Join(dir, "owners", id+".json")
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil {
		return nil, err
	}
	if len(b) > 65536 {
		return nil, os.ErrInvalid
	}
	o := &spoolOwner{dir: dir, file: name}
	if err = json.Unmarshal(b, &o.data); err != nil {
		return nil, err
	}
	if o.data.Version != 1 || o.data.ID != id || o.data.Name != "upload-"+id {
		return nil, os.ErrInvalid
	}
	return o, nil
}
func newOwnedSpool(ctx context.Context, dir, target string) (*os.File, *spoolOwner, error) {
	owners := filepath.Join(dir, "owners")
	if err := os.MkdirAll(owners, 0700); err != nil {
		return nil, nil, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return nil, nil, err
		}
		id := hex.EncodeToString(entropy[:])
		lease, err := lockUploadCheckpoint(filepath.Join(owners, "lease-"+id[:2]))
		if err != nil {
			continue
		}
		catalog, err := lockCheckpointCatalog(ctx, filepath.Join(owners, "catalog-lease"))
		if err != nil {
			lease.Close()
			return nil, nil, err
		}
		entries, err := os.ReadDir(owners)
		count := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".json") {
				count++
			}
		}
		if err != nil || count >= 1024 {
			catalog.Close()
			lease.Close()
			if err == nil {
				err = errors.New("正文清理记录达到 1024 个上限，请先清理待处理正文")
			}
			return nil, nil, err
		}
		o := &spoolOwner{dir: dir, file: filepath.Join(owners, id+".json"), lease: lease, data: spoolOwnerData{Version: 1, ID: id, Name: "upload-" + id, Target: target, Created: time.Now().UTC()}}
		if err = writePrivateJSON(o.file, o.data); err != nil {
			catalog.Close()
			lease.Close()
			return nil, nil, err
		}
		catalog.Close()
		body, err := os.OpenFile(filepath.Join(dir, o.data.Name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			_ = os.Remove(o.file)
			lease.Close()
			return nil, nil, err
		}
		return body, o, nil
	}
	return nil, nil, errors.New("正文清理锁繁忙，请稍后重试")
}
func (o *spoolOwner) cleanup() error {
	if err := removeSpoolBody(o.dir, o.data.Name, ""); err != nil {
		return err
	}
	if err := os.Remove(o.file); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
func cleanupRegisteredSpool(dir, name, keep string) error {
	if name != "" && !validSpoolName(name) {
		return errors.New("续传记录的暂存文件名无效，保留记录等待核验")
	}
	if name == keep || name == "" {
		return removeSpoolBody(dir, name, keep)
	}
	id := strings.TrimPrefix(name, "upload-")
	if validOwnerID(id) {
		if _, err := os.Stat(filepath.Join(dir, "owners", id+".json")); err == nil {
			lease, err := lockUploadCheckpoint(filepath.Join(dir, "owners", "lease-"+id[:2]))
			if err != nil {
				return err
			}
			defer lease.Close()
			o, err := readSpoolOwner(dir, id)
			if err != nil {
				return err
			}
			return o.cleanup()
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return removeSpoolBody(dir, name, keep)
}

func (d *davFile) finishSpool() error {
	closeErr := d.tmp.Close()
	var cleanupErr error
	if d.spoolOwner != nil {
		cleanupErr = d.spoolOwner.cleanup()
		_ = d.spoolOwner.lease.Close()
	} else {
		cleanupErr = os.Remove(d.tmp.Name())
		if os.IsNotExist(cleanupErr) {
			cleanupErr = nil
		}
	}
	if closeErr != nil {
		logDAV(d.ctx, runLogEntry{Operation: "关闭本地正文句柄", Source: "本机", Result: "关闭返回错误", Error: closeErr.Error()})
	}
	return cleanupErr
}

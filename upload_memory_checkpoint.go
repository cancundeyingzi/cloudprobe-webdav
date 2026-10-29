package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// Read each replayed part through the bounded memory source, even if the cloud
// already acknowledged it. This also keeps the sequential producer advancing.
func prepareCheckpointPart(ctx context.Context, source io.ReaderAt, index, off, n int64) (string, bool, error) {
	cp := checkpointFrom(ctx)
	if cp == nil {
		return "", false, nil
	}
	if cp.data.Mode == "memory" {
		h := sha256.New()
		if _, err := copyWithContext(ctx, h, uploadSection(ctx, source, off, n)); err != nil {
			return "", false, err
		}
		digest := hex.EncodeToString(h.Sum(nil))
		cp.mu.Lock()
		defer cp.mu.Unlock()
		if cp.data.Hashes == nil {
			return "", false, errors.New("内存续传记录缺少分片校验表")
		}
		if cp.data.Invalid {
			return "", false, errors.New("上传内容或会话已变化，需要客户端重新上传")
		}
		prior, exists := cp.data.Hashes[index]
		if exists && prior != digest {
			cp.data.Invalid = true
			if err := cp.saveLocked(); err != nil {
				return "", false, err
			}
			return "", false, &davStatusError{Code: 409, Message: "重传内容与旧会话不一致，已停止混合上传；请重新提交完整文件以创建新会话"}
		}
		if !exists {
			_, confirmed := cp.data.Parts[index]
			if cp.data.Uploaded != nil || confirmed {
				return "", false, errors.New("已完成内存任务缺少分片校验，拒绝复用")
			}
			cp.data.Hashes[index] = digest
			if err := cp.saveLocked(); err != nil {
				delete(cp.data.Hashes, index)
				return "", false, err
			}
		}
		tag, reused := cp.data.Parts[index]
		return tag, reused, nil
	}
	tag, reused := cp.part(index)
	return tag, reused, nil
}

func releaseCheckpointPart(source io.ReaderAt, off, n int64) {
	if s, ok := source.(interface{ ConfirmPart(int64, int64) }); ok {
		s.ConfirmPart(off, n)
	}
}

func (s *memoryUploadSource) drainCheckpoint(ctx context.Context) error {
	for off := int64(0); off < s.size; off += s.partSize {
		n := min(s.partSize, s.size-off)
		if checkpointFrom(ctx) != nil {
			if _, _, err := prepareCheckpointPart(ctx, s, off/s.partSize+1, off, n); err != nil {
				return err
			}
		} else if _, err := copyWithContext(ctx, io.Discard, uploadSection(ctx, s, off, n)); err != nil {
			return err
		}
		s.ConfirmPart(off, n)
	}
	return s.waitInput(ctx)
}

func (s *memoryUploadSource) waitInput(ctx context.Context) error {
	for {
		s.mu.Lock()
		done, err, changed := s.inputDone, s.err, s.changed
		s.mu.Unlock()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (d *davFile) finishMemoryCheckpoint(entry davEntry) error {
	cp := checkpointFrom(d.ctx)
	if cp == nil {
		return d.commitUpload(entry, d.memory.size, d.original)
	}
	cp.mu.Lock()
	if cp.data.Uploaded == nil {
		cp.data.Uploaded = &entry
		cp.data.UploadedVersion = entry.ContentVersion
	}
	err := cp.saveLocked()
	cp.mu.Unlock()
	if err != nil {
		return err
	}
	entry, err = d.uploadWithCheckpoint(cp) // verifies the durable object; never uploads a body
	if err != nil {
		return err
	}
	if d.committed == nil {
		err = d.commitUpload(entry, d.memory.size, d.original)
	}
	if err == nil {
		if e := os.Remove(cp.file); e != nil && !os.IsNotExist(e) {
			logDAV(d.ctx, runLogEntry{Operation: "清理续传记录", Source: "本机", Result: "待清理", Error: e.Error()})
		}
	}
	return err
}

func invalidateMemorySession(ctx context.Context, drive davDrive, parent string, err error) error {
	cp := checkpointFrom(ctx)
	var expired *quarkPartError
	if cp != nil && errors.As(err, &expired) && expired.Code == "NoSuchUpload" && expired.HTTP.code == 404 {
		entries, checkErr := drive.List(directoryLogContext(ctx), parent)
		if checkErr != nil {
			return fmt.Errorf("云端会话结果未能核验，保留续传记录：%w", checkErr)
		}
		entry, checkErr := uniqueNamedEntry(entries, cp.data.Stage)
		if checkErr != nil {
			return checkErr
		}
		if entry != nil {
			return errors.New("云端临时对象已存在但尚未确认内容，保留记录，未重建会话")
		}
		cp.mu.Lock()
		cp.data.Invalid = true
		saveErr := cp.saveLocked()
		cp.mu.Unlock()
		if saveErr != nil {
			return saveErr
		}
		return fmt.Errorf("云端会话已失效，正文未落盘；请客户端重新提交完整文件重建会话：%w", err)
	}
	return err
}

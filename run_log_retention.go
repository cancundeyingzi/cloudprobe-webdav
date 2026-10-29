package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var runtimeLogName = regexp.MustCompile(`^\d{8}-\d{6}-[0-9a-f]{8}(-\d{4,})?\.jsonl$`)

func (l *runtimeLog) openLocked(name string) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	l.file, l.path, l.fileBytes = f, name, 0
	l.segments = append(l.segments, name)
	return nil
}

func (l *runtimeLog) rotateLocked() error {
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			l.file = nil
			return err
		}
		l.file = nil
	}
	l.segment++
	file := filepath.Join(filepath.Dir(l.path), fmt.Sprintf("%s-%04d.jsonl", l.runID, l.segment))
	return l.openLocked(file)
}

func (l *runtimeLog) checkRetentionLocked(rotating bool) bool {
	ready, err := l.pruneLocked(rotating)
	l.retentionErr = ""
	if err != nil {
		l.retentionErr = err.Error()
		l.retentionRetry = time.Now().Add(time.Second)
	} else {
		l.retentionRetry = time.Time{}
	}
	l.diskPaused = !ready
	return ready
}

// Only files created by this logger are eligible for automatic retention.
// Reserve the new segment BEFORE opening it, so a blocked deletion cannot keep
// adding empty segments. Cleanup warnings do not stop writes while space exists.
func (l *runtimeLog) pruneLocked(rotating bool) (bool, error) {
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return false, err
	}
	type logFile struct {
		path string
		info os.FileInfo
	}
	var files []logFile
	var total int64
	present := make(map[string]bool)
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !runtimeLogName.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		name := filepath.Join(filepath.Dir(l.path), entry.Name())
		files = append(files, logFile{name, info})
		present[name] = true
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].info.ModTime().Equal(files[j].info.ModTime()) {
			return strings.TrimSuffix(files[i].path, ".jsonl") < strings.TrimSuffix(files[j].path, ".jsonl")
		}
		return files[i].info.ModTime().Before(files[j].info.ModTime())
	})
	count := len(files)
	// Reserve room for the current segment to grow to its maximum size.
	reserve := max(int64(0), l.maxFileBytes-l.fileBytes)
	if rotating || !present[l.path] {
		count++
		reserve = l.maxFileBytes
	}
	var cleanupErr error
	for _, file := range files {
		if file.path == l.path {
			continue
		}
		if count <= l.maxFiles && total+reserve <= l.maxTotalBytes && time.Since(file.info.ModTime()) <= 7*24*time.Hour {
			continue
		}
		if err := os.Remove(file.path); err != nil && !os.IsNotExist(err) {
			if cleanupErr == nil {
				cleanupErr = fmt.Errorf("运行日志保留清理失败：%w", err)
			}
			continue
		}
		delete(present, file.path)
		total -= file.info.Size()
		count--
	}
	kept := l.segments[:0]
	for _, path := range l.segments {
		if present[path] {
			kept = append(kept, path)
		}
	}
	l.segments = kept
	if count > l.maxFiles || total+reserve > l.maxTotalBytes {
		return false, errors.Join(cleanupErr, fmt.Errorf("运行日志已达到保留上限，暂缓落盘并自动重试清理"))
	}
	return true, cleanupErr
}

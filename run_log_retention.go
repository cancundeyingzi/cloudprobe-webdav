package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

var runtimeLogName = regexp.MustCompile(`^\d{8}-\d{6}-[0-9a-f]{8}(-\d{4,})?\.jsonl$`)

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
	f, err := os.OpenFile(file, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	l.file, l.path, l.fileBytes = f, file, 0
	l.segments = append(l.segments, file)
	return l.pruneLocked()
}

// Only files created by this logger are eligible for automatic retention.
func (l *runtimeLog) pruneLocked() error {
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return err
	}
	type logFile struct {
		path string
		info os.FileInfo
	}
	var files []logFile
	var total int64
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !runtimeLogName.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		name := filepath.Join(filepath.Dir(l.path), entry.Name())
		files = append(files, logFile{name, info})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().Before(files[j].info.ModTime()) })
	count := len(files)
	// Reserve room for the current segment to grow to its maximum size.
	reserve := max(int64(0), l.maxFileBytes-l.fileBytes)
	for _, file := range files {
		if file.path == l.path {
			continue
		}
		if count <= l.maxFiles && total+reserve <= l.maxTotalBytes && time.Since(file.info.ModTime()) <= 7*24*time.Hour {
			continue
		}
		if err := os.Remove(file.path); err != nil {
			return fmt.Errorf("运行日志保留清理失败：%w", err)
		}
		total -= file.info.Size()
		count--
	}
	kept := l.segments[:0]
	for _, path := range l.segments {
		if _, err := os.Stat(path); err == nil {
			kept = append(kept, path)
		}
	}
	l.segments = kept
	if count > l.maxFiles || total+reserve > l.maxTotalBytes {
		return fmt.Errorf("运行日志已达到保留上限")
	}
	return nil
}

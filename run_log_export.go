package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var runtimeLogExportName = regexp.MustCompile(`^export-\d{8}-\d{6}-[0-9a-f]{8}\.tmp$`)

// Snapshots have their own single-export budget (at most maxTotalBytes). Remove
// abandoned snapshots on startup and before export; never touch unrelated files.
func clearRuntimeLogExports(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !runtimeLogExportName.MatchString(entry.Name()) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("日志导出快照清理失败：%w", err)
		}
	}
	return nil
}

// Caller holds exportMu until the HTTP response is finished. Only bounded local
// copying holds mu; no source log handle survives into the network transfer.
func (l *runtimeLog) exportSnapshot(ctx context.Context) (snapshot *os.File, size int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	defer func() {
		l.exportErr = ""
		if err != nil {
			l.exportErr = err.Error()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, 0, err
	}
	if l.file == nil {
		return nil, 0, fmt.Errorf("运行日志文件不可用")
	}
	dir := filepath.Dir(l.path)
	if err = clearRuntimeLogExports(dir); err != nil {
		return nil, 0, err
	}
	file, err := os.OpenFile(filepath.Join(dir, "export-"+l.runID+".tmp"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err != nil {
			_ = file.Close()
			if cleanupErr := os.Remove(file.Name()); cleanupErr != nil && !os.IsNotExist(cleanupErr) {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	for _, name := range l.segments {
		n, copyErr := copyLogSegment(ctx, file, name, l.maxTotalBytes-size)
		if copyErr != nil {
			return nil, 0, copyErr
		}
		size += n
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	return file, size, nil
}

func copyLogSegment(ctx context.Context, dst io.Writer, name string, budget int64) (int64, error) {
	source, err := os.Open(name)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() > budget {
		return 0, fmt.Errorf("日志导出超过快照容量上限")
	}
	n, err := copyWithContext(ctx, dst, io.NewSectionReader(source, 0, info.Size()))
	if err == nil && n != info.Size() {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (l *runtimeLog) closeExportSnapshot(file *os.File) {
	closeErr := file.Close()
	removeErr := os.Remove(file.Name())
	if os.IsNotExist(removeErr) {
		removeErr = nil
	}
	if err := errors.Join(closeErr, removeErr); err != nil {
		l.mu.Lock()
		l.exportErr = "日志导出快照清理失败：" + err.Error()
		l.mu.Unlock()
	}
}

package main

import (
	"context"
	"errors"
	"time"
)

type logDiskWork struct {
	data    []byte
	barrier chan struct{}
}
type logDiskStatus struct {
	path, err, retentionErr, exportErr string
	paused                             bool
	fileLimit, totalLimit              int64
	files                              int
}

// Caller owns diskMu (or is constructing the not-yet-published logger).
func (l *runtimeLog) publishDiskStatus() {
	l.diskStatus.Store(&logDiskStatus{l.path, l.err, l.retentionErr, l.exportErr, l.diskPaused, l.maxFileBytes, l.maxTotalBytes, len(l.segments)})
}

func (l *runtimeLog) diskLoop() {
	defer func() { close(l.done); l.closePending.Store(false) }()
	for work := range l.queue {
		if work.barrier != nil {
			close(work.barrier)
			continue
		}
		l.mu.Lock()
		l.queueBytes -= int64(len(work.data))
		l.mu.Unlock()
		l.diskBusy.Store(true)
		l.diskMu.Lock()
		l.writeEntry(work.data)
		l.publishDiskStatus()
		l.diskMu.Unlock()
		l.diskBusy.Store(false)
	}
	l.diskMu.Lock()
	if l.file != nil {
		if err := l.file.Sync(); err != nil {
			l.err = err.Error()
		}
		if err := l.file.Close(); err != nil {
			l.err = err.Error()
		}
		l.file = nil
	}
	l.publishDiskStatus()
	l.diskMu.Unlock()
}

func (l *runtimeLog) flush(ctx context.Context) error {
	if l == nil {
		return nil
	}
	barrier := make(chan struct{})
	for {
		l.mu.Lock()
		if l.closed {
			l.mu.Unlock()
			select {
			case <-l.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case l.queue <- logDiskWork{barrier: barrier}:
			l.mu.Unlock()
			select {
			case <-barrier:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			l.mu.Unlock()
		}
		if err := davWait(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
}

func (l *runtimeLog) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.queue)
	}
	l.mu.Unlock()
	select {
	case <-l.done:
	case <-time.After(2 * time.Second):
		l.closePending.Store(true)
		select {
		case <-l.done:
			l.closePending.Store(false)
		default:
		}
	}
}

func (l *runtimeLog) status() map[string]any {
	if l == nil {
		return map[string]any{}
	}
	l.mu.Lock()
	seq, pending, bytes := l.seq, len(l.queue), l.queueBytes
	l.mu.Unlock()
	d := l.diskStatus.Load()
	if d == nil {
		d = &logDiskStatus{}
	}
	exportErr := d.exportErr
	if e := l.exportStatus.Load(); e != nil {
		exportErr = *e
	}
	err := d.err
	if l.closePending.Load() {
		err = "日志退出等待超时，仍有待处理的磁盘操作"
	}
	return map[string]any{"run_id": l.runID, "last_id": seq, "file": d.path, "error": err, "retention_error": d.retentionErr, "export_error": exportErr, "disk_paused": d.paused, "dropped_entries": l.dropped.Load(), "file_limit_bytes": d.fileLimit, "total_limit_bytes": d.totalLimit, "export_limit_bytes": d.totalLimit, "retained_files": d.files, "pending_entries": pending, "pending_bytes": bytes, "disk_busy": l.diskBusy.Load()}
}

func (l *runtimeLog) lockDisk(ctx context.Context) error {
	for !l.diskMu.TryLock() {
		if err := davWait(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		l.diskMu.Unlock()
		return err
	}
	return nil
}

var errLogQueueBusy = errors.New("日志队列尚未落盘，请稍后重试导出")

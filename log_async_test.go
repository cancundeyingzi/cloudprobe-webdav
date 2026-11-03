package main

import (
	"context"
	"testing"
	"time"
)

func flushTestLog(t *testing.T, l *runtimeLog) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := l.flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedLogDiskDoesNotBlockRecoveryOrStatus(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	entered, release := make(chan struct{}), make(chan struct{})
	l.writeRecord = func(b []byte) (int, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return len(b), nil
	}
	defer l.close()
	defer close(release)
	l.add(runLogEntry{Operation: "blocked disk"})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			l.add(runLogEntry{Operation: "queued"})
		}
		_ = l.status()
		r := newUploadRecovery(transferOptions{}.defaults())
		r.manual = true
		r.active = 1
		r.logCtx = newRunTrace(context.Background(), l, "PUT", "/test")
		r.observe(time.Now())
		r.observe(time.Now().Add(10 * time.Second))
		_ = r.status()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disk blocked logger/recovery/status")
	}
	status := l.status()
	if status["pending_entries"].(int) > 256 || status["pending_bytes"].(int64) > 8<<20 || status["dropped_entries"].(uint64) == 0 {
		t.Fatal("queue not bounded/reported", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := l.exportSnapshot(ctx); err == nil {
		t.Fatal("blocked export ignored cancellation")
	}
}

func TestLogCloseIsBoundedAndEventuallyFinishes(t *testing.T) {
	l := newRuntimeLog(t.TempDir(), nil)
	entered, release := make(chan struct{}), make(chan struct{})
	l.writeRecord = func(b []byte) (int, error) { close(entered); <-release; return len(b), nil }
	l.add(runLogEntry{Operation: "blocked shutdown"})
	<-entered
	finished := make(chan struct{})
	go func() { l.close(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("logger shutdown hung")
	}
	if l.status()["error"] == "" {
		close(release)
		t.Fatal("unfinished disk shutdown hidden")
	}
	close(release)
	select {
	case <-l.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish")
	}
}

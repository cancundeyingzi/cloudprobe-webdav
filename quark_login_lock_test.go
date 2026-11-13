package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type quarkLoginRedactionSignal struct{ entered chan struct{} }

func (e *quarkLoginRedactionSignal) Error() string {
	close(e.entered)
	return "login fixture failed"
}

func TestQuarkLoginRedactionDoesNotBlockStopOrClose(t *testing.T) {
	for _, action := range []string{"stop", "close"} {
		t.Run(action, func(t *testing.T) {
			g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
			m := newQuarkManager(g)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			flow := &quarkLoginFlow{stage: "waiting for login"}
			m.mu.Lock()
			m.generation = 1
			m.flow, m.cancel = flow, cancel
			m.mu.Unlock()
			g.mu.Lock()
			var unlock sync.Once
			t.Cleanup(func() { unlock.Do(g.mu.Unlock) })
			entered, finished := make(chan struct{}), make(chan struct{})
			go func() {
				m.finishLogin(1, flow, &quarkLoginRedactionSignal{entered: entered})
				close(finished)
			}()
			// Error() is evaluated immediately before redaction tries app.mu.
			// Hold that lock until Stop/Close has completed to prove the manager
			// state lock is not held across the redaction wait.
			quarkCredentialSignal(t, entered)
			done := make(chan error, 1)
			go func() {
				if action == "stop" {
					m.Stop()
					done <- nil
					return
				}
				closeCtx, stopClose := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer stopClose()
				done <- m.Close(closeCtx)
			}()
			if err := quarkCredentialWait(t, done); err != nil {
				t.Fatal("login redaction blocked lifecycle cancellation", err)
			}
			if ctx.Err() != context.Canceled {
				t.Fatal("login context was not canceled while redaction waited")
			}
			before := m.Status()
			unlock.Do(g.mu.Unlock)
			quarkCredentialSignal(t, finished)
			after := m.Status()
			if after["stage"] != before["stage"] || after["error"] != before["error"] || after["running"] != false {
				t.Fatal("stale login completion overwrote cancellation after redaction", after)
			}
			closeCtx, stopClose := context.WithTimeout(context.Background(), time.Second)
			defer stopClose()
			if err := m.Close(closeCtx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQuarkLoginAlreadyStaleCompletionSkipsRedaction(t *testing.T) {
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	m := newQuarkManager(g)
	m.generation = 2
	flow := &quarkLoginFlow{}
	entered := make(chan struct{})
	g.mu.Lock()
	var unlock sync.Once
	t.Cleanup(func() { unlock.Do(g.mu.Unlock) })
	finished := make(chan struct{})
	go func() {
		m.finishLogin(1, flow, &quarkLoginRedactionSignal{entered: entered})
		close(finished)
	}()
	quarkCredentialSignal(t, finished)
	select {
	case <-entered:
		t.Fatal("stale login attempted error redaction")
	default:
	}
	unlock.Do(g.mu.Unlock)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

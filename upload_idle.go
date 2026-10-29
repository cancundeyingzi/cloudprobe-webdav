package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type uploadIdleError struct{ duration time.Duration }

func (e *uploadIdleError) Error() string {
	return fmt.Sprintf("上传连接连续 %s 无正文进度", e.duration)
}
func (e *uploadIdleError) Timeout() bool   { return true }
func (e *uploadIdleError) Temporary() bool { return true }

// Activity extends an attempt; elapsed wall time alone never restarts a part.
// Cancellation still closes a transport waiting for more body bytes or a reply.
type uploadIdleGuard struct {
	context         context.Context
	cancel          context.CancelCauseFunc
	mu              sync.Mutex
	timer           *time.Timer
	last            time.Time
	timeout         time.Duration
	closed, expired bool
}

func newUploadIdleGuard(parent context.Context, timeout time.Duration) *uploadIdleGuard {
	ctx, cancel := context.WithCancelCause(parent)
	g := &uploadIdleGuard{context: ctx, cancel: cancel, last: time.Now(), timeout: timeout}
	if timeout > 0 {
		g.timer = time.AfterFunc(timeout, g.expire)
	}
	return g
}
func (g *uploadIdleGuard) expire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.expired {
		return
	}
	if remaining := g.timeout - time.Since(g.last); remaining > 0 {
		g.timer.Reset(remaining)
		return
	}
	g.expired = true
	g.cancel(&uploadIdleError{duration: g.timeout})
}
func (g *uploadIdleGuard) touch() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.expired || g.timer == nil {
		return
	}
	g.last = time.Now()
	g.timer.Reset(g.timeout)
}
func (g *uploadIdleGuard) stop() {
	g.mu.Lock()
	g.closed = true
	if g.timer != nil {
		g.timer.Stop()
	}
	g.mu.Unlock()
	g.cancel(context.Canceled)
}
func (g *uploadIdleGuard) reader(reader io.Reader) io.Reader {
	return uploadActivityReader{reader: reader, guard: g}
}

type uploadActivityBody struct {
	io.ReadCloser
	guard *uploadIdleGuard
}

func (b uploadActivityBody) Read(p []byte) (int, error) { return b.guard.reader(b.ReadCloser).Read(p) }
func (g *uploadIdleGuard) body(body io.ReadCloser) io.ReadCloser {
	return uploadActivityBody{ReadCloser: body, guard: g}
}

// A client's former whole-attempt timeout is replaced by the activity guard.
// Cloning avoids changing a shared client or its connection pool.
func uploadActiveClient(client *http.Client) *http.Client {
	copy := *client
	copy.Timeout = 0
	return &copy
}

func uploadIdleTimeout(configured time.Duration, client *http.Client) time.Duration {
	if configured > 0 {
		return configured
	}
	// Injected local clients can keep short activity windows in tests.
	if client != nil && client.Timeout > 0 {
		return client.Timeout
	}
	return 90 * time.Second
}

type uploadActivityReader struct {
	reader io.Reader
	guard  *uploadIdleGuard
}

func (r uploadActivityReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.guard.touch()
	}
	return n, err
}

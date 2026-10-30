package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const davBodyIdleTimeout = 90 * time.Second

// Arm the socket deadline only while waiting for client bytes. Cloud writes,
// queueing and memory backpressure between reads do not consume this timeout.
type davInboundBody struct {
	io.ReadCloser
	controller *http.ResponseController
	writer     http.ResponseWriter
	timeout    time.Duration
	remaining  int64
	done       bool
}

func newDAVInboundBody(w http.ResponseWriter, r *http.Request, timeout time.Duration) *davInboundBody {
	if timeout <= 0 {
		timeout = davBodyIdleTimeout
	}
	return &davInboundBody{ReadCloser: r.Body, controller: http.NewResponseController(w), writer: w, timeout: timeout, remaining: r.ContentLength, done: r.Body == nil || r.Body == http.NoBody}
}

func (b *davInboundBody) Read(p []byte) (int, error) {
	if err := b.controller.SetReadDeadline(time.Now().Add(b.timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return 0, err
	}
	defer b.controller.SetReadDeadline(time.Time{})
	n, err := b.ReadCloser.Read(p)
	if b.remaining >= 0 {
		b.remaining -= int64(n)
	}
	if err == io.EOF || b.remaining == 0 {
		b.done = true
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		b.done = false
		return n, &davStatusError{Code: http.StatusRequestTimeout, Message: fmt.Sprintf("客户端正文连续 %s 无数据，请求已终止", b.timeout), Err: err}
	}
	return n, err
}

func (b *davInboundBody) Close() error {
	b.finish()
	return b.ReadCloser.Close()
}

func (b *davInboundBody) finish() {
	if !b.done {
		b.writer.Header().Set("Connection", "close")
		// net/http closes the original body after flushing the response, even
		// with Connection: close. Bound that final drain as well.
		_ = b.controller.SetReadDeadline(time.Now())
	}
}

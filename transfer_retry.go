package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type upstreamHTTPError struct {
	code   int
	after  time.Duration
	source string
}

func (e *upstreamHTTPError) Error() string { return fmt.Sprintf("%s HTTP %d", e.source, e.code) }
func retryableHTTP(code int) bool {
	return code == 401 || code == 403 || code == 408 || code == 425 || code == 429 || code >= 500
}
func retryAfterHeader(h http.Header) time.Duration {
	s := h.Get("Retry-After")
	if seconds, err := strconv.ParseInt(s, 10, 32); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(s); err == nil {
		return max(0, time.Until(at))
	}
	return 0
}
func retryableTransfer(err error) bool {
	var h *upstreamHTTPError
	if errors.As(err, &h) {
		return retryableHTTP(h.code)
	}
	var network net.Error
	if errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if err != nil {
		for _, text := range []string{"授权/会话失效", "token expired", "HTTP 401", "HTTP 403", "HTTP 408", "HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504"} {
			if strings.Contains(err.Error(), text) {
				return true
			}
		}
	}
	return false
}

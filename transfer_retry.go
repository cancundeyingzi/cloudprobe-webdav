package main

import (
	"fmt"
	"net/http"
	"strconv"
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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

func (d *mobileDAVDrive) sendUploadPart(ctx context.Context, hc *http.Client, endpoint, task, name string, size, off, n, parts, index int64, last bool, f io.ReaderAt, sent, confirmed *atomic.Int64) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := time.Now()
		release := func() {}
		if d.budget != nil {
			lease, e := d.budget.acquire(ctx)
			if e != nil {
				return e
			}
			release = lease
		}
		guard := newUploadIdleGuard(ctx, uploadIdleTimeout(0, hc))
		attemptCtx := guard.context
		var local atomic.Int64
		var progress *uploadProgressReader
		body := newUploadBody(attemptCtx, func(readCtx context.Context) io.Reader {
			progress = &uploadProgressReader{reader: uploadSection(readCtx, f, off, n), ctx: readCtx, source: "移动上传节点", index: index, parts: parts, total: n, attempt: attempt, started: started, last: started, sent: sent, local: &local, confirmed: confirmed}
			return guard.reader(progress)
		})
		req, err := http.NewRequestWithContext(attemptCtx, "POST", endpoint, body)
		if err != nil {
			body.Close()
			guard.stop()
			release()
			return err
		}
		req.ContentLength = n
		req.Header.Set("Content-Type", contentType(name)+`; name="`+url.PathEscape(name)+`"`)
		req.Header.Set("x-NameCoding", "urlencoding")
		req.Header.Set("uploadtaskID", task)
		req.Header.Set("rangeType", "1")
		req.Header.Set("contentSize", strconv.FormatInt(n, 10))
		if last {
			req.Header.Set("rangeType", "2")
			req.Header.Set("contentSize", strconv.FormatInt(size, 10))
		}
		if n > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+n-1))
		}
		resp, err := uploadActiveClient(hc).Do(req)
		body.Close()
		progress.finish()
		var wait time.Duration
		if err == nil {
			resp.Body = guard.body(resp.Body)
			raw, readErr := readBounded(resp.Body, 1<<20)
			resp.Body.Close()
			if readErr != nil {
				err = readErr
			} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				wait = retryAfterHeader(resp.Header)
				err = &upstreamHTTPError{code: resp.StatusCode, after: wait, source: "移动上传分片"}
			} else {
				_, err = mobileResult(raw)
			}
		}
		if _, idle := context.Cause(attemptCtx).(*uploadIdleError); idle && ctx.Err() == nil {
			err = context.Cause(attemptCtx)
		}
		guard.stop()
		release()
		if err == nil && local.Load() != n {
			err = errors.New("移动节点提前返回，分片正文未完整发送")
		}
		if err == nil {
			confirmed.Add(n)
			if runtime := transfersFrom(ctx); runtime != nil {
				runtime.recovery.confirmed.Add(n)
			}
			if trace, _ := ctx.Value(runTraceKey{}).(*runTrace); trace != nil {
				trace.confirmed.Add(n)
			}
			logDAV(ctx, runLogEntry{Operation: "上传分片", Source: "移动上传节点", Endpoint: nodeHost(endpoint), Result: "已确认", Part: progress.index, Parts: parts, Attempt: attempt, SentBytes: local.Load(), ConfirmedBytes: n, DurationMs: time.Since(started).Milliseconds()})
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryableTransfer(err) && !mobileAuthError(err) {
			return err
		}
		wait = max(wait, transferRetryDelay(attempt, 0))
		logDAV(ctx, runLogEntry{Operation: "分片重试", Source: "移动上传节点", Result: "等待重试", Attempt: attempt, Error: err.Error(), Detail: fmt.Sprintf("次数无上限；复用上传任务与字节范围；等待 %d ms", wait.Milliseconds())})
		if err = davWait(ctx, wait); err != nil {
			return err
		}
	}
}

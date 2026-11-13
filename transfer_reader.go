package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var errRangeUnavailable = errors.New("上游不支持准确 Range")

type transferReadCloser interface {
	io.Reader
	io.Seeker
	Close()
}
type readChunkResult struct {
	data []byte
	err  error
}
type downloadAttempt struct {
	*http.Response
	release    func()
	index      int
	generation uint64
}

func (a *downloadAttempt) close() { a.Body.Close(); a.release() }

type downloadSession struct {
	ctx         context.Context
	routes      []*resolvedReadRoute
	health      *routeHealth
	signature   downloadSignature
	budget      *fairLimiter
	size        int64
	client      *http.Client
	preferred   int
	mu          sync.Mutex
	bytesRead   atomic.Int64
	retryDelay  time.Duration
	idleTimeout time.Duration
}

func (s *downloadSession) record(ctx context.Context, n int) {
	s.bytesRead.Add(int64(n))
	if tr, _ := ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
		tr.downloaded.Add(int64(n))
	}
}

type idleReadBody struct {
	io.ReadCloser
	ctx, parent context.Context
	cancel      context.CancelCauseFunc
	timeout     time.Duration
}

type downloadIdleError struct{}

func (*downloadIdleError) Error() string {
	return "下载正文持续无进度，重新选择读取通道"
}
func (*downloadIdleError) Timeout() bool   { return true }
func (*downloadIdleError) Temporary() bool { return true }

func (b *idleReadBody) Read(p []byte) (int, error) {
	timer := time.AfterFunc(b.timeout, func() { b.cancel(&downloadIdleError{}) })
	defer timer.Stop()
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		if parentErr := b.parent.Err(); parentErr != nil {
			return n, parentErr
		}
		var idle *downloadIdleError
		if errors.As(context.Cause(b.ctx), &idle) {
			return n, idle
		}
	}
	return n, err
}
func (b *idleReadBody) Close() error { b.cancel(nil); return b.ReadCloser.Close() }

func (s *downloadSession) order() []int {
	s.mu.Lock()
	p := s.preferred
	s.mu.Unlock()
	var good, cool []int
	for n := 0; n < len(s.routes); n++ {
		i := (p + n) % len(s.routes)
		if s.health.cool(s.routes[i].route.Name) {
			cool = append(cool, i)
		} else {
			good = append(good, i)
		}
	}
	return append(good, cool...)
}
func (s *downloadSession) open(ctx context.Context, start, end int64, ranged bool) (*downloadAttempt, error) {
	for cycle := 1; ; cycle++ {
		var failures []error
		noRange, retryable, refreshNow := false, false, false
		var retryAt time.Time
		recordFailure := func(err error) {
			failures = append(failures, err)
			var pending *readRouteRetry
			if errors.As(err, &pending) {
				retryable = true
				if retryAt.IsZero() || pending.at.Before(retryAt) {
					retryAt = pending.at
				}
			} else if errors.Is(err, errReadRouteChanged) || retryableTransfer(err) {
				retryable, refreshNow = true, true
			}
		}
		for _, index := range s.order() {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			route := s.routes[index]
			if err := route.denials.stopped(); err != nil {
				failures = append(failures, err)
				continue
			}
			target, generation, e := route.get(ctx, s.retryDelay)
			if e != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				recordFailure(e)
				continue
			}
			var release func()
			if s.budget != nil {
				release, e = s.budget.acquire(ctx)
				if e != nil {
					return nil, e
				}
			} else {
				release = func() {}
			}
			if e = route.ready(ctx, generation); e != nil {
				release()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				recordFailure(e)
				continue
			}
			requestCtx, cancelRequest := context.WithCancelCause(ctx)
			req, e := http.NewRequestWithContext(requestCtx, "GET", target.URL, nil)
			if e != nil {
				cancelRequest(nil)
				release()
				return nil, e
			}
			req.Header = target.Headers.Clone()
			if req.Header == nil {
				req.Header = make(http.Header)
			}
			if req.Header.Get("User-Agent") == "" {
				req.Header.Set("User-Agent", "CloudProbe/"+version)
			}
			req.Header.Set("Accept-Encoding", "identity")
			if ranged {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
			} else if start > 0 {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
			}
			if ranged || start > 0 {
				if condition := s.signature.condition(route.route); condition != "" {
					req.Header.Set("If-Range", condition)
				}
			}
			client := s.client
			if client == nil {
				client = &http.Client{Transport: transferDownloadTransport, CheckRedirect: func(r *http.Request, via []*http.Request) error {
					if len(via) > 8 {
						return errors.New("下载重定向过多")
					}
					if len(via) > 0 && (r.URL.Host != via[0].URL.Host || r.URL.Scheme != via[0].URL.Scheme) {
						if via[0].URL.Scheme == "https" && r.URL.Scheme != "https" && (target.Headers.Get("Cookie") != "" || target.Headers.Get("Authorization") != "") {
							return errors.New("携带账号凭据的下载不允许从 HTTPS 降级到 HTTP")
						}
						r.Header.Del("Authorization")
						r.Header.Del("Cookie")
					}
					return validDAVURL(r.URL.String())
				}}
			}
			resp, e := client.Do(req)
			if e == nil {
				timeout := s.idleTimeout
				if timeout <= 0 {
					timeout = 35 * time.Second
				}
				resp.Body = &idleReadBody{ReadCloser: resp.Body, ctx: requestCtx, parent: ctx, cancel: cancelRequest, timeout: timeout}
				if resp.StatusCode == 200 && ranged {
					resp.Body.Close()
					release()
					noRange = true
					route.succeeded(generation)
					logFallback(ctx, route.route.Name, "尝试支持 Range 的备用读取方案", errRangeUnavailable)
					continue
				}
				if resp.StatusCode != 200 && resp.StatusCode != 206 {
					e = &upstreamHTTPError{code: resp.StatusCode, after: retryAfterHeader(resp.Header), source: route.route.Name + " 正文"}
				}
				if e == nil && (ranged || start > 0 || resp.StatusCode == 206) {
					var a, b, total int64
					_, e = fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &a, &b, &total)
					if resp.StatusCode != 206 || e != nil || a != start || total != s.size || b < start || ranged && b != end || !ranged && b != s.size-1 {
						e = errors.New("上游 Content-Range 与请求不符，未报告读取成功")
					}
				}
				if e == nil && !ranged && start == 0 && resp.ContentLength >= 0 && resp.ContentLength != s.size {
					e = errors.New("正文长度与目录大小不符")
				}
				if e == nil {
					e = s.signature.accept(route.route, resp.Header)
				}
				if e == nil {
					s.mu.Lock()
					s.preferred = index
					s.mu.Unlock()
					route.succeeded(generation)
					s.health.good(route.route.Name)
					logDAV(ctx, runLogEntry{Operation: "下载正文", Source: route.route.Name, Endpoint: resp.Request.URL.Host, Result: "开始", Detail: fmt.Sprintf("偏移 %d—%d；HTTP %d", start, end, resp.StatusCode), Protocol: resp.Proto})
					return &downloadAttempt{Response: resp, release: release, index: index, generation: generation}, nil
				}
				resp.Body.Close()
			}
			if ctx.Err() != nil {
				cancelRequest(nil)
				release()
				return nil, ctx.Err()
			}
			e = route.failure(generation, e, s.retryDelay)
			cancelRequest(nil)
			release()
			recordFailure(e)
			s.health.bad(route.route.Name)
			logFallback(ctx, route.route.Name, "下一读取方案", e)
		}
		if noRange {
			return nil, errRangeUnavailable
		}
		if !retryable {
			return nil, fmt.Errorf("所有正文读取方案失败，需核对文件或授权：%w", errors.Join(failures...))
		}
		// No body slot or route lock is held while every candidate is deferred.
		// A stale failure can leave a fresh generation ready immediately.
		delay := max(time.Duration(0), time.Until(retryAt))
		if retryAt.IsZero() || refreshNow {
			delay = 0
		}
		logDAV(ctx, runLogEntry{Operation: "下载故障重试", Source: "全部读取方案", Result: "等待重试", Attempt: cycle, Detail: fmt.Sprintf("次数无上限；等待 %d ms；刷新地址后重新尝试候选", delay.Milliseconds())})
		if e := davWait(ctx, delay); e != nil {
			return nil, e
		}
	}
}
func (s *downloadSession) chunk(ctx context.Context, start, end int64) ([]byte, error) {
	for {
		started := time.Now()
		response, e := s.open(ctx, start, end, true)
		if e != nil {
			return nil, e
		}
		index := response.index
		data := make([]byte, end-start+1)
		n, readErr := io.ReadFull(response.Body, data)
		e = readErr
		s.record(ctx, n)
		if e != nil && ctx.Err() != nil {
			response.close()
			return nil, ctx.Err()
		}
		if e != nil {
			e = s.routes[index].failure(response.generation, e, s.retryDelay)
		}
		response.close()
		if e == nil {
			s.routes[index].completed(response.generation)
			logDAV(ctx, runLogEntry{Operation: "下载分段", Source: s.routes[index].route.Name, Result: "已读取", Bytes: int64(n), DurationMs: time.Since(started).Milliseconds(), Detail: fmt.Sprintf("偏移 %d—%d", start, end)})
			return data, nil
		}
		s.health.bad(s.routes[index].route.Name)
		s.mu.Lock()
		s.preferred = (index + 1) % len(s.routes)
		s.mu.Unlock()
		logFallback(ctx, s.routes[index].route.Name, "按相同偏移重读未交付的分段", e)
		if !retryableTransfer(e) {
			return nil, e
		}
	}
}

var transferDownloadTransport = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 160, MaxIdleConnsPerHost: 80}

type transferReader struct {
	ctx                 context.Context
	session             *downloadSession
	size, pos           int64
	threads             int
	partSize            int64
	parallel            bool
	stream              *downloadAttempt
	streamPos           int64
	prefix              []byte
	wave                []chan readChunkResult
	waveStart, waveNext int64
	waveCancel          context.CancelFunc
	waveWG              sync.WaitGroup
	memory              *byteBudget
	waveRelease         func()
	buffer              []byte
	bufferOffset        int
	closed              bool
	rangeChecked        bool
	started             time.Time
	delivered           int64
}

func newTransferReader(ctx context.Context, m *davMount, it davEntry, runtime *transferRuntime) *transferReader {
	o := transferOptions{}.defaults()
	var budget *fairLimiter
	memory := defaultDownloadMemory
	if runtime != nil {
		o = runtime.options
		budget = runtime.connections
		memory = runtime.downloadMemory
	}
	routes := routesFor(ctx, m.drive, it)
	resolved := make([]*resolvedReadRoute, len(routes))
	for i, r := range routes {
		resolved[i] = &resolvedReadRoute{route: r}
	}
	parallel := o.TransferScheduling != "client" && it.Size > int64(o.DownloadPartMiB)<<20 && o.DownloadConcurrency > 1
	if state := davState(ctx); state != nil && (state.externalRange || state.method == "HEAD") {
		parallel = false
	}
	part := int64(o.DownloadPartMiB) << 20
	if o.DownloadPartMode == "auto" {
		part = max(part, (it.Size+int64(o.DownloadConcurrency*32)-1)/int64(o.DownloadConcurrency*32))
		part = min(part, 32<<20)
	}
	return &transferReader{ctx: ctx, memory: memory, size: it.Size, threads: o.DownloadConcurrency, partSize: part, parallel: parallel, started: time.Now(), session: &downloadSession{ctx: ctx, routes: resolved, health: m.health, budget: budget, size: it.Size}}
}

var defaultDownloadMemory = newByteBudget(128 << 20)

func (d *transferReader) closeStream() {
	if d.stream != nil {
		d.stream.close()
		d.stream = nil
	}
}
func (d *transferReader) stopWave() {
	if d.waveCancel != nil {
		d.waveCancel()
		d.waveWG.Wait()
		d.waveCancel = nil
	}
	d.wave = nil
	d.buffer = nil
	d.bufferOffset = 0
	if d.waveRelease != nil {
		d.waveRelease()
		d.waveRelease = nil
	}
}
func (d *transferReader) Close() {
	if d.closed {
		return
	}
	d.closed = true
	d.closeStream()
	d.stopWave()
	d.prefix = nil
	logDAV(d.ctx, runLogEntry{Operation: "下载读取结束", Source: "云盘节点", Result: "结束", Bytes: d.session.bytesRead.Load(), DurationMs: time.Since(d.started).Milliseconds(), Detail: fmt.Sprintf("交给 WebDAV/界面的正文 %d B；节点读取量包含探测、重试和预取", d.delivered)})
}
func (d *transferReader) Seek(off int64, whence int) (int64, error) {
	if d.closed {
		return 0, io.ErrClosedPipe
	}
	target := off
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		target = d.pos + off
	case io.SeekEnd:
		target = d.size + off
	default:
		return 0, errors.New("无效 seek")
	}
	if target < 0 {
		return 0, errors.New("无效偏移")
	}
	if target != d.pos {
		// ServeContent seeks to EOF to query size between its sniff and rewind.
		// Keep the original stream parked after the small replayable prefix;
		// Read will reopen only if the requested position cannot use that stream.
		if d.stream == nil || d.streamPos != int64(len(d.prefix)) {
			d.closeStream()
		}
		d.stopWave()
		d.pos = target
	}
	return target, nil
}
func (d *transferReader) startWave() error {
	n := min(d.threads, int((d.size-d.pos+d.partSize-1)/d.partSize))
	// Reserve the entire window before starting workers. Independent, out of
	// order reservations can deadlock while each reader waits for its first part.
	if d.memory == nil {
		d.memory = defaultDownloadMemory
	}
	n = min(n, max(1, int(d.memory.limit/d.partSize)))
	release, err := d.memory.acquire(d.ctx, min(int64(n)*d.partSize, d.size-d.pos))
	if err != nil {
		return err
	}
	d.waveRelease = release
	ctx, cancel := context.WithCancel(d.ctx)
	d.waveCancel = cancel
	d.waveStart = d.pos
	d.waveNext = 0
	d.wave = make([]chan readChunkResult, n)
	for i := 0; i < n; i++ {
		start := d.pos + int64(i)*d.partSize
		end := min(d.size-1, start+d.partSize-1)
		ch := make(chan readChunkResult, 1)
		d.wave[i] = ch
		d.waveWG.Go(func() { data, e := d.session.chunk(ctx, start, end); ch <- readChunkResult{data, e} })
	}
	return nil
}
func (d *transferReader) Read(p []byte) (int, error) {
	if d.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if d.pos >= d.size {
		return 0, io.EOF
	}
	if d.ctx.Err() != nil {
		return 0, d.ctx.Err()
	}
	if d.pos < int64(len(d.prefix)) {
		n := copy(p, d.prefix[d.pos:])
		d.pos += int64(n)
		d.count(n)
		return n, nil
	}
	if d.stream != nil && d.streamPos != d.pos {
		d.closeStream()
	}
	// ServeContent may read 512 bytes to detect Content-Type, then seek back.
	// Do not launch an entire prefetch window for that metadata probe.
	if d.parallel {
		if !d.rangeChecked {
			_, e := d.session.chunk(d.ctx, d.pos, d.pos)
			d.rangeChecked = true
			verifiable := d.session.signature.verifiable()
			if errors.Is(e, errRangeUnavailable) || e == nil && !verifiable {
				if d.pos != 0 {
					return 0, errRangeUnavailable
				}
				d.parallel = false
				if d.delivered == 0 {
					d.session.signature.discardProbe()
				}
				logDAV(d.ctx, runLogEntry{Operation: "下载模式回退", Source: "本机", Result: "单路读取", Detail: "上游未支持准确 Range 或缺少可核对的对象版本"})
				return d.Read(p)
			}
			if e != nil {
				return 0, e
			}
		}
	}
	if d.parallel && (len(p) > 512 || d.wave != nil) {
		d.closeStream()
		if d.wave == nil {
			if err := d.startWave(); err != nil {
				return 0, err
			}
		}
		if d.bufferOffset >= len(d.buffer) {
			if int(d.waveNext) >= len(d.wave) {
				d.stopWave()
				if err := d.startWave(); err != nil {
					return 0, err
				}
			}
			var result readChunkResult
			select {
			case result = <-d.wave[d.waveNext]:
			case <-d.ctx.Done():
				d.stopWave()
				return 0, d.ctx.Err()
			}
			d.waveNext++
			if result.err != nil {
				d.stopWave()
				if errors.Is(result.err, errRangeUnavailable) && d.pos == 0 {
					d.parallel = false
					logDAV(d.ctx, runLogEntry{Operation: "下载模式回退", Source: "本机", Result: "单路读取", Detail: "上游未支持准确 Range；完整下载改用单路流式读取"})
					return d.Read(p)
				}
				return 0, result.err
			}
			d.buffer = result.data
			d.bufferOffset = 0
		}
		n := copy(p, d.buffer[d.bufferOffset:])
		d.rememberPrefix(p[:n])
		d.bufferOffset += n
		d.pos += int64(n)
		d.count(n)
		if d.bufferOffset == len(d.buffer) && int(d.waveNext) == len(d.wave) {
			d.stopWave()
		}
		return n, nil
	}
	for {
		if d.stream == nil {
			attempt, e := d.session.open(d.ctx, d.pos, d.size-1, false)
			if e != nil {
				return 0, e
			}
			d.stream = attempt
			d.streamPos = d.pos
		}
		if int64(len(p)) > d.size-d.pos {
			p = p[:d.size-d.pos]
		}
		n, e := d.stream.Body.Read(p)
		d.session.record(d.ctx, n)
		d.rememberPrefix(p[:n])
		d.pos += int64(n)
		d.streamPos += int64(n)
		d.count(n)
		if e != nil && d.ctx.Err() != nil {
			d.closeStream()
			return n, d.ctx.Err()
		}
		if e == io.EOF && d.pos < d.size {
			e = io.ErrUnexpectedEOF
		}
		if d.pos == d.size && (e == nil || e == io.EOF) {
			d.session.routes[d.stream.index].completed(d.stream.generation)
		}
		if e != nil && d.pos < d.size {
			index, generation := d.stream.index, d.stream.generation
			e = d.session.routes[index].failure(generation, e, d.session.retryDelay)
			d.closeStream()
			d.session.health.bad(d.session.routes[index].route.Name)
			d.session.mu.Lock()
			d.session.preferred = (index + 1) % len(d.session.routes)
			d.session.mu.Unlock()
			logFallback(d.ctx, d.session.routes[index].route.Name, "从已交付字节偏移恢复正文", e)
			if !retryableTransfer(e) {
				return n, e
			}
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, e
	}
}

func (d *transferReader) rememberPrefix(p []byte) {
	if d.pos == int64(len(d.prefix)) && len(d.prefix) < 512 {
		d.prefix = append(d.prefix, p[:min(len(p), 512-len(d.prefix))]...)
	}
}

func (d *transferReader) count(n int) {
	d.delivered += int64(n)
}

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

type wopanUploadSession struct {
	endpoint, access, name, parent, encrypted, unique, source string
	size, partSize                                            int64
	concurrency                                               int
	slots                                                     chan struct{}
	client                                                    *http.Client
	mode                                                      string
	retryDelay                                                time.Duration
	uploadIdle                                                time.Duration
	retrySlots                                                chan struct{}
	connections                                               *uploadConnections
	budget                                                    *fairLimiter
	recovery                                                  *uploadRecovery
	authLock                                                  contextMutex
	refreshAuth                                               func(context.Context) (string, string, error)
	lastAuthRefresh                                           time.Time
}

func uploadConcurrency(n int) int {
	if n >= 1 && n <= 16 {
		return n
	}
	return 4
}
func (d *wopanDAVDrive) source() string {
	if d.usingWeb {
		return "联通网页授权"
	}
	return "联通绑定授权直连"
}
func (d *wopanDAVDrive) prepareUpload(ctx context.Context, parent, name string, size int64) (*wopanUploadSession, error) {
	if err := d.directMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer d.directMu.Unlock()
	if err := d.ready(); err != nil {
		return nil, err
	}
	base := d.w.ZoneURL
	if base == "" {
		return nil, errors.New("联通未返回上传节点")
	}
	endpoint := strings.TrimRight(base, "/") + "/openapi/client/upload2C"
	if err := validDownloadURL(endpoint); err != nil {
		return nil, err
	}
	access, _ := d.w.GetToken()
	info := wopan.Json{"spaceType": wopan.SpaceTypePersonal, "directoryId": parent, "batchNo": time.Now().Format("20060102150405"), "fileName": name, "fileSize": size, "fileType": d.w.GetFileType(name)}
	encrypted, err := d.w.EncryptParam(wopan.ChannelWoHome, info)
	if err != nil {
		return nil, err
	}
	partMiB := d.uploadPartMiB
	if partMiB == 0 {
		partMiB = 5
	}
	u := &wopanUploadSession{endpoint: endpoint, access: access, name: name, parent: parent, encrypted: encrypted, size: size, partSize: int64(partMiB) << 20, unique: strconv.FormatInt(time.Now().UnixMilli(), 10) + "_" + randomKey()[:10], source: d.source(), concurrency: uploadConcurrency(d.uploadConcurrency), slots: d.uploadSlots, mode: uploadMode(d.uploadMode), budget: d.uploadBudget}
	if runtime := transfersFrom(ctx); runtime != nil {
		u.recovery = runtime.recovery
	}
	u.refreshAuth = func(ctx context.Context) (string, string, error) {
		if err := d.directMu.LockContext(ctx); err != nil {
			return "", "", err
		}
		defer d.directMu.Unlock()
		if d.usingWeb {
			return "", "", errors.New("官网授权暂不可用，任务保留并低频重试；更新官网登录需要先停止服务")
		}
		if err := d.refreshNative(ctx); err != nil {
			return "", "", err
		}
		access, _ := d.w.GetToken()
		encrypted, err := d.w.EncryptParam(wopan.ChannelWoHome, info)
		return access, encrypted, err
	}
	return u, nil
}
func (d *wopanDAVDrive) uploadParts(ctx context.Context, parent, name string, f *os.File, size int64) (string, error) {
	u, err := d.prepareUpload(ctx, parent, name, size)
	if err != nil {
		return "", &davNotExecuted{err}
	}
	fid, err := u.run(ctx, f)
	if err != nil && ctx.Err() == nil {
		// A response can be lost after cloud-side assembly. Check the unique
		// staged name before returning an uncertain outcome; never replay PUT.
		started := time.Now()
		entries, checkErr := d.List(directoryLogContext(ctx), parent)
		for _, it := range entries {
			if it.Name == name && !it.Directory && it.Size == size {
				if tr, _ := ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
					tr.confirmed.Store(size)
				}
				logDAV(ctx, runLogEntry{Operation: "上传结果核对", Source: d.source(), Result: "目录已确认", DurationMs: time.Since(started).Milliseconds(), ConfirmedBytes: size, Detail: "部分分片响应缺失，但唯一临时文件的名称和大小已核对；未重放整个上传"})
				return it.FID, nil
			}
		}
		if checkErr != nil {
			logDAV(ctx, runLogEntry{Operation: "上传结果核对", Source: d.source(), Result: "失败", Error: checkErr.Error()})
		}
	}
	return fid, err
}

func (u *wopanUploadSession) run(ctx context.Context, f io.ReaderAt) (string, error) {
	started := time.Now()
	if u.partSize <= 0 || u.size < 0 {
		return "", errors.New("上传大小或分片大小无效")
	}
	u.connections = &uploadConnections{}
	u.retrySlots = make(chan struct{}, 1)
	parts := max(int64(1), (u.size+u.partSize-1)/u.partSize)
	workers := min(uploadConcurrency(u.concurrency), int(parts))
	logDAV(ctx, runLogEntry{Operation: "云端上传", Source: u.source, Endpoint: nodeHost(u.endpoint) + " /openapi/client/upload2C", Result: "开始", Bytes: u.size, Parts: parts, Detail: fmt.Sprintf("分片大小 %d B；并发 %d；传输模式 %s；云端临时名称 %s", u.partSize, workers, uploadMode(u.mode), u.name)})
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next, sent, confirmed, retries atomic.Int64
	var mu sync.Mutex
	var firstErr error
	var fid string
	var wg sync.WaitGroup
	pools := make([]*uploadClientPool, workers)
	for i := range pools {
		if i > 0 && uploadMode(u.mode) == "http2" {
			pools[i] = pools[0]
		} else {
			pools[i] = u.newPool()
		}
	}
	defer func() {
		for _, pool := range pools {
			pool.close()
		}
	}()
	for worker := 0; worker < workers; worker++ {
		workerID, pool := worker+1, pools[worker]
		wg.Go(func() {
			for {
				index := next.Add(1)
				if index > parts || workerCtx.Err() != nil {
					return
				}
				value, err := u.part(workerCtx, f, index, parts, workerID, pool, &sent, &confirmed, &retries)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
					// ReaderAt may be waiting for future client bytes. Cancel the
					// producer's context as well, before joining HTTP body readers.
					if source, ok := f.(interface{ Abort(error) }); ok {
						source.Abort(err)
					}
					cancel()
				}
				if value != "" {
					fid = value
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		})
	}
	wg.Wait() // Spool and session stay alive until canceled HTTP readers finish.
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	entry := runLogEntry{Operation: "云端上传", Source: u.source, Endpoint: nodeHost(u.endpoint), Result: "成功", DurationMs: time.Since(started).Milliseconds(), Bytes: u.size, SentBytes: sent.Load(), ConfirmedBytes: confirmed.Load(), BytesPerSecond: rateFor(confirmed.Load(), time.Since(started)), Parts: parts, Detail: fmt.Sprintf("并发 %d；重试 %d 次；发送量包含重试；速度按已确认正文计算", workers, retries.Load())}
	if firstErr != nil {
		entry.Result, entry.Error = "失败", firstErr.Error()
	}
	logDAV(ctx, entry)
	return fid, firstErr
}
func (u *wopanUploadSession) part(ctx context.Context, f io.ReaderAt, index, parts int64, workerID int, pool *uploadClientPool, sent, confirmed, retries *atomic.Int64) (string, error) {
	off := (index - 1) * u.partSize
	n := min(u.partSize, u.size-off)
	var err error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if source, ok := f.(interface {
			WaitReadable(context.Context, int64) error
		}); ok {
			if err := source.WaitReadable(ctx, off); err != nil {
				return "", err
			}
		}
		if err := u.authLock.LockContext(ctx); err != nil {
			return "", err
		}
		access, encrypted := u.access, u.encrypted
		u.authLock.Unlock()
		fields := map[string]string{"uniqueId": u.unique, "accessToken": access, "fileName": u.name, "psToken": "undefined", "fileSize": strconv.FormatInt(u.size, 10), "totalPart": strconv.FormatInt(parts, 10), "partSize": strconv.FormatInt(n, 10), "partIndex": strconv.FormatInt(index, 10), "channel": "wocloud", "directoryId": u.parent, "fileInfo": encrypted}
		var framing bytes.Buffer
		form := multipart.NewWriter(&framing)
		for k, v := range fields {
			if err := form.WriteField(k, v); err != nil {
				return "", err
			}
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": u.name}))
		header.Set("Content-Type", contentType(u.name))
		if _, err := form.CreatePart(header); err != nil {
			return "", err
		}
		prefixLength := framing.Len()
		if err := form.Close(); err != nil {
			return "", err
		}
		prefix, suffix := framing.Bytes()[:prefixLength], framing.Bytes()[prefixLength:]
		queued := time.Now()
		if attempt > 1 {
			select {
			case u.retrySlots <- struct{}{}:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		releaseBudget := func() {}
		if u.budget != nil {
			var e error
			releaseBudget, e = u.budget.acquire(ctx)
			if e != nil {
				if attempt > 1 {
					<-u.retrySlots
				}
				return "", e
			}
		}
		if u.slots != nil {
			select {
			case u.slots <- struct{}{}:
			case <-ctx.Done():
				releaseBudget()
				if attempt > 1 {
					<-u.retrySlots
				}
				return "", ctx.Err()
			}
		}
		queueMs := time.Since(queued).Milliseconds()
		started := time.Now()
		var wrote atomic.Int64
		var payloadBytes atomic.Int64
		connection := &uploadConnectionTrace{ctx: ctx, registry: u.connections, source: u.source, worker: workerID, part: index, parts: parts, attempt: attempt}
		trace := connection.hooks(func(info httptrace.WroteRequestInfo) { wrote.Store(time.Now().UnixNano()) })
		traced := httptrace.WithClientTrace(context.WithValue(ctx, uploadConnectionKey{}, connection), trace)
		client, generation := pool.get()
		guard := newUploadIdleGuard(traced, uploadIdleTimeout(u.uploadIdle, u.client))
		attemptCtx := guard.context
		logDAV(ctx, runLogEntry{Operation: "上传分片", Source: u.source, Endpoint: nodeHost(u.endpoint), Result: "发送中", Part: index, Parts: parts, Attempt: attempt, Bytes: n, QueueMs: queueMs})
		var progress *uploadProgressReader
		body := newUploadBody(attemptCtx, func(readCtx context.Context) io.Reader {
			progress = &uploadProgressReader{reader: uploadSection(readCtx, f, off, n), ctx: readCtx, source: u.source, index: index, parts: parts, attempt: attempt, total: n, started: started, last: started, sent: sent, local: &payloadBytes, confirmed: confirmed, recovery: u.recovery}
			return guard.reader(io.MultiReader(bytes.NewReader(prefix), progress, bytes.NewReader(suffix)))
		})
		req, re := http.NewRequestWithContext(attemptCtx, "POST", u.endpoint, body)
		if re != nil {
			body.Close()
			guard.stop()
			releaseBudget()
			if u.slots != nil {
				<-u.slots
			}
			if attempt > 1 {
				<-u.retrySlots
			}
			return "", re
		}
		req.ContentLength = int64(len(prefix)) + n + int64(len(suffix))
		req.Header.Set("Content-Type", form.FormDataContentType())
		req.Header.Set("Origin", "https://pan.wo.cn")
		req.Header.Set("Referer", "https://pan.wo.cn/")
		req.Header.Set("User-Agent", wopan.DefaultUA)
		req.Header.Set("Accesstoken", access)
		result, sendErr := sendWopanPartActive(client, req, guard)
		// Join any in-flight disk read and prevent later transport reads from
		// touching the spool after this upload has returned to its caller.
		_ = body.Close()
		if _, idle := context.Cause(attemptCtx).(*uploadIdleError); idle && ctx.Err() == nil {
			sendErr = &wopanPartError{Message: context.Cause(attemptCtx).Error(), Retry: true, ResetConnection: true}
		}
		guard.stop()
		progress.finish()
		releaseBudget()
		if u.slots != nil {
			<-u.slots
		}
		if attempt > 1 {
			<-u.retrySlots
		}
		err = sendErr
		end := time.Now()
		sendDuration := end.Sub(started)
		waitDuration := time.Duration(0)
		if ns := wrote.Load(); ns > 0 {
			sendDuration = time.Unix(0, ns).Sub(started)
			waitDuration = end.Sub(time.Unix(0, ns))
		}
		entry := runLogEntry{Operation: "上传分片", Source: u.source, Endpoint: nodeHost(u.endpoint), Result: "已确认", Part: index, Parts: parts, Attempt: attempt, Bytes: n, SentBytes: payloadBytes.Load(), DurationMs: end.Sub(started).Milliseconds(), SendMs: sendDuration.Milliseconds(), ConfirmWaitMs: waitDuration.Milliseconds(), QueueMs: queueMs}
		connection.apply(&entry)
		if err == nil && payloadBytes.Load() != n {
			err = errors.New("上传节点提前返回，分片正文未被完整读取，不能当作已确认")
		}
		if err == nil {
			confirmed.Add(n)
			if u.recovery != nil {
				u.recovery.confirmed.Add(n)
			}
			if source, ok := f.(interface{ ConfirmPart(int64, int64) }); ok {
				source.ConfirmPart(off, n)
			}
			if tr, _ := ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
				tr.confirmed.Add(n)
			}
			entry.ConfirmedBytes = n
			logDAV(ctx, entry)
			return result.Data.Fid, nil
		}
		entry.Result, entry.Error = "失败", err.Error()
		logDAV(ctx, entry)
		var transient *wopanPartError
		if !errors.As(err, &transient) || !transient.Retry || ctx.Err() != nil {
			return "", err
		}
		if transient.Auth && u.refreshAuth != nil {
			if lockErr := u.authLock.LockContext(ctx); lockErr != nil {
				return "", lockErr
			}
			if time.Since(u.lastAuthRefresh) >= 5*time.Second {
				u.lastAuthRefresh = time.Now()
				access, encrypted, authErr := u.refreshAuth(ctx)
				if authErr == nil {
					u.access, u.encrypted = access, encrypted
				} else {
					logDAV(ctx, runLogEntry{Operation: "上传授权恢复", Source: u.source, Result: "等待授权", Error: authErr.Error()})
				}
			}
			u.authLock.Unlock()
		}
		retries.Add(1)
		if transient.ResetConnection {
			pool.retire(generation)
			if u.mode == "auto" {
				pool.switchMode()
				logFallback(ctx, entry.Protocol, "备用上传协议", err)
			}
		}
		delay := transferRetryDelay(attempt, u.retryDelay)
		if transient.RetryAfter > delay {
			delay = transient.RetryAfter
		}
		retry := runLogEntry{Operation: "分片重试", Source: u.source, Result: "重试", Part: index, Parts: parts, Attempt: attempt + 1, Error: err.Error(), Detail: fmt.Sprintf("复用上传会话和分片编号；次数无上限；等待 %d ms；重试传输合计最多 1 片；失效连接将重建", delay.Milliseconds())}
		connection.apply(&retry)
		logDAV(ctx, retry)
		if err = davWait(ctx, delay); err != nil {
			return "", err
		}
	}
}

type uploadProgressReader struct {
	reader                 io.Reader
	ctx                    context.Context
	source                 string
	index, parts, total    int64
	attempt                int
	started, last          time.Time
	sent, local, confirmed *atomic.Int64
	recovery               *uploadRecovery
	end                    func()
	complete               bool
}

func (r *uploadProgressReader) finish() {
	if r.end != nil {
		r.end()
		r.end = nil
	}
}

func (r *uploadProgressReader) Read(p []byte) (int, error) {
	if r.recovery == nil {
		if runtime := transfersFrom(r.ctx); runtime != nil {
			r.recovery = runtime.recovery
		}
	}
	if !r.complete && r.end == nil && r.recovery != nil {
		r.end = r.recovery.begin(r.ctx)
	}
	if err := r.recovery.wait(r.ctx); err != nil {
		r.finish()
		return 0, err
	}
	n, err := r.reader.Read(p)
	if r.recovery != nil {
		r.recovery.sent.Add(int64(n))
	}
	r.sent.Add(int64(n))
	local := r.local.Add(int64(n))
	if local >= r.total || err != nil {
		r.complete = true
	}
	if tr, _ := r.ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
		tr.sent.Add(int64(n))
	}
	if n > 0 && time.Since(r.last) >= time.Second {
		r.last = time.Now()
		logDAV(r.ctx, runLogEntry{Operation: "分片发送进度", Source: r.source, Result: "传输中", Part: r.index, Parts: r.parts, Attempt: r.attempt, Bytes: r.total, SentBytes: local, ConfirmedBytes: r.confirmed.Load(), DurationMs: time.Since(r.started).Milliseconds(), BytesPerSecond: rateFor(local, time.Since(r.started)), Detail: "发送量为已交给 HTTP 传输的文件正文；确认量为本文件各分片累计"})
	}
	return n, err
}

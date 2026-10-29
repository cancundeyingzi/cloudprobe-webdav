package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type transferOptions struct {
	TransferScheduling   string  `json:"transfer_scheduling"`
	UploadPartMiB        int     `json:"upload_part_mib"`
	DownloadConcurrency  int     `json:"download_concurrency"`
	DownloadPartMiB      int     `json:"download_part_mib"`
	DownloadPartMode     string  `json:"download_part_mode"`
	ConcurrentUploads    int     `json:"concurrent_uploads"`
	ConcurrentDownloads  int     `json:"concurrent_downloads"`
	TotalConnections     int     `json:"total_connections"`
	UploadStorageMode    string  `json:"upload_storage_mode"`
	UploadMemoryMiB      int     `json:"upload_memory_mib"`
	TotalUploadMemoryMiB int     `json:"total_upload_memory_mib"`
	UploadPrefetchMiB    int     `json:"upload_prefetch_mib"`
	UploadRecoveryMode   string  `json:"upload_recovery_mode"`
	UploadLowSpeedMB     float64 `json:"upload_low_speed_mb"`
	UploadLowSeconds     int     `json:"upload_low_seconds"`
	UploadRecoveryTries  int     `json:"upload_recovery_tries"`
}

func (o transferOptions) defaults() transferOptions {
	// An omitted option preserves the queueing behavior of older API clients
	// and saved profiles. Fresh GUI profiles explicitly select client mode.
	if o.TransferScheduling == "" {
		o.TransferScheduling = "server"
	}
	if o.UploadPartMiB == 0 {
		o.UploadPartMiB = 5
	}
	if o.DownloadConcurrency == 0 {
		o.DownloadConcurrency = 8
	}
	if o.DownloadPartMiB == 0 {
		o.DownloadPartMiB = 5
	}
	if o.DownloadPartMode == "" {
		o.DownloadPartMode = "auto"
	}
	if o.ConcurrentUploads == 0 {
		o.ConcurrentUploads = 3
	}
	if o.ConcurrentDownloads == 0 {
		o.ConcurrentDownloads = 4
	}
	if o.TotalConnections == 0 {
		o.TotalConnections = 64
	}
	if o.UploadStorageMode == "" {
		o.UploadStorageMode = "memory"
	}
	if o.UploadMemoryMiB == 0 {
		o.UploadMemoryMiB = 32
	}
	if o.TotalUploadMemoryMiB == 0 {
		o.TotalUploadMemoryMiB = 128
	}
	if o.UploadPrefetchMiB == 0 {
		o.UploadPrefetchMiB = 10
	}
	if o.UploadRecoveryMode == "" {
		o.UploadRecoveryMode = "auto"
	}
	if o.UploadLowSpeedMB == 0 {
		o.UploadLowSpeedMB = 1
	}
	if o.UploadLowSeconds == 0 {
		o.UploadLowSeconds = 5
	}
	if o.UploadRecoveryTries == 0 {
		o.UploadRecoveryTries = 3
	}
	return o
}
func (o transferOptions) validate() error {
	if o.TransferScheduling != "client" && o.TransferScheduling != "server" {
		return errors.New("并发调度方式需为 client 或 server")
	}
	if err := o.validateUploadPolicy(); err != nil {
		return err
	}
	if o.UploadPartMiB < 5 || o.UploadPartMiB > 16 || o.DownloadConcurrency < 1 || o.DownloadConcurrency > 16 || o.DownloadPartMiB < 4 || o.DownloadPartMiB > 32 || o.ConcurrentUploads < 1 || o.ConcurrentUploads > 5 || o.ConcurrentDownloads < 1 || o.ConcurrentDownloads > 5 || o.TotalConnections < 1 || o.TotalConnections > 160 {
		return errors.New("上传分片 5—16 MiB、下载并发 1—16、下载分片 4—32 MiB、同时文件数 1—5、总连接预算 1—160")
	}
	if o.DownloadPartMode != "auto" && o.DownloadPartMode != "fixed" {
		return errors.New("下载分片模式需为 auto 或 fixed")
	}
	return nil
}

type limiterWaiter struct {
	ready   chan struct{}
	granted bool
}
type fairLimiter struct {
	mu            sync.Mutex
	limit, active int
	queue         []*limiterWaiter
}

func newFairLimiter(limit int) *fairLimiter { return &fairLimiter{limit: max(1, limit)} }
func (l *fairLimiter) acquire(ctx context.Context) (func(), error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	w := &limiterWaiter{ready: make(chan struct{})}
	l.mu.Lock()
	l.queue = append(l.queue, w)
	l.pump()
	l.mu.Unlock()
	select {
	case <-w.ready:
		if err := ctx.Err(); err != nil {
			l.release()
			return nil, err
		}
		var once sync.Once
		return func() { once.Do(l.release) }, nil
	case <-ctx.Done():
		l.mu.Lock()
		if w.granted {
			l.active--
		} else {
			for i, v := range l.queue {
				if v == w {
					l.queue = append(l.queue[:i], l.queue[i+1:]...)
					break
				}
			}
		}
		l.pump()
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}
func (l *fairLimiter) pump() {
	for (l.limit == 0 || l.active < l.limit) && len(l.queue) > 0 {
		w := l.queue[0]
		l.queue = l.queue[1:]
		l.active++
		w.granted = true
		close(w.ready)
	}
}
func (l *fairLimiter) release() { l.mu.Lock(); l.active--; l.pump(); l.mu.Unlock() }
func (l *fairLimiter) status() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]int{"active": l.active, "queued": len(l.queue), "limit": l.limit}
}

// Requests for different byte ranges of the same file share one file-level
// lease; each HTTP body still consumes the separate global connection budget.
type fileLease struct {
	ready   chan struct{}
	refs    int
	err     error
	release func()
}
type fileScheduler struct {
	mu      sync.Mutex
	limiter *fairLimiter
	files   map[string]*fileLease
}

func newFileScheduler(n int) *fileScheduler {
	limiter := newFairLimiter(n)
	if n == 0 {
		// Only file scheduling can be unbounded. HTTP bodies and upload buffers
		// retain their separate, finite resource budgets in either mode.
		limiter.limit = 0
	}
	return &fileScheduler{limiter: limiter, files: map[string]*fileLease{}}
}
func (s *fileScheduler) acquire(ctx context.Context, key string) (func(), error) {
	s.mu.Lock()
	if existing := s.files[key]; existing != nil {
		existing.refs++
		s.mu.Unlock()
		select {
		case <-existing.ready:
		case <-ctx.Done():
			s.drop(key, existing)
			return nil, ctx.Err()
		}
		if existing.err != nil {
			s.drop(key, existing)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return s.acquire(ctx, key)
		}
		var once sync.Once
		return func() { once.Do(func() { s.drop(key, existing) }) }, nil
	}
	lease := &fileLease{ready: make(chan struct{}), refs: 1}
	s.files[key] = lease
	s.mu.Unlock()
	release, err := s.limiter.acquire(ctx)
	s.mu.Lock()
	lease.release, lease.err = release, err
	if err != nil && s.files[key] == lease {
		delete(s.files, key)
	}
	close(lease.ready)
	s.mu.Unlock()
	if err != nil {
		s.drop(key, lease)
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { s.drop(key, lease) }) }, nil
}
func (s *fileScheduler) drop(key string, l *fileLease) {
	s.mu.Lock()
	l.refs--
	if l.refs == 0 {
		if s.files[key] == l {
			delete(s.files, key)
		}
		if l.release != nil {
			l.release()
		}
	}
	s.mu.Unlock()
}

type transferRuntime struct {
	options            transferOptions
	uploads, downloads *fileScheduler
	connections        *fairLimiter
	memory             *byteBudget
	recovery           *uploadRecovery
}

func newTransferRuntime(o transferOptions) *transferRuntime {
	o = o.defaults()
	uploads, downloads := o.ConcurrentUploads, o.ConcurrentDownloads
	if o.TransferScheduling == "client" {
		uploads, downloads = 0, 0
	}
	return &transferRuntime{options: o, uploads: newFileScheduler(uploads), downloads: newFileScheduler(downloads), connections: newFairLimiter(o.TotalConnections), memory: newByteBudget(int64(o.TotalUploadMemoryMiB) << 20), recovery: newUploadRecovery(o)}
}
func (t *transferRuntime) status() map[string]any {
	if t == nil {
		return map[string]any{}
	}
	return map[string]any{"uploads": t.uploads.limiter.status(), "downloads": t.downloads.limiter.status(), "connections": t.connections.status(), "upload_memory": t.memory.status(), "upload_recovery": t.recovery.status()}
}

type transferRuntimeKey struct{}

func withTransfers(ctx context.Context, t *transferRuntime) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, transferRuntimeKey{}, t)
}
func transfersFrom(ctx context.Context) *transferRuntime {
	t, _ := ctx.Value(transferRuntimeKey{}).(*transferRuntime)
	return t
}
func (t *transferRuntime) file(ctx context.Context, kind, key string) (func(), error) {
	start := time.Now()
	s := t.downloads
	if kind == "上传" {
		s = t.uploads
	}
	logDAV(ctx, runLogEntry{Operation: "文件调度", Source: "本机", Result: "等待", Detail: kind + "文件：" + key})
	release, err := s.acquire(ctx, key)
	detail := fmt.Sprintf("%s文件；服务端调度：上传上限 %d，下载上限 %d", kind, t.options.ConcurrentUploads, t.options.ConcurrentDownloads)
	if t.options.TransferScheduling == "client" {
		detail = kind + "文件；客户端调度：不额外限制同时文件数"
	}
	detail += fmt.Sprintf("；总正文请求并发预算 %d（HTTP/2 可复用连接），上传内存预算继续生效", t.options.TotalConnections)
	e := runLogEntry{Operation: "文件调度", Source: "本机", Result: "开始", DurationMs: time.Since(start).Milliseconds(), Detail: detail}
	if err != nil {
		e.Result = "取消"
		e.Error = err.Error()
	}
	logDAV(ctx, e)
	return release, err
}

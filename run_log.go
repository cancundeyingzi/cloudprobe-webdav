package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Only these explicitly selected fields reach the runtime log. Never log request
// headers, API payloads, signed download URLs, or file contents here.
type runLogEntry struct {
	ID               uint64       `json:"id"`
	Time             string       `json:"time"`
	RequestID        string       `json:"request_id,omitempty"`
	Method           string       `json:"method,omitempty"`
	Mount            string       `json:"mount,omitempty"`
	Provider         string       `json:"provider,omitempty"`
	Path             string       `json:"path,omitempty"`
	Operation        string       `json:"operation"`
	Source           string       `json:"source"`
	Endpoint         string       `json:"endpoint,omitempty"`
	Worker           int          `json:"worker,omitempty"`
	ConnectionID     string       `json:"connection_id,omitempty"`
	Protocol         string       `json:"protocol,omitempty"`
	LocalAddress     string       `json:"local_address,omitempty"`
	RemoteAddress    string       `json:"remote_address,omitempty"`
	ConnectionReused bool         `json:"connection_reused,omitempty"`
	HTTPStatus       int          `json:"http_status,omitempty"`
	Result           string       `json:"result"`
	Detail           string       `json:"detail,omitempty"`
	Error            string       `json:"error,omitempty"`
	DurationMs       int64        `json:"duration_ms,omitempty"`
	SendMs           int64        `json:"send_ms,omitempty"`
	DiskWriteMs      int64        `json:"disk_write_ms,omitempty"`
	ConfirmWaitMs    int64        `json:"confirm_wait_ms,omitempty"`
	QueueMs          int64        `json:"queue_ms,omitempty"`
	Bytes            int64        `json:"bytes,omitempty"`
	ReceivedBytes    int64        `json:"received_bytes,omitempty"`
	SentBytes        int64        `json:"sent_bytes,omitempty"`
	ConfirmedBytes   int64        `json:"confirmed_bytes,omitempty"`
	ResponseBytes    int64        `json:"response_bytes,omitempty"`
	BytesPerSecond   float64      `json:"bytes_per_second,omitempty"`
	Part             int64        `json:"part,omitempty"`
	Parts            int64        `json:"parts,omitempty"`
	Attempt          int          `json:"attempt,omitempty"`
	Count            int          `json:"count,omitempty"`
	Items            []runLogItem `json:"items,omitempty"`
}
type runLogItem struct {
	Name      string `json:"name"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
	Kind      string `json:"kind,omitempty"`
}
type runtimeLog struct {
	mu                                     sync.Mutex
	file                                   *os.File
	path                                   string
	runID                                  string
	seq                                    uint64
	entries                                []runLogEntry
	err                                    string
	clean                                  func(string) string
	fileBytes, maxFileBytes, maxTotalBytes int64
	maxFiles, segment                      int
	segments                               []string
}

func newRuntimeLog(profile string, clean func(string) string) *runtimeLog {
	id := time.Now().Format("20060102-150405") + "-" + randomKey()[:8]
	l := &runtimeLog{runID: id, path: filepath.Join(profile, "run-logs", id+".jsonl"), clean: clean}
	l.maxFileBytes, l.maxTotalBytes, l.maxFiles = 16<<20, 128<<20, 8
	if e := os.MkdirAll(filepath.Dir(l.path), 0700); e != nil {
		l.err = e.Error()
	} else if f, e := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600); e != nil {
		l.err = e.Error()
	} else {
		l.file = f
		l.segments = []string{l.path}
		if err := l.pruneLocked(); err != nil {
			l.err = err.Error()
		}
	}
	return l
}
func (l *runtimeLog) sanitize(s string) string {
	if l.clean != nil {
		s = l.clean(s)
	}
	if len(s) > 4096 {
		s = s[:4096] + "…"
	}
	// Even opaque signatures in the URL path must not enter exported diagnostics.
	s = urlPattern.ReplaceAllStringFunc(s, func(raw string) string {
		u, e := url.Parse(raw)
		if e != nil {
			return "[URL 已隐藏]"
		}
		return u.Scheme + "://" + u.Host + "/[地址已隐藏]"
	})
	return strings.Map(func(r rune) rune {
		if r < 32 && r != '\t' {
			return ' '
		}
		return r
	}, s)
}
func (l *runtimeLog) add(e runLogEntry) {
	if l == nil {
		return
	}
	e.RequestID = l.sanitize(e.RequestID)
	e.Method = l.sanitize(e.Method)
	e.Mount = l.sanitize(e.Mount)
	e.Path = l.sanitize(e.Path)
	e.Source = l.sanitize(e.Source)
	e.Endpoint = l.sanitize(e.Endpoint)
	e.Detail = l.sanitize(e.Detail)
	e.Error = l.sanitize(e.Error)
	e.Items = append([]runLogItem(nil), e.Items[:min(100, len(e.Items))]...)
	for i := range e.Items {
		e.Items[i].Name = l.sanitize(e.Items[i].Name)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	e.ID, e.Time = l.seq, time.Now().Format(time.RFC3339Nano)
	l.entries = append(l.entries, e)
	if len(l.entries) > 800 {
		l.entries = l.entries[len(l.entries)-800:]
	}
	if l.file != nil && l.err == "" {
		data, err := json.Marshal(e)
		data = append(data, '\n')
		if err == nil && l.fileBytes+int64(len(data)) > l.maxFileBytes {
			err = l.rotateLocked()
		}
		if err == nil {
			var n int
			n, err = l.file.Write(data)
			l.fileBytes += int64(n)
		}
		if err != nil {
			l.err = err.Error()
		}
	}
}
func (l *runtimeLog) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		_ = l.file.Sync()
		_ = l.file.Close()
		l.file = nil
	}
}
func (l *runtimeLog) status() map[string]any {
	if l == nil {
		return map[string]any{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{"run_id": l.runID, "last_id": l.seq, "file": l.path, "error": l.err, "file_limit_bytes": l.maxFileBytes, "total_limit_bytes": l.maxTotalBytes, "retained_files": len(l.segments)}
}

type runTraceKey struct{}
type runTrace struct {
	log                               *runtimeLog
	id, method, mount, provider, path string
	sent, confirmed, downloaded       *atomic.Int64
}

func newRunTrace(ctx context.Context, log *runtimeLog, method, target string) context.Context {
	return context.WithValue(ctx, runTraceKey{}, &runTrace{log: log, id: randomKey()[:12], method: method, path: target, sent: &atomic.Int64{}, confirmed: &atomic.Int64{}, downloaded: &atomic.Int64{}})
}
func scopedDAV(ctx context.Context, m *davMount, target string) context.Context {
	t, _ := ctx.Value(runTraceKey{}).(*runTrace)
	if t == nil {
		return ctx
	}
	copy := *t
	if m != nil {
		copy.mount, copy.provider = m.Alias, m.Provider
	}
	if target != "" {
		copy.path = target
	}
	return context.WithValue(ctx, runTraceKey{}, &copy)
}
func directoryLogContext(ctx context.Context) context.Context {
	if tr, _ := ctx.Value(runTraceKey{}).(*runTrace); tr != nil {
		return scopedDAV(ctx, nil, path.Dir(tr.path))
	}
	return ctx
}
func logDAV(ctx context.Context, e runLogEntry) {
	if ctx == nil {
		return
	}
	t, _ := ctx.Value(runTraceKey{}).(*runTrace)
	if t == nil || t.log == nil {
		return
	}
	e.RequestID, e.Method, e.Mount, e.Provider = t.id, t.method, t.mount, t.provider
	if e.Path == "" {
		e.Path = t.path
	}
	t.log.add(e)
}
func logPhase(ctx context.Context, op, source, detail string) func(error) {
	start := time.Now()
	logDAV(ctx, runLogEntry{Operation: op, Source: source, Result: "开始", Detail: detail})
	return func(err error) {
		e := runLogEntry{Operation: op, Source: source, Result: "成功", Detail: detail, DurationMs: time.Since(start).Milliseconds()}
		if err != nil {
			e.Result, e.Error = "失败", err.Error()
		}
		logDAV(ctx, e)
	}
}
func logList(ctx context.Context, source, endpoint string, start time.Time, entries []davEntry, err error) {
	e := runLogEntry{Operation: "读取目录", Source: source, Endpoint: endpoint, Result: "成功", Count: len(entries), DurationMs: time.Since(start).Milliseconds()}
	if err != nil {
		e.Result, e.Error = "失败", err.Error()
		logDAV(ctx, e)
		return
	}
	// Chunk the item details so large directories do not create a single huge row.
	for offset := 0; offset < len(entries) || offset == 0; offset += 100 {
		row := e
		if offset > 0 {
			row.Operation = "目录条目详情"
			row.DurationMs = 0
		}
		for _, it := range entries[offset:min(offset+100, len(entries))] {
			row.Items = append(row.Items, runLogItem{Name: it.Name, Directory: it.Directory, Size: it.Size, Kind: it.Kind})
		}
		logDAV(ctx, row)
		if len(entries) == 0 {
			break
		}
	}
}
func logFallback(ctx context.Context, from, to string, err error) {
	e := runLogEntry{Operation: "切换备用接口", Source: from + " → " + to, Result: "回退"}
	if err != nil {
		e.Error = err.Error()
	}
	logDAV(ctx, e)
}
func nodeHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "未知节点"
	}
	return u.Host
}
func logURL(ctx context.Context, source, endpoint string, start time.Time, raw string, err error) {
	e := runLogEntry{Operation: "获取下载地址", Source: source, Endpoint: endpoint, Result: "成功", DurationMs: time.Since(start).Milliseconds()}
	if err != nil {
		e.Result, e.Error = "失败", err.Error()
	} else {
		e.Detail = "正文节点：" + nodeHost(raw) + "；此步骤只获取地址"
	}
	logDAV(ctx, e)
}
func rateFor(n int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(n) / elapsed.Seconds()
}
func (g *guiApp) runLogAPI(w http.ResponseWriter, r *http.Request) {
	l := g.runLog
	if l == nil {
		respondJSON(w, 200, map[string]any{"entries": []runLogEntry{}})
		return
	}
	if r.URL.Path == "/api/run-log/export" {
		l.mu.Lock()
		if l.file == nil {
			l.mu.Unlock()
			g.fail(w, fmt.Errorf("运行日志文件不可用"))
			return
		}
		var readers []io.Reader
		var files []*os.File
		var size int64
		defer func() {
			for _, f := range files {
				_ = f.Close()
			}
		}()
		for _, path := range l.segments {
			f, err := os.Open(path)
			if err != nil {
				l.mu.Unlock()
				g.fail(w, err)
				return
			}
			files = append(files, f)
			fi, err := f.Stat()
			if err != nil {
				l.mu.Unlock()
				g.fail(w, err)
				return
			}
			size += fi.Size()
			readers = append(readers, io.NewSectionReader(f, 0, fi.Size()))
		}
		name := "cloudprobe-run-" + l.runID + ".jsonl"
		l.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		_, _ = io.Copy(w, io.MultiReader(readers...))
		return
	}
	after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	l.mu.Lock()
	rows := []runLogEntry{}
	for _, e := range l.entries {
		if e.ID > after && len(rows) < 300 {
			rows = append(rows, e)
		}
	}
	gap := len(l.entries) > 0 && after+1 < l.entries[0].ID
	last, runID, fileError := l.seq, l.runID, l.err
	l.mu.Unlock()
	respondJSON(w, 200, map[string]any{"entries": rows, "last_id": last, "run_id": runID, "gap": gap, "file_error": fileError})
}

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func (o transferOptions) validateUploadPolicy() error {
	if o.UploadStorageMode != "memory" && o.UploadStorageMode != "disk" && o.UploadStorageMode != "auto" {
		return errors.New("上传缓存请选择严格纯内存、内存优先或磁盘兼容模式")
	}
	if o.UploadRecoveryMode != "auto" && o.UploadRecoveryMode != "off" {
		return errors.New("低速恢复模式无效")
	}
	if math.IsNaN(o.UploadLowSpeedMB) || math.IsInf(o.UploadLowSpeedMB, 0) || o.UploadLowSpeedMB < 0.01 || o.UploadLowSpeedMB > 10000 || o.UploadLowSeconds < 1 || o.UploadLowSeconds > 120 || o.UploadRecoveryTries < 1 || o.UploadRecoveryTries > 100 {
		return errors.New("总体低速阈值需 0.01—10000 MB/s，检测窗口 1—120 秒，无效次数 1—100")
	}
	if o.UploadMemoryMiB < o.UploadPartMiB || o.UploadMemoryMiB > 1024 || o.TotalUploadMemoryMiB < o.UploadMemoryMiB || o.TotalUploadMemoryMiB > 4096 || o.UploadPrefetchMiB < 1 || o.UploadPrefetchMiB > o.UploadMemoryMiB {
		return errors.New("单文件内存需至少容纳一片且不超过 1024 MiB；总内存需不小于单文件且不超过 4096 MiB；预读需 1—单文件内存 MiB")
	}
	return nil
}

// One controller belongs to the WebDAV service, never to an individual file,
// provider, part or worker. Sent counts are body bytes handed to HTTP transport;
// confirmed is tracked separately because part acknowledgements arrive in bursts.
type uploadRecovery struct {
	mu                            sync.Mutex
	options                       transferOptions
	sent, confirmed               atomic.Int64
	active, starved               int
	running, manual               bool
	baseAt                        time.Time
	baseBytes                     int64
	rate                          float64
	pauseUntil                    time.Time
	pauseDuration                 time.Duration
	evaluating, limited           bool
	baseline                      float64
	failures, attempts, recovered int
	logCtx                        context.Context
}

func newUploadRecovery(o transferOptions) *uploadRecovery {
	return &uploadRecovery{options: o, pauseDuration: time.Second}
}
func (c *uploadRecovery) begin(ctx context.Context) func() {
	if c == nil {
		return func() {}
	}
	c.mu.Lock()
	c.active++
	c.logCtx = ctx
	if !c.running && !c.manual {
		c.running = true
		go c.loop()
	}
	c.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { c.mu.Lock(); c.active--; c.mu.Unlock() }) }
}
func (c *uploadRecovery) loop() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for now := range ticker.C {
		c.observe(now)
		c.mu.Lock()
		if c.active == 0 {
			c.running = false
			c.baseAt = time.Time{}
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}
func (c *uploadRecovery) waitingForClient() func() {
	if c == nil {
		return func() {}
	}
	c.mu.Lock()
	c.starved++
	c.mu.Unlock()
	return func() { c.mu.Lock(); c.starved--; c.mu.Unlock() }
}
func (c *uploadRecovery) wait(ctx context.Context) error {
	if c == nil {
		return ctx.Err()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		until := c.pauseUntil
		c.mu.Unlock()
		if delay := time.Until(until); delay > 0 {
			if err := davWait(ctx, delay); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}
func (c *uploadRecovery) observe(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	bytes := c.sent.Load()
	if c.active == 0 || c.active <= c.starved {
		c.baseAt = time.Time{}
		c.rate = 0
		return
	}
	if now.Before(c.pauseUntil) {
		c.baseAt = time.Time{}
		return
	}
	if c.baseAt.IsZero() {
		c.baseAt = now
		c.baseBytes = bytes
		return
	}
	elapsed := now.Sub(c.baseAt)
	c.rate = float64(bytes-c.baseBytes) / elapsed.Seconds()
	if elapsed < time.Duration(c.options.UploadLowSeconds)*time.Second {
		return
	}
	c.baseAt = now
	c.baseBytes = bytes
	if c.options.UploadRecoveryMode == "off" || c.limited {
		return
	}
	threshold := c.options.UploadLowSpeedMB * 1e6
	if c.evaluating {
		improved := c.rate >= threshold || c.baseline > 0 && c.rate >= c.baseline*1.2 || c.baseline == 0 && c.rate >= math.Min(threshold*0.2, 64<<10)
		c.evaluating = false
		if improved {
			c.failures = 0
			c.recovered++
			logDAV(c.logCtx, runLogEntry{Operation: "总体上传低速恢复", Source: "全部上传任务", Result: "速度改善", BytesPerSecond: c.rate, Detail: fmt.Sprintf("暂停前 %.0f B/s；所有文件和线程合计；重新观察低速窗口", c.baseline)})
			return
		}
		c.failures++
		if c.failures >= c.options.UploadRecoveryTries {
			c.limited = true
			logDAV(c.logCtx, runLogEntry{Operation: "总体上传低速恢复", Source: "全部上传任务", Result: "按带宽受限处理", BytesPerSecond: c.rate, Detail: fmt.Sprintf("连续 %d 次无明显改善；本次服务运行停止主动暂停，传输及故障无限重试继续", c.failures)})
			return
		}
	}
	if c.rate < threshold {
		c.baseline = c.rate
		c.attempts++
		c.evaluating = true
		c.pauseUntil = now.Add(c.pauseDuration)
		c.baseAt = time.Time{}
		logDAV(c.logCtx, runLogEntry{Operation: "总体上传低速恢复", Source: "全部上传任务", Result: "统一暂停", BytesPerSecond: c.rate, Attempt: c.attempts, Detail: fmt.Sprintf("合计速度低于 %.2f MB/s；全部上传正文暂停 %d ms 后自动恢复；检测窗口 %d 秒；无效 %d/%d", c.options.UploadLowSpeedMB, c.pauseDuration.Milliseconds(), c.options.UploadLowSeconds, c.failures, c.options.UploadRecoveryTries)})
	}
}
func (c *uploadRecovery) status() map[string]any {
	if c == nil {
		return map[string]any{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]any{"scope": "all_uploads", "bytes_per_second": c.rate, "sent_bytes": c.sent.Load(), "confirmed_bytes": c.confirmed.Load(), "active_bodies": c.active, "waiting_for_client": c.starved, "paused": time.Now().Before(c.pauseUntil), "attempts": c.attempts, "consecutive_failures": c.failures, "recovered": c.recovered, "bandwidth_limited": c.limited, "enabled": c.options.UploadRecoveryMode != "off"}
}

func transferRetryDelay(attempt int, override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	return min(30*time.Second, 5*time.Second*time.Duration(1<<min(max(attempt-1, 0), 3)))
}

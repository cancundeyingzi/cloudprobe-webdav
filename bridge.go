package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BridgeReply struct {
	ID            int64  `json:"id"`
	OK            bool   `json:"ok"`
	Ready         bool   `json:"ready"`
	Error         string `json:"error"`
	Code          int    `json:"code"`
	Body          string `json:"body"`
	PNG           string `json:"png"`
	AppID         string `json:"app_id"`
	AppSecret     string `json:"app_secret"`
	NotExecuted   bool   `json:"not_executed,omitempty"`
	ResultUnknown bool   `json:"result_unknown,omitempty"`
}

type OriginalBridge struct {
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	writerOnce sync.Once
	closeOnce  sync.Once
	queue      chan *bridgeCommand
	stopping   chan struct{}
	writerDone chan struct{}
	mu         sync.Mutex
	pending    map[int64]chan BridgeReply
	ready      chan BridgeReply
	done       chan struct{}
	seq        atomic.Int64
	lastErr    error
	stderr     *engineErrorTail
}

func (b *OriginalBridge) Alive() bool {
	if b == nil {
		return false
	}
	select {
	case <-b.stopping:
		return false
	case <-b.done:
		return false
	default:
		return true
	}
}

func startOriginalBridge(ctx context.Context, sdk, bridgeFile, data string) (*OriginalBridge, error) {
	if runtime.GOOS != "windows" || runtime.GOARCH != "amd64" {
		return nil, engineProblem("unsupported_platform", "原版 SDK 只支持 Windows x64。", runtime.GOOS+"/"+runtime.GOARCH)
	}
	configFile := strings.TrimSuffix(bridgeFile, filepath.Ext(bridgeFile)) + ".runtimeconfig.json"
	for _, p := range []string{filepath.Join(sdk, "ntkernel.dll"), filepath.Join(sdk, "BaoMiHua.dll"), filepath.Join(sdk, "QRCoder.dll"), bridgeFile, configFile} {
		info, e := os.Stat(p)
		if e != nil || !info.Mode().IsRegular() {
			code, detail := "component_unreadable", p
			if os.IsNotExist(e) {
				code = "component_missing"
			}
			if e != nil {
				detail += "\n" + e.Error()
			}
			return nil, engineProblem(code, "无法读取运行组件："+filepath.Base(p), detail)
		}
	}
	dotnet, e := exec.LookPath("dotnet")
	if e != nil {
		return nil, engineProblem("runtime_missing", "PATH 中未找到 dotnet 程序。", "需要 Microsoft .NET 8 Windows x64 运行时。")
	}
	if e = checkEngineRuntime(ctx, dotnet); e != nil {
		return nil, e
	}
	if e = os.MkdirAll(data, 0700); e != nil {
		return nil, engineProblem("data_unwritable", "无法创建引擎资料目录。", data+"\n"+e.Error())
	}
	cmd := exec.Command(dotnet, bridgeFile, sdk, data)
	hideChildWindow(cmd)
	cmd.Dir = filepath.Dir(bridgeFile)
	in, e := cmd.StdinPipe()
	if e != nil {
		return nil, e
	}
	out, e := cmd.StdoutPipe()
	if e != nil {
		return nil, e
	}
	tail := &engineErrorTail{}
	cmd.Stderr = tail
	b := &OriginalBridge{cmd: cmd, stdin: in, pending: map[int64]chan BridgeReply{}, ready: make(chan BridgeReply, 1), done: make(chan struct{}), stderr: tail}
	if e = cmd.Start(); e != nil {
		return nil, engineProblem("process_start", "无法启动 .NET 桥接进程。", dotnet+"\n"+bridgeFile+"\n"+e.Error())
	}
	b.startWriter()
	go func() {
		scanner := bufio.NewScanner(out)
		scanner.Buffer(make([]byte, 64<<10), 24<<20)
		for scanner.Scan() {
			var r BridgeReply
			if json.Unmarshal(scanner.Bytes(), &r) != nil {
				continue
			}
			if r.ID == 0 {
				select {
				case b.ready <- r:
				default:
				}
				continue
			}
			b.mu.Lock()
			ch := b.pending[r.ID]
			b.mu.Unlock()
			if ch != nil {
				select {
				case ch <- r:
				default:
				}
			}
		}
		e := cmd.Wait()
		b.mu.Lock()
		b.lastErr = e
		b.mu.Unlock()
		close(b.done)
	}()
	select {
	case r := <-b.ready:
		if !r.OK {
			b.Close()
			return nil, describeEngineError(errors.New(r.Error))
		}
		return b, nil
	case <-b.done:
		// The child may report a structured initialization error and exit in
		// the same scheduling turn. Prefer that reason over a generic exit.
		select {
		case r := <-b.ready:
			if !r.OK {
				return nil, describeEngineError(errors.New(r.Error))
			}
		default:
		}
		return nil, b.exitError()
	case <-ctx.Done():
		b.Close()
		return nil, ctx.Err()
	case <-time.After(25 * time.Second):
		b.Close()
		return nil, engineProblem("timeout", "原版引擎在 25 秒内未报告就绪。", tail.Text())
	}
}
func (b *OriginalBridge) exitError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	detail := "尚未取得进程退出码。"
	if b.cmd != nil && b.cmd.ProcessState != nil {
		detail = fmt.Sprintf("退出码：%d", b.cmd.ProcessState.ExitCode())
	}
	if b.lastErr != nil {
		detail += "\n进程错误：" + b.lastErr.Error()
	}
	if b.cmd != nil {
		detail += "\ndotnet：" + b.cmd.Path
	}
	if tail := b.stderr.Text(); tail != "" {
		detail += "\n标准错误输出：\n" + tail
	}
	detail = redactEngineDetail(detail, (&Redactor{}).Text)
	return describeEngineError(errors.New(detail))
}

type bridgeCommand struct {
	ctx    context.Context
	id     int64
	data   []byte
	result chan bridgeCallResult
	state  atomic.Int32 // 0 queued, 1 dispatched, 2 canceled before dispatch.
}
type bridgeCallResult struct {
	reply BridgeReply
	err   error
}

func (b *OriginalBridge) startWriter() {
	b.writerOnce.Do(func() {
		b.queue = make(chan *bridgeCommand, 32)
		b.stopping = make(chan struct{})
		b.writerDone = make(chan struct{})
		if b.pending == nil {
			b.pending = map[int64]chan BridgeReply{}
		}
		go func() {
			defer close(b.writerDone)
			for {
				select {
				case <-b.stopping:
					return
				case <-b.done:
					return
				case command := <-b.queue:
					if command.ctx.Err() != nil {
						continue
					}
					r, err := b.exchange(command)
					command.result <- bridgeCallResult{r, err}
				}
			}
		}()
	})
}
func (b *OriginalBridge) stopInput() {
	b.startWriter()
	b.closeOnce.Do(func() {
		close(b.stopping)
		if b.stdin != nil {
			_ = b.stdin.Close()
		}
	})
}
func (b *OriginalBridge) kill() {
	b.stopInput()
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
	}
}

// Only one command may be dispatched to the serial SDK at a time. Canceled
// commands remain in the Go queue, where they can be discarded before any IPC.
func (b *OriginalBridge) exchange(command *bridgeCommand) (BridgeReply, error) {
	ch := make(chan BridgeReply, 1)
	b.mu.Lock()
	b.pending[command.id] = ch
	b.mu.Unlock()
	defer func() { b.mu.Lock(); delete(b.pending, command.id); b.mu.Unlock() }()
	if err := command.ctx.Err(); err != nil {
		return BridgeReply{}, err
	}
	if !command.state.CompareAndSwap(0, 1) {
		return BridgeReply{}, command.ctx.Err()
	}
	stopCancel := context.AfterFunc(command.ctx, b.kill)
	n, err := b.stdin.Write(command.data)
	canceledDuringWrite := !stopCancel()
	if err != nil || n != len(command.data) || canceledDuringWrite {
		b.kill() // A partial JSON line must never be followed by another request.
		return BridgeReply{}, errors.New("桥接发送中断，SDK 操作结果未知")
	}
	deadline, _ := command.ctx.Deadline()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case r := <-ch:
		if !r.OK {
			if r.NotExecuted {
				return r, &davNotExecuted{errors.New(r.Error)}
			}
			if r.ResultUnknown {
				b.kill()
				return r, fmt.Errorf("SDK 操作结果未知，已终止桥接：%s", r.Error)
			}
			return r, errors.New(r.Error)
		}
		return r, nil
	case <-b.stopping:
		return BridgeReply{}, errors.New("桥接已关闭，SDK 操作结果未知")
	case <-b.done:
		return BridgeReply{}, b.exitError()
	case <-timer.C:
		b.kill()
		return BridgeReply{}, errors.New("SDK 响应超时，操作结果未知；已终止桥接")
	}
}
func (b *OriginalBridge) Call(ctx context.Context, command map[string]any) (BridgeReply, error) {
	b.startWriter()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return BridgeReply{}, &davNotExecuted{err}
	}
	id := b.seq.Add(1)
	copy := make(map[string]any, len(command)+2)
	for key, value := range command {
		copy[key] = value
	}
	copy["id"] = id
	deadline, _ := ctx.Deadline()
	copy["deadline_unix_ms"] = deadline.UnixMilli()
	data, err := compactJSON(copy)
	if err != nil {
		return BridgeReply{}, &davNotExecuted{err}
	}
	c := &bridgeCommand{ctx: ctx, id: id, data: append(data, '\n'), result: make(chan bridgeCallResult, 1)}
	select {
	case <-b.stopping:
		return BridgeReply{}, &davNotExecuted{errors.New("桥接已关闭")}
	case <-b.done:
		return BridgeReply{}, &davNotExecuted{b.exitError()}
	default:
	}
	select {
	case b.queue <- c:
	default:
		return BridgeReply{}, &davNotExecuted{errors.New("桥接请求队列已满，请稍后重试")}
	}
	select {
	case result := <-c.result:
		return result.reply, result.err
	case <-ctx.Done():
		if c.state.CompareAndSwap(0, 2) {
			return BridgeReply{}, &davNotExecuted{ctx.Err()}
		}
		return BridgeReply{}, fmt.Errorf("SDK 操作可能已提交，结果未知：%w", ctx.Err())
	case <-b.stopping:
		return BridgeReply{}, errors.New("桥接已关闭，请核对已提交操作的结果")
	case <-b.done:
		return BridgeReply{}, b.exitError()
	}
}
func (b *OriginalBridge) Request(ctx context.Context, method, path string, parameters []byte, headers map[string]string) ([]byte, error) {
	if len(parameters) == 0 {
		parameters = []byte("{}")
	}
	r, e := b.Call(ctx, map[string]any{"kind": "request", "method": method, "path": path, "parameters": string(parameters), "headers": headers})
	return []byte(r.Body), e
}
func (b *OriginalBridge) Close() {
	if b == nil {
		return
	}
	b.stopInput() // Closing the pipe is independent of the normal IPC queue.
	select {
	case <-b.done:
		return
	case <-time.After(2 * time.Second):
		b.kill()
	}
}

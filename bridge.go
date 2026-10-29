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
	"sync"
	"sync/atomic"
	"time"
)

type BridgeReply struct {
	ID        int64  `json:"id"`
	OK        bool   `json:"ok"`
	Ready     bool   `json:"ready"`
	Error     string `json:"error"`
	Code      int    `json:"code"`
	Body      string `json:"body"`
	PNG       string `json:"png"`
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type OriginalBridge struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[int64]chan BridgeReply
	ready   chan BridgeReply
	done    chan struct{}
	seq     atomic.Int64
	lastErr error
}

func (b *OriginalBridge) Alive() bool {
	if b == nil {
		return false
	}
	select {
	case <-b.done:
		return false
	default:
		return true
	}
}

func startOriginalBridge(ctx context.Context, sdk, bridgeFile, data string) (*OriginalBridge, error) {
	for _, p := range []string{filepath.Join(sdk, "ntkernel.dll"), filepath.Join(sdk, "BaoMiHua.dll"), filepath.Join(sdk, "QRCoder.dll"), bridgeFile} {
		if _, e := os.Stat(p); e != nil {
			return nil, fmt.Errorf("缺少运行组件 %s", filepath.Base(p))
		}
	}
	dotnet, e := exec.LookPath("dotnet")
	if e != nil {
		return nil, errors.New("未找到 dotnet；此原版桥接需要 .NET 8 x64 运行时")
	}
	if e = os.MkdirAll(data, 0700); e != nil {
		return nil, e
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
	cmd.Stderr = io.Discard
	b := &OriginalBridge{cmd: cmd, stdin: in, pending: map[int64]chan BridgeReply{}, ready: make(chan BridgeReply, 1), done: make(chan struct{})}
	if e = cmd.Start(); e != nil {
		return nil, e
	}
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
			return nil, errors.New(r.Error)
		}
		return b, nil
	case <-b.done:
		return nil, b.exitError()
	case <-ctx.Done():
		b.Close()
		return nil, ctx.Err()
	case <-time.After(25 * time.Second):
		b.Close()
		return nil, errors.New("原版引擎初始化超时")
	}
}
func (b *OriginalBridge) exitError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return fmt.Errorf("原版桥接进程已退出（%v）；请检查 .NET 8 运行时和 SDK 依赖", b.lastErr)
}
func (b *OriginalBridge) Call(ctx context.Context, command map[string]any) (BridgeReply, error) {
	id := b.seq.Add(1)
	command["id"] = id
	ch := make(chan BridgeReply, 1)
	b.mu.Lock()
	b.pending[id] = ch
	b.mu.Unlock()
	defer func() { b.mu.Lock(); delete(b.pending, id); b.mu.Unlock() }()
	data, e := compactJSON(command)
	if e != nil {
		return BridgeReply{}, e
	}
	data = append(data, '\n')
	b.writeMu.Lock()
	_, e = b.stdin.Write(data)
	b.writeMu.Unlock()
	if e != nil {
		return BridgeReply{}, errors.New("原版桥接连接已关闭")
	}
	select {
	case r := <-ch:
		if !r.OK {
			return r, fmt.Errorf("%s", r.Error)
		}
		return r, nil
	case <-ctx.Done():
		return BridgeReply{}, ctx.Err()
	case <-b.done:
		return BridgeReply{}, b.exitError()
	case <-time.After(40 * time.Second):
		return BridgeReply{}, errors.New("等待原版引擎响应超时")
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = b.Call(ctx, map[string]any{"kind": "stop"})
	_ = b.stdin.Close()
	select {
	case <-b.done:
		return
	case <-time.After(2 * time.Second):
		if b.cmd.Process != nil {
			_ = b.cmd.Process.Kill()
		}
	}
}

package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
)

// A canceled request must be able to leave a metadata lock's queue.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.gate = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *contextMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *contextMutex) Unlock() { <-m.gate }

type davStatusError struct {
	Code    int
	Message string
	Err     error
}

func (e *davStatusError) Error() string { return e.Message }
func (e *davStatusError) Unwrap() error { return e.Err }
func davLocalError(ctx context.Context, code int, message string) error {
	err := &davStatusError{Code: code, Message: message}
	if state := davState(ctx); state != nil {
		state.err = err
	}
	return err
}

type davReservation struct {
	owner *davRequestState
	paths []string
}
type davReservations struct {
	mu     sync.Mutex
	next   uint64
	active map[uint64]davReservation
}

func pathsOverlap(a, b string) bool {
	return a == b || a == "/" || b == "/" || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func (r *davReservations) acquire(ctx context.Context, targets ...string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(targets))
	for _, target := range targets {
		clean, err := davPath(target)
		if err != nil {
			return nil, err
		}
		paths = append(paths, clean)
	}
	owner := davState(ctx)
	if owner == nil {
		owner = &davRequestState{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, held := range r.active {
		if held.owner == owner {
			continue
		}
		for _, a := range paths {
			for _, b := range held.paths {
				if pathsOverlap(a, b) {
					logDAV(ctx, runLogEntry{Operation: "路径写入冲突", Source: "本机", Result: "占用", Detail: "目标或其祖先/子目录正在写入；独立文件不受影响"})
					return nil, davLocalError(ctx, http.StatusLocked, "目标或其祖先/子目录正在写入，请稍后重试")
				}
			}
		}
	}
	if r.active == nil {
		r.active = make(map[uint64]davReservation)
	}
	r.next++
	id := r.next
	r.active[id] = davReservation{owner: owner, paths: paths}
	var once sync.Once
	return func() { once.Do(func() { r.mu.Lock(); delete(r.active, id); r.mu.Unlock() }) }, nil
}
func (f *davFS) reserve(ctx context.Context, targets ...string) (func(), error) {
	release, err := f.reservations.acquire(ctx, targets...)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, davLocalError(ctx, http.StatusRequestTimeout, "请求已取消或超时，未执行写入")
	}
	return release, err
}

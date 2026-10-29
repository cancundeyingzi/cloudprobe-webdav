package main

import (
	"context"
	"io"
	"sync"
	"time"
)

type contextualReaderAt interface {
	ReadAtContext(context.Context, []byte, int64) (int, error)
}
type requestReaderAt struct {
	ctx    context.Context
	source contextualReaderAt
}

func (r requestReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return r.source.ReadAtContext(r.ctx, p, off)
}
func uploadSection(ctx context.Context, source io.ReaderAt, off, n int64) io.Reader {
	if r, ok := source.(contextualReaderAt); ok {
		return io.NewSectionReader(requestReaderAt{ctx: ctx, source: r}, off, n)
	}
	return io.NewSectionReader(source, off, n)
}

// Closing one HTTP attempt cancels only its reads. The file's cached bytes stay
// available for retries. No mutex is held across a potentially blocking Read.
type uploadBody struct {
	mu     sync.Mutex
	reader io.Reader
	cancel context.CancelFunc
	closed bool
	reads  sync.WaitGroup
	once   sync.Once
}

func newUploadBody(ctx context.Context, makeReader func(context.Context) io.Reader) *uploadBody {
	readCtx, cancel := context.WithCancel(ctx)
	return &uploadBody{reader: makeReader(readCtx), cancel: cancel}
}

func uploadAttemptContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}
func (b *uploadBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.reads.Add(1)
	reader := b.reader
	b.mu.Unlock()
	defer b.reads.Done()
	return reader.Read(p)
}
func (b *uploadBody) Close() error {
	b.once.Do(func() { b.mu.Lock(); b.closed = true; b.mu.Unlock(); b.cancel() })
	b.reads.Wait()
	return nil
}

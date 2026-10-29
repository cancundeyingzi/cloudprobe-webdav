package main

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
)

type byteWaiter struct {
	n       int64
	ready   chan struct{}
	granted bool
}
type byteBudget struct {
	mu                  sync.Mutex
	limit, used, peak   int64
	queue               []*byteWaiter
	slabs               chan []byte
	slabSize, allocated int64
}

func newByteBudget(n int64) *byteBudget { return &byteBudget{limit: n} }
func (b *byteBudget) pump() {
	for len(b.queue) > 0 && b.queue[0].n <= b.limit-b.used {
		w := b.queue[0]
		b.queue = b.queue[1:]
		b.used += w.n
		b.peak = max(b.peak, b.used)
		w.granted = true
		close(w.ready)
	}
}
func (b *byteBudget) acquire(ctx context.Context, n int64) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n < 0 || n > b.limit {
		return nil, errors.New("单个分片超出内存预算")
	}
	w := &byteWaiter{n: n, ready: make(chan struct{})}
	b.mu.Lock()
	b.queue = append(b.queue, w)
	b.pump()
	b.mu.Unlock()
	select {
	case <-w.ready:
		if err := ctx.Err(); err != nil {
			b.release(n)
			return nil, err
		}
		var once sync.Once
		return func() { once.Do(func() { b.release(n) }) }, nil
	case <-ctx.Done():
		b.mu.Lock()
		if w.granted {
			b.used -= n
		} else {
			for i, q := range b.queue {
				if q == w {
					b.queue = append(b.queue[:i], b.queue[i+1:]...)
					break
				}
			}
		}
		b.pump()
		b.mu.Unlock()
		return nil, ctx.Err()
	}
}
func (b *byteBudget) release(n int64) { b.mu.Lock(); b.used -= n; b.pump(); b.mu.Unlock() }
func (b *byteBudget) status() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return map[string]any{"used_bytes": b.used, "peak_bytes": b.peak, "limit_bytes": b.limit, "queued": len(b.queue), "allocated_bytes": b.allocated}
}

type uploadBuffer struct {
	blocks  [][]byte
	quantum int
}

func (b *uploadBuffer) copyIn(off int, p []byte) int {
	written := 0
	for len(p) > 0 {
		n := copy(b.blocks[off/b.quantum][off%b.quantum:], p)
		written += n
		off += n
		p = p[n:]
	}
	return written
}
func (b *uploadBuffer) copyOut(p []byte, off, end int) int {
	written := 0
	for len(p) > 0 && off < end {
		block := b.blocks[off/b.quantum]
		n := copy(p, block[off%b.quantum:min(len(block), off%b.quantum+end-off)])
		written += n
		off += n
		p = p[n:]
	}
	return written
}

// One small reusable block pool serves different cloud part sizes. Reserve all
// blocks of a part atomically, so concurrent files cannot each hold an incomplete
// part and exhaust the global budget before any cloud acknowledgement.
func (b *byteBudget) reservation(size int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.slabs == nil {
		b.slabSize = min(int64(64<<10), max(int64(1), size), max(int64(1), b.limit))
		count := b.limit / b.slabSize
		b.slabs = make(chan []byte, int(count))
		for range count {
			b.slabs <- nil
		}
	}
	return ((size + b.slabSize - 1) / b.slabSize) * b.slabSize
}
func (b *byteBudget) takePart(ctx context.Context, size int64) (*uploadBuffer, func(), error) {
	reserved := b.reservation(size)
	releaseBudget, err := b.acquire(ctx, reserved)
	if err != nil {
		return nil, nil, err
	}
	buffer := &uploadBuffer{quantum: int(b.slabSize)}
	for range reserved / b.slabSize {
		var block []byte
		select {
		case block = <-b.slabs:
		case <-ctx.Done():
			for _, prior := range buffer.blocks {
				b.slabs <- prior
			}
			releaseBudget()
			return nil, nil, ctx.Err()
		}
		if block == nil {
			block = make([]byte, b.slabSize)
			b.mu.Lock()
			b.allocated += b.slabSize
			b.mu.Unlock()
		}
		buffer.blocks = append(buffer.blocks, block)
	}
	var once sync.Once
	return buffer, func() {
		once.Do(func() {
			for _, block := range buffer.blocks {
				b.slabs <- block
			}
			buffer.blocks = nil
			releaseBudget()
		})
	}, nil
}

type memoryPart struct {
	data    *uploadBuffer
	length  int
	filled  int
	release func()
}

// A sequential producer and concurrent ReaderAt consumers. Retain every part
// until cloud acknowledgement; a retry reads the same bytes with no disk spool.
type memoryUploadSource struct {
	ctx                      context.Context
	cancel                   context.CancelFunc
	mu                       sync.Mutex
	changed                  chan struct{}
	parts                    map[int64]*memoryPart
	size, partSize, received int64
	fileBudget, global       *byteBudget
	recovery                 *uploadRecovery
	inputDone                bool
	err                      error
	md5Hash, sha1Hash        hash.Hash
}

func newMemoryUploadSource(ctx context.Context, size, partSize, fileLimit int64, global *byteBudget, recovery *uploadRecovery) *memoryUploadSource {
	ctx, cancel := context.WithCancel(ctx)
	return &memoryUploadSource{ctx: ctx, cancel: cancel, size: size, partSize: partSize, fileBudget: newByteBudget(fileLimit), global: global, recovery: recovery, changed: make(chan struct{}), parts: map[int64]*memoryPart{}}
}
func (s *memoryUploadSource) signal()         { close(s.changed); s.changed = make(chan struct{}) }
func (s *memoryUploadSource) Received() int64 { s.mu.Lock(); defer s.mu.Unlock(); return s.received }
func (s *memoryUploadSource) Write(p []byte) (written int, err error) {
	for len(p) > 0 {
		if err = s.ctx.Err(); err != nil {
			s.mu.Lock()
			if s.err != nil {
				err = s.err
			}
			s.mu.Unlock()
			return written, err
		}
		s.mu.Lock()
		if s.err != nil || s.inputDone {
			err = s.err
			if err == nil {
				err = io.ErrClosedPipe
			}
			s.mu.Unlock()
			return written, err
		}
		off := s.received
		if int64(len(p)) > s.size-off {
			s.mu.Unlock()
			return written, errors.New("客户端正文超过声明文件长度")
		}
		index := off / s.partSize
		part := s.parts[index]
		s.mu.Unlock()
		if part == nil {
			n := min(s.partSize, s.size-index*s.partSize)
			releaseFile, e := s.fileBudget.acquire(s.ctx, s.global.reservation(s.partSize))
			if e != nil {
				return written, e
			}
			buf, releaseGlobal, e := s.global.takePart(s.ctx, s.partSize)
			if e != nil {
				releaseFile()
				return written, e
			}
			part = &memoryPart{data: buf, length: int(n), release: func() { releaseGlobal(); releaseFile() }}
			s.mu.Lock()
			if s.err != nil || s.ctx.Err() != nil {
				s.mu.Unlock()
				part.release()
				return written, s.ctx.Err()
			}
			s.parts[index] = part
			s.signal()
			s.mu.Unlock()
		}
		s.mu.Lock()
		n := part.data.copyIn(part.filled, p[:min(len(p), part.length-part.filled)])
		if s.md5Hash != nil {
			_, _ = s.md5Hash.Write(p[:n])
			_, _ = s.sha1Hash.Write(p[:n])
		}
		part.filled += n
		s.received += int64(n)
		s.signal()
		s.mu.Unlock()
		written += n
		p = p[n:]
	}
	return written, nil
}
func (s *memoryUploadSource) ReadAt(p []byte, off int64) (read int, err error) {
	return s.ReadAtContext(s.ctx, p, off)
}

// Wait outside the HTTP-request budget. Once a part has bytes, its complete
// buffer is already reserved, so the producer can fill it without more RAM.
func (s *memoryUploadSource) WaitReadable(ctx context.Context, off int64) error {
	if off < 0 {
		return errors.New("负数读取偏移")
	}
	if off >= s.size {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return err
		}
		part := s.parts[off/s.partSize]
		if part != nil && off%s.partSize < int64(part.filled) {
			s.mu.Unlock()
			return nil
		}
		if part == nil && off < s.received {
			s.mu.Unlock()
			return errors.New("分片已确认并释放，不能重放")
		}
		if s.inputDone {
			s.mu.Unlock()
			return io.ErrUnexpectedEOF
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *memoryUploadSource) ReadAtContext(ctx context.Context, p []byte, off int64) (read int, err error) {
	if off < 0 {
		return 0, errors.New("负数读取偏移")
	}
	for len(p) > 0 {
		if err = ctx.Err(); err != nil {
			return read, err
		}
		if off >= s.size {
			return read, io.EOF
		}
		if err = s.ctx.Err(); err != nil {
			return read, err
		}
		s.mu.Lock()
		if s.err != nil {
			err = s.err
			s.mu.Unlock()
			return read, err
		}
		index := off / s.partSize
		at := int(off % s.partSize)
		part := s.parts[index]
		// The producer allocates sequentially. Missing bytes behind received
		// were acknowledged and released; no per-file-size acknowledgement map.
		if part == nil && index*s.partSize < s.received {
			s.mu.Unlock()
			return read, errors.New("已确认并释放的分片不可重新读取；需要客户端重新上传")
		}
		if part != nil && at < part.filled {
			n := part.data.copyOut(p, at, part.filled)
			s.mu.Unlock()
			read += n
			off += int64(n)
			p = p[n:]
			continue
		}
		if s.inputDone {
			s.mu.Unlock()
			return read, io.ErrUnexpectedEOF
		}
		changed := s.changed
		s.mu.Unlock()
		resume := s.recovery.waitingForClient()
		select {
		case <-changed:
		case <-ctx.Done():
			resume()
			return read, ctx.Err()
		case <-s.ctx.Done():
			resume()
			return read, s.ctx.Err()
		}
		resume()
	}
	return read, nil
}
func (s *memoryUploadSource) ConfirmPart(off, n int64) {
	s.mu.Lock()
	index := off / s.partSize
	p := s.parts[index]
	if p != nil && int64(p.filled) == n {
		delete(s.parts, index)
		p.data = nil
		s.signal()
	}
	s.mu.Unlock()
	if p != nil && int64(p.filled) == n {
		p.release()
	}
}
func (s *memoryUploadSource) FinishInput() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.received != s.size {
		return fmt.Errorf("客户端正文不完整：%d/%d 字节", s.received, s.size)
	}
	s.inputDone = true
	s.signal()
	return nil
}

func (s *memoryUploadSource) enableHashes() {
	s.mu.Lock()
	s.md5Hash, s.sha1Hash = md5.New(), sha1.New()
	s.mu.Unlock()
}
func (s *memoryUploadSource) digests(ctx context.Context) (string, string, error) {
	for {
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return "", "", err
		}
		if s.inputDone && s.received == s.size && s.md5Hash != nil {
			md, sh := fmt.Sprintf("%x", s.md5Hash.Sum(nil)), fmt.Sprintf("%x", s.sha1Hash.Sum(nil))
			s.mu.Unlock()
			return md, sh, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-s.ctx.Done():
			return "", "", s.ctx.Err()
		}
	}
}
func (s *memoryUploadSource) Abort(err error) {
	if err == nil {
		err = io.ErrClosedPipe
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.signal()
	s.mu.Unlock()
	s.cancel()
}

// Call only after all cloud HTTP readers have joined.
func (s *memoryUploadSource) Close() {
	s.cancel()
	s.mu.Lock()
	parts := s.parts
	s.parts = map[int64]*memoryPart{}
	s.signal()
	s.mu.Unlock()
	for _, p := range parts {
		p.release()
	}
}

type streamingDAVDrive interface {
	UploadStream(context.Context, string, string, int64, *memoryUploadSource) (davEntry, error)
}

func streamDriveAvailable(d davDrive) bool {
	if b, ok := d.(*backupDAVDrive); ok {
		return streamDriveAvailable(b.primary)
	}
	if _, ok := d.(*quarkDAVDrive); ok {
		return true
	}
	_, ok := d.(streamingDAVDrive)
	return ok
}

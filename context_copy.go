package main

import (
	"context"
	"io"
)

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	// Hide os.File.WriteTo so io.Copy checks cancellation between bounded reads.
	return io.CopyBuffer(dst, contextReader{ctx, src}, make([]byte, 64<<10))
}

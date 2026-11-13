package main

import (
	"context"
	"net/http"
)

func (g *guiApp) requestLifetime() context.Context {
	g.requestOnce.Do(func() {
		parent := g.lifetime
		if parent == nil {
			parent = context.Background()
		}
		g.requestContext, g.cancelRequests = context.WithCancel(parent)
	})
	return g.requestContext
}

func (g *guiApp) bindRequest(r *http.Request) (*http.Request, func()) {
	ctx, cancel := context.WithCancel(r.Context())
	lifetime := g.requestLifetime()
	stop := context.AfterFunc(lifetime, cancel)
	if lifetime.Err() != nil {
		cancel()
	}
	return r.WithContext(ctx), func() { stop(); cancel() }
}

func (g *guiApp) stopRequests() {
	g.requestLifetime()
	g.cancelRequests()
}

func (g *guiApp) quit() {
	g.shutdownOnce.Do(func() {
		g.stopRequests()
		if g.shutdown != nil {
			g.shutdown()
		}
	})
}

func (g *guiApp) lockOperation(w http.ResponseWriter, r *http.Request) bool {
	if err := g.op.LockContext(r.Context()); err != nil {
		g.fail(w, err)
		return false
	}
	return true
}

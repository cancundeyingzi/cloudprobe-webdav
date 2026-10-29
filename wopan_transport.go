package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"
)

func uploadMode(mode string) string {
	if mode == "auto" || mode == "" {
		return "auto"
	}
	if mode == "http2" {
		return mode
	}
	return "http1"
}

// HTTP/1.1 owns one pool per worker. The HTTP/2 comparison deliberately shares
// one pool, allowing traces to show actual streams versus TCP connections.
type uploadClientPool struct {
	mu         sync.Mutex
	makeClient func() *http.Client
	client     *http.Client
	generation uint64
	mode       string
}

func (p *uploadClientPool) switchMode() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode == "http2" {
		p.mode = "http1"
	} else {
		p.mode = "http2"
	}
	if p.client != nil {
		p.client.CloseIdleConnections()
		p.client = nil
	}
}

func (p *uploadClientPool) get() (*http.Client, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		p.client = p.makeClient()
		p.generation++
	}
	return p.client, p.generation
}
func (p *uploadClientPool) retire(generation uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil && p.generation == generation {
		p.client.CloseIdleConnections()
		p.client = nil
	}
}
func (p *uploadClientPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		p.client.CloseIdleConnections()
		p.client = nil
	}
}
func (u *wopanUploadSession) newPool() *uploadClientPool {
	pool := &uploadClientPool{mode: uploadMode(u.mode)}
	if pool.mode == "auto" {
		pool.mode = "http1"
	}
	pool.makeClient = func() *http.Client {
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		var transport *http.Transport
		if u.client != nil {
			copy := *u.client
			client = &copy
			client.Timeout = 0
			if u.client.Transport != nil {
				var ok bool
				transport, ok = u.client.Transport.(*http.Transport)
				if !ok {
					return client
				}
			}
		}
		if transport == nil {
			transport = &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ResponseHeaderTimeout: 90 * time.Second, TLSHandshakeTimeout: 15 * time.Second, IdleConnTimeout: 30 * time.Second}
		} else {
			transport = transport.Clone()
		}
		dial := transport.DialContext
		if dial == nil {
			dial = (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext
		}
		registry := u.connections
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dial(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if registry == nil {
				return conn, nil
			}
			return registry.track(conn), nil
		}
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		protocols.SetHTTP2(pool.mode == "http2")
		transport.Protocols = protocols
		transport.ForceAttemptHTTP2 = pool.mode == "http2"
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		}
		if pool.mode == "http1" {
			// A cloned TLS config can still advertise h2 even when Transport's
			// protocol policy is HTTP/1.1. Keep ALPN and the parser in agreement.
			transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
			transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		} else {
			transport.TLSClientConfig.NextProtos = []string{"h2", "http/1.1"}
			transport.TLSNextProto = nil
		}
		transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost = 1, 1, 1
		client.Transport = transport
		return client
	}
	return pool
}

type uploadConnections struct {
	mu   sync.Mutex
	next uint64
	ids  map[net.Conn]string
}

func (c *uploadConnections) id(conn net.Conn) string {
	for {
		wrapped, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			break
		}
		next := wrapped.NetConn()
		if next == conn {
			break
		}
		conn = next
	}
	if tracked, ok := conn.(*trackedUploadConn); ok && tracked.registry == c {
		return tracked.id
	}
	// Injected transports may supply connections we do not own. Do not retain
	// them without a Close notification; production connections are tracked.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	return fmt.Sprintf("upload-conn-%d", c.next)
}

type trackedUploadConn struct {
	net.Conn
	registry *uploadConnections
	id       string
	once     sync.Once
}

func (c *uploadConnections) track(conn net.Conn) net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = make(map[net.Conn]string)
	}
	c.next++
	tracked := &trackedUploadConn{Conn: conn, registry: c, id: fmt.Sprintf("upload-conn-%d", c.next)}
	c.ids[tracked] = tracked.id
	return tracked
}
func (c *trackedUploadConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.registry.mu.Lock(); delete(c.registry.ids, c); c.registry.mu.Unlock() })
	return err
}

type uploadConnectionTrace struct {
	mu                          sync.Mutex
	ctx                         context.Context
	registry                    *uploadConnections
	source                      string
	worker, attempt             int
	part, parts                 int64
	id, protocol, local, remote string
	reused                      bool
	status                      int
}
type uploadConnectionKey struct{}

func (t *uploadConnectionTrace) hooks(wrote func(httptrace.WroteRequestInfo)) *httptrace.ClientTrace {
	return &httptrace.ClientTrace{WroteRequest: wrote, GotConn: func(info httptrace.GotConnInfo) {
		protocol := "HTTP/1.1"
		if conn, ok := info.Conn.(*tls.Conn); ok && conn.ConnectionState().NegotiatedProtocol == "h2" {
			protocol = "HTTP/2.0"
		}
		t.mu.Lock()
		t.id, t.protocol, t.local, t.remote, t.reused = t.registry.id(info.Conn), protocol, info.Conn.LocalAddr().String(), info.Conn.RemoteAddr().String(), info.Reused
		e := runLogEntry{Operation: "上传连接", Source: t.source, Result: "新连接", Worker: t.worker, Part: t.part, Parts: t.parts, Attempt: t.attempt, ConnectionID: t.id, Protocol: t.protocol, LocalAddress: t.local, RemoteAddress: t.remote}
		if info.Reused {
			e.Result = "复用连接"
		}
		t.mu.Unlock()
		logDAV(t.ctx, e)
	}}
}
func (t *uploadConnectionTrace) response(protocol string, status int) {
	t.mu.Lock()
	t.protocol, t.status = protocol, status
	t.mu.Unlock()
}
func (t *uploadConnectionTrace) apply(e *runLogEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e.Worker, e.ConnectionID, e.Protocol, e.LocalAddress, e.RemoteAddress, e.ConnectionReused, e.HTTPStatus = t.worker, t.id, t.protocol, t.local, t.remote, t.reused, t.status
}

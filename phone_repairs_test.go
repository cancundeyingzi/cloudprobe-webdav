package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type phoneRepairTransport func(*http.Request) (*http.Response, error)

func (f phoneRepairTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func phoneRepairResponse(r *http.Request, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: r}
}

func TestPhoneRepairRejectsQuarkSnapshotHashAfterSourceChanges(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged fallback", true: "same size changed content"}[changed], func(t *testing.T) {
			original := bytes.Repeat([]byte("a"), 4096)
			replacement := original
			if changed {
				replacement = bytes.Repeat([]byte("b"), len(original))
			}
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"original"`)
				w.Header().Set("Content-Length", "4096")
				w.Write(original[:100])
				w.(http.Flusher).Flush()
				c, _, _ := w.(http.Hijacker).Hijack()
				c.Close()
			}))
			defer first.Close()
			var fallbackHits atomic.Int64
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackHits.Add(1)
				tag := `"original"`
				if changed {
					tag = `"replacement"`
				}
				w.Header().Set("ETag", tag)
				http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(replacement))
			}))
			defer second.Close()
			fixture, drive := newQuarkFixture(t)
			fixture.add("0", "f.bin", original, false)
			drive.client.mu.Lock()
			drive.client.credentials.OpenVerified = false
			drive.client.mu.Unlock()
			api := fixture.server.Config.Handler
			fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/file/download") {
					endpoint := first.URL
					if strings.HasPrefix(r.URL.Path, "/web/") {
						endpoint = second.URL
					}
					respondJSON(w, 200, map[string]any{"code": 0, "status": 200, "data": []any{map[string]any{"fid": "1", "size": 4096, "download_url": endpoint}}})
					return
				}
				api.ServeHTTP(w, r)
			})
			hash := sha1.Sum(original)
			entry := davEntry{Item: Item{ID: "1", Name: "f.bin", Size: int64(len(original))}, ContentVersion: "sha1:" + hex.EncodeToString(hash[:]) + ":4096", Parent: "0"}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			reader := newTransferReader(ctx, &davMount{drive: drive, health: &routeHealth{}}, entry, newTransferRuntime(transferOptions{TransferScheduling: "client"}))
			defer reader.Close()
			reader.session.retryDelay = time.Millisecond
			got, err := io.ReadAll(reader)
			if fallbackHits.Load() == 0 {
				t.Fatal("did not exercise the second source")
			}
			if changed {
				if err == nil || len(got) >= len(original) {
					t.Fatalf("mixed old/new content reported as complete: bytes=%d error=%v", len(got), err)
				}
			} else if err != nil || !bytes.Equal(got, original) {
				t.Fatal("unchanged fallback lost", err)
			}
		})
	}
}

type phoneRepairCancelContext struct {
	context.Context
	identityDone atomic.Bool
	checked      chan struct{}
	resume       chan struct{}
	once         sync.Once
}

func (c *phoneRepairCancelContext) Err() error {
	err := c.Context.Err()
	if c.identityDone.Load() {
		c.once.Do(func() { close(c.checked); <-c.resume })
	}
	return err
}

type phoneRepairIdentityBody struct {
	io.Reader
	onClose func()
}

func (b phoneRepairIdentityBody) Close() error { b.onClose(); return nil }
func TestPhoneRepairCancelledIdentityCannotPersistAnAccount(t *testing.T) {
	app := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	manager := newQuarkManager(app)
	app.quark = manager
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &phoneRepairCancelContext{Context: base, checked: make(chan struct{}), resume: make(chan struct{})}
	manager.cancel = cancel
	manager.validateClient = func(c quarkCredentials) *quarkClient {
		q := newQuarkClient(c, nil)
		q.metadata = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
			return phoneRepairResponse(r, phoneRepairIdentityBody{Reader: strings.NewReader(`{"success":true,"code":"OK","data":{"uid":"cancelled-user"}}`), onClose: func() { ctx.identityDone.Store(true) }}), nil
		})}
		return q
	}
	done := make(chan error, 1)
	go func() {
		done <- manager.accept(ctx, quarkCredentials{Cookies: map[string]string{"__pus": "cancelled-credential"}}, "")
	}()
	select {
	case <-ctx.checked:
	case <-time.After(time.Second):
		t.Fatal("identity verification did not reach commit boundary")
	}
	manager.Stop()
	close(ctx.resume)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled login was committed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled login remained blocked")
	}
	if manager.loggedClients() {
		t.Fatal("cancelled account became visible")
	}
	if _, err := os.Stat(filepath.Join(app.data, "quark-accounts.bin")); !os.IsNotExist(err) {
		t.Fatal("cancelled login created a vault", err)
	}
}

func TestPhoneRepairProviderHeadersReachDownloadNode(t *testing.T) {
	for _, providerUA := range []string{"", "required-provider-agent"} {
		t.Run(providerUA, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ua := r.UserAgent()
				ok := strings.HasPrefix(ua, "CloudProbe/")
				if providerUA != "" {
					ok = ua == providerUA
				}
				if !ok {
					http.Error(w, "required header changed", 403)
					return
				}
				w.Header().Set("ETag", `"fixed"`)
				io.WriteString(w, "body")
			}))
			defer server.Close()
			route := readRoute{Name: "required headers", Resolve: func(context.Context) (readTarget, error) {
				headers := make(http.Header)
				if providerUA != "" {
					headers.Set("User-Agent", providerUA)
				}
				return readTarget{URL: server.URL, Headers: headers}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			reader := newTransferReader(ctx, &davMount{drive: &routeFixtureDrive{routes: []readRoute{route}}, health: &routeHealth{}}, davEntry{Item: Item{ID: "f", Size: 4}}, newTransferRuntime(transferOptions{TransferScheduling: "client"}))
			defer reader.Close()
			body, err := io.ReadAll(reader)
			if err != nil || string(body) != "body" {
				t.Fatal("provider header was replaced", err)
			}
		})
	}
}

func TestPhoneRepairActiveUploadHasNoFixedAttemptDeadline(t *testing.T) {
	_, drive := newQuarkFixture(t)
	var calls atomic.Int64
	drive.bodyHTTP = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if _, has := r.Context().Deadline(); has {
			response := phoneRepairResponse(r, io.NopCloser(strings.NewReader("fixed deadline prevents slow-progress compatibility")))
			response.StatusCode = 400
			return response, nil
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, err
		}
		resp := phoneRepairResponse(r, io.NopCloser(strings.NewReader("")))
		resp.Header.Set("ETag", `"part"`)
		return resp, nil
	})}
	upload := &quarkUpload{drive: drive, route: quarkWeb, task: "task", uploadID: "up", bucket: "bucket", key: "key", base: "http://127.0.0.1/part", mime: "application/octet-stream"}
	var sent, confirmed atomic.Int64
	if _, err := upload.part(context.Background(), bytes.NewReader([]byte("body")), 1, 1, 0, 4, &sent, &confirmed); err != nil {
		t.Fatal("progressing upload cannot finish", err)
	}
	if calls.Load() != 1 || confirmed.Load() != 4 {
		t.Fatal("unexpected replay or confirmation")
	}
}

type phoneRepairBrokenBody struct{ sent bool }

func (b *phoneRepairBrokenBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	return copy(p, []byte("<CompleteMultipartUploadResult>")), io.ErrUnexpectedEOF
}
func (b *phoneRepairBrokenBody) Close() error { return nil }
func TestPhoneRepairOSSCompletionRejectsIncompleteOrErrorBody(t *testing.T) {
	for _, test := range []struct {
		name, body      string
		broken, success bool
	}{
		{"valid XML", "<CompleteMultipartUploadResult/>", false, true},
		{"valid callback", "{\"status\":200,\"code\":0,\"data\":{}}", false, true},
		{"native callback", "{\"Status\":\"OK\"}", false, true},
		{"native callback error", "{\"Status\":\"ERROR\"}", false, false},
		{"HTTP 200 XML error", "<Error><Code>InvalidPart</Code><Message>bad part</Message></Error>", false, false},
		{"HTTP 200 JSON error", "{\"status\":500,\"code\":1}", false, false},
		{"unknown response", "unknown", false, false},
		{"cut response", "", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, drive := newQuarkFixture(t)
			drive.bodyHTTP = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
				var body io.ReadCloser = io.NopCloser(strings.NewReader(test.body))
				if test.broken {
					body = &phoneRepairBrokenBody{}
				}
				return phoneRepairResponse(r, body), nil
			})}
			upload := &quarkUpload{drive: drive, route: quarkWeb, task: "task", uploadID: "up", bucket: "bucket", key: "key", base: "http://127.0.0.1/complete", mime: "application/octet-stream", callback: map[string]any{"task_id": "task"}}
			err := upload.commitOSS(context.Background(), []string{`"etag"`})
			if test.success && err != nil {
				t.Fatal("valid completion rejected", err)
			}
			if !test.success && err == nil {
				t.Fatal("unknown/failed completion treated as successful")
			}
		})
	}
}

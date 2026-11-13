package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func readRouteAfterBackoff(ctx context.Context, route *resolvedReadRoute) (readTarget, uint64, error) {
	for {
		target, generation, err := route.get(ctx, time.Nanosecond)
		var pending *readRouteRetry
		if !errors.As(err, &pending) {
			return target, generation, err
		}
		if err := davWait(ctx, time.Until(pending.at)); err != nil {
			return readTarget{}, generation, err
		}
	}
}

func readMultipartBodies(response *httptest.ResponseRecorder) ([]string, error) {
	_, params, err := mime.ParseMediaType(response.Header().Get("Content-Type"))
	if err != nil {
		return nil, err
	}
	parts := multipart.NewReader(response.Body, params["boundary"])
	var bodies []string
	for {
		part, err := parts.NextPart()
		if err == io.EOF {
			return bodies, nil
		}
		if err != nil {
			return bodies, err
		}
		body, err := io.ReadAll(part)
		if err != nil {
			return bodies, err
		}
		bodies = append(bodies, string(body))
	}
}

func TestReadSessionMultipartKeepsVersionAcrossSeeks(t *testing.T) {
	for _, ranges := range []string{"bytes=4-7,0-3", "bytes=0-3,1024-1027"} {
		for _, identity := range []string{"stable", "changed", "missing"} {
			t.Run(ranges+"/"+identity, func(t *testing.T) {
				const size = 2048
				reader := fixtureReader(t, size, []readRoute{fixtureRoute("body", "https://example.invalid/file", "")})
				reader.parallel = false
				var calls atomic.Int64
				reader.session.client = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
					call := calls.Add(1)
					header := make(http.Header)
					fill := "a"
					if identity != "missing" {
						header.Set("ETag", `"original"`)
					}
					if identity == "changed" && call > 1 {
						header.Set("ETag", `"replacement"`)
						fill = "b"
					}
					var start int64
					code := http.StatusOK
					if r.Header.Get("Range") != "" {
						if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err != nil {
							return nil, err
						}
						code = http.StatusPartialContent
						header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
					}
					payload := strings.Repeat(fill, size-int(start))
					return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(payload)), ContentLength: int64(len(payload)), Request: r}, nil
				})}
				request := httptest.NewRequest(http.MethodGet, "http://localhost/file.bin", nil)
				request.Header.Set("Range", ranges)
				response := httptest.NewRecorder()
				response.Header().Set("Content-Type", "application/octet-stream")
				http.ServeContent(response, request, "file.bin", time.Time{}, reader)
				bodies, err := readMultipartBodies(response)
				if identity == "stable" {
					if err != nil || len(bodies) != 2 || bodies[0] != "aaaa" || bodies[1] != "aaaa" {
						t.Fatalf("same-version ranges failed: %q, %v", bodies, err)
					}
				} else if err == nil && len(bodies) == 2 {
					t.Fatalf("unverified multipart response completed: %q", bodies)
				}
				if calls.Load() != 2 {
					t.Fatalf("expected one reopen, got %d calls", calls.Load())
				}
			})
		}
	}
}

func TestReadSessionContentSniffReplaysOriginalStream(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		for _, tagged := range []bool{false, true} {
			for _, size := range []int{8, 2048} {
				t.Run(fmt.Sprintf("parallel=%v/tagged=%v/size=%d", parallel, tagged, size), func(t *testing.T) {
					payload := bytes.Repeat([]byte("x"), size)
					var calls atomic.Int64
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						if tagged {
							w.Header().Set("ETag", `"stable"`)
						}
						http.ServeContent(w, r, "body.bin", time.Time{}, bytes.NewReader(payload))
					}))
					defer server.Close()
					reader := fixtureReader(t, int64(size), []readRoute{fixtureRoute("body", server.URL, "")})
					reader.parallel = parallel
					response := httptest.NewRecorder()
					request := httptest.NewRequest(http.MethodGet, "http://localhost/file", nil)
					http.ServeContent(response, request, "file", time.Time{}, reader)
					if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), payload) {
						t.Fatalf("sniff/size/rewind broke download: status=%d length=%d", response.Code, response.Body.Len())
					}
					if !tagged {
						want := int64(1)
						if parallel {
							want++ // Discarded capability probe precedes the one body stream.
						}
						if calls.Load() != want {
							t.Fatalf("unversioned body reopened: calls=%d, want=%d", calls.Load(), want)
						}
					}
				})
			}
		}
	}
}

func TestReadSessionUnversionedInterruptedBodyCannotResume(t *testing.T) {
	var calls atomic.Int64
	reader := fixtureReader(t, 2048, []readRoute{fixtureRoute("body", "https://example.invalid/file", "")})
	reader.parallel = false
	reader.session.retryDelay = time.Nanosecond
	reader.session.budget = newFairLimiter(1)
	reader.session.client = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		header := make(http.Header)
		payload, code, length := "aaaa", http.StatusOK, int64(2048)
		if calls.Add(1) > 1 {
			payload, code, length = strings.Repeat("b", 2044), http.StatusPartialContent, 2044
			header.Set("Content-Range", "bytes 4-2047/2048")
		}
		return &http.Response{StatusCode: code, Header: header, Body: io.NopCloser(strings.NewReader(payload)), ContentLength: length, Request: r}, nil
	})}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err == nil || string(data) != "aaaa" || calls.Load() != 2 || reader.session.budget.status()["active"] != 0 {
		t.Fatalf("unverifiable resume: data=%q calls=%d budget=%v error=%v", data, calls.Load(), reader.session.budget.status(), err)
	}
}

func TestReadRouteConcurrentResolutionHonorsRetryAfter(t *testing.T) {
	var calls atomic.Int64
	route := &resolvedReadRoute{route: readRoute{Name: "OpenList", Resolve: func(context.Context) (readTarget, error) {
		calls.Add(1)
		return readTarget{}, &openListResponseError{HTTP: &apiHTTPError{Code: http.StatusOK, Err: errors.New("busy")}, BusinessCode: http.StatusTooManyRequests, After: time.Second}
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	budget := newFairLimiter(1)
	session := &downloadSession{routes: []*resolvedReadRoute{route}, size: 1, retryDelay: time.Millisecond, budget: budget}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, err := session.open(ctx, 0, 0, false)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("unexpected termination: %v", err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 || budget.status()["active"] != 0 {
		t.Fatalf("concurrent retries bypassed delay or held slots: calls=%d budget=%v", calls.Load(), budget.status())
	}
}

func TestReadRouteWaitsForEarliestCandidate(t *testing.T) {
	var slowCalls, fastCalls atomic.Int64
	routes := []*resolvedReadRoute{
		{route: readRoute{Name: "slow", Resolve: func(context.Context) (readTarget, error) {
			slowCalls.Add(1)
			return readTarget{}, &upstreamHTTPError{code: 429, after: time.Second}
		}}},
		{route: readRoute{Name: "fast", Resolve: func(context.Context) (readTarget, error) {
			if fastCalls.Add(1) == 1 {
				return readTarget{}, &upstreamHTTPError{code: 503, after: 10 * time.Millisecond}
			}
			return readTarget{URL: "https://example.invalid/file"}, nil
		}}},
	}
	session := &downloadSession{routes: routes, size: 1, retryDelay: time.Millisecond, client: &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("x")), ContentLength: 1, Request: r}, nil
	})}}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	response, err := session.open(ctx, 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	response.close()
	if slowCalls.Load() != 1 || fastCalls.Load() != 2 {
		t.Fatalf("wrong candidate retried: slow=%d fast=%d", slowCalls.Load(), fastCalls.Load())
	}
}

func TestReadRouteQueuedBodiesCannotBypassRetryAfter(t *testing.T) {
	var requests atomic.Int64
	budget := newFairLimiter(1)
	session := &downloadSession{routes: []*resolvedReadRoute{{route: fixtureRoute("body", "https://example.invalid/file", "")}}, size: 1, retryDelay: time.Millisecond, budget: budget}
	session.client = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader("")), ContentLength: 0, Request: r}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// Keep the only slot occupied until every worker has resolved and queued.
	release, err := budget.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, err := session.open(ctx, 0, 0, false)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("unexpected termination: %v", err)
			}
		})
	}
	for budget.status()["queued"] != 8 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("workers did not queue")
	}
	release()
	wg.Wait()
	if requests.Load() != 1 || budget.status()["active"] != 0 {
		t.Fatalf("queued body bypassed retry deadline: requests=%d budget=%v", requests.Load(), budget.status())
	}
}

func TestReadRouteBodyRetryAfterUsesOtherCandidate(t *testing.T) {
	var badCalls atomic.Int64
	reader := fixtureReader(t, 1, []readRoute{fixtureRoute("limited", "https://example.invalid/limited", ""), fixtureRoute("ready", "https://example.invalid/ready", "")})
	reader.parallel = false
	reader.session.budget = newFairLimiter(1)
	reader.session.client = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		status, payload := http.StatusOK, "x"
		header := make(http.Header)
		if r.URL.Path == "/limited" {
			badCalls.Add(1)
			status, payload = http.StatusTooManyRequests, ""
			header.Set("Retry-After", "60")
		}
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(payload)), ContentLength: int64(len(payload)), Request: r}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	reader.ctx = ctx
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(data) != "x" || badCalls.Load() != 1 || reader.session.budget.status()["active"] != 0 {
		t.Fatalf("body fallback waited on limited route: data=%q err=%v budget=%v", data, err, reader.session.budget.status())
	}
}

func TestReadRouteDuplicateFailureMergesDelayWithoutExtraDenial(t *testing.T) {
	route := &resolvedReadRoute{route: fixtureRoute("body", "https://example.invalid/file", "")}
	_, generation, err := route.get(context.Background(), time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	route.failure(generation, &upstreamHTTPError{code: 403, after: time.Millisecond}, time.Nanosecond)
	first := route.retryAt
	route.failure(generation, &upstreamHTTPError{code: 403, after: time.Second}, time.Nanosecond)
	route.succeeded(generation) // A concurrent success cannot erase this round's failure.
	if !route.retryAt.After(first) || route.denials.count != 1 {
		t.Fatalf("same generation lost delay or counted twice: count=%d first=%v current=%v", route.denials.count, first, route.retryAt)
	}
}

func TestReadRouteOldFailureCannotDelayFreshAddress(t *testing.T) {
	route := &resolvedReadRoute{route: fixtureRoute("body", "https://example.invalid/file", "")}
	_, old, err := route.get(context.Background(), time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	route.failure(old, &upstreamHTTPError{code: 403}, time.Nanosecond)
	_, current, err := readRouteAfterBackoff(context.Background(), route)
	if err != nil {
		t.Fatal(err)
	}
	route.succeeded(current)
	route.failure(old, &upstreamHTTPError{code: 429, after: time.Hour}, time.Nanosecond)
	_, actual, err := route.get(context.Background(), time.Nanosecond)
	if err != nil || actual != current || !route.retryAt.IsZero() {
		t.Fatalf("old response delayed fresh generation: current=%d actual=%d deadline=%v err=%v", current, actual, route.retryAt, err)
	}
}

func TestReadSessionSeekCancelsAndJoinsPrefetch(t *testing.T) {
	const size = 3 * 64 << 10
	started := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var start, end int64
		fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
		w.Header().Set("ETag", `"stable"`)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		if start == 0 {
			w.Write(bytes.Repeat([]byte("x"), int(end-start+1)))
			return
		}
		w.(http.Flusher).Flush()
		once.Do(func() { close(started) })
		<-r.Context().Done()
	}))
	defer server.Close()
	reader := fixtureReader(t, size, []readRoute{fixtureRoute("body", server.URL, "")})
	defer reader.Close()
	reader.parallel, reader.threads = true, 3
	reader.memory = newByteBudget(size)
	reader.session.budget = newFairLimiter(3)
	if n, err := reader.Read(make([]byte, 513)); n != 513 || err != nil {
		t.Fatal(n, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if reader.session.budget.status()["active"] != 0 || reader.memory.status()["used_bytes"].(int64) != 0 || reader.waveCancel != nil {
		t.Fatalf("seek retained prefetch resources: connections=%v memory=%v", reader.session.budget.status(), reader.memory.status())
	}
	if !reader.session.signature.verifiable() {
		t.Fatal("seek discarded the logical session version")
	}
}

func TestReadBodyRecoversFromIdleAndRepeatedTruncation(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, failure := range []string{"idle", "truncated"} {
			t.Run(fmt.Sprintf("chunked=%v/%s", chunked, failure), func(t *testing.T) {
				payload := bytes.Repeat([]byte("abcdefgh"), 512)
				failures := int64(1)
				if failure == "truncated" {
					failures = 2
				}
				var calls atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					call := calls.Add(1)
					start, end := 0, len(payload)-1
					w.Header().Set("ETag", `"stable"`)
					status := http.StatusOK
					if r.Header.Get("Range") != "" {
						fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end)
						w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
						status = http.StatusPartialContent
					}
					w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
					w.WriteHeader(status)
					if call > failures {
						w.Write(payload[start : end+1])
						return
					}
					w.Write(payload[start : start+8])
					w.(http.Flusher).Flush()
					if failure == "idle" {
						<-r.Context().Done()
						return
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				reader := fixtureReader(t, int64(len(payload)), []readRoute{fixtureRoute("body", server.URL, "")})
				defer reader.Close()
				reader.ctx, reader.parallel = ctx, false
				reader.session.retryDelay = time.Millisecond
				reader.session.idleTimeout = 50 * time.Millisecond
				reader.session.budget = newFairLimiter(1)
				route := reader.session.routes[0]
				var failureCounts []int
				route.route.Resolve = func(context.Context) (readTarget, error) {
					// Resolve runs under the route lock; capture the actual retry
					// rounds without waiting 5, 10, and 20 seconds in the test.
					failureCounts = append(failureCounts, route.failures)
					return readTarget{URL: server.URL}, nil
				}
				var data []byte
				var err error
				if chunked {
					data, err = reader.session.chunk(ctx, 0, int64(len(payload)-1))
				} else {
					data, err = io.ReadAll(reader)
				}
				reader.Close()
				if err != nil || !bytes.Equal(data, payload) || ctx.Err() != nil || calls.Load() != failures+1 {
					t.Fatalf("body recovery failed: length=%d calls=%d ctx=%v err=%v", len(data), calls.Load(), ctx.Err(), err)
				}
				if len(failureCounts) != int(failures)+1 || route.failures != 0 {
					t.Fatalf("retry rounds or complete-body reset incorrect: rounds=%v remaining=%d", failureCounts, route.failures)
				}
				for round, count := range failureCounts {
					if count != round {
						t.Fatalf("headers reset consecutive body failures: rounds=%v", failureCounts)
					}
				}
				if reader.session.budget.status()["active"] != 0 {
					t.Fatal("body recovery retained a connection slot")
				}
			})
		}
	}
}

func TestReadBodyExternalCancellationDoesNotRetry(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
			entered := make(chan struct{})
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("ETag", `"stable"`)
				w.Header().Set("Content-Length", "8")
				if r.Header.Get("Range") != "" {
					w.Header().Set("Content-Range", "bytes 0-7/8")
					w.WriteHeader(http.StatusPartialContent)
				}
				w.(http.Flusher).Flush()
				if call == 1 {
					close(entered)
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := fixtureReader(t, 8, []readRoute{fixtureRoute("body", server.URL, "")})
			defer reader.Close()
			reader.ctx, reader.parallel = ctx, false
			reader.session.idleTimeout = time.Hour
			reader.session.retryDelay = time.Millisecond
			reader.session.budget = newFairLimiter(1)
			done := make(chan error, 1)
			go func() {
				var err error
				if chunked {
					_, err = reader.session.chunk(ctx, 0, 7)
				} else {
					_, err = io.ReadAll(reader)
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("body request did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("external cancellation changed into retryable timeout: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("external cancellation did not stop the body promptly")
			}
			reader.Close()
			if calls.Load() != 1 || reader.session.budget.status()["active"] != 0 {
				t.Fatalf("external cancellation retried or retained resources: calls=%d budget=%v", calls.Load(), reader.session.budget.status())
			}
		})
	}
}

func TestReadRouteCanceledResolutionDoesNotPublishState(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		for _, response := range []string{"success", "forbidden", "context error"} {
			t.Run(fmt.Sprintf("deadline=%v/%s", deadline, response), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
				}
				defer cancel()
				expired := time.Now().Add(-time.Minute)
				oldTarget := readTarget{URL: "https://example.invalid/old"}
				route := &resolvedReadRoute{
					target: oldTarget, until: expired, retryAt: expired,
					generation: 7, failedGeneration: 6, failures: 3,
					denials: forbiddenRetries{count: 2},
					route: readRoute{Name: "body", Resolve: func(ctx context.Context) (readTarget, error) {
						if !deadline {
							cancel()
						}
						<-ctx.Done()
						switch response {
						case "forbidden":
							return readTarget{}, &upstreamHTTPError{code: http.StatusForbidden, after: time.Hour}
						case "context error":
							return readTarget{}, ctx.Err()
						default:
							return readTarget{URL: "https://example.invalid/late"}, nil
						}
					}},
				}
				target, generation, err := route.get(ctx, time.Millisecond)
				if !errors.Is(err, ctx.Err()) || target.URL != "" || generation != 8 {
					t.Fatalf("canceled resolution published a result: target=%q generation=%d err=%v", target.URL, generation, err)
				}
				if route.failures != 3 || route.denials.count != 2 || route.denials.terminal != nil || route.failedGeneration != 6 || !route.retryAt.Equal(expired) {
					t.Fatalf("canceled resolution penalized other readers: failures=%d denials=%d failedGeneration=%d retryAt=%v", route.failures, route.denials.count, route.failedGeneration, route.retryAt)
				}
				if route.target.URL != oldTarget.URL || !route.until.Equal(expired) {
					t.Fatal("canceled resolution replaced the cached target")
				}
			})
		}
	}
}

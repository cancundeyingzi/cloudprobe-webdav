package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelDownloadRefreshesExpiredAddressOncePerRound(t *testing.T) {
	for _, workers := range []int{8, 16} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			var stale, fresh, resolutions atomic.Int64
			gate := make(chan struct{})
			payload := []byte(strings.Repeat("x", workers))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/stale" {
					if stale.Add(1) == int64(workers) {
						close(gate)
					}
					select {
					case <-gate:
					case <-r.Context().Done():
						return
					}
					http.Error(w, "expired URL", http.StatusForbidden)
					return
				}
				fresh.Add(1)
				w.Header().Set("ETag", `"version-1"`)
				http.ServeContent(w, r, "f.bin", time.Unix(100000, 0), bytes.NewReader(payload))
			}))
			defer server.Close()
			route := &resolvedReadRoute{route: readRoute{Name: "fixture", Resolve: func(context.Context) (readTarget, error) {
				if resolutions.Add(1) == 1 {
					return readTarget{URL: server.URL + "/stale"}, nil
				}
				return readTarget{URL: server.URL + "/fresh"}, nil
			}}}
			budget := newFairLimiter(workers)
			session := &downloadSession{routes: []*resolvedReadRoute{route}, health: &routeHealth{}, size: int64(workers), client: server.Client(), budget: budget, retryDelay: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			failures := make(chan error, workers)
			var wg sync.WaitGroup
			for index := 0; index < workers; index++ {
				wg.Add(1)
				go func(start int64) {
					defer wg.Done()
					response, err := session.open(ctx, start, start, true)
					if err != nil {
						failures <- err
						return
					}
					defer response.close()
					data, err := io.ReadAll(response.Body)
					if err != nil || string(data) != "x" {
						failures <- fmt.Errorf("range data=%q error=%v", data, err)
					}
				}(int64(index))
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
			if resolutions.Load() != 2 || fresh.Load() != int64(workers) || budget.status()["active"] != 0 {
				t.Fatalf("resolutions=%d stale=%d fresh=%d budget=%v", resolutions.Load(), stale.Load(), fresh.Load(), budget.status())
			}
		})
	}
}

func TestDownloadRouteIgnoresOldResponsesAndCountsRefreshRounds(t *testing.T) {
	route := &resolvedReadRoute{route: fixtureRoute("fixture", "https://example.invalid/file", "")}
	ctx := context.Background()
	_, oldGeneration, err := route.get(ctx, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	denied := &upstreamHTTPError{code: http.StatusForbidden}
	for index := 0; index < 16; index++ {
		var terminal *persistentForbiddenError
		if errors.As(route.failure(oldGeneration, denied, time.Nanosecond), &terminal) {
			t.Fatal("one address exhausted the refresh budget")
		}
	}
	_, generation, err := readRouteAfterBackoff(ctx, route)
	if err != nil {
		t.Fatal(err)
	}
	route.succeeded(generation)
	route.failure(oldGeneration, denied, time.Nanosecond)
	route.invalidate(oldGeneration)
	_, current, err := route.get(ctx, time.Nanosecond)
	if err != nil || current != generation {
		t.Fatal("old response invalidated the newer target", current, generation, err)
	}
	for round := 1; round <= 5; round++ {
		_, generation, err = readRouteAfterBackoff(ctx, route)
		if err != nil {
			t.Fatal("refresh ended early", round, err)
		}
		failure := route.failure(generation, denied, time.Nanosecond)
		route.succeeded(oldGeneration) // An old success must not clear current denials.
		var terminal *persistentForbiddenError
		if errors.As(failure, &terminal) != (round == 5) {
			t.Fatalf("round %d: %v", round, failure)
		}
	}
}

func TestCanceledDownloadDoesNotWaitForAnotherAddressResolution(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	route := &resolvedReadRoute{route: readRoute{Name: "fixture", Resolve: func(context.Context) (readTarget, error) {
		close(entered)
		<-release
		return readTarget{URL: "https://example.invalid/file"}, nil
	}}}
	resolved := make(chan struct{})
	go func() { defer close(resolved); route.get(context.Background(), time.Nanosecond) }()
	defer func() { close(release); <-resolved }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, _, err := route.get(ctx, time.Nanosecond); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled reader waited for another resolution")
	}
}

func TestOpenListBusinessFailuresRetryAndRespectServerDelay(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/body" {
			fmt.Fprint(w, "abc")
			return
		}
		switch calls.Add(1) {
		case 1:
			fmt.Fprint(w, `{"code":503,"message":"unavailable"}`)
		case 2:
			fmt.Fprint(w, `{"code":429,"message":"busy"}`)
		default:
			fmt.Fprintf(w, `{"code":200,"data":{"name":"f.bin","size":3,"is_dir":false,"raw_url":"http://%s/body"}}`, r.Host)
		}
	}))
	defer server.Close()
	client, err := newOpenListClient(openListInput{Address: server.URL, Root: "/"})
	if err != nil {
		t.Fatal(err)
	}
	reader := fixtureReader(t, 3, []readRoute{{Name: "OpenList", Resolve: func(ctx context.Context) (readTarget, error) { return client.link(ctx, "f.bin", 3) }}})
	reader.session.retryDelay = time.Millisecond
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "abc" || calls.Load() != 3 {
		t.Fatalf("data=%q calls=%d err=%v", data, calls.Load(), err)
	}
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusNotFound} {
		client.client.Transport = protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("gateway HTML")), Header: http.Header{"Retry-After": {"17"}}}, nil
		})
		_, err := client.link(context.Background(), "f.bin", 3)
		if transferHTTPStatus(err) != status || retryableTransfer(err) != (status == 503) || transferRetryAfter(err) != 17*time.Second {
			t.Fatal(status, err)
		}
	}
}

func TestOpenListBusinessForbiddenStopsAfterFiveRefreshes(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"code":403,"message":"denied"}`)
	}))
	defer server.Close()
	client, err := newOpenListClient(openListInput{Address: server.URL, Root: "/"})
	if err != nil {
		t.Fatal(err)
	}
	reader := fixtureReader(t, 3, []readRoute{{Name: "OpenList", Resolve: func(ctx context.Context) (readTarget, error) { return client.link(ctx, "f.bin", 3) }}})
	reader.session.retryDelay = time.Millisecond
	_, err = io.ReadAll(reader)
	var terminal *persistentForbiddenError
	if !errors.As(err, &terminal) || calls.Load() != 5 {
		t.Fatalf("calls=%d error=%v", calls.Load(), err)
	}
}

func TestQuarkPartRetriesBusinessAuthenticationFailure(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/part" {
			w.Header().Set("ETag", "confirmed")
			return
		}
		if calls.Add(1) == 1 {
			fmt.Fprint(w, `{"status":1,"code":"31001","message":"token expired"}`)
			return
		}
		fmt.Fprintf(w, `{"status":0,"data":{"upload_urls":[{"part_number":1,"upload_url":"http://%s/part","signature_info":{"signature":"fixture"}}]}}`, r.Host)
	}))
	defer server.Close()
	client := newQuarkClient(quarkCredentials{}, nil)
	client.endpoints[quarkOpen], client.testHosts = server.URL, true
	drive := newQuarkDrive(client, newTransferRuntime(transferOptions{}), 1, "http1")
	drive.bodyHTTP = server.Client()
	drive.retryWait = func(ctx context.Context, _ time.Duration) error { return davWait(ctx, time.Millisecond) }
	upload := &quarkUpload{drive: drive, route: quarkOpen, size: 1, partSize: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var sent, confirmed atomic.Int64
	tag, err := upload.part(ctx, bytes.NewReader([]byte("x")), 1, 1, 0, 1, &sent, &confirmed)
	if err != nil || tag != "confirmed" || calls.Load() != 2 || drive.budget.status()["active"] != 0 {
		t.Fatalf("tag=%q calls=%d error=%v", tag, calls.Load(), err)
	}
}

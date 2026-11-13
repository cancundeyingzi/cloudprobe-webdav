package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

func TestTransferErrorClassificationUsesStructuredEvidence(t *testing.T) {
	for _, test := range []struct {
		name        string
		err         error
		auth, retry bool
	}{
		{"text", errors.New("file 9999 on port 1001 mentions HTTP 403 and token expired"), false, false},
		{"sdk detail", &wopan.ResponseError{HTTPStatus: 200, RspCode: "12345", Detail: "port 1001 file 9999"}, false, false},
		{"sdk code", &wopan.ResponseError{HTTPStatus: 200, RspCode: "1001"}, true, true},
		{"sdk message", &wopan.ResponseError{Message: "token expired"}, true, true},
		{"proxy code", &apiBusinessError{Code: "9999"}, true, true},
		{"bridge code", &bridgeResponseError{Code: 10011}, true, true},
		{"bridge not executed", &davNotExecuted{&bridgeResponseError{Code: 10011}}, true, true},
		{"stage business code", stage("响应解析", &apiBusinessError{Code: "9999"}), true, true},
		{"quark business auth", &quarkAPIError{HTTP: 200, Code: "31001", Auth: true}, false, true},
		{"quark permanent business rejection", &quarkAPIError{HTTP: 200, Code: "FILE_NOT_FOUND"}, false, false},
		{"permission", &apiHTTPError{Code: 403, Err: errors.New("denied")}, false, true},
		{"server", &upstreamHTTPError{code: 503}, false, true},
		{"not found", &upstreamHTTPError{code: 404}, false, false},
		{"body interrupted", io.ErrUnexpectedEOF, false, true},
		{"terminal", &persistentForbiddenError{Err: &upstreamHTTPError{code: 403}}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := fmt.Errorf("outer context: %w", test.err)
			if got := wopanAuthError(err); got != test.auth {
				t.Fatalf("auth=%v, want %v", got, test.auth)
			}
			if got := retryableTransfer(err); got != test.retry {
				t.Fatalf("retry=%v, want %v", got, test.retry)
			}
		})
	}
}

func TestClientRequestPreservesNetworkFailureThroughStage(t *testing.T) {
	cause := &net.DNSError{Err: "temporary network failure", Name: "fixture.invalid", IsTemporary: true}
	client := &Client{cfg: defaultConfig("wopan"), redact: &Redactor{}, http: &http.Client{Transport: protocolRoundTripper(func(*http.Request) (*http.Response, error) { return nil, cause })}}
	client.cfg.Transport = "plain"
	_, err := client.request(context.Background(), "GET", "/fixture", nil, nil, nil)
	if !errors.Is(err, cause) || !retryableTransfer(err) {
		t.Fatalf("network cause lost in stage wrapper: %v", err)
	}
}

func TestPersistentForbiddenStopsDownloadAndReleasesBudget(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "denied", 403)
	}))
	defer server.Close()
	reader := fixtureReader(t, 1, []readRoute{fixtureRoute("denied", server.URL, "")})
	reader.session.retryDelay = time.Millisecond
	reader.session.budget = newFairLimiter(1)
	_, err := io.ReadAll(reader)
	reader.Close()
	var stopped *persistentForbiddenError
	if !errors.As(err, &stopped) || calls.Load() != 5 {
		t.Fatalf("persistent denial: calls=%d err=%v", calls.Load(), err)
	}
	if reader.session.budget.status()["active"] != 0 {
		t.Fatal("failed download retained a body slot")
	}
}

func TestPersistentForbiddenStopsWopanPart(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		http.Error(w, "denied", 403)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session := &wopanUploadSession{endpoint: server.URL, size: 1, partSize: 1, client: server.Client(), retryDelay: time.Millisecond, budget: newFairLimiter(1)}
	_, err := session.run(ctx, bytes.NewReader([]byte("x")))
	var stopped *persistentForbiddenError
	if !errors.As(err, &stopped) || calls.Load() != 5 || session.budget.status()["active"] != 0 {
		t.Fatalf("persistent Wopan denial: calls=%d err=%v budget=%v", calls.Load(), err, session.budget.status())
	}
}

func TestPersistentForbiddenStopsQuarkPart(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/part" {
			calls.Add(1)
			http.Error(w, "denied", 403)
			return
		}
		fmt.Fprintf(w, `{"status":0,"data":{"upload_urls":[{"part_number":1,"upload_url":"http://%s/part","signature_info":{"signature":"fixture"}}]}}`, r.Host)
	}))
	defer server.Close()
	client := newQuarkClient(quarkCredentials{}, nil)
	client.endpoints[quarkOpen] = server.URL
	client.testHosts = true
	drive := newQuarkDrive(client, newTransferRuntime(transferOptions{}), 1, "http1")
	drive.bodyHTTP = server.Client()
	drive.retryWait = func(ctx context.Context, _ time.Duration) error { return davWait(ctx, time.Millisecond) }
	upload := &quarkUpload{drive: drive, route: quarkOpen, size: 1, partSize: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var sent, confirmed atomic.Int64
	_, err := upload.part(ctx, bytes.NewReader([]byte("x")), 1, 1, 0, 1, &sent, &confirmed)
	var stopped *persistentForbiddenError
	if !errors.As(err, &stopped) || calls.Load() != 5 || drive.budget.status()["active"] != 0 {
		t.Fatalf("persistent Quark denial: calls=%d err=%v", calls.Load(), err)
	}
}

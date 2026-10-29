package main

// Regression probes from the 1.9.0 review. They assert safety properties using
// synthetic fixtures; no real provider or account is used.

import (
	"bytes"
	"context"
	"crypto/sha1"
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

func TestReviewConditionalMutationsHonorIfMatch(t *testing.T) {
	for _, method := range []string{"DELETE", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			server, _, _ := testDAVService(t, true)
			file := "/联通云盘-1/condition.txt"
			wantDAV(t, server.URL, "PUT", file, "preserve me", 201, nil)
			headers := map[string]string{"If-Match": `"stale-version"`}
			if method == "MOVE" {
				headers["Destination"] = server.URL + "/dav/联通云盘-1/moved.txt"
			}
			status, _, _ := davRequest(t, server.URL, method, file, nil, headers)
			if status != http.StatusPreconditionFailed {
				t.Fatalf("stale If-Match was ignored: %s status=%d; want 412 without mutation", method, status)
			}
		})
	}
}

type reviewCallbackReader struct {
	io.Reader
	once   sync.Once
	change func()
}

func (r *reviewCallbackReader) Read(p []byte) (int, error) {
	r.once.Do(r.change)
	return r.Reader.Read(p)
}

func TestReviewDiskUploadPreservesChangeDuringClientReceive(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	file := "/联通云盘-1/conflict.txt"
	wantDAV(t, server.URL, "PUT", file, "original", 201, nil)
	_, _, header := davRequest(t, server.URL, "HEAD", file, nil, nil)
	entries, _ := drive.List(context.Background(), "0")
	oldID := entries[0].ID
	reader := &reviewCallbackReader{Reader: strings.NewReader("webdav replacement"), change: func() {
		drive.mu.Lock()
		it := drive.entries[oldID]
		it.Modified = it.Modified.Add(time.Minute)
		it.Size = int64(len("official-client change"))
		drive.entries[oldID] = it
		drive.contents[oldID] = []byte("official-client change")
		drive.mu.Unlock()
		fs.invalidate(fs.mounts[0])
	}}
	request := httptest.NewRequest("PUT", server.URL+"/dav"+file, reader)
	request.ContentLength = int64(len("webdav replacement"))
	request.SetBasicAuth("tester", "test-password-123")
	request.Header.Set("If-Match", header.Get("ETag"))
	recorder := httptest.NewRecorder()
	server.Config.Handler.ServeHTTP(recorder, request)
	remaining, _ := drive.List(context.Background(), "0")
	drive.mu.Lock()
	preserved := false
	for _, it := range remaining {
		if it.Name == "conflict.txt" && string(drive.contents[it.ID]) == "official-client change" {
			preserved = true
		}
	}
	drive.mu.Unlock()
	if recorder.Code < 400 || !preserved {
		t.Fatalf("external change during client receive overwritten: status=%d preserved=%v", recorder.Code, preserved)
	}
}

func TestReviewPutResponseETagMatchesCommittedObject(t *testing.T) {
	server, _, _ := testDAVService(t, true)
	file := "/联通云盘-1/etag.txt"
	_, _, put := davRequest(t, server.URL, "PUT", file, strings.NewReader("hello"), nil)
	_, _, head := davRequest(t, server.URL, "HEAD", file, nil, nil)
	if put.Get("ETag") == "" || put.Get("ETag") != head.Get("ETag") {
		t.Fatalf("PUT ETag differs from immediate HEAD: put=%s head=%s", put.Get("ETag"), head.Get("ETag"))
	}
}

func TestReviewContentVersionInvalidatesIfRange(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	file := "/联通云盘-1/same-size.bin"
	wantDAV(t, server.URL, "PUT", file, "aaaaaaaa", 201, nil)
	entries, _ := drive.List(context.Background(), "0")
	id := entries[0].ID
	drive.mu.Lock()
	it := drive.entries[id]
	it.FID = "" // Quark uses an object ID, not an immutable Wopan FID.
	it.ContentVersion = fmt.Sprintf("sha1:%x:8", sha1.Sum([]byte("aaaaaaaa")))
	drive.entries[id] = it
	drive.mu.Unlock()
	fs.invalidate(fs.mounts[0])
	_, _, old := davRequest(t, server.URL, "HEAD", file, nil, nil)
	drive.mu.Lock()
	it.ContentVersion = fmt.Sprintf("sha1:%x:8", sha1.Sum([]byte("bbbbbbbb")))
	drive.entries[id] = it // Same ID, length and provider timestamp.
	drive.contents[id] = []byte("bbbbbbbb")
	drive.mu.Unlock()
	fs.invalidate(fs.mounts[0])
	status, body, header := davRequest(t, server.URL, "GET", file, nil, map[string]string{"Range": "bytes=4-7", "If-Range": old.Get("ETag")})
	if status != http.StatusOK || len(body) != 8 || old.Get("ETag") == header.Get("ETag") {
		t.Fatalf("changed content accepted for old If-Range: status=%d returned=%d same_etag=%v; want full new representation", status, len(body), old.Get("ETag") == header.Get("ETag"))
	}
}

func TestReviewResumeDoesNotDowngradeStrongValidator(t *testing.T) {
	const size = 4096
	var hits atomic.Int64
	stamp := time.Unix(1700000000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("ETag", `"original-version"`)
			w.Header().Set("Last-Modified", stamp.Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(size))
			w.Write(bytes.Repeat([]byte("a"), 1024))
			w.(http.Flusher).Flush()
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		// An updated same-size representation loses its ETag but retains a
		// coarse Last-Modified value. Old bytes must not be appended to this.
		http.ServeContent(w, r, "same.bin", stamp, bytes.NewReader(bytes.Repeat([]byte("b"), size)))
	}))
	defer server.Close()
	reader := fixtureReader(t, size, []readRoute{fixtureRoute("same-source", server.URL, "")})
	reader.parallel = false
	reader.session.retryDelay = time.Millisecond
	got, err := io.ReadAll(reader)
	if err == nil {
		t.Fatalf("mixed versions reported complete after ETag disappeared: bytes=%d old_prefix=%v new_suffix=%v", len(got), bytes.Equal(got[:min(1024, len(got))], bytes.Repeat([]byte("a"), 1024)), bytes.HasSuffix(got, []byte("bbbb")))
	}
}

func TestReviewQuarkCredentialsRequireHTTPS(t *testing.T) {
	client := newQuarkClient(quarkCredentials{UID: "review-only", Cookies: map[string]string{"__puus": "synthetic-review-session"}}, nil)
	client.metadata = &http.Client{Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		doc := `{"code":0,"status":200,"data":[{"fid":"1","size":4,"download_url":"http://files.quark.cn/audit.bin"}]}`
		return phoneRepairResponse(r, io.NopCloser(strings.NewReader(doc))), nil
	})}
	drive := newQuarkDrive(client, nil, 1, "auto")
	target, err := drive.downloadTarget(context.Background(), quarkWeb, davEntry{Item: Item{ID: "1", Name: "audit.bin", Size: 4}})
	if err == nil {
		t.Fatalf("insecure first-hop download accepted: http=%v account_cookie_attached=%v", strings.HasPrefix(target.URL, "http://"), target.Headers.Get("Cookie") != "")
	}
}

func TestReviewWopanActiveProgressSurvivesAttemptDuration(t *testing.T) {
	var progress atomic.Int64
	var attempts atomic.Int64
	client := http.Client{Timeout: 35 * time.Millisecond, Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		defer r.Body.Close()
		// MIME framing is not file progress. Drain that first MultiReader
		// segment, then consume the actual file bytes at a steady rate.
		if _, err := r.Body.Read(make([]byte, 64<<10)); err != nil {
			return nil, err
		}
		buffer := make([]byte, 32)
		for {
			select {
			case <-r.Context().Done():
				return nil, r.Context().Err()
			case <-time.After(5 * time.Millisecond):
			}
			n, err := r.Body.Read(buffer)
			progress.Add(int64(n))
			if err == io.EOF {
				return phoneRepairResponse(r, io.NopCloser(strings.NewReader(`{"code":"0000","data":{"fid":"review-fid"}}`))), nil
			}
			if err != nil {
				return nil, err
			}
		}
	})} // Scale the production absolute 3m timeout to a local probe.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	u := &wopanUploadSession{endpoint: "http://127.0.0.1/review-only", name: "slow.bin", size: 1024, partSize: 1024, concurrency: 1, client: &client, source: "review", retryDelay: time.Millisecond}
	_, err := u.run(ctx, bytes.NewReader(bytes.Repeat([]byte("a"), 1024)))
	if err != nil {
		t.Fatalf("active progress repeatedly aborted by absolute attempt timeout: attempts=%d sent=%d error=%v", attempts.Load(), progress.Load(), err)
	}
}

func TestReviewUploadPartialPUTAndPATCHRemainUnsupported(t *testing.T) {
	server, _, _ := testDAVService(t, true)
	file := "/联通云盘-1/resume.bin"
	put, _, _ := davRequest(t, server.URL, "PUT", file, strings.NewReader("tail"), map[string]string{"Content-Range": "bytes 4-7/8"})
	patch, _, _ := davRequest(t, server.URL, "PATCH", file, strings.NewReader("tail"), map[string]string{"Upload-Offset": "4", "Tus-Resumable": "1.0.0", "Content-Type": "application/offset+octet-stream"})
	if put != 501 || patch < 400 {
		t.Fatalf("unexpected unsupported upload-resume status: put=%d patch=%d", put, patch)
	}
	t.Logf("capability boundary confirmed: partial PUT=%d PATCH=%d; new full PUT needed", put, patch)
}

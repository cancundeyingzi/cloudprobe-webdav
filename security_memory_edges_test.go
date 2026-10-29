package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func memoryQuarkDAV(t *testing.T, mode string) (*httptest.Server, *quarkFixture, *davFS, *quarkDAVDrive) {
	t.Helper()
	fixture, drive := newQuarkFixture(t)
	server, _, fs := testDAVService(t, true)
	fs.mounts[0].drive, fs.mounts[0].Provider = drive, "quark"
	fs.mounts[0].UploadStorageMode = mode
	fs.transfers = newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "memory", UploadMemoryMiB: 5, TotalUploadMemoryMiB: 5, UploadPrefetchMiB: 1, TotalConnections: 1, UploadRecoveryMode: "off"})
	drive.budget, drive.recovery = fs.transfers.connections, fs.transfers.recovery
	return server, fixture, fs, drive
}

func TestQuarkMemoryPipelinesHashesAfterPartsAndNeverSpools(t *testing.T) {
	server, fixture, fs, _ := memoryQuarkDAV(t, "memory")
	payload := bytes.Repeat([]byte("0123456789abcdef"), (6<<20)/16+3)
	partSeen := make(chan struct{}, 1)
	var hashes atomic.Int64
	base := fixture.server.Config.Handler
	fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/file/update/hash") {
			data, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(data))
			var values map[string]any
			_ = json.Unmarshal(data, &values)
			if values["md5"] != fmt.Sprintf("%x", md5.Sum(payload)) || values["sha1"] != fmt.Sprintf("%x", sha1.Sum(payload)) {
				t.Error("stream hashes do not match original input")
			}
			hashes.Add(1)
		}
		base.ServeHTTP(w, r)
		if strings.HasPrefix(r.URL.Path, "/objects/") && r.Method == "PUT" {
			select {
			case partSeen <- struct{}{}:
			default:
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	release := make(chan struct{})
	go func() {
		if _, err := writer.Write(payload[:512<<10]); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		select {
		case <-release:
		case <-ctx.Done():
			_ = writer.CloseWithError(ctx.Err())
			return
		}
		_, err := writer.Write(payload[512<<10:])
		_ = writer.CloseWithError(err)
	}()
	request, _ := http.NewRequestWithContext(ctx, "PUT", server.URL+"/dav/联通云盘-1/stream.bin", reader)
	request.ContentLength = int64(len(payload))
	request.SetBasicAuth("tester", "test-password-123")
	type response struct {
		value *http.Response
		err   error
	}
	done := make(chan response, 1)
	go func() { r, e := http.DefaultClient.Do(request); done <- response{r, e} }()
	select {
	case <-partSeen:
	case <-ctx.Done():
		t.Fatal("cloud did not receive data before client EOF")
	}
	if hashes.Load() != 0 {
		t.Fatal("hash was submitted before complete client input")
	}
	close(release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	body, _ := io.ReadAll(result.value.Body)
	result.value.Body.Close()
	if result.value.StatusCode != 201 {
		t.Fatalf("stream upload: %d %s", result.value.StatusCode, body)
	}
	if hashes.Load() != 1 {
		t.Fatal("full input hash not submitted exactly once")
	}
	got := wantDAV(t, server.URL, "GET", "/联通云盘-1/stream.bin", "", 200, nil)
	if sha256.Sum256(got) != sha256.Sum256(payload) {
		t.Fatal("stream content differs")
	}
	_, _, head := davRequest(t, server.URL, "HEAD", "/联通云盘-1/stream.bin", nil, nil)
	if result.value.Header.Get("ETag") != head.Get("ETag") {
		t.Fatal("stream PUT validator is not final validator")
	}
	files, _ := os.ReadDir(fs.spool)
	if len(files) != 0 {
		t.Fatal("strict memory upload used disk spool")
	}
	stats := fs.transfers.memory.status()
	if stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 5<<20 || stats["allocated_bytes"].(int64) > 5<<20 {
		t.Fatal("stream memory leak or budget overrun", stats)
	}
}

func TestQuarkMemoryRetriesRetainOriginalHashAndBytes(t *testing.T) {
	server, fixture, fs, _ := memoryQuarkDAV(t, "memory")
	fixture.partFailureLimit = 7
	payload := bytes.Repeat([]byte("retry-contents"), 16000)
	code, body, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/retry.bin", bytes.NewReader(payload), nil)
	if code != 201 {
		t.Fatalf("memory retries: %d %s", code, body)
	}
	fixture.mu.Lock()
	for _, file := range fixture.files {
		if !bytes.Equal(file.Content, payload) {
			t.Error("retry changed file bytes")
		}
	}
	hashCalls := fixture.apiCalls["/desktop/file/update/hash"]
	attempts := fixture.partCalls[1]
	fixture.mu.Unlock()
	if attempts != 8 || hashCalls != 1 {
		t.Fatalf("retry/hash count differs: attempts=%d hashes=%d", attempts, hashCalls)
	}
	if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
		t.Fatal("retained failed parts leaked")
	}
}

func TestMemoryPoolSharesDifferentCloudPartSizesWithoutDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	budget := newByteBudget(6 << 20)
	var wg sync.WaitGroup
	for _, part := range []int64{4 << 20, 5 << 20} {
		wg.Go(func() {
			source := newMemoryUploadSource(ctx, 3*part+17, part, part, budget, nil)
			source.enableHashes()
			defer source.Close()
			payload := bytes.Repeat([]byte{byte(part >> 20)}, int(source.size))
			produced := make(chan error, 1)
			go func() {
				_, e := source.Write(payload)
				if e == nil {
					e = source.FinishInput()
				}
				produced <- e
			}()
			for off := int64(0); off < source.size; off += part {
				n := min(part, source.size-off)
				buf := make([]byte, n)
				if _, err := source.ReadAt(buf, off); err != nil {
					source.Abort(err)
					t.Error("mixed part sizes blocked", err)
					<-produced
					return
				}
				if !bytes.Equal(buf, payload[off:off+n]) {
					t.Error("mixed part cache corrupted bytes")
				}
				source.ConfirmPart(off, n)
			}
			if err := <-produced; err != nil {
				t.Error(err)
			}
			md, sh, err := source.digests(ctx)
			if err != nil || md != fmt.Sprintf("%x", md5.Sum(payload)) || sh != fmt.Sprintf("%x", sha1.Sum(payload)) {
				t.Error("mixed cache hashes differ", err)
			}
		})
	}
	wg.Wait()
	stats := budget.status()
	if stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 6<<20 || stats["allocated_bytes"].(int64) > 6<<20 {
		t.Fatal("mixed pool is not globally bounded", stats)
	}
}

func TestQuarkAutoUsesDiskForOpenOnlyAndStrictModeRejects(t *testing.T) {
	for _, mode := range []string{"auto", "memory"} {
		t.Run(mode, func(t *testing.T) {
			server, fixture, fs, drive := memoryQuarkDAV(t, mode)
			drive.client.mu.Lock()
			drive.client.credentials.Cookies = nil
			drive.client.mu.Unlock()
			code, body, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/open.bin", strings.NewReader("open fallback"), nil)
			if mode == "memory" {
				if code != 501 {
					t.Fatalf("strict open-only upload: %d %s", code, body)
				}
				fixture.mu.Lock()
				count := len(fixture.files)
				fixture.mu.Unlock()
				if count != 0 {
					t.Fatal("strict mode wrote a cloud file")
				}
			} else if code != 201 {
				t.Fatalf("auto open fallback: %d %s", code, body)
			}
			if fs.transfers.memory.status()["peak_bytes"].(int64) != 0 {
				t.Fatal("open-only upload allocated streaming RAM")
			}
		})
	}
}

func TestQuarkAutoUnknownLengthUsesDiskWithoutStartingStream(t *testing.T) {
	server, _, fs, _ := memoryQuarkDAV(t, "auto")
	reader, writer := io.Pipe()
	go func() { _, err := io.WriteString(writer, "unknown-length body"); _ = writer.CloseWithError(err) }()
	code, body, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/chunked.bin", reader, nil)
	if code != 201 {
		t.Fatalf("auto chunked body: %d %s", code, body)
	}
	if fs.transfers.memory.status()["peak_bytes"].(int64) != 0 {
		t.Fatal("unknown-length upload started a streaming session")
	}
}

func TestMutationConditionsUseFreshStrongVersions(t *testing.T) {
	server, drive, fs := testDAVService(t, true)
	file := "/联通云盘-1/condition.bin"
	wantDAV(t, server.URL, "PUT", file, "original", 201, nil)
	_, _, head := davRequest(t, server.URL, "HEAD", file, nil, nil)
	entries, _ := drive.List(context.Background(), "0")
	drive.mu.Lock()
	entry := drive.entries[entries[0].ID]
	entry.Modified = entry.Modified.Add(time.Minute)
	drive.entries[entry.ID] = entry
	drive.mu.Unlock()
	wantDAV(t, server.URL, "DELETE", file, "", 412, map[string]string{"If-Match": head.Get("ETag")})
	wantDAV(t, server.URL, "COPY", file, "", 412, map[string]string{"If-Match": `"stale"`, "Destination": server.URL + "/dav/联通云盘-1/copy.bin"})
	wantDAV(t, server.URL, "MOVE", file, "", 412, map[string]string{"If-Unmodified-Since": time.Unix(1, 0).UTC().Format(http.TimeFormat), "Destination": server.URL + "/dav/联通云盘-1/moved.bin"})
	_, _, head = davRequest(t, server.URL, "HEAD", file, nil, nil)
	wantDAV(t, server.URL, "DELETE", file, "", 412, map[string]string{"If-Match": "W/" + head.Get("ETag")})
	wantDAV(t, server.URL, "PUT", file, "x", 412, map[string]string{"If-None-Match": "W/" + head.Get("ETag")})
	wantDAV(t, server.URL, "DELETE", file, "", 204, map[string]string{"If-Match": `"opaque,comma", ` + head.Get("ETag")})
	fs.invalidate(fs.mounts[0])
}

func TestMetadataOnlyETagIsWeak(t *testing.T) {
	etag, _ := (davInfo{davEntry{Item: Item{ID: "mutable", Name: "weak.bin", Size: 4}}}).ETag(context.Background())
	if !strings.HasPrefix(etag, "W/") {
		t.Fatal("metadata-only object advertised a strong validator")
	}
	for _, value := range []string{`"a",`, `*, "a"`, `"a" "b"`, `"bad space"`} {
		if _, err := parseDAVTags(value); err == nil {
			t.Fatal("invalid condition accepted", value)
		}
	}
}

func TestQuarkMemoryFatalNodeErrorReleasesBlockedProducer(t *testing.T) {
	server, fixture, fs, _ := memoryQuarkDAV(t, "memory")
	base := fixture.server.Config.Handler
	fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/objects/") && r.Method == "PUT" {
			http.Error(w, "permanent synthetic rejection", 400)
			return
		}
		base.ServeHTTP(w, r)
	})
	code, _, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/rejected.bin", bytes.NewReader(make([]byte, 8<<20)), nil)
	if code != 502 {
		t.Fatalf("permanent node error: %d", code)
	}
	if fs.transfers.memory.status()["used_bytes"].(int64) != 0 || fs.transfers.connections.status()["active"] != 0 {
		t.Fatal("fatal node error retained RAM or a body slot")
	}
}

func TestQuarkMemoryBudgetFallbackHappensBeforeReceiving(t *testing.T) {
	for _, mode := range []string{"auto", "memory"} {
		t.Run(mode, func(t *testing.T) {
			server, fixture, fs, _ := memoryQuarkDAV(t, mode)
			base := fixture.server.Config.Handler
			fixture.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/file/upload/pre") {
					recorder := httptest.NewRecorder()
					base.ServeHTTP(recorder, r)
					var doc map[string]any
					_ = json.Unmarshal(recorder.Body.Bytes(), &doc)
					doc["data"].(map[string]any)["part_size"] = 16 << 20
					respondJSON(w, recorder.Code, doc)
					return
				}
				base.ServeHTTP(w, r)
			})
			code, body, _ := davRequest(t, server.URL, "PUT", "/联通云盘-1/budget.bin", strings.NewReader("bounded"), nil)
			want := 201
			if mode == "memory" {
				want = 501
			}
			if code != want {
				t.Fatalf("part budget policy: %d %s", code, body)
			}
			if fs.transfers.memory.status()["peak_bytes"].(int64) != 0 {
				t.Fatal("incompatible cloud part allocated body RAM")
			}
		})
	}
}

func TestMobileContinuousProgressHasNoAbsoluteAttemptDeadline(t *testing.T) {
	var calls atomic.Int64
	client := &http.Client{Timeout: 35 * time.Millisecond, Transport: phoneRepairTransport(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		defer r.Body.Close()
		buffer := make([]byte, 32)
		for {
			if err := davWait(r.Context(), 5*time.Millisecond); err != nil {
				return nil, err
			}
			_, err := r.Body.Read(buffer)
			if err == io.EOF {
				return phoneRepairResponse(r, io.NopCloser(strings.NewReader(`<result resultCode="0"/>`))), nil
			}
			if err != nil {
				return nil, err
			}
		}
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sent, confirmed atomic.Int64
	drive := &mobileDAVDrive{}
	if err := drive.sendUploadPart(ctx, client, "http://127.0.0.1/test-only", "task", "slow.bin", 1024, 0, 1024, 1, 1, true, bytes.NewReader(make([]byte, 1024)), &sent, &confirmed); err != nil {
		t.Fatal("progressing mobile request was restarted", err)
	}
	if calls.Load() != 1 || confirmed.Load() != 1024 {
		t.Fatal("unexpected mobile replay or confirmation")
	}
}

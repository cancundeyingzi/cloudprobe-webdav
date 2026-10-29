package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestUploadRecoveryUsesAllFilesCombined(t *testing.T) {
	o := transferOptions{UploadLowSeconds: 1}.defaults()
	c := newUploadRecovery(o)
	c.manual = true
	endA, endB := c.begin(context.Background()), c.begin(context.Background())
	defer endA()
	defer endB()
	at := time.Now()
	c.observe(at)
	// Two files each at 0.6 MB/s exceed the 1 MB/s aggregate threshold.
	for i := 1; i <= 3; i++ {
		c.sent.Add(600000)
		c.sent.Add(600000)
		c.observe(at.Add(time.Duration(i) * time.Second))
	}
	if c.attempts != 0 {
		t.Fatal("individual-file speeds triggered a shared pause")
	}
	// Both files together now supply only 0.4 MB/s.
	c.sent.Add(200000)
	c.sent.Add(200000)
	c.observe(at.Add(4 * time.Second))
	if c.attempts != 1 || !c.evaluating {
		t.Fatal("aggregate low speed failed to pause all uploads")
	}
	if c.status()["scope"] != "all_uploads" {
		t.Fatal("controller scope changed")
	}
}
func TestUploadRecoveryStopsAfterYAndIgnoresClientStarvation(t *testing.T) {
	c := newUploadRecovery(transferOptions{UploadLowSeconds: 1, UploadRecoveryTries: 3}.defaults())
	c.manual = true
	end := c.begin(context.Background())
	defer end()
	at := time.Now()
	resume := c.waitingForClient()
	c.observe(at)
	c.observe(at.Add(10 * time.Second))
	resume()
	if c.attempts != 0 {
		t.Fatal("client starvation counted as cloud low speed")
	}
	for i := 0; i <= 10; i++ {
		c.observe(at.Add(time.Duration(11+i) * time.Second))
	}
	if c.attempts != 3 || c.failures != 3 || !c.limited {
		t.Fatalf("Y limit failed: attempts=%d failures=%d limited=%v", c.attempts, c.failures, c.limited)
	}
	c.observe(at.Add(100 * time.Second))
	if c.attempts != 3 {
		t.Fatal("recovery resumed after bandwidth-limited decision")
	}
}
func TestUploadRecoverySharedGateAndCancellation(t *testing.T) {
	c := newUploadRecovery(transferOptions{}.defaults())
	c.mu.Lock()
	c.pauseUntil = time.Now().Add(50 * time.Millisecond)
	c.mu.Unlock()
	start := time.Now()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			if err := c.wait(context.Background()); err != nil {
				t.Error(err)
			}
			if time.Since(start) < 40*time.Millisecond {
				t.Error("an upload escaped global pause")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("pause did not honor cancellation", err)
	}
	wg.Wait()
}

func TestUploadRecoveryRecognizesRelativeImprovementBelowX(t *testing.T) {
	c := newUploadRecovery(transferOptions{UploadLowSpeedMB: 1000, UploadLowSeconds: 1, UploadRecoveryTries: 1}.defaults())
	c.manual = true
	end := c.begin(context.Background())
	defer end()
	at := time.Now()
	c.observe(at)
	c.sent.Add(1000000)
	c.observe(at.Add(time.Second))
	c.observe(at.Add(2 * time.Second))
	c.sent.Add(1500000)
	c.observe(at.Add(3 * time.Second))
	if c.recovered != 1 || c.limited || c.failures != 0 {
		t.Fatal("clear relative improvement was treated as unchanged speed")
	}
}

func TestMemoryBookkeepingBoundWithOnePermanentlyUnconfirmedPart(t *testing.T) {
	const part = 1024
	source := newMemoryUploadSource(context.Background(), 101*part, part, 2*part, newByteBudget(2*part), nil)
	done := make(chan error, 1)
	go func() {
		_, e := source.Write(make([]byte, 101*part))
		if e == nil {
			e = source.FinishInput()
		}
		done <- e
	}()
	buf := make([]byte, part)
	if _, e := source.ReadAt(buf, 0); e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= 100; i++ {
		if _, e := source.ReadAt(buf, int64(i*part)); e != nil {
			t.Fatal(e)
		}
		source.ConfirmPart(int64(i*part), part)
		source.mu.Lock()
		count := len(source.parts)
		source.mu.Unlock()
		if count > 2 {
			t.Fatal("acknowledgement bookkeeping grew with uploaded file length", count)
		}
	}
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if _, e := source.ReadAt(buf, 0); e != nil {
		t.Fatal("retained first part cannot be retried", e)
	}
	source.ConfirmPart(0, part)
	source.Close()
}
func TestMemorySourceBackpressureRetryRetentionAndPoolBound(t *testing.T) {
	const part = 64 << 10
	global := newByteBudget(2 * part)
	source := newMemoryUploadSource(context.Background(), 8*part, part, 2*part, global, nil)
	data := bytes.Repeat([]byte("a"), 8*part)
	done := make(chan error, 1)
	go func() {
		_, err := source.Write(data)
		if err == nil {
			err = source.FinishInput()
		}
		done <- err
	}()
	buf := make([]byte, part)
	for i := 0; i < 8; i++ {
		if _, err := source.ReadAt(buf, int64(i*part)); err != nil {
			t.Fatal(err)
		}
		// Read an unacknowledged part twice to model an upload retry.
		if _, err := source.ReadAt(buf, int64(i*part)); err != nil || !bytes.Equal(buf, data[i*part:(i+1)*part]) {
			t.Fatal("retry data lost", err)
		}
		if i == 0 {
			select {
			case <-done:
				t.Fatal("producer bypassed bounded cache")
			default:
			}
		}
		source.ConfirmPart(int64(i*part), part)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	source.Close()
	stats := global.status()
	if stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 2*part || stats["allocated_bytes"].(int64) > 2*part {
		t.Fatal("body memory budget exceeded", stats)
	}
	if len(source.parts) != 0 {
		t.Fatal("retired-part metadata grew with file size")
	}
}
func TestMemorySourceCancelUnblocksProducerAndReleasesAll(t *testing.T) {
	const part = 1024
	s := newMemoryUploadSource(context.Background(), 10*part, part, part, newByteBudget(part), nil)
	done := make(chan error, 1)
	go func() { _, err := s.Write(make([]byte, 10*part)); done <- err }()
	buf := make([]byte, part)
	if _, err := s.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	s.Abort(context.Canceled)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.Close()
	if s.global.status()["used_bytes"].(int64) != 0 {
		t.Fatal("canceled upload retained active buffers")
	}
}

type streamingFixtureDrive struct {
	*fakeDAVDrive
	node     *httptest.Server
	mu       sync.Mutex
	parts    map[string]map[int][]byte
	counts   map[string]int
	failures int
	fatal    bool
	gate     <-chan struct{}
	started  chan struct{}
	once     sync.Once
}

func newStreamingFixture(t *testing.T) *streamingFixtureDrive {
	d := &streamingFixtureDrive{fakeDAVDrive: newFakeDAV(t), parts: map[string]map[int][]byte{}, counts: map[string]int{}, started: make(chan struct{})}
	d.node = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(8 << 20); err != nil {
			if !d.fatal {
				t.Error(err)
			}
			http.Error(w, "multipart", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		data, _ := io.ReadAll(file)
		file.Close()
		key := r.FormValue("uniqueId")
		index, _ := strconv.Atoi(r.FormValue("partIndex"))
		name := r.FormValue("fileName")
		d.once.Do(func() { close(d.started) })
		if d.gate != nil {
			select {
			case <-d.gate:
			case <-r.Context().Done():
				return
			}
		}
		d.mu.Lock()
		d.counts[key+":"+strconv.Itoa(index)]++
		count := d.counts[key+":"+strconv.Itoa(index)]
		if d.parts[name] == nil {
			d.parts[name] = map[int][]byte{}
		}
		old := d.parts[name][index]
		if old != nil && !bytes.Equal(old, data) {
			t.Error("retry bytes changed")
		}
		d.parts[name][index] = data
		fail := index == 1 && count <= d.failures
		d.mu.Unlock()
		if d.fatal {
			io.WriteString(w, `{"code":"500020001","msg":"permanent denial"}`)
			return
		}
		if fail {
			http.Error(w, "temporary", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"code":"0000","data":{"fid":"fixture-fid"}}`)
	}))
	t.Cleanup(d.node.Close)
	return d
}
func (d *streamingFixtureDrive) UploadStream(ctx context.Context, parent, name string, size int64, s *memoryUploadSource) (davEntry, error) {
	runtime := transfersFrom(ctx)
	u := &wopanUploadSession{endpoint: d.node.URL, name: name, parent: parent, unique: randomKey(), size: size, partSize: s.partSize, concurrency: 4, source: "测试云端", retryDelay: time.Millisecond, budget: runtime.connections, recovery: runtime.recovery}
	if _, err := u.run(ctx, s); err != nil {
		return davEntry{}, err
	}
	d.mu.Lock()
	var contents []byte
	for i := 1; i <= len(d.parts[name]); i++ {
		contents = append(contents, d.parts[name][i]...)
	}
	d.mu.Unlock()
	if int64(len(contents)) != size {
		return davEntry{}, errors.New("assembled bytes differ")
	}
	d.fakeDAVDrive.mu.Lock()
	defer d.fakeDAVDrive.mu.Unlock()
	d.seq++
	id := fmt.Sprint(d.seq)
	entry := davEntry{Item: Item{ID: id, FID: "fixture-fid", Name: name, Size: size}, Modified: time.Now()}
	d.entries[id] = entry
	d.parents[id] = parent
	d.contents[id] = contents
	return entry, nil
}
func installMemoryFixture(t *testing.T) (*httptest.Server, *streamingFixtureDrive, *davFS) {
	server, _, fs := testDAVService(t, true)
	d := newStreamingFixture(t)
	fs.mounts[0].drive = d
	fs.transfers = newTransferRuntime(transferOptions{UploadMemoryMiB: 10, TotalUploadMemoryMiB: 20, UploadPrefetchMiB: 5, UploadRecoveryMode: "off"})
	fs.spool = filepath.Join(t.TempDir(), "must-not-be-created")
	return server, d, fs
}
func TestMemoryWebDAVPipelinesBeforeClientEOFAndNeverSpools(t *testing.T) {
	server, d, fs := installMemoryFixture(t)
	release := make(chan struct{})
	d.gate = release
	data := bytes.Repeat([]byte("bounded-stream-"), 3<<20) // > 40 MiB, larger than both windows.
	result := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest("PUT", server.URL+"/dav/联通云盘-1/big.bin", bytes.NewReader(data))
		req.SetBasicAuth("tester", "test-password-123")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 201 {
				err = fmt.Errorf("PUT %d: %s", resp.StatusCode, body)
			}
		}
		result <- err
	}()
	select {
	case <-d.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cloud upload did not start while client was sending")
	}
	select {
	case err := <-result:
		t.Fatalf("PUT completed before cloud acknowledgement: %v", err)
	default:
	}
	if _, err := os.Stat(fs.spool); !os.IsNotExist(err) {
		t.Fatal("memory upload created a disk spool", err)
	}
	wantDAV(t, server.URL, "MKCOL", "/联通云盘-1/旁路目录", "", 201, nil)
	wantDAV(t, server.URL, "DELETE", "/联通云盘-1/旁路目录", "", 204, nil)
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	_, got, _ := davRequest(t, server.URL, "GET", "/联通云盘-1/big.bin", nil, nil)
	if sha256.Sum256(got) != sha256.Sum256(data) {
		t.Fatal("streamed file hash differs")
	}
	stats := fs.transfers.memory.status()
	if stats["used_bytes"].(int64) != 0 || stats["peak_bytes"].(int64) > 20<<20 || stats["allocated_bytes"].(int64) > 20<<20 {
		t.Fatal(stats)
	}
	if _, err := os.Stat(fs.spool); !os.IsNotExist(err) {
		t.Fatal("post-upload spool exists")
	}
}
func TestMemoryUploadRetriesBeyondOldLimitAndPreservesOverwrite(t *testing.T) {
	server, d, fs := installMemoryFixture(t)
	d.failures = 7
	first := bytes.Repeat([]byte("a"), 6<<20)
	second := bytes.Repeat([]byte("b"), 7<<20)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/test.bin", string(first), 201, nil)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/test.bin", string(second), 201, nil)
	_, got, _ := davRequest(t, server.URL, "GET", "/联通云盘-1/test.bin", nil, nil)
	if !bytes.Equal(got, second) {
		t.Fatal("memory overwrite failed")
	}
	d.mu.Lock()
	for key, count := range d.counts {
		if strings.HasSuffix(key, ":1") && count < 8 {
			t.Error("old retry limit remains", count)
		}
	}
	d.mu.Unlock()
	if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
		t.Fatal("successful retry retained buffers")
	}
}
func TestMemoryModeRejectsUnknownLengthAndUnsupportedDrive(t *testing.T) {
	server, _, fs := installMemoryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "PUT", server.URL+"/dav/联通云盘-1/unknown.bin", io.NopCloser(strings.NewReader("abc")))
	req.SetBasicAuth("tester", "test-password-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 411 {
		t.Fatal("unknown length was silently spooled", resp.StatusCode)
	}
	fs.mounts[0].drive = newFakeDAV(t)
	wantDAV(t, server.URL, "PUT", "/联通云盘-1/unsupported.bin", "abc", 501, nil)
}

func TestMemoryFatalCloudErrorCancelsBlockedReadersAndProducer(t *testing.T) {
	server, d, fs := installMemoryFixture(t)
	d.fatal = true
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "PUT", server.URL+"/dav/联通云盘-1/fatal.bin", io.LimitReader(repeatUploadByte(33), 40<<20))
	req.ContentLength = 40 << 20
	req.SetBasicAuth("tester", "test-password-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("cloud error hung until client timeout", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 400 {
		t.Fatal("permanent rejection reported success", resp.StatusCode)
	}
	deadline := time.Now().Add(time.Second)
	for fs.transfers.memory.status()["used_bytes"].(int64) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fs.transfers.memory.status()["used_bytes"].(int64) != 0 {
		t.Fatal("permanent error leaked upload memory")
	}
	if fs.transfers.uploads.limiter.status()["active"] != 0 {
		t.Fatal("permanent error retained file slot")
	}
}
func TestDownloadKeepsRetryingBeyondPriorLimitAndCancels(t *testing.T) {
	var calls atomic.Int64
	data := []byte("eventually available")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 7 {
			http.Error(w, "temporary", 503)
			return
		}
		w.Header().Set("ETag", `"same"`)
		http.ServeContent(w, r, "f", time.Time{}, bytes.NewReader(data))
	}))
	defer s.Close()
	r := fixtureReader(t, int64(len(data)), []readRoute{fixtureRoute("body", s.URL, "")})
	r.session.retryDelay = time.Millisecond
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) || calls.Load() != 8 {
		t.Fatal("unlimited retry failed", calls.Load(), err)
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer bad.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r2 := fixtureReader(t, 20, []readRoute{fixtureRoute("bad", bad.URL, "")})
	r2.ctx = ctx
	r2.session.retryDelay = time.Millisecond
	if _, err = io.ReadAll(r2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("endless retry ignored cancellation", err)
	}
}

func TestMultipleMemoryUploadsShareGlobalBudget(t *testing.T) {
	server, d, fs := installMemoryFixture(t)
	gate := make(chan struct{})
	d.gate = gate
	data := bytes.Repeat([]byte("parallel-memory"), 2<<20)
	var wg sync.WaitGroup
	errorsOut := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Go(func() {
			req, _ := http.NewRequest("PUT", fmt.Sprintf("%s/dav/联通云盘-1/f%d.bin", server.URL, i), bytes.NewReader(data))
			req.SetBasicAuth("tester", "test-password-123")
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					err = fmt.Errorf("PUT %d", resp.StatusCode)
				}
			}
			errorsOut <- err
		})
	}
	select {
	case <-d.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no cloud requests")
	}
	close(gate)
	wg.Wait()
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Error(err)
		}
	}
	stats := fs.transfers.memory.status()
	if stats["peak_bytes"].(int64) > 20<<20 || stats["allocated_bytes"].(int64) > 20<<20 || stats["used_bytes"].(int64) != 0 {
		t.Fatal("global memory exceeded or leaked", stats)
	}
	if fs.transfers.recovery.sent.Load() != int64(len(data))*3 {
		t.Fatal("global throughput omitted a file", fs.transfers.recovery.sent.Load())
	}
}

func TestWopanRetryRefreshesMultipartCredentials(t *testing.T) {
	var calls atomic.Int64
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		call := calls.Add(1)
		expected := "old"
		if call > 1 {
			expected = "new"
		}
		if r.FormValue("accessToken") != expected || r.Header.Get("Accesstoken") != expected || r.FormValue("fileInfo") != expected+"-info" {
			t.Error("multipart authorization did not match refreshed token")
		}
		if call == 1 {
			http.Error(w, "expired", 401)
			return
		}
		io.WriteString(w, `{"code":"0000","data":{"fid":"fixture"}}`)
	}))
	defer node.Close()
	u := &wopanUploadSession{endpoint: node.URL, access: "old", encrypted: "old-info", name: "auth.bin", size: 64 << 10, partSize: 64 << 10, concurrency: 1, retryDelay: time.Millisecond, refreshAuth: func(context.Context) (string, string, error) { return "new", "new-info", nil }}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := u.run(ctx, bytes.NewReader(make([]byte, 64<<10))); err != nil || calls.Load() != 2 {
		t.Fatal("credential refresh failed", err, calls.Load())
	}
}

func TestRclonePureMemoryUploads(t *testing.T) {
	bin := os.Getenv("CLOUDPROBE_RCLONE")
	if bin == "" {
		t.Skip("设置 CLOUDPROBE_RCLONE 后验证第三方客户端纯内存上传")
	}
	server, _, fs := installMemoryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bin, "obscure", "test-password-123")
	hideChildWindow(command)
	pass, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "rclone.conf")
	os.WriteFile(config, []byte("[cp]\ntype = webdav\nurl = "+server.URL+"/dav/\nvendor = other\nuser = tester\npass = "+strings.TrimSpace(string(pass))+"\n"), 0600)
	input := filepath.Join(dir, "input")
	os.Mkdir(input, 0700)
	data := bytes.Repeat([]byte("rclone-memory"), 500000)
	for i := 0; i < 3; i++ {
		os.WriteFile(filepath.Join(input, fmt.Sprintf("f%d.bin", i)), data, 0600)
	}
	command = exec.CommandContext(ctx, bin, "copy", input, "cp:联通云盘-1/", "--transfers", "3", "--config", config, "--retries", "1", "--low-level-retries", "1", "--log-level", "ERROR")
	hideChildWindow(command)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("rclone memory upload failed: %v %s", err, output)
	}
	for i := 0; i < 3; i++ {
		_, got, _ := davRequest(t, server.URL, "GET", fmt.Sprintf("/联通云盘-1/f%d.bin", i), nil, nil)
		if !bytes.Equal(got, data) {
			t.Fatal("third-party memory upload differs")
		}
	}
	if _, err := os.Stat(fs.spool); !os.IsNotExist(err) {
		t.Fatal("rclone upload spooled to disk")
	}
	t.Log("rclone: three concurrent PUT requests streamed from bounded RAM; downloaded bytes matched")
}

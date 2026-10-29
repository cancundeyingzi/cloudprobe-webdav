package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type quarkMockFile struct {
	ID, Parent, Name string
	Directory        bool
	Content          []byte
}
type quarkMockUpload struct {
	ID, Parent, Name string
	Size             int64
	Parts            map[int][]byte
	Content          []byte
}
type quarkFixture struct {
	server           *httptest.Server
	mu               sync.Mutex
	files            map[string]quarkMockFile
	uploads          map[string]*quarkMockUpload
	next             int
	desktopFailure   bool
	unknownList      bool
	missingQuota     bool
	failPartOnce     bool
	partFailureLimit int
	loseCommit       bool
	partFailures     atomic.Int64
	apiCalls         map[string]int
	partCalls        map[int]int
	bodyRequests     atomic.Int64
}

func newQuarkFixture(t *testing.T) (*quarkFixture, *quarkDAVDrive) {
	t.Helper()
	f := &quarkFixture{files: map[string]quarkMockFile{}, uploads: map[string]*quarkMockUpload{}, apiCalls: map[string]int{}, partCalls: map[int]int{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	c := newQuarkClient(quarkCredentials{UID: "account", Cookies: map[string]string{"__pus": "fixture-cookie"}, AccessToken: "fixture-access", RefreshToken: "fixture-refresh", OpenVerified: true}, nil)
	c.metadata = f.server.Client()
	c.bodies = f.server.Client()
	c.testHosts = true
	c.pan = f.server.URL + "/pan"
	c.endpoints = map[quarkRoute]string{quarkDesktop: f.server.URL + "/desktop", quarkWeb: f.server.URL + "/web", quarkOpen: f.server.URL + "/open"}
	d := newQuarkDrive(c, newTransferRuntime(transferOptions{TransferScheduling: "client", UploadStorageMode: "disk"}), 4, "auto")
	d.retryWait = func(ctx context.Context, _ time.Duration) error { return davWait(ctx, time.Millisecond) }
	return f, d
}
func (f *quarkFixture) row(it quarkMockFile, open bool) map[string]any {
	row := map[string]any{"fid": it.ID, "file_name": it.Name, "filename": it.Name, "size": len(it.Content), "dir": it.Directory, "updated_at": int64(1700000000000)}
	if it.Directory {
		row["file_type"] = 0
	} else {
		row["file_type"] = 1
		hash := sha1.Sum(it.Content)
		row["sha1"] = hex.EncodeToString(hash[:])
	}
	_ = open
	return row
}
func (f *quarkFixture) add(parent, name string, content []byte, dir bool) quarkMockFile {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	it := quarkMockFile{ID: fmt.Sprint(f.next), Parent: parent, Name: name, Content: append([]byte(nil), content...), Directory: dir}
	f.files[it.ID] = it
	return it
}
func (f *quarkFixture) serve(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/objects/") {
		f.object(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/download/") {
		f.bodyRequests.Add(1)
		id := strings.TrimPrefix(r.URL.Path, "/download/")
		f.mu.Lock()
		it, ok := f.files[id]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		hash := md5.Sum(it.Content)
		w.Header().Set("ETag", `"`+hex.EncodeToString(hash[:])+`"`)
		http.ServeContent(w, r, it.Name, time.Unix(1700000000, 0), bytes.NewReader(it.Content))
		return
	}
	var b map[string]any
	if r.Method == "POST" {
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		if err := dec.Decode(&b); err != nil {
			http.Error(w, "JSON", 400)
			return
		}
	}
	open := strings.HasPrefix(r.URL.Path, "/open/")
	path := strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/desktop"), "/web"), "/open")
	f.mu.Lock()
	f.apiCalls[r.URL.Path]++
	desktopFail := f.desktopFailure && strings.HasPrefix(r.URL.Path, "/desktop/")
	f.mu.Unlock()
	if desktopFail {
		respondJSON(w, 501, map[string]any{"code": 501, "message": "not supported"})
		return
	}
	reply := func(data any, metadata map[string]any) {
		out := map[string]any{"code": 0, "status": 200, "data": data}
		if open {
			out["status"] = 0
		}
		if metadata != nil {
			out["metadata"] = metadata
		}
		respondJSON(w, 200, out)
	}
	switch path {
	case "/file/sort", "/open/v1/file/list":
		if f.unknownList {
			reply(map[string]any{"unknown": true}, nil)
			return
		}
		parent := r.URL.Query().Get("pdir_fid")
		page, _ := strconv.Atoi(r.URL.Query().Get("_page"))
		if open {
			parent = valueString(b["parent_fid"])
			page = 1
			if cursor, ok := b["query_cursor"].(map[string]any); ok {
				page, _ = strconv.Atoi(valueString(cursor["token"]))
			}
		}
		f.mu.Lock()
		var all []quarkMockFile
		for id := 1; id <= f.next; id++ {
			if it, ok := f.files[fmt.Sprint(id)]; ok && it.Parent == parent {
				all = append(all, it)
			}
		}
		f.mu.Unlock()
		start := min((page-1)*100, len(all))
		end := min(start+100, len(all))
		rows := []any{}
		for _, it := range all[start:end] {
			rows = append(rows, f.row(it, open))
		}
		if open {
			reply(map[string]any{"file_list": rows, "last_page": end == len(all), "next_query_cursor": map[string]any{"token": fmt.Sprint(page + 1)}}, nil)
		} else {
			reply(map[string]any{"list": rows}, map[string]any{"_total": len(all)})
		}
	case "/member", "/open/v1/user/get_vip_info":
		if f.missingQuota {
			reply(map[string]any{}, nil)
			return
		}
		if open {
			reply(map[string]any{"capacity": "10000000000000000", "used": "9007199254740993"}, nil)
		} else {
			reply(map[string]any{"total_capacity": "10000000000000000", "use_capacity": "9007199254740993"}, nil)
		}
	case "/file/download", "/open/v1/file/get_download_url":
		id := valueString(b["fid"])
		if ids, ok := b["fids"].([]any); ok && len(ids) > 0 {
			id = valueString(ids[0])
		}
		f.mu.Lock()
		it, ok := f.files[id]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		data := map[string]any{"fid": id, "size": len(it.Content), "download_url": f.server.URL + "/download/" + id}
		if open {
			reply(data, nil)
		} else {
			reply([]any{data}, nil)
		}
	case "/file", "/open/v1/dir":
		name := valueString(b["file_name"])
		if open {
			name = valueString(b["dir_path"])
		}
		it := f.add(valueString(b["pdir_fid"]), name, nil, true)
		reply(map[string]any{"fid": it.ID}, nil)
	case "/file/rename", "/open/v1/file/rename":
		id := valueString(b["fid"])
		f.mu.Lock()
		it := f.files[id]
		it.Name = valueString(b["file_name"])
		f.files[id] = it
		f.mu.Unlock()
		reply(map[string]any{}, nil)
	case "/file/move", "/open/v1/file/move", "/file/delete", "/open/v1/file/delete":
		key := "filelist"
		if open {
			key = "fid_list"
		}
		ids, _ := b[key].([]any)
		f.mu.Lock()
		for _, id := range ids {
			fid := valueString(id)
			if strings.HasSuffix(path, "/delete") {
				delete(f.files, fid)
			} else {
				it := f.files[fid]
				it.Parent = valueString(b["to_pdir_fid"])
				f.files[fid] = it
			}
		}
		f.mu.Unlock()
		reply(map[string]any{}, nil)
	case "/file/upload/pre", "/open/v1/file/upload_pre":
		f.mu.Lock()
		f.next++
		task := fmt.Sprint(f.next)
		size, _ := nonnegativeInteger(b["size"])
		f.uploads[task] = &quarkMockUpload{ID: task, Parent: valueString(b["pdir_fid"]), Name: valueString(b["file_name"]), Size: size, Parts: map[int][]byte{}}
		f.mu.Unlock()
		reply(map[string]any{"task_id": task, "part_size": 65536, "upload_id": "up-" + task, "bucket": "bucket", "obj_key": "objects/" + task, "auth_info": "fixture-auth", "callback": map[string]any{"callbackUrl": "fixture"}, "upload_url": f.server.URL}, map[string]any{"part_size": 65536})
	case "/file/update/hash":
		reply(map[string]any{"finish": false}, nil)
	case "/file/upload/auth":
		reply(map[string]any{"auth_key": "fixture-oss-key"}, nil)
	case "/open/v1/file/get_upload_urls":
		parts, _ := b["part_info_list"].([]any)
		p, _ := parts[0].(map[string]any)
		task := valueString(b["task_id"])
		number := valueString(p["part_number"])
		reply(map[string]any{"upload_urls": []any{map[string]any{"part_number": p["part_number"], "upload_url": f.server.URL + "/objects/" + task + "?partNumber=" + number, "signature_info": map[string]any{"signature": "fixture-open-key"}}}}, nil)
	case "/file/upload/finish", "/open/v1/file/upload_finish":
		task := valueString(b["task_id"])
		f.mu.Lock()
		u := f.uploads[task]
		content := f.assemble(u)
		it := quarkMockFile{ID: task, Parent: u.Parent, Name: u.Name, Content: content}
		f.files[task] = it
		f.mu.Unlock()
		reply(map[string]any{"finish": true, "fid": task}, nil)
	default:
		respondJSON(w, 404, map[string]any{"code": 404, "message": "unknown fixture route"})
	}
}
func (f *quarkFixture) assemble(u *quarkMockUpload) []byte {
	var body []byte
	for i := 1; i <= len(u.Parts); i++ {
		body = append(body, u.Parts[i]...)
	}
	return body
}
func (f *quarkFixture) object(w http.ResponseWriter, r *http.Request) {
	task := strings.TrimPrefix(r.URL.Path, "/objects/")
	if r.Method == "PUT" {
		data, _ := io.ReadAll(r.Body)
		number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
		f.mu.Lock()
		f.partCalls[number]++
		u := f.uploads[task]
		u.Parts[number] = data
		fail := (f.failPartOnce && f.partCalls[number] == 1) || f.partCalls[number] <= f.partFailureLimit
		f.mu.Unlock()
		if fail {
			f.partFailures.Add(1)
			http.Error(w, "retry", 503)
			return
		}
		hash := md5.Sum(data)
		w.Header().Set("ETag", `"`+hex.EncodeToString(hash[:])+`"`)
		w.WriteHeader(200)
		return
	}
	if r.Method == "POST" {
		io.Copy(io.Discard, r.Body)
		f.mu.Lock()
		u := f.uploads[task]
		if f.loseCommit {
			f.files[task] = quarkMockFile{ID: task, Parent: u.Parent, Name: u.Name, Content: f.assemble(u)}
		}
		lose := f.loseCommit
		f.mu.Unlock()
		if lose {
			http.Error(w, "lost assembly response", 502)
			return
		}
		io.WriteString(w, "<CompleteMultipartUploadResult/>")
		return
	}
	http.NotFound(w, r)
}

func TestQuarkDirectoryPaginationFallbackAndStrictSchema(t *testing.T) {
	f, d := newQuarkFixture(t)
	f.desktopFailure = true
	for i := 0; i < 137; i++ {
		f.add("0", fmt.Sprintf("file-%03d.txt", i), []byte("body"), false)
	}
	items, err := d.List(context.Background(), "0")
	if err != nil || len(items) != 137 {
		t.Fatalf("listing %d %v", len(items), err)
	}
	if f.apiCalls["/web/file/sort"] != 2 {
		t.Fatal("did not paginate alternate route")
	}
	d.client.mu.Lock()
	d.client.credentials.Cookies = nil
	d.client.mu.Unlock()
	items, err = d.List(context.Background(), "0")
	if err != nil || len(items) != 137 {
		t.Fatalf("open cursor pagination: %d %v", len(items), err)
	}
	f.unknownList = true
	if _, err = d.List(context.Background(), "0"); err == nil {
		t.Fatal("unknown schema accepted as an empty directory")
	}
}
func TestQuarkQuotaPreservesIntegersAndUnknown(t *testing.T) {
	f, d := newQuarkFixture(t)
	f.desktopFailure = true
	q, err := d.Quota(context.Background())
	if err != nil || q.Used != 9007199254740993 || q.Total != 10000000000000000 {
		t.Fatalf("capacity precision %v %v", q, err)
	}
	f.missingQuota = true
	if _, err = d.Quota(context.Background()); err == nil {
		t.Fatal("missing capacity converted to zeros")
	}
}
func TestQuarkUploadWebAndOpenRetainPartsAndVerifyHash(t *testing.T) {
	for _, route := range []string{"web", "open"} {
		t.Run(route, func(t *testing.T) {
			f, d := newQuarkFixture(t)
			f.desktopFailure = true
			f.failPartOnce = true
			if route == "open" {
				d.client.mu.Lock()
				d.client.credentials.Cookies = nil
				d.client.mu.Unlock()
			}
			content := bytes.Repeat([]byte("quark-upload-content"), 11000)
			file, err := os.CreateTemp(t.TempDir(), "upload-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			file.Write(content)
			entry, err := d.Upload(context.Background(), "0", "upload.txt", file)
			if err != nil {
				t.Fatal(err)
			}
			if entry.Size != int64(len(content)) || entry.ContentVersion == "" {
				t.Fatal("upload not verified")
			}
			f.mu.Lock()
			saved := f.files[entry.ID]
			attempts := len(f.partCalls)
			f.mu.Unlock()
			if !bytes.Equal(saved.Content, content) || attempts < 3 || f.partFailures.Load() != int64(attempts) {
				t.Fatalf("content/retry failed parts=%d failures=%d", attempts, f.partFailures.Load())
			}
		})
	}
}
func TestQuarkLostCompletionResponseIsReconciledWithoutReplay(t *testing.T) {
	f, d := newQuarkFixture(t)
	f.loseCommit = true
	payload := bytes.Repeat([]byte("verified"), 15000)
	file, _ := os.CreateTemp(t.TempDir(), "lost-*")
	defer file.Close()
	file.Write(payload)
	entry, err := d.Upload(context.Background(), "0", "CloudProbe-upload-unique.txt", file)
	if err != nil || entry.ID == "" {
		t.Fatal(entry, err)
	}
	if f.apiCalls["/web/file/upload/pre"] != 0 {
		t.Fatal("uncertain upload replayed through another route")
	}
}
func TestQuarkWebDAVCRUDRangeQuotaAndIndependentAccount(t *testing.T) {
	f, drive := newQuarkFixture(t)
	server, _, fs := testDAVService(t, true)
	fs.transfers = newTransferRuntime(transferOptions{UploadStorageMode: "memory", TransferScheduling: "client"})
	drive.budget = fs.transfers.connections
	fs.mounts[0].UploadStorageMode = "disk"
	fs.mounts[0].drive = drive
	fs.mounts[0].Provider = "quark"
	fs.mounts[0].Identity = davIdentityHash("quark", "account")
	root := "/联通云盘-1"
	wantDAV(t, server.URL, "MKCOL", root+"/test", "", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"/test/text.txt", "initial", 201, nil)
	wantDAV(t, server.URL, "PUT", root+"/test/text.txt", "replacement", 201, nil)
	body := wantDAV(t, server.URL, "GET", root+"/test/text.txt", "", 200, nil)
	if string(body) != "replacement" {
		t.Fatal("overwrite lost")
	}
	body = wantDAV(t, server.URL, "GET", root+"/test/text.txt", "", 206, map[string]string{"Range": "bytes=2-5"})
	if string(body) != "plac" {
		t.Fatal("range changed", string(body))
	}
	before := f.bodyRequests.Load()
	wantDAV(t, server.URL, "HEAD", root+"/test/text.txt", "", 200, nil)
	if before != f.bodyRequests.Load() {
		t.Fatal("HEAD downloaded content")
	}
	dest, _ := url.Parse(server.URL + "/dav" + root + "/test/renamed.txt")
	wantDAV(t, server.URL, "MOVE", root+"/test/text.txt", "", 201, map[string]string{"Destination": dest.String()})
	dest, _ = url.Parse(server.URL + "/dav" + root + "/test/copy.txt")
	wantDAV(t, server.URL, "COPY", root+"/test/renamed.txt", "", 201, map[string]string{"Destination": dest.String()})
	used, available, err := fs.Quota(context.Background(), root)
	if err != nil || used != 9007199254740993 || available != 992800745259007 {
		t.Fatal("WebDAV quota", used, available, err)
	}
	wantDAV(t, server.URL, "DELETE", root+"/test/copy.txt", "", 204, nil)
	wantDAV(t, server.URL, "DELETE", root+"/test/renamed.txt", "", 204, nil)
	wantDAV(t, server.URL, "PUT", root+"/test/empty.txt", "", 201, nil)
	empty := wantDAV(t, server.URL, "GET", root+"/test/empty.txt", "", 200, nil)
	if len(empty) != 0 {
		t.Fatal("zero byte upload changed")
	}
	if fs.transfers.memory.status()["peak_bytes"] != int64(0) {
		t.Fatal("explicit disk mount accidentally used global memory pipeline")
	}
}
func TestQuarkQRWaitingWithNullDataIsNotALoginFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, 200, map[string]any{"status": 50004001, "data": nil})
	}))
	defer s.Close()
	flow := &quarkLoginFlow{http: s.Client(), credentials: quarkCredentials{DeviceID: "fixture", DeviceName: "fixture"}}
	data, err := flow.uop(context.Background(), s.URL+"/getServiceTicketByQrcodeToken", nil)
	if err != nil || valueString(data["_login_status"]) != "50004001" {
		t.Fatal(data, err)
	}
}

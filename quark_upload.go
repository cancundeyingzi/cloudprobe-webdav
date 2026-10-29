package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type quarkUpload struct {
	drive                                                 *quarkDAVDrive
	route                                                 quarkRoute
	parent, name, task, uploadID, bucket, key, base, mime string
	auth                                                  any
	callback                                              any
	size, partSize                                        int64
	md5, sha1                                             string
	completeFID                                           string
	beforeComplete                                        func(context.Context) error
}

type quarkCheckpointSession struct {
	Kind                                           string
	Route                                          quarkRoute
	Task, UploadID, Bucket, Key, Base, CompleteFID string
	Auth, Callback                                 any
	PartSize                                       int64
}

func (d *quarkDAVDrive) uploadFile(ctx context.Context, parent, name string, f *os.File) (davEntry, error) {
	stat, err := f.Stat()
	if err != nil {
		return davEntry{}, err
	}
	size := stat.Size()
	hashStarted := time.Now()
	logDAV(ctx, runLogEntry{Operation: "夸克上传散列", Source: "本机磁盘缓存", Result: "开始", Bytes: size, Detail: "计算完整 MD5/SHA1 后建立云端上传会话；此阶段尚未发送文件正文"})
	h1, h2 := md5.New(), sha1.New()
	if _, err = io.Copy(io.MultiWriter(h1, h2), quarkHashReader{ctx: ctx, reader: uploadSection(ctx, f, 0, size)}); err != nil {
		return davEntry{}, err
	}
	logDAV(ctx, runLogEntry{Operation: "夸克上传散列", Source: "本机磁盘缓存", Result: "完成", Bytes: size, DurationMs: time.Since(hashStarted).Milliseconds()})
	md, sh := hex.EncodeToString(h1.Sum(nil)), hex.EncodeToString(h2.Sum(nil))
	var last error
	for _, route := range d.metadataRoutes("upload") {
		u := &quarkUpload{drive: d, route: route, parent: parent, name: name, size: size, md5: md, sha1: sh, mime: contentType(name)}
		err = u.prepare(ctx)
		if err != nil {
			last = err
			d.metadataHealth.bad(string(route) + ":upload")
			var api *quarkAPIError
			if errors.As(err, &api) && (api.Rejected || api.HTTP < 500 && api.Code != "" && api.Code != "0") {
				logFallback(ctx, route.label(), "其他夸克上传准备通道", err)
				continue
			}
			return davEntry{}, &davMutationError{Phase: "夸克上传准备", Err: err}
		}
		d.metadataHealth.good(string(route) + ":upload")
		if u.completeFID != "" {
			return d.verifyQuarkUpload(ctx, parent, name, size, u.completeFID, sh, true)
		}
		err = u.run(ctx, f)
		if err != nil {
			// A lost completion response must not restart a different session.
			if ctx.Err() == nil {
				if entry, check := d.verifyQuarkUpload(ctx, parent, name, size, "", sh, false, md); check == nil {
					return entry, nil
				}
			}
			return davEntry{}, &davMutationError{Phase: "夸克上传", Err: err}
		}
		return d.verifyQuarkUpload(ctx, parent, name, size, "", sh, true)
	}
	if last == nil {
		last = errors.New("夸克没有可用上传授权")
	}
	return davEntry{}, &davNotExecuted{last}
}
func (u *quarkUpload) restoreSession(ctx context.Context) (bool, error) {
	cp := checkpointFrom(ctx)
	var saved quarkCheckpointSession
	if exists, err := cp.session(&saved); err != nil {
		return false, err
	} else if exists {
		if saved.Kind != "quark" || (saved.Task == "" && saved.CompleteFID == "") || saved.PartSize <= 0 || saved.PartSize > 256<<20 || (saved.Route != quarkDesktop && saved.Route != quarkWeb && saved.Route != quarkOpen) {
			return false, errors.New("夸克续传会话无效")
		}
		if saved.CompleteFID == "" && saved.Route != quarkOpen && (saved.UploadID == "" || u.drive.client.validBodyURL(saved.Base) != nil) {
			return false, errors.New("夸克 OSS 续传地址无效")
		}
		u.route, u.task, u.uploadID, u.bucket, u.key, u.base, u.completeFID = saved.Route, saved.Task, saved.UploadID, saved.Bucket, saved.Key, saved.Base, saved.CompleteFID
		u.auth, u.callback, u.partSize = saved.Auth, saved.Callback, saved.PartSize
		return true, nil
	}
	return false, nil
}
func (u *quarkUpload) prepare(ctx context.Context) error {
	if found, err := u.restoreSession(ctx); found || err != nil {
		return err
	}
	if err := u.prepareSession(ctx); err != nil {
		return err
	}
	if u.route != quarkOpen && u.completeFID == "" {
		if err := u.submitHash(ctx); err != nil {
			return err
		}
	}
	if u.completeFID != "" {
		return nil
	}
	return u.saveSession(ctx)
}

func (u *quarkUpload) saveSession(ctx context.Context) error {
	return checkpointFrom(ctx).setSession(quarkCheckpointSession{Kind: "quark", Route: u.route, Task: u.task, UploadID: u.uploadID, Bucket: u.bucket, Key: u.key, Base: u.base, Auth: u.auth, Callback: u.callback, PartSize: u.partSize, CompleteFID: u.completeFID})
}

func (u *quarkUpload) prepareSession(ctx context.Context) error {
	path := "/file/upload/pre"
	body := map[string]any{"file_name": u.name, "pdir_fid": u.parent, "dir_name": "", "size": u.size, "format_type": u.mime, "l_created_at": time.Now().UnixMilli(), "l_updated_at": time.Now().UnixMilli(), "ccp_hash_update": true}
	if u.route == quarkOpen {
		path = "/open/v1/file/upload_pre"
		body["md5"] = u.md5
		body["sha1"] = u.sha1
		body["same_path_file_reuse"] = false
		body["hash_update"] = false
		body["parallel_upload"] = false
	}
	out, err := u.drive.client.call(ctx, u.route, "POST", path, nil, body)
	if err != nil {
		return err
	}
	data, err := quarkObject(out["data"])
	if err != nil {
		return err
	}
	u.task = quarkStr(data, "task_id")
	if done, ok := quarkBool(data["finish"]); ok && done {
		u.completeFID = quarkStr(data, "fid")
		if u.completeFID == "" {
			return errors.New("夸克秒传响应缺少文件 ID")
		}
		return nil
	}
	if u.task == "" {
		return errors.New("夸克预上传响应缺少 task_id")
	}
	meta, _ := out["metadata"].(map[string]any)
	u.partSize, err = nonnegativeInteger(data["part_size"])
	if err != nil || u.partSize == 0 {
		u.partSize, err = nonnegativeInteger(meta["part_size"])
	}
	if err != nil || u.partSize < 1 || u.partSize > 256<<20 {
		return errors.New("夸克预上传响应缺少有效分片大小（最大 256 MiB）")
	}
	logDAV(ctx, runLogEntry{Operation: "夸克上传准备", Source: u.route.label(), Result: "会话已建立", Bytes: u.size, Detail: fmt.Sprintf("云端分片 %d B；云端任务并发 %d", u.partSize, u.drive.concurrency)})
	if u.route == quarkOpen {
		return nil
	}
	u.uploadID = quarkStr(data, "upload_id")
	u.bucket = quarkStr(data, "bucket")
	u.key = quarkStr(data, "obj_key")
	u.auth = data["auth_info"]
	u.callback = data["callback"]
	raw := quarkStr(data, "upload_url")
	base, err := url.Parse(raw)
	if err != nil || base.Host == "" || base.User != nil {
		return errors.New("夸克预上传返回无效 OSS 地址")
	}
	if !u.drive.client.testHosts {
		base.Scheme = "https"
		if u.bucket == "" || strings.ContainsAny(u.bucket, "/\\@:\r\n") {
			return errors.New("夸克 OSS bucket 无效")
		}
		base.Host = u.bucket + "." + base.Host
	}
	base.Path = "/" + strings.TrimPrefix(u.key, "/")
	base.RawQuery = ""
	u.base = base.String()
	if err = u.drive.client.validBodyURL(u.base); err != nil {
		return err
	}
	if u.uploadID == "" || u.key == "" || u.auth == nil {
		return errors.New("夸克预上传缺少 OSS 会话字段")
	}
	return nil
}

func (u *quarkUpload) submitHash(ctx context.Context) error {
	hash, err := u.drive.client.call(ctx, u.route, "POST", "/file/update/hash", nil, map[string]any{"task_id": u.task, "md5": u.md5, "sha1": u.sha1})
	if err != nil {
		return err
	}
	m, err := quarkObject(hash["data"])
	if err != nil {
		return err
	}
	if done, ok := quarkBool(m["finish"]); ok && done {
		u.completeFID = quarkStr(m, "fid")
		if u.completeFID == "" {
			entry, e := u.drive.verifyQuarkUpload(ctx, u.parent, u.name, u.size, "", u.sha1, true)
			if e != nil {
				return e
			}
			u.completeFID = entry.ID
		}
	}
	return nil
}
func (u *quarkUpload) openPart(ctx context.Context, index, off, n int64) (string, http.Header, error) {
	part := map[string]any{"part_number": index, "part_size": n}
	out, err := u.drive.client.call(ctx, u.route, "POST", "/open/v1/file/get_upload_urls", nil, map[string]any{"task_id": u.task, "part_info_list": []any{part}})
	if err != nil {
		return "", nil, err
	}
	m, err := quarkObject(out["data"])
	if err != nil {
		return "", nil, err
	}
	rows, ok := m["upload_urls"].([]any)
	if !ok || len(rows) != 1 {
		return "", nil, errors.New("夸克分片地址响应缺少唯一地址")
	}
	item, err := quarkObject(rows[0])
	if err != nil {
		return "", nil, err
	}
	number, e := nonnegativeInteger(item["part_number"])
	if e != nil || number != index {
		return "", nil, errors.New("夸克分片地址编号不一致")
	}
	raw := quarkStr(item, "upload_url")
	if err = u.drive.client.validBodyURL(raw); err != nil {
		return "", nil, err
	}
	info, _ := item["signature_info"].(map[string]any)
	signature := quarkStr(info, "signature")
	if signature == "" {
		return "", nil, errors.New("夸克分片缺少签名")
	}
	_ = off
	return raw, http.Header{"Authorization": {signature}, "Content-Type": {u.mime}}, nil
}

const quarkOSSAgent = "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit"

func (u *quarkUpload) ossAuth(ctx context.Context, method string, part int64, contentMD5, callback string) (string, http.Header, error) {
	stamp := time.Now().UTC().Format(http.TimeFormat)
	query := "uploadId=" + u.uploadID
	if part > 0 {
		query = "partNumber=" + strconv.FormatInt(part, 10) + "&" + query
	}
	mime := u.mime
	if method == "POST" {
		mime = "application/xml"
	}
	canonical := method + "\n" + contentMD5 + "\n" + mime + "\n" + stamp + "\n"
	if callback != "" {
		canonical += "x-oss-callback:" + callback + "\n"
	}
	canonical += "x-oss-date:" + stamp + "\nx-oss-user-agent:" + quarkOSSAgent + "\n/" + u.bucket + "/" + u.key + "?" + query
	out, err := u.drive.client.call(ctx, u.route, "POST", "/file/upload/auth", nil, map[string]any{"task_id": u.task, "auth_info": u.auth, "auth_meta": canonical})
	if err != nil {
		return "", nil, err
	}
	m, err := quarkObject(out["data"])
	if err != nil {
		return "", nil, err
	}
	auth := quarkStr(m, "auth_key")
	if auth == "" {
		return "", nil, errors.New("夸克 OSS 授权缺少 auth_key")
	}
	raw, _ := url.Parse(u.base)
	v := raw.Query()
	v.Set("uploadId", u.uploadID)
	if part > 0 {
		v.Set("partNumber", strconv.FormatInt(part, 10))
	}
	raw.RawQuery = v.Encode()
	headers := http.Header{"Authorization": {auth}, "Content-Type": {mime}, "Referer": {"https://pan.quark.cn/"}, "x-oss-date": {stamp}, "x-oss-user-agent": {quarkOSSAgent}}
	if contentMD5 != "" {
		headers.Set("Content-MD5", contentMD5)
	}
	if callback != "" {
		headers.Set("x-oss-callback", callback)
	}
	return raw.String(), headers, nil
}
func (u *quarkUpload) run(ctx context.Context, source io.ReaderAt) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	parts := max(int64(1), (u.size+u.partSize-1)/u.partSize)
	if parts > 100000 {
		return errors.New("夸克上传分片数超过保护上限")
	}
	etags := make([]string, int(parts))
	// The native Quark sessions used here require sequential OSS parts.
	// Multiple files can still upload concurrently through the DAV scheduler.
	workers := 1
	logDAV(ctx, runLogEntry{Operation: "夸克上传调度", Source: u.route.label(), Result: "顺序分片", Parts: parts, Detail: "上游要求按分片号顺序提交；保留客户端多文件并发、正文预算及故障重试"})
	jobs := make(chan int64)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	var sent, confirmed atomic.Int64
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for index := range jobs {
				off := (index - 1) * u.partSize
				n := min(u.partSize, u.size-off)
				tag, reused, err := prepareCheckpointPart(ctx, source, index, off, n)
				if err == nil && reused {
					confirmed.Add(n)
					checkpointProgress(ctx, n, index)
				} else if err == nil {
					tag, err = u.part(ctx, source, index, parts, off, n, &sent, &confirmed)
					if err == nil {
						err = checkpointFrom(ctx).confirm(index, tag)
					}
				}
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
				etags[index-1] = tag
				releaseCheckpointPart(source, off, n)
			}
		})
	}
loop:
	for i := int64(1); i <= parts; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break loop
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.beforeComplete != nil {
		if err := u.beforeComplete(ctx); err != nil {
			return err
		}
		if u.completeFID != "" {
			return nil
		}
	}
	if u.route == quarkOpen {
		rows := make([]any, parts)
		for i, tag := range etags {
			rows[i] = map[string]any{"part_number": i + 1, "etag": strings.Trim(tag, "\""), "part_size": min(u.partSize, u.size-int64(i)*u.partSize)}
		}
		out, err := u.drive.client.call(ctx, u.route, "POST", "/open/v1/file/upload_finish", nil, map[string]any{"task_id": u.task, "part_info_list": rows})
		if err != nil {
			return err
		}
		data, err := quarkObject(out["data"])
		if err != nil {
			return err
		}
		return u.drive.task(ctx, u.route, data)
	}
	if err := u.commitOSS(ctx, etags); err != nil {
		return err
	}
	out, err := u.drive.client.call(ctx, u.route, "POST", "/file/upload/finish", nil, map[string]any{"task_id": u.task, "obj_key": u.key})
	if err != nil {
		return err
	}
	if m, ok := out["data"].(map[string]any); ok {
		return u.drive.task(ctx, u.route, m)
	}
	return nil
}

type quarkPartError struct {
	HTTP   *upstreamHTTPError
	Detail string
	Code   string
}

func (e *quarkPartError) Error() string { return e.HTTP.Error() + e.Detail }
func (e *quarkPartError) Unwrap() error { return e.HTTP }

func (u *quarkUpload) part(ctx context.Context, source io.ReaderAt, index, parts, off, n int64, sent, confirmed *atomic.Int64) (string, error) {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if ready, ok := source.(interface {
			WaitReadable(context.Context, int64) error
		}); ok {
			if err := ready.WaitReadable(ctx, off); err != nil {
				return "", err
			}
		}
		var raw string
		var headers http.Header
		var err error
		if u.route == quarkOpen {
			raw, headers, err = u.openPart(ctx, index, off, n)
		} else {
			raw, headers, err = u.ossAuth(ctx, "PUT", index, "", "")
		}
		if err == nil {
			var release = func() {}
			if u.drive.budget != nil {
				release, err = u.drive.budget.acquire(ctx)
			}
			if err != nil {
				return "", err
			}
			guard := newUploadIdleGuard(ctx, u.drive.uploadIdle)
			reqCtx := guard.context
			var local atomic.Int64
			var progress *uploadProgressReader
			start := time.Now()
			body := newUploadBody(reqCtx, func(readCtx context.Context) io.Reader {
				progress = &uploadProgressReader{reader: uploadSection(readCtx, source, off, n), ctx: readCtx, source: u.route.label(), index: index, parts: parts, attempt: attempt, total: n, started: start, last: start, sent: sent, local: &local, confirmed: confirmed, recovery: u.drive.recovery}
				return guard.reader(progress)
			})
			req, re := http.NewRequestWithContext(reqCtx, "PUT", raw, body)
			if re != nil {
				body.Close()
				guard.stop()
				release()
				return "", re
			}
			req.ContentLength = n
			if n == 0 {
				req.Body = http.NoBody
			}
			req.Header = headers
			resp, re := u.drive.bodyHTTP.Do(req)
			tag := ""
			status := 0
			var after time.Duration
			if re == nil {
				resp.Body = guard.body(resp.Body)
				status = resp.StatusCode
				after = retryAfterHeader(resp.Header)
				responseData, readError := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
				re = readError
				resp.Body.Close()
				tag = resp.Header.Get("ETag")
				if status < 200 || status >= 300 {
					var failure struct {
						Code    string `xml:"Code"`
						Message string `xml:"Message"`
					}
					_ = xml.Unmarshal(responseData, &failure)
					detail := ""
					if failure.Code != "" {
						detail = "（" + failure.Code + "：" + u.drive.client.scrub(failure.Message) + "）"
					}
					re = &quarkPartError{HTTP: &upstreamHTTPError{code: status, after: after, source: u.route.label() + "上传节点"}, Detail: detail, Code: failure.Code}
				} else if tag == "" {
					re = errors.New("夸克分片缺少 ETag，未确认成功")
				}
			}
			body.Close()
			if _, idle := context.Cause(reqCtx).(*uploadIdleError); idle && ctx.Err() == nil {
				re = context.Cause(reqCtx)
			}
			guard.stop()
			progress.finish()
			release()
			err = re
			if err == nil && local.Load() != n {
				err = errors.New("夸克节点提前返回，分片正文未完整发送")
			}
			entry := runLogEntry{Operation: "上传分片", Source: u.route.label(), Endpoint: nodeHost(raw), Part: index, Parts: parts, Attempt: attempt, Bytes: n, SentBytes: local.Load(), DurationMs: time.Since(start).Milliseconds(), HTTPStatus: status, Result: "已确认"}
			if err == nil {
				confirmed.Add(n)
				entry.ConfirmedBytes = confirmed.Load()
				if trace, _ := ctx.Value(runTraceKey{}).(*runTrace); trace != nil {
					trace.confirmed.Add(n)
				}
				logDAV(ctx, entry)
				return tag, nil
			}
			entry.Result = "失败"
			entry.Error = u.drive.client.scrub(err.Error())
			logDAV(ctx, entry)
		}
		if !retryableTransfer(err) {
			return "", err
		}
		delay := min(time.Duration(attempt)*5*time.Second, 30*time.Second)
		var h *upstreamHTTPError
		if errors.As(err, &h) {
			delay = max(delay, h.after)
		}
		var api *quarkAPIError
		if errors.As(err, &api) {
			delay = max(delay, api.After)
		}
		logDAV(ctx, runLogEntry{Operation: "夸克上传故障重试", Source: u.route.label(), Result: "等待重试", Part: index, Parts: parts, Attempt: attempt, Detail: fmt.Sprintf("保留同一 task/upload 会话；等待 %d ms；次数无上限", delay.Milliseconds())})
		if err = u.drive.retryWait(ctx, delay); err != nil {
			return "", err
		}
	}
}
func (u *quarkUpload) commitOSS(ctx context.Context, tags []string) error {
	type part struct {
		Number int    `xml:"PartNumber"`
		ETag   string `xml:"ETag"`
	}
	payload := struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []part   `xml:"Part"`
	}{Parts: make([]part, len(tags))}
	for i, tag := range tags {
		payload.Parts[i] = part{i + 1, tag}
	}
	data, err := xml.Marshal(payload)
	if err != nil {
		return err
	}
	hash := md5.Sum(data)
	md := base64.StdEncoding.EncodeToString(hash[:])
	callbackJSON, err := json.Marshal(u.callback)
	if err != nil {
		return err
	}
	callback := base64.StdEncoding.EncodeToString(callbackJSON)
	raw, h, err := u.ossAuth(ctx, "POST", 0, md, callback)
	if err != nil {
		return err
	}
	guard := newUploadIdleGuard(ctx, u.drive.uploadIdle)
	defer guard.stop()
	req, err := http.NewRequestWithContext(guard.context, "POST", raw, guard.reader(bytes.NewReader(data)))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	req.Header = h
	// A completion is a mutation: inspect its bounded reply and reconcile an
	// uncertain result against the staged object, never replay it blindly.
	resp, err := u.drive.bodyHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = readQuarkOSSCompletion(resp, guard.reader(resp.Body))
	if _, idle := context.Cause(guard.context).(*uploadIdleError); idle && ctx.Err() == nil {
		return context.Cause(guard.context)
	}
	return err

}
func (d *quarkDAVDrive) verifyQuarkUpload(ctx context.Context, parent, name string, size int64, fid, sha string, acknowledged bool, md5Values ...string) (davEntry, error) {
	for attempt := 0; attempt < 5; attempt++ {
		entries, err := d.List(ctx, parent)
		if err != nil {
			return davEntry{}, err
		}
		var matches []davEntry
		for _, it := range entries {
			if !it.Directory && it.Name == name && it.Size == size && (fid == "" || it.ID == fid) {
				matches = append(matches, it)
			}
		}
		if len(matches) == 1 {
			it := matches[0]
			matchesHash := it.ContentVersion == "sha1:"+sha+":"+strconv.FormatInt(size, 10)
			for _, md := range md5Values {
				if quarkHashValid(md, 16) && it.ContentVersion == "md5:"+md+":"+strconv.FormatInt(size, 10) {
					matchesHash = true
				}
			}
			if acknowledged || matchesHash {
				return it, nil
			}
			return davEntry{}, errors.New("夸克上传结果缺少可核对的内容版本，未盲目认定提交成功")
		}
		if len(matches) > 1 {
			return davEntry{}, errors.New("夸克上传目标存在多个同名对象，未猜测结果")
		}
		if attempt < 4 {
			if err = d.retryWait(ctx, time.Second); err != nil {
				return davEntry{}, err
			}
		}
	}
	return davEntry{}, errors.New("夸克上传完成后尚未确认唯一目标文件")
}

// Bound cancellation latency during full-file hashing, before any cloud part
// is sent. os.File is not itself a contextualReaderAt.
type quarkHashReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r quarkHashReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

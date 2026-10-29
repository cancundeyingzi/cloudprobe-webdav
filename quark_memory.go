package main

import (
	"context"
	"fmt"
	"strings"
)

type memoryUnavailable struct{ reason string }

func (e *memoryUnavailable) Error() string { return e.reason }

type memoryUploadPlan struct {
	partSize int64
	workers  int
	hashes   bool
	run      func(context.Context, *memoryUploadSource) (davEntry, error)
}

func planMemoryUpload(ctx context.Context, drive davDrive, parent, name string, size int64, options transferOptions) (memoryUploadPlan, error) {
	if backup, ok := drive.(*backupDAVDrive); ok {
		if strings.HasPrefix(parent, "openlist:") {
			return memoryUploadPlan{}, &memoryUnavailable{"此备用目录需要磁盘兼容上传"}
		}
		plan, err := planMemoryUpload(ctx, backup.primary, parent, name, size, options)
		if err != nil {
			return plan, err
		}
		run := plan.run
		plan.run = func(ctx context.Context, source *memoryUploadSource) (davEntry, error) {
			entry, err := run(ctx, source)
			if err == nil {
				backup.remember(parent, []davEntry{entry})
			}
			return entry, err
		}
		return plan, nil
	}
	if quark, ok := drive.(*quarkDAVDrive); ok {
		return quark.planStream(ctx, parent, name, size)
	}
	stream, ok := drive.(streamingDAVDrive)
	if !ok {
		return memoryUploadPlan{}, &memoryUnavailable{"此通道不支持纯内存上传"}
	}
	partSize := int64(options.UploadPartMiB) << 20
	var saved struct{ PartSize int64 }
	if found, err := checkpointFrom(ctx).session(&saved); err != nil {
		return memoryUploadPlan{}, err
	} else if found {
		partSize = saved.PartSize
	}
	workers := 4
	if native := nativeWopanDrive(drive); native != nil {
		workers = uploadConcurrency(native.uploadConcurrency)
	}
	return memoryUploadPlan{partSize: partSize, workers: workers, run: func(ctx context.Context, source *memoryUploadSource) (davEntry, error) {
		return stream.UploadStream(ctx, parent, name, size, source)
	}}, nil
}

func (d *quarkDAVDrive) planStream(ctx context.Context, parent, name string, size int64) (memoryUploadPlan, error) {
	var rejected error
	for _, route := range d.metadataRoutes("upload") {
		if route == quarkOpen {
			continue
		} // This channel needs hashes in upload_pre.
		u := &quarkUpload{drive: d, route: route, parent: parent, name: name, size: size, mime: contentType(name)}
		restored, prepareErr := u.restoreSession(ctx)
		if prepareErr == nil && !restored {
			prepareErr = u.prepareSession(ctx)
		}
		if prepareErr == nil && u.route == quarkOpen {
			prepareErr = &memoryUnavailable{"旧会话需要预先完整散列，不能作为纯内存会话恢复"}
		}
		if err := prepareErr; err != nil {
			if !quarkRejected(err) {
				return memoryUploadPlan{}, &davMutationError{Phase: "夸克内存会话准备", Err: err}
			}
			d.metadataHealth.bad(string(route) + ":upload")
			rejected = err
			logFallback(ctx, route.label(), "其他原生内存上传通道", err)
			continue
		}
		if u.completeFID != "" {
			u.partSize = 1 << 20
		}
		if err := u.saveSession(ctx); err != nil {
			return memoryUploadPlan{}, err
		}
		d.metadataHealth.good(string(route) + ":upload")
		partSize := u.partSize
		if u.completeFID != "" {
			partSize = 1 << 20
		}
		return memoryUploadPlan{partSize: partSize, workers: 1, hashes: true, run: func(ctx context.Context, source *memoryUploadSource) (davEntry, error) {
			u.beforeComplete = func(ctx context.Context) error {
				md, sh, err := source.digests(ctx)
				if err != nil {
					return err
				}
				u.md5, u.sha1 = md, sh
				if u.completeFID != "" {
					return nil
				}
				logDAV(ctx, runLogEntry{Operation: "夸克流式散列", Source: route.label(), Result: "完整接收后提交", Bytes: size, Detail: "MD5/SHA1 随客户端原始正文只计算一次；重传分片不重复计入；正文未落盘"})
				return u.submitHash(ctx)
			}
			var err error
			if u.completeFID != "" {
				err = source.drainCheckpoint(ctx)
				if err == nil {
					err = u.beforeComplete(ctx)
				}
			} else {
				err = u.run(ctx, source)
			}
			if err != nil {
				if ctx.Err() == nil && u.sha1 != "" {
					if entry, check := d.verifyQuarkUpload(ctx, parent, name, size, "", u.sha1, false, u.md5); check == nil {
						return entry, nil
					}
				}
				return davEntry{}, &davMutationError{Phase: "夸克内存上传", Err: err}
			}
			// A prepare-time instant upload did not see this request's bytes.
			// Require its content hash rather than trusting only its ID and size.
			return d.verifyQuarkUpload(ctx, parent, name, size, u.completeFID, u.sha1, u.completeFID == "", u.md5)
		}}, nil
	}
	return memoryUploadPlan{}, &memoryUnavailable{fmt.Sprintf("夸克原生内存通道不可用；开放平台需要完整预先散列：%v", rejected)}
}

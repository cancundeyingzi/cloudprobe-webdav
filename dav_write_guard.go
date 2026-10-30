package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
)

// Run before the DAV handler can rename an existing destination to a backup.
func (f *davFS) checkNativeWrite(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var p davNode
	var err error
	// A mount root is virtual in DAV but is a real directory in the drive. LOCK
	// and PROPPATCH address that directory, not its unwritable virtual parent.
	if state := davState(ctx); state != nil && (state.method == "LOCK" || state.method == "PROPPATCH") && path.Dir(strings.TrimRight(name, "/")) == "/" {
		p, err = f.lookup(ctx, name)
		if err == nil && (p.mount == nil || !p.mount.Writable) {
			err = os.ErrPermission
		}
	} else {
		p, _, err = f.parent(ctx, name)
	}
	if err != nil {
		return err
	}
	// A failed native credential preflight already makes these writes unusable.
	// Reject before receiving a body or publishing an empty resume checkpoint.
	if native := nativeWopanDrive(p.mount.drive); native != nil {
		if err := native.ready(); err != nil {
			return err
		}
	}
	b, ok := p.mount.drive.(*backupDAVDrive)
	if !ok {
		return ctx.Err()
	}
	if strings.HasPrefix(p.entry.ID, "openlist:") {
		return davLocalError(ctx, http.StatusForbidden, errOpenListReadOnly.Error())
	}
	if _, err := b.primary.List(directoryLogContext(ctx), p.entry.ID); err != nil {
		return &davStatusError{Code: http.StatusServiceUnavailable, Message: fmt.Sprintf("原生目录接口不可用，OpenList 备用仅支持读取，未执行写入：%v", err), Err: err}
	}
	n, err := f.lookup(ctx, name)
	if err != nil && !os.IsNotExist(err) {
		return &davStatusError{Code: http.StatusServiceUnavailable, Message: fmt.Sprintf("未能核对原生目标，未执行写入：%v", err), Err: err}
	}
	if err == nil && strings.HasPrefix(n.entry.ID, "openlist:") {
		return davLocalError(ctx, http.StatusForbidden, errOpenListReadOnly.Error())
	}
	return ctx.Err()
}

// Classify failures only to choose the response; every failure stops dispatch.
// Keep the cause in request state so davResponse cannot turn a deliberately
// selected 403/408/409/504 into a stale upstream 502.
func davWriteCheckFailure(ctx context.Context, name string, err error) *davStatusError {
	failure := &davStatusError{Code: http.StatusBadGateway, Message: err.Error(), Err: err}
	var local *davStatusError
	var timeout net.Error
	if errors.As(err, &local) {
		failure.Code = local.Code
	}
	switch {
	case errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled):
		failure.Code = http.StatusRequestTimeout
	case errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()):
		failure.Code = http.StatusGatewayTimeout
	case errors.Is(err, os.ErrPermission):
		failure.Code = http.StatusForbidden
	case cloudWriteDenied(err):
		failure.Code = http.StatusForbidden
	case errors.Is(err, os.ErrInvalid):
		failure.Code = http.StatusBadRequest
	case errors.Is(err, os.ErrNotExist):
		failure.Code = http.StatusNotFound
		if state := davState(ctx); state != nil {
			if state.method == "PUT" || state.method == "MKCOL" || state.method == "COPY" || state.method == "LOCK" || (state.method == "MOVE" && name == state.destination) {
				failure.Code = http.StatusConflict
			}
		}
	}
	if state := davState(ctx); state != nil {
		state.err = failure
	}
	return failure
}

func validateDAVMountResources(mounts []davMountInput, accounts map[string]guiAccount) error {
	type selected struct {
		alias    string
		writable bool
	}
	seen := map[string]selected{}
	for _, m := range mounts {
		a, ok := accounts[m.Account]
		if !ok {
			return fmt.Errorf("所选账号已变化：%s", m.Alias)
		}
		key := davIdentity(a)
		if key == "" {
			return fmt.Errorf("挂载 %s 的账号缺少稳定身份标识，请重新读取账号", m.Alias)
		}
		if previous, ok := seen[key]; ok && (previous.writable || m.Writable) {
			return fmt.Errorf("挂载 %s 与 %s 属于同账号；存在可写挂载时，该账号仅允许一个挂载。多个挂载必须全部只读，以免重叠目录绕过文件锁", previous.alias, m.Alias)
		}
		seen[key] = selected{m.Alias, m.Writable}
	}
	return nil
}

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Run before the DAV handler can rename an existing destination to a backup.
func (f *davFS) checkNativeWrite(ctx context.Context, name string) error {
	p, _, err := f.parent(ctx, name)
	if err != nil {
		return err
	}
	b, ok := p.mount.drive.(*backupDAVDrive)
	if !ok {
		return nil
	}
	if strings.HasPrefix(p.entry.ID, "openlist:") {
		return davLocalError(ctx, http.StatusForbidden, errOpenListReadOnly.Error())
	}
	if _, err := b.primary.List(directoryLogContext(ctx), p.entry.ID); err != nil {
		return davLocalError(ctx, http.StatusServiceUnavailable, fmt.Sprintf("原生目录接口不可用，OpenList 备用仅支持读取，未执行写入：%v", err))
	}
	n, err := f.lookup(ctx, name)
	if err != nil && !os.IsNotExist(err) {
		return davLocalError(ctx, http.StatusServiceUnavailable, fmt.Sprintf("未能核对原生目标，未执行写入：%v", err))
	}
	if err == nil && strings.HasPrefix(n.entry.ID, "openlist:") {
		return davLocalError(ctx, http.StatusForbidden, errOpenListReadOnly.Error())
	}
	return nil
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
		root := m.Root
		if root == "" {
			root = a.Root
		}
		if (a.Provider == "quark" || a.Provider == "wopan") && root == "root" {
			root = "0"
		}
		key := davIdentity(a) + "\x00" + root
		if previous, ok := seen[key]; ok && (previous.writable || m.Writable) {
			return fmt.Errorf("挂载 %s 与 %s 指向同账号同根目录，存在可写挂载时禁止重复挂载，以免绕过文件锁", previous.alias, m.Alias)
		}
		seen[key] = selected{m.Alias, m.Writable}
	}
	return nil
}

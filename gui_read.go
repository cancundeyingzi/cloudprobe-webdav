package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (g *guiApp) guiReadDrive(key string, cl *Client) (davDrive, *transferRuntime, error) {
	g.mu.Lock()
	account, ok := g.accounts[key]
	account = cloneGUIAccount(account)
	g.mu.Unlock()
	if !ok {
		return nil, nil, fmt.Errorf("云盘账号已变化")
	}
	if g.dav != nil {
		g.dav.mu.Lock()
		for _, mount := range g.dav.mounts {
			if g.dav.server != nil && mount.Identity != "" && mount.Identity == davIdentity(account) {
				drive, runtime := mount.drive, g.dav.transfers
				g.dav.mu.Unlock()
				return drive, runtime, nil
			}
		}
		g.dav.mu.Unlock()
	}
	if account.Provider == "quark" {
		runtime := newTransferRuntime(transferOptions{TransferScheduling: "client"})
		drive, err := g.quarkDrive(key, runtime, 4, "auto")
		return drive, runtime, err
	}
	if account.Provider == "wopan" {
		return newWopanDAVDrive(cl, account.credentials, account.Root, nil), newTransferRuntime(transferOptions{}), nil
	}
	return &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}}, newTransferRuntime(transferOptions{}), nil
}
func guiFetchRedundant(ctx context.Context, drive davDrive, runtime *transferRuntime, cl *Client, entry davEntry, name string) (receipt DownloadReceipt, err error) {
	receipt = DownloadReceipt{Mode: "live-redundant", ID: entry.ID, FID: entry.FID, File: name}
	if err = safeFilename(name); err != nil {
		return receipt, err
	}
	if entry.ID == "" || entry.Directory {
		return receipt, fmt.Errorf("请先查询目录并选择一个普通文件")
	}
	if runtime != nil {
		release, e := runtime.file(ctx, "下载", "界面:"+entry.ID)
		if e != nil {
			return receipt, e
		}
		defer release()
	}
	r := newTransferReader(ctx, &davMount{drive: drive, health: &routeHealth{}}, entry, runtime)
	defer r.Close()
	f, e := os.OpenFile(filepath.Join(cl.folder, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return receipt, e
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(f.Name())
		}
	}()
	if entry.Size > cl.cfg.DownloadMaxBytes {
		return receipt, fmt.Errorf("文件超过界面验证上限，请使用 WebDAV 下载")
	}
	h := sha256.New()
	receipt.Bytes, err = io.Copy(io.MultiWriter(f, h), io.LimitReader(r, cl.cfg.DownloadMaxBytes+1))
	if err != nil {
		return receipt, err
	}
	if receipt.Bytes != entry.Size {
		return receipt, fmt.Errorf("下载字节数与目录不符")
	}
	receipt.SHA256 = hex.EncodeToString(h.Sum(nil))
	err = f.Sync()
	return receipt, err
}

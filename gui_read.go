package main

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
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
		return g.newWopanAccountDrive(cl, account, account.Root, nil), newTransferRuntime(transferOptions{}), nil
	}
	if valueString(account.credentials["mobile_web_token"]) != "" {
		return g.mobileAccountDrive(account), newTransferRuntime(transferOptions{}), nil
	}
	return &mobileDAVDrive{originalDAVDrive: originalDAVDrive{client: cl}}, newTransferRuntime(transferOptions{}), nil
}
func guiFetchRedundant(ctx context.Context, drive davDrive, runtime *transferRuntime, cl *Client, entry davEntry, name string, validate ...func() error) (receipt DownloadReceipt, err error) {
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
	parts := strings.Split(entry.ContentVersion, ":")
	var versionHash hash.Hash
	if len(parts) == 3 {
		switch parts[0] {
		case "sha1":
			versionHash = sha1.New()
		case "md5":
			versionHash = md5.New()
		}
	}
	writers := []io.Writer{f, h}
	if versionHash != nil {
		writers = append(writers, versionHash)
	}
	receipt.Bytes, err = io.Copy(io.MultiWriter(writers...), io.LimitReader(r, cl.cfg.DownloadMaxBytes+1))
	if err != nil {
		return receipt, err
	}
	if receipt.Bytes != entry.Size {
		return receipt, fmt.Errorf("下载字节数与目录不符")
	}
	receipt.SHA256 = hex.EncodeToString(h.Sum(nil))
	if versionHash != nil {
		if hex.EncodeToString(versionHash.Sum(nil)) != parts[1] {
			return receipt, fmt.Errorf("下载正文与所选目录快照的内容版本不符，请刷新后重试")
		}
	}
	err = f.Sync()
	if err == nil && len(validate) > 0 {
		err = validate[0]()
	}
	return receipt, err
}

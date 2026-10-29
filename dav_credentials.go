package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type davCredentialSavePendingError struct{ Err error }

func (e *davCredentialSavePendingError) Error() string { return e.Err.Error() }
func (e *davCredentialSavePendingError) Unwrap() error { return e.Err }

func saveRefreshedDAVCredential(ctx context.Context, save func(string, string) error, access, refresh string) error {
	if save == nil {
		return nil
	}
	err := save(access, refresh)
	var pending *davCredentialSavePendingError
	if errors.As(err, &pending) {
		logDAV(ctx, runLogEntry{Operation: "凭据保存", Source: "本机", Result: "待重试", Error: err.Error(), Detail: "上游刷新成功，使用内存中的新令牌；持久化单独重试"})
		return nil
	}
	return err
}

func readDAVCredentials(file string) (map[string]davCredential, error) {
	all := map[string]davCredential{}
	raw, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return all, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取凭据存储失败，保留原文件：%w", err)
	}
	plain, err := unprotectLocal(raw)
	if err != nil {
		return nil, fmt.Errorf("解密凭据存储失败，保留原文件：%w", err)
	}
	if err = json.Unmarshal(plain, &all); err != nil || all == nil {
		return nil, errors.New("凭据存储格式损坏，保留原文件")
	}
	return all, nil
}

// Call with g.mu held. Pending access tokens were already rotated upstream and
// must remain available in memory even if their disk copy cannot be updated.
func (g *guiApp) persistDAVCredentialsLocked(webIdentity, webToken string) error {
	file := filepath.Join(g.data, "webdav-credentials.bin")
	all, err := readDAVCredentials(file)
	if err != nil {
		return err
	}
	for identity, pending := range g.pendingDAVCredentials {
		c := all[identity]
		c.Access, c.Refresh, c.BaseHash = pending.Access, pending.Refresh, pending.BaseHash
		all[identity] = c
	}
	if webIdentity != "" {
		c := all[webIdentity]
		c.WebToken = webToken
		all[webIdentity] = c
	}
	plain, err := json.Marshal(all)
	if err != nil {
		return err
	}
	protected, err := protectLocal(plain)
	if err != nil {
		return err
	}
	if err = writePrivateBytes(file, protected); err != nil {
		return fmt.Errorf("原子保存凭据失败：%w", err)
	}
	clear(g.pendingDAVCredentials)
	g.credentialSaveError = ""
	if g.credentialRetry != nil {
		g.credentialRetry.Stop()
		g.credentialRetry = nil
	}
	return nil
}

func (g *guiApp) scheduleCredentialRetryLocked() {
	if g.credentialRetry != nil {
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(30*time.Second, func() {
		g.mu.Lock()
		// A stopped timer may already be waiting for this mutex. It must not
		// replace a newer retry or rewrite a store after pending saves cleared.
		if g.credentialRetry != timer || len(g.pendingDAVCredentials) == 0 {
			g.mu.Unlock()
			return
		}
		g.credentialRetry = nil
		err := g.persistDAVCredentialsLocked("", "")
		if err != nil {
			g.credentialSaveError = "新令牌在内存中使用，尚未保存；每 30 秒重试，损坏的原文件会保留"
			g.scheduleCredentialRetryLocked()
		}
		g.mu.Unlock()
		if err == nil {
			g.event("凭据保存重试成功，新令牌已持久化")
		}
	})
	g.credentialRetry = timer
}

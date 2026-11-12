package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"slices"
	"time"
)

type davLoadState struct {
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}

func cloneLoadStates(src map[string]davLoadState) map[string]davLoadState {
	dst := map[string]davLoadState{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func appendUnique(values []string, value string) []string {
	if value != "" && !slices.Contains(values, value) {
		return append(values, value)
	}
	return values
}
func savedMountInput(s davMountInput, a guiAccount) davSavedMount {
	m := davSavedMount{Identity: davIdentity(a), Alias: s.Alias, Root: s.Root, Writable: s.Writable, UploadStorageMode: s.UploadStorageMode}
	if s.OpenList != nil {
		m.OpenListEnabled = s.OpenList.Enabled
		m.OpenListAddress = s.OpenList.Address
		m.OpenListRoot = s.OpenList.Root
	}
	return m
}
func (d *davManager) AutoStart(ctx context.Context) {
	d.mu.Lock()
	c := d.config
	d.mu.Unlock()
	port := c.Port
	if port == 0 {
		port = 50146
	}
	_, err := d.Start(ctx, davStartInput{background: true, restore: true, Listen: c.Listen, Port: port, Username: c.Username, MaxUploadMiB: c.MaxUploadMiB, UploadConcurrency: c.UploadConcurrency, UploadMode: c.UploadMode, transferOptions: c.transferOptions})
	if err != nil {
		msg := d.app.redactDAV(err.Error())
		d.mu.Lock()
		d.lastError = msg
		d.mu.Unlock()
		d.app.event("WebDAV 自动启动失败：" + msg)
	}
}
func (d *davManager) watchMounts(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	slots := make(chan struct{}, 4)
	for {
		d.reconcileMounts(ctx, slots)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (d *davManager) reconcileMounts(ctx context.Context, slots chan struct{}) {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	d.mu.Lock()
	if d.serviceContext != ctx || d.server == nil {
		d.mu.Unlock()
		return
	}
	cfg := d.config
	d.mu.Unlock()
	d.app.mu.Lock()
	accounts := make([]guiAccount, 0, len(d.app.accounts))
	for _, a := range d.app.accounts {
		accounts = append(accounts, cloneGUIAccount(a))
	}
	d.app.mu.Unlock()
	// Removing an account also removes its published mount, without touching other transfers.
	active := map[string]bool{}
	for _, a := range accounts {
		active[davIdentity(a)] = true
	}
	d.mu.Lock()
	kept := make([]*davMount, 0, len(d.mounts))
	for _, m := range d.mounts {
		if active[m.Identity] {
			kept = append(kept, m)
		}
	}
	if len(kept) != len(d.mounts) {
		d.mounts = kept
		d.fs.mountMu.Lock()
		d.fs.mounts = append([]*davMount{}, kept...)
		d.fs.mountMu.Unlock()
	}
	for key, a := range d.attempted {
		if !active[davIdentity(a)] {
			delete(d.attempted, key)
			delete(d.loads, a.Key)
			delete(d.jobs, key)
		}
	}
	d.mu.Unlock()
	slices.SortFunc(accounts, func(a, b guiAccount) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	for _, a := range accounts {
		id := davIdentity(a)
		if id == "" {
			continue
		}
		var selected *davSavedMount
		for i := range cfg.Mounts {
			if cfg.Mounts[i].Identity == id {
				selected = &cfg.Mounts[i]
				break
			}
		}
		if selected == nil && !slices.Contains(cfg.KnownAccounts, id) {
			aliasBase := map[string]string{"mobile": "移动云盘", "wopan": "联通云盘", "quark": "夸克网盘"}[a.Provider]
			alias := ""
			for n := 1; ; n++ {
				alias = fmt.Sprintf("%s-%d", aliasBase, n)
				if !slices.ContainsFunc(cfg.Mounts, func(m davSavedMount) bool { return m.Alias == alias }) {
					break
				}
			}
			root := a.Root
			if (a.Provider == "wopan" || a.Provider == "quark") && root == "root" {
				root = "0"
			}
			m := davSavedMount{Identity: id, Alias: alias, Root: root, Writable: true}
			if a.Provider == "quark" {
				m.UploadStorageMode = "auto"
			}
			next := cfg
			next.Mounts = append(append([]davSavedMount{}, cfg.Mounts...), m)
			next.KnownAccounts = appendUnique(append([]string{}, cfg.KnownAccounts...), id)
			lockCtx, lockCancel := context.WithTimeout(ctx, 2*time.Second)
			lease, err := lockFileWait(lockCtx, filepath.Join(d.app.data, "webdav-config.lock"))
			lockCancel()
			if err == nil {
				disk, readErr := readDAVConfig(filepath.Join(d.app.data, "webdav.json"))
				if readErr != nil {
					err = readErr
				} else if !sameDAVConfig(disk, cfg) {
					err = errors.New("挂载配置已被其他进程修改，请停止并重新启动服务")
				} else {
					err = writePrivateJSON(filepath.Join(d.app.data, "webdav.json"), next)
				}
				lease.Close()
			}
			if err != nil {
				d.mu.Lock()
				d.loads[a.Key] = davLoadState{State: "failed", Error: "保存挂载配置失败：" + d.app.redactDAV(err.Error())}
				d.mu.Unlock()
				continue
			}
			cfg = next
			d.mu.Lock()
			d.config = cfg
			d.mu.Unlock()
			selected = &cfg.Mounts[len(cfg.Mounts)-1]
		}
		if selected == nil {
			continue
		}
		selections := []davSavedMount{}
		for _, m := range cfg.Mounts {
			if m.Identity == id {
				selections = append(selections, m)
			}
		}
		for _, selected := range selections {
			d.mu.Lock()
			old, attempted := d.attempted[selected.Alias]
			state := d.jobs[selected.Alias]
			if state.State == "loading" || attempted && reflect.DeepEqual(old.credentials, a.credentials) && reflect.DeepEqual(old.Channels, a.Channels) {
				d.mu.Unlock()
				continue
			}
			select {
			case slots <- struct{}{}:
			default:
				d.mu.Unlock()
				continue
			}
			d.attempted[selected.Alias] = a
			d.setLoadState(selected.Alias, a.Key, davLoadState{State: "loading"})
			runtime := d.transfers
			fs := d.fs
			d.mu.Unlock()
			sel := davMountInput{Account: a.Key, Alias: selected.Alias, Root: selected.Root, Writable: selected.Writable, UploadStorageMode: selected.UploadStorageMode}
			d.mu.Lock()
			if original, ok := d.mountInputs[selected.Alias]; ok {
				sel.OpenList = original.OpenList
			}
			d.mu.Unlock()
			go func(a guiAccount, sel davMountInput, cfg davSavedConfig) {
				defer func() { <-slots }()
				probe, cancel := context.WithTimeout(ctx, 80*time.Second)
				defer cancel()
				var m *davMount
				var err error
				if d.mountLoader != nil {
					m, err = d.mountLoader(probe, sel, a)
				} else if safeFilename(sel.Alias) != nil {
					err = errors.New("挂载名称无效，请停止服务后修改")
				} else {
					m, _, err = d.prepareMount(probe, sel, a, cfg, runtime)
				}
				d.controlMu.Lock()
				defer d.controlMu.Unlock()
				if ctx.Err() != nil {
					return
				}
				d.app.mu.Lock()
				current, exists := d.app.accounts[a.Key]
				d.app.mu.Unlock()
				if !exists || davIdentity(current) != davIdentity(a) {
					return
				}
				message := ""
				if err != nil {
					message = d.app.redactDAV(err.Error())
				}
				d.mu.Lock()
				defer d.mu.Unlock()
				if d.serviceContext != ctx || d.server == nil {
					return
				}
				state := davLoadState{State: "mounted"}
				if err != nil {
					state = davLoadState{State: "failed", Error: message}
				}
				d.setLoadState(sel.Alias, a.Key, state)
				next := make([]*davMount, 0, len(d.mounts)+1)
				for _, existing := range d.mounts {
					if existing.Alias != sel.Alias {
						next = append(next, existing)
					}
				}
				if err == nil {
					next = append(next, m)
				}
				slices.SortFunc(next, func(a, b *davMount) int {
					if a.Alias < b.Alias {
						return -1
					}
					if a.Alias > b.Alias {
						return 1
					}
					return 0
				})
				d.mounts = next
				fs.mountMu.Lock()
				fs.mounts = append([]*davMount{}, next...)
				fs.mountMu.Unlock()
				fs.cacheMu.Lock()
				fs.cache = map[string]davCache{}
				fs.quotas = nil
				fs.cacheVersion++
				fs.cacheMu.Unlock()
			}(a, sel, cfg)
		}
	}
}
func (d *davManager) retryMount(account string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.server == nil {
		return errors.New("WebDAV 已停止，请先启动服务")
	}
	if d.loads[account].State == "loading" {
		return nil
	}
	for alias, a := range d.attempted {
		if a.Key == account {
			delete(d.attempted, alias)
		}
	}
	return nil
}

// Caller holds d.mu. One account can have several read-only mount roots.
func (d *davManager) setLoadState(alias, account string, state davLoadState) {
	d.jobs[alias] = state
	aggregate := davLoadState{State: "mounted"}
	for key, a := range d.attempted {
		if a.Key != account {
			continue
		}
		v := d.jobs[key]
		if v.State == "loading" {
			aggregate = v
			break
		}
		if v.State == "failed" {
			aggregate = v
		}
	}
	d.loads[account] = aggregate
}

func sameDAVConfig(a, b davSavedConfig) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

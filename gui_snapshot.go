package main

import (
	"context"
	"errors"
	"time"
)

type guiBrowseSnapshot struct {
	Account, Identity, Directory string
	Created                      time.Time
	Entries                      map[string]davEntry
}

func (g *guiApp) storeBrowseSnapshot(account, directory string, items []Item, native []davEntry) (string, error) {
	if len(items) > 50000 {
		return "", errors.New("目录过大，无法保存下载快照，请进入子目录选择文件")
	}
	entries := make(map[string]davEntry, len(items))
	for _, it := range items {
		if _, ok := entries[it.ID]; ok {
			return "", errors.New("目录存在重复对象 ID，无法生成可靠下载快照")
		}
		entries[it.ID] = davEntry{Item: it, Parent: directory}
	}
	for _, it := range native {
		if _, ok := entries[it.ID]; ok {
			entries[it.ID] = it
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	a, ok := g.accounts[account]
	if !ok {
		return "", errors.New("账号已变化，请重新查询")
	}
	if davIdentity(a) == "" {
		return "", errors.New("账号缺少稳定身份标识，无法保存可靠的下载快照")
	}
	if g.browseSnapshots == nil {
		g.browseSnapshots = map[string]guiBrowseSnapshot{}
	}
	now := time.Now()
	total := 0
	for key, s := range g.browseSnapshots {
		if now.Sub(s.Created) > 10*time.Minute {
			delete(g.browseSnapshots, key)
		} else {
			total += len(s.Entries)
		}
	}
	for len(g.browseSnapshots) >= 32 || total+len(entries) > 100000 {
		oldest := ""
		var at time.Time
		for key, s := range g.browseSnapshots {
			if oldest == "" || s.Created.Before(at) {
				oldest, at = key, s.Created
			}
		}
		if oldest == "" {
			break
		}
		total -= len(g.browseSnapshots[oldest].Entries)
		delete(g.browseSnapshots, oldest)
	}
	id := randomKey()
	g.browseSnapshots[id] = guiBrowseSnapshot{account, davIdentity(a), directory, now, entries}
	return id, nil
}

func (g *guiApp) snapshotEntry(account, directory, snapshot, id string) (davEntry, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s, ok := g.browseSnapshots[snapshot]
	a, present := g.accounts[account]
	if !ok || !present || s.Account != account || s.Directory != directory || s.Identity != davIdentity(a) || time.Since(s.Created) > 10*time.Minute {
		return davEntry{}, errors.New("目录快照不存在、已过期或账号已变化，请重新查询目录")
	}
	e, ok := s.Entries[id]
	if !ok || e.Directory {
		return davEntry{}, errors.New("文件不属于所选目录快照，请重新查询")
	}
	return e, nil
}

type guiVersionUnavailable struct{}

func (*guiVersionUnavailable) Error() string {
	return "此通道未提供可靠的内容版本，无法保证正文仍是列目录时的版本；可刷新目录，或明确选择下载当前内容"
}

func verifyGUIEntry(ctx context.Context, drive davDrive, directory string, selected davEntry, allowCurrent ...bool) (davEntry, error) {
	entries, err := drive.List(directoryLogContext(ctx), directory)
	if err != nil {
		return davEntry{}, err
	}
	var found *davEntry
	for _, it := range entries {
		if it.ID == selected.ID {
			if found != nil {
				return davEntry{}, errors.New("对象 ID 不唯一，无法核验下载")
			}
			copy := it
			found = &copy
		}
	}
	before, _ := (davInfo{selected}).ETag(ctx)
	after := ""
	if found != nil {
		after, _ = (davInfo{*found}).ETag(ctx)
	}
	if found == nil || found.Name != selected.Name || found.Directory != selected.Directory || found.FID != selected.FID || before != after {
		return davEntry{}, errors.New("文件已移动、改名或版本已变化，请刷新目录后重新选择")
	}
	if selected.ContentVersion == "" && selected.FID == "" && (len(allowCurrent) == 0 || !allowCurrent[0]) {
		return davEntry{}, &guiVersionUnavailable{}
	}
	return *found, nil
}

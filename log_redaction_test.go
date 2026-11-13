package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestLogRedactionSnapshotsCurrentCredentialsPerEntry(t *testing.T) {
	app := &guiApp{accounts: map[string]guiAccount{"a": {credentials: map[string]any{"token": "old-private-token"}}}}
	log := newRuntimeLog(t.TempDir(), app.redactDAV)
	log.snapshotClean = app.snapshotDAVRedactor
	defer log.close()
	log.add(runLogEntry{Error: "old-private-token"})
	app.mu.Lock()
	app.accounts["a"] = guiAccount{credentials: map[string]any{"token": "rotated-private-token"}}
	app.rememberSecretsLocked("old-private-token", "old-private-token")
	count := len(app.extraSecrets)
	app.mu.Unlock()
	log.add(runLogEntry{Error: "rotated-private-token old-private-token", Items: []runLogItem{{Name: "rotated-private-token"}}})
	if count != 1 {
		t.Fatal("remembered secrets were not deduplicated")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, entry := range log.entries {
		if strings.Contains(entry.Error, "private-token") {
			t.Fatal("rotated credential leaked into a later entry")
		}
		for _, item := range entry.Items {
			if strings.Contains(item.Name, "private-token") {
				t.Fatal("directory item leaked a credential")
			}
		}
	}
}

func BenchmarkLogDirectoryRedaction(b *testing.B) {
	app := &guiApp{accounts: map[string]guiAccount{}}
	for index := 0; index < 4; index++ {
		key := fmt.Sprint(index)
		app.accounts[key] = guiAccount{credentials: map[string]any{"token": strings.Repeat("t", 40) + key, "refresh_token": strings.Repeat("r", 40) + key, "wopan_web_token": strings.Repeat("w", 60) + key}}
	}
	for index := 0; index < 30; index++ {
		app.rememberSecretsLocked(fmt.Sprintf("extra-%d-%s", index, strings.Repeat("z", 30)))
	}
	for _, snapshot := range []bool{false, true} {
		b.Run(fmt.Sprintf("snapshot=%v", snapshot), func(b *testing.B) {
			log := &runtimeLog{clean: app.redactDAV, closed: true}
			if snapshot {
				log.snapshotClean = app.snapshotDAVRedactor
			}
			entry := runLogEntry{Operation: "读取目录", Source: "云盘", Path: "/mount/dir", Items: make([]runLogItem, 100)}
			for index := range entry.Items {
				entry.Items[index].Name = fmt.Sprintf("示例文件-%03d.mkv", index)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				log.add(entry)
			}
		})
	}
}

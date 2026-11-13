package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Keep the existing account-overlap regression tables on the real config
// assembly path, including normalization and final merged validation.
func validateDAVMountResources(mounts []davMountInput, accounts map[string]guiAccount) error {
	in := davStartInput{Mounts: append([]davMountInput(nil), mounts...)}
	return configureDAVMounts(&in, &davSavedConfig{}, davSavedConfig{}, accounts)
}

func TestDAVMergedMountAliasesIncludeAbsentAccounts(t *testing.T) {
	old := guiAccount{Key: "old", Provider: "quark", Root: "0", credentials: map[string]any{"user_id": "old-uid"}}
	current := guiAccount{Key: "current", Provider: "quark", Root: "0", credentials: map[string]any{"user_id": "current-uid"}}
	previous := davSavedConfig{Mounts: []davSavedMount{{Identity: davIdentity(old), Alias: "disk", Root: "0", Writable: true}}}
	in := davStartInput{Mounts: []davMountInput{{Account: current.Key, Alias: "disk", Root: "0", Writable: true}}}
	var cfg davSavedConfig
	if err := configureDAVMounts(&in, &cfg, previous, map[string]guiAccount{current.Key: current}); err == nil {
		t.Fatal("absent account and new account were allowed to share an alias")
	}
	in.Mounts[0].Alias = "new-disk"
	cfg = davSavedConfig{}
	if err := configureDAVMounts(&in, &cfg, previous, map[string]guiAccount{current.Key: current}); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Mounts) != 2 || cfg.Mounts[0] != previous.Mounts[0] {
		t.Fatal("unrelated retained mount was lost")
	}
	restored := davSavedConfig{}
	if err := configureDAVMounts(&davStartInput{restore: true}, &restored, cfg, map[string]guiAccount{old.Key: old, current.Key: current}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Mounts, restored.Mounts) {
		t.Fatal("returning account changed the saved routing")
	}
}

func TestDAVSavedMountValidationProtectsRecovery(t *testing.T) {
	base := []davSavedMount{{Identity: "account", Alias: "root", Root: "0"}, {Identity: "account", Alias: "sub", Root: "sub-id"}}
	for _, test := range []struct {
		name string
		edit func([]davSavedMount)
	}{
		{"duplicate alias", func(m []davSavedMount) { m[1].Alias = m[0].Alias }},
		{"writable aliases", func(m []davSavedMount) { m[0].Writable = true }},
		{"missing identity", func(m []davSavedMount) { m[0].Identity = "" }},
		{"invalid root", func(m []davSavedMount) { m[0].Root = "bad\nroot" }},
		{"invalid storage", func(m []davSavedMount) { m[0].UploadStorageMode = "unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mounts := append([]davSavedMount(nil), base...)
			test.edit(mounts)
			file := filepath.Join(t.TempDir(), "webdav.json")
			if err := writePrivateJSON(file, davSavedConfig{Mounts: mounts}); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(file)
			if _, err := readDAVConfig(file); err == nil {
				t.Fatal("invalid saved mounts accepted during recovery")
			}
			after, _ := os.ReadFile(file)
			if string(before) != string(after) {
				t.Fatal("invalid configuration was overwritten")
			}
			if err := configureDAVMounts(&davStartInput{restore: true}, &davSavedConfig{}, davSavedConfig{Mounts: mounts}, nil); err == nil {
				t.Fatal("restore bypassed full configuration validation")
			}
		})
	}
	if err := validateSavedDAVMounts(base); err != nil {
		t.Fatal("multiple read-only roots rejected", err)
	}
}

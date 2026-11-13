package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestGUIInstanceReusesVerifiedSessionAndReleasesOnExit(t *testing.T) {
	data := t.TempDir()
	first, existing, err := claimGUIInstance(context.Background(), data)
	if err != nil || existing != "" {
		t.Fatal("first claim failed", err)
	}
	defer first.Close()
	app := &guiApp{nonce: randomKey(), accounts: map[string]guiAccount{}}
	server := httptest.NewServer(app)
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	app.host = parsed.Host
	address := server.URL + "/#" + app.nonce
	if err := first.publish(address); err != nil {
		t.Fatal(err)
	}
	protected, err := os.ReadFile(filepath.Join(data, "gui-instance.bin"))
	if err != nil || localCredentialsEncrypted && bytes.Contains(protected, []byte(app.nonce)) {
		t.Fatal("instance session was not protected", err)
	}
	second, reused, err := claimGUIInstance(context.Background(), data)
	if err != nil || second != nil || reused != address {
		t.Fatal("running instance was not reused", err)
	}
	first.Close()
	third, reused, err := claimGUIInstance(context.Background(), data)
	if err != nil || third == nil || reused != "" {
		t.Fatal("instance lease not released", err)
	}
	third.Close()
	if _, err := os.Stat(filepath.Join(data, "gui-instance.bin")); !os.IsNotExist(err) {
		t.Fatal("instance session record remained after exit", err)
	}
}

func TestGUIInstanceAddressRejectsExternalTargets(t *testing.T) {
	for _, address := range []string{
		"https://example.com/#" + randomKey(),
		"http://localhost:1234/#" + randomKey(),
		"http://127.0.0.1:1234/evil#" + randomKey(),
		"http://user@127.0.0.1:1234/#" + randomKey(),
		"http://127.0.0.1:1234/?redirect=evil#" + randomKey(),
		"http://127.0.0.1:1234/#invalid",
	} {
		if _, err := validateGUIAddress(address); err == nil {
			t.Fatal("untrusted instance address accepted")
		}
	}
}

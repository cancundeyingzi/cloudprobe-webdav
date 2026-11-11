package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wopan "github.com/OpenListTeam/wopan-sdk-go"
)

func TestWopanWebsiteCookieLayers(t *testing.T) {
	for _, raw := range []string{
		`%22abcdefghijklmnop%2Btail%3D%22`,
		`"abcdefghijklmnop+tail="`,
		`abcdefghijklmnop%2Btail%3D`,
		`WoCloud-Web-Token=%22abcdefghijklmnop%2Btail%3D%22; other=value`,
		`WoCloud-Web-Token="abcdefghijklmnop+tail="`,
	} {
		var got string
		var err error
		if strings.HasPrefix(raw, `"`) {
			got, err = decodeWopanWebCookie(raw)
		} else {
			got, err = webTokenFromInput(raw)
		}
		if err != nil || got != "abcdefghijklmnop+tail=" {
			t.Fatalf("cookie layers decoded incorrectly: %q, %v", raw, err)
		}
	}
	for _, raw := range []string{`%22unterminated`, `%ZZ`, `%22x%5Cnsecret%22`} {
		if _, err := decodeWopanWebCookie(raw); err == nil {
			t.Fatal("malformed cookie accepted")
		}
	}
}

func TestWopanLegacyQuotedCredentialUsesCanonicalEncryptionKey(t *testing.T) {
	const token = "abcdefghijklmnop-new-session"
	cred := map[string]any{"user_id": "13800000000", "wopan_web_token": `"` + token + `"`}
	cfg := defaultConfig("wopan")
	cfg.Transport = "bridge"
	cl, err := newClient(cfg, "")
	if err != nil {
		t.Fatal(err)
	}
	d := newWopanDAVDrive(cl, cred, "0", nil)
	actual, _ := d.w.GetToken()
	if actual != token {
		t.Fatal("legacy quote reached SDK")
	}
	param := wopan.Json{"parentDirectoryId": "0"}
	encrypted, err := d.w.EncryptParam(wopan.ChannelWoHome, param)
	if err != nil {
		t.Fatal(err)
	}
	expected := wopan.NewCrypto()
	if err := expected.SetAccessToken(token); err != nil {
		t.Fatal(err)
	}
	plain, err := expected.Decrypt(encrypted, wopan.ChannelWoHome)
	if err != nil || !bytes.Contains([]byte(plain), []byte("parentDirectoryId")) {
		t.Fatal("file request encrypted with JSON quote in key", err)
	}
	g := &guiApp{data: t.TempDir(), accounts: map[string]guiAccount{}}
	if err = g.acceptCloudAccount(context.Background(), guiAccount{Provider: "wopan", Root: "0", credentials: cred}); err != nil {
		t.Fatal(err)
	}
	restored := &guiApp{data: g.data, accounts: map[string]guiAccount{}}
	restored.restoreCloudAccounts()
	if len(restored.accounts) != 1 {
		t.Fatal("legacy account lost")
	}
	for _, a := range restored.accounts {
		if valueString(a.credentials["wopan_web_token"]) != token {
			t.Fatal("legacy quote restored")
		}
	}
}

func TestLiveWopanWebTokenReadOnly(t *testing.T) {
	profile := os.Getenv("CLOUDPROBE_WOPAN_TOKEN_VERIFY_PROFILE")
	if profile == "" {
		t.Skip("explicit account profile required; read-only account/directory verification")
	}
	raw, err := os.ReadFile(filepath.Join(profile, "cloud-accounts.bin"))
	if err != nil {
		t.Fatal("cannot read account store")
	}
	plain, err := unprotectLocal(raw)
	if err != nil {
		t.Fatal("cannot decrypt account store")
	}
	var saved []savedCloudAccount
	if json.Unmarshal(plain, &saved) != nil {
		t.Fatal("invalid account store")
	}
	checked := 0
	for _, a := range saved {
		if a.Provider != "wopan" {
			continue
		}
		token := valueString(a.Credentials["wopan_web_token"])
		if token == "" {
			continue
		}
		checked++
		redactor := &Redactor{}
		redactor.Add(token)
		var canonical string
		if json.Unmarshal([]byte(strings.TrimSpace(token)), &canonical) != nil {
			canonical = token
		}
		redactor.Add(canonical)
		t.Logf("account=%d JSON_wrapped_token=%v", checked, canonical != token)
		for _, sample := range []struct{ name, token string }{{"stored", token}, {"normalized", canonical}} {
			w := wopan.DefaultWithAccessToken(sample.token)
			w.SetHttpClient(providerMetadataClient(15 * time.Second))
			w.SetUserAgent(wopan.DefaultUA)
			w.OnRefreshToken(func(string, string) {})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, err = w.AppQueryUser(woContext(ctx))
			t.Logf("%s identity_ok=%v code=%s", sample.name, err == nil, businessErrorCode(err))
			var data map[string]any
			_, err = w.RequestWoHome(wopan.KeyQueryAllFiles, wopan.Json{"spaceType": "0", "parentDirectoryId": "0", "pageNum": 0, "pageSize": 100, "sortRule": 6, "clientId": wopan.DefaultClientID}, wopan.JsonSecret, &data, woContext(ctx))
			cancel()
			t.Logf("%s directory_ok=%v code=%s", sample.name, err == nil, businessErrorCode(err))
			if err != nil {
				t.Log(redactor.Text(err.Error()))
			}
			if sample.name == "normalized" && err != nil {
				t.Error("normalized credential directory request failed")
			}
		}

		cfg := defaultConfig("wopan")
		cfg.Transport = "bridge"
		cl, err := newClient(cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		drive := newWopanDAVDrive(cl, a.Credentials, "0", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		entries, err := drive.List(ctx, "0")
		if err != nil {
			cancel()
			t.Fatal("production driver directory failed:", redactor.Text(err.Error()))
		}
		t.Logf("production_driver directory_ok=true entries=%d", len(entries))
		if err = drive.probe(ctx); err != nil {
			cancel()
			t.Fatal("read-only write-channel preflight failed:", redactor.Text(err.Error()))
		}
		t.Log("production_driver write_preflight_ok=true (no cloud mutation)")
		_, err = drive.Quota(ctx)
		cancel()
		if err != nil {
			t.Fatal("production driver quota failed:", redactor.Text(err.Error()))
		}
		t.Log("production_driver quota_ok=true")
	}
	if checked == 0 {
		t.Fatal("no official Wopan account in selected profile")
	}
}

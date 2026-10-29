package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type terminationPermissionTransport struct {
	base http.RoundTripper
	dir  string
	once sync.Once
	err  error
}

func (p *terminationPermissionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "PUT" {
		p.once.Do(func() { p.err = os.Chmod(p.dir, 0500) })
	}
	return p.base.RoundTrip(r)
}

func TestTerminationLinuxCleanupDenialKeepsBodyOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires non-root; enforced in Linux CI")
	}
	for _, kind := range []string{"success", "aborted"} {
		t.Run(kind, func(t *testing.T) {
			g := &guiApp{data: t.TempDir(), host: "localhost", nonce: "cleanup-test"}
			_, drive := newQuarkFixture(t)
			server, _, fs := testDAVService(t, true)
			fs.spool = filepath.Join(g.data, "webdav-spool")
			fs.mounts[0].drive = drive
			fs.mounts[0].Provider = "quark"
			fs.mounts[0].Identity = "cleanup-account"
			fs.mounts[0].UploadStorageMode = "disk"
			defer os.Chmod(fs.spool, 0700)
			if kind == "success" {
				trip := &terminationPermissionTransport{base: drive.bodyHTTP.Transport, dir: fs.spool}
				client := *drive.bodyHTTP
				client.Transport = trip
				drive.bodyHTTP = &client
				wantDAV(t, server.URL, "PUT", "/联通云盘-1/file.bin", "cloud-success", 201, nil)
				trip.once.Do(func() {})
				if trip.err != nil {
					t.Fatal(trip.err)
				}
			} else {
				f, err := fs.OpenFile(context.Background(), "/联通云盘-1/file.bin", os.O_CREATE|os.O_RDWR, 0600)
				if err != nil {
					t.Fatal(err)
				}
				d := f.(*davFile)
				d.Write([]byte("partial-body"))
				d.failed = true
				if err = os.Chmod(fs.spool, 0500); err != nil {
					t.Fatal(err)
				}
				if err = d.Close(); err == nil {
					t.Fatal("aborted body accepted")
				}
			}
			w := terminationAPI(g, "GET", "upload-spool", nil, true)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var data struct {
				Bodies []struct {
					ID string `json:"id"`
				} `json:"bodies"`
			}
			json.Unmarshal(w.Body.Bytes(), &data)
			if len(data.Bodies) != 1 {
				t.Fatal("cleanup ownership missing", w.Body.String())
			}
			id := data.Bodies[0].ID
			w = terminationAPI(g, "POST", "upload-spool", map[string]any{"id": id}, true)
			if w.Code < 400 || !strings.Contains(w.Body.String(), "permission denied") {
				t.Fatal("expected real deletion denial", w.Code, w.Body.String())
			}
			if _, err := readSpoolOwner(fs.spool, id); err != nil {
				t.Fatal("failed cleanup dropped owner", err)
			}
			if err := os.Chmod(fs.spool, 0700); err != nil {
				t.Fatal(err)
			}
			if w = terminationAPI(g, "POST", "upload-spool", map[string]any{"id": id}, true); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if _, err := os.Stat(filepath.Join(fs.spool, "upload-"+id)); !os.IsNotExist(err) {
				t.Fatal("body survived cleanup", err)
			}
		})
	}
}

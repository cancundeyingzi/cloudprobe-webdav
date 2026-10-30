package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type backupFaultDrive struct {
	*fakeDAVDrive
	mode     string
	backedUp bool
}

func (d *backupFaultDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	if d.backedUp && d.mode == "unreadable" {
		return nil, errors.New("native listing unavailable")
	}
	return d.fakeDAVDrive.List(ctx, parent)
}

func (d *backupFaultDrive) Move(ctx context.Context, it davEntry, parent, name string) error {
	if strings.HasPrefix(name, "CloudProbe-backup-") {
		if d.mode == "not-executed" {
			return errors.New("rename rejected")
		}
		if err := d.fakeDAVDrive.Move(ctx, it, parent, name); err != nil {
			return err
		}
		d.backedUp = true
		if d.mode == "external-target" {
			d.mu.Lock()
			d.entries["external"] = davEntry{Item: Item{ID: "external", FID: "external-version", Name: it.Name, Size: 8}}
			d.parents["external"] = "0"
			d.contents["external"] = []byte("external")
			d.mu.Unlock()
		}
		return errors.New("backup rename acknowledgement lost")
	}
	if d.backedUp && name == "target.txt" {
		restoring := strings.HasPrefix(it.Name, "CloudProbe-backup-")
		if !restoring && (d.mode == "rollback" || d.mode == "rollback-lost-ack") {
			return errors.New("final rename rejected")
		}
		if restoring && d.mode == "rollback-lost-ack" {
			if err := d.fakeDAVDrive.Move(ctx, it, parent, name); err != nil {
				return err
			}
			return errors.New("restore acknowledgement lost")
		}
	}
	return d.fakeDAVDrive.Move(ctx, it, parent, name)
}

func TestDAVBackupAcknowledgementRecovery(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		for _, mode := range []string{"lost-ack", "not-executed", "unreadable", "external-target", "rollback", "rollback-lost-ack"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				server, base, fs := testDAVService(t, true)
				wantDAV(t, server.URL, "PUT", "/联通云盘-1/source.txt", "source", 201, nil)
				wantDAV(t, server.URL, "PUT", "/联通云盘-1/target.txt", "old", 201, nil)
				fs.mounts[0].drive = &backupFaultDrive{fakeDAVDrive: base, mode: mode}
				dest := server.URL + "/dav/" + url.PathEscape("联通云盘-1") + "/target.txt"
				code, body, _ := davRequest(t, server.URL, method, "/联通云盘-1/source.txt", nil, map[string]string{"Destination": dest, "Overwrite": "T"})
				if mode == "lost-ack" {
					if code != 204 {
						t.Fatalf("expected recovered success: %d %s", code, body)
					}
				} else if code < 400 {
					t.Fatalf("unexpected success: %d", code)
				}
				entries, _ := base.List(context.Background(), "0")
				target, err := uniqueNamedEntry(entries, "target.txt")
				if err != nil {
					t.Fatal(err)
				}
				backupCount := 0
				for _, it := range entries {
					if strings.HasPrefix(it.Name, "CloudProbe-backup-") {
						backupCount++
					}
				}
				want := "old"
				switch mode {
				case "lost-ack":
					want = "source"
				case "external-target":
					want = "external"
				case "unreadable":
					if target != nil || backupCount != 1 || !strings.Contains(string(body), "CloudProbe-backup-") {
						t.Fatalf("ambiguous state not retained: %+v %s", entries, body)
					}
					return
				}
				if target == nil {
					t.Fatalf("target disappeared: %+v %s", entries, body)
				}
				base.mu.Lock()
				content := string(base.contents[target.ID])
				base.mu.Unlock()
				if content != want {
					t.Fatalf("target=%q want=%q", content, want)
				}
				wantBackups := 0
				if mode == "external-target" {
					wantBackups = 1
				}
				if backupCount != wantBackups {
					t.Fatalf("backups=%d want=%d", backupCount, wantBackups)
				}
			})
		}
	}
}

func auditTCP(t *testing.T, server string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", strings.TrimPrefix(server, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err = c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDAVRejectsUnreadBodiesWithoutDrain(t *testing.T) {
	for _, mode := range []string{"unauthenticated", "overload", "bad-path"} {
		t.Run(mode, func(t *testing.T) {
			srv, _, _ := testDAVService(t, true)
			svc := srv.Config.Handler.(*davService)
			auth := ""
			path := "/dav/"
			want := 401
			if mode == "overload" {
				svc.requestOnce.Do(func() { svc.requestSlots = make(chan struct{}, 1); svc.requestSlots <- struct{}{} })
				want = 503
			} else if mode == "bad-path" {
				path = "/not-dav"
				want = 404
				auth = "Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("tester:test-password-123")) + "\r\n"
			}
			c := auditTCP(t, srv.URL)
			fmt.Fprintf(c, "POST %s HTTP/1.1\r\nHost: localhost\r\n%sContent-Length: 1\r\n\r\n", path, auth)
			resp, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				t.Fatal("refusal waited for body", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != want || !resp.Close {
				t.Fatalf("status=%d close=%v", resp.StatusCode, resp.Close)
			}
			if _, err = io.ReadAll(resp.Body); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("connection not closed: %v", err)
			}
		})
	}
}

func TestDAVIncomingBodyTimeoutReleasesUpload(t *testing.T) {
	srv, _, fs := testDAVService(t, true)
	svc := srv.Config.Handler.(*davService)
	svc.bodyIdleTimeout = 50 * time.Millisecond
	c := auditTCP(t, srv.URL)
	auth := base64.StdEncoding.EncodeToString([]byte("tester:test-password-123"))
	fmt.Fprintf(c, "PUT /dav/%s/stalled.txt HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic %s\r\nContent-Length: 4\r\n\r\nx", url.PathEscape("联通云盘-1"), auth)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 408 || !resp.Close {
		t.Fatalf("status=%d close=%v", resp.StatusCode, resp.Close)
	}
	io.Copy(io.Discard, resp.Body)
	wantDAV(t, srv.URL, "PUT", "/联通云盘-1/stalled.txt", "done", 201, nil)
	fs.reservations.mu.Lock()
	active := len(fs.reservations.active)
	fs.reservations.mu.Unlock()
	if active != 0 {
		t.Fatalf("reservations leaked: %d", active)
	}
}

func TestDAVIncomingDeadlineExcludesServerBackpressure(t *testing.T) {
	paused := make(chan struct{})
	resume := make(chan struct{})
	result := make(chan error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := newDAVInboundBody(w, r, 100*time.Millisecond)
		buf := make([]byte, 1)
		if _, err := io.ReadFull(b, buf); err != nil {
			result <- err
			return
		}
		close(paused)
		<-resume
		_, err := io.ReadFull(b, buf)
		result <- err
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c := auditTCP(t, srv.URL)
	fmt.Fprint(c, "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\na")
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("first byte not consumed")
	}
	time.Sleep(250 * time.Millisecond)
	close(resume)
	fmt.Fprint(c, "b")
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("server work consumed client timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second read blocked")
	}
}

func TestDAVIncomingDeadlineRenewsWithProgress(t *testing.T) {
	srv, _, _ := testDAVService(t, true)
	srv.Config.Handler.(*davService).bodyIdleTimeout = 300 * time.Millisecond
	c := auditTCP(t, srv.URL)
	auth := base64.StdEncoding.EncodeToString([]byte("tester:test-password-123"))
	fmt.Fprintf(c, "PUT /dav/%s/progress.txt HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic %s\r\nContent-Length: 8\r\n\r\n", url.PathEscape("联通云盘-1"), auth)
	for i := 0; i < 8; i++ {
		if _, err := fmt.Fprint(c, "x"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(60 * time.Millisecond)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 || resp.Close {
		t.Fatalf("active upload was interrupted: status=%d close=%v", resp.StatusCode, resp.Close)
	}
	io.Copy(io.Discard, resp.Body)
}

func TestDAVBackupRecoveryNeverUsesFallbackAbsence(t *testing.T) {
	base := newFakeDAV(t)
	backup, _ := newOpenListFixture(t, newFakeDAV(t))
	backup.client = &http.Client{Transport: protocolRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Error("rollback consulted the read-only fallback")
		return nil, errors.New("fallback must not be called")
	})}
	original := davEntry{Item: Item{ID: "old", FID: "old-version", Name: "target.txt", Size: 3}}
	base.entries[original.ID] = original
	base.parents[original.ID] = "0"
	base.contents[original.ID] = []byte("old")
	if err := base.Move(context.Background(), original, "", "CloudProbe-backup-test.txt"); err != nil {
		t.Fatal(err)
	}
	mount := &davMount{Alias: "test", Root: "0", Writable: true, drive: &backupDAVDrive{primary: &backupFaultDrive{fakeDAVDrive: base, mode: "unreadable", backedUp: true}, backup: backup}}
	state := &davRequestState{backups: []davBackup{{mount: mount, entry: original, parent: "0", target: "/test/target.txt", backupName: "CloudProbe-backup-test.txt"}}}
	fs := &davFS{mounts: []*davMount{mount}}
	err := fs.finishBackups(context.WithValue(context.Background(), davRequestKey{}, state), state, false)
	if err == nil || !strings.Contains(err.Error(), "CloudProbe-backup-test.txt") {
		t.Fatalf("missing actionable recovery error: %v", err)
	}
	entries, _ := base.List(context.Background(), "0")
	if len(entries) != 1 || entries[0].Name != "CloudProbe-backup-test.txt" {
		t.Fatalf("uncertain backup was mutated: %+v", entries)
	}
}

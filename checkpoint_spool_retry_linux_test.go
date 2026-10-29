package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSpoolRetryLinuxPermissionDeniedRetainsAssociation(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires an unprivileged process; enforced by Linux CI")
	}
	for _, action := range []string{"reset", "resume"} {
		t.Run(action, func(t *testing.T) {
			d, g, c, old := spoolRetryFixture(t)
			if err := os.Chmod(d.fs.spool, 0500); err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(d.fs.spool, 0700)
			// The resume subdirectory remains writable, isolating body deletion
			// failure from checkpoint replacement and lease acquisition.
			err := assertSpoolRetryBlocked(t, d, c)
			if !errors.Is(err, os.ErrPermission) {
				t.Errorf("expected real permission error, got %v", err)
			}
			if err = os.Chmod(d.fs.spool, 0700); err != nil {
				t.Fatal(err)
			}
			if action == "resume" {
				next, release, err := d.openCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				release()
				if next.data.Spool != filepath.Base(d.tmp.Name()) {
					t.Fatal("new body not attached")
				}
				if _, err = os.Stat(old); !os.IsNotExist(err) {
					t.Fatal("old body survived resume", err)
				}
				d.tmp.Close()
			}
			if w := lifecycleRequest(g, "reset", c.data.Key); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if _, err = os.Stat(old); !os.IsNotExist(err) {
				t.Fatal("original body orphaned", err)
			}
			if _, err = os.Stat(c.file); !os.IsNotExist(err) {
				t.Fatal("record remained", err)
			}
		})
	}
}

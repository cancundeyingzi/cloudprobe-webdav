package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSpoolRetryWindowsLockedBodyRetainsAssociation(t *testing.T) {
	for _, action := range []string{"reset", "resume"} {
		t.Run(action, func(t *testing.T) {
			d, g, c, old := spoolRetryFixture(t)
			held, err := os.Open(old)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			assertSpoolRetryBlocked(t, d, c)
			held.Close()
			if action == "resume" {
				next, release, err := d.openCheckpoint()
				if err != nil {
					t.Fatal(err)
				}
				release()
				if next.data.Spool != filepath.Base(d.tmp.Name()) {
					t.Fatal("resume did not attach new body")
				}
				if _, err = os.Stat(old); !os.IsNotExist(err) {
					t.Fatal("resume did not reclaim old body", err)
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

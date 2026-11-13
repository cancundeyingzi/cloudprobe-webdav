package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type quarkMoveFixture struct {
	files          *quarkFixture
	before         func(string, map[string]any) int
	after          func(string, map[string]any) int
	renames, moves int
}

func newQuarkMoveFixture(t *testing.T) (*quarkMoveFixture, *quarkDAVDrive) {
	t.Helper()
	f := &quarkMoveFixture{files: &quarkFixture{files: map[string]quarkMockFile{}, uploads: map[string]*quarkMockUpload{}, apiCalls: map[string]int{}, partCalls: map[int]int{}}}
	q := newQuarkClient(quarkCredentials{UID: "test", AccessToken: "fixture-token", OpenVerified: true}, nil)
	q.endpoints[quarkOpen] = "https://fixture.invalid/open"
	q.metadata = &http.Client{Transport: protocolRoundTripper(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var input map[string]any
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		action := ""
		if strings.HasSuffix(r.URL.Path, "/file/rename") {
			action = "rename"
			f.renames++
		}
		if strings.HasSuffix(r.URL.Path, "/file/move") {
			action = "move"
			f.moves++
		}
		reject := func(code int) (*http.Response, error) {
			respondJSON(w, code, map[string]any{"code": code, "message": "fixture failure"})
			return w.Result(), nil
		}
		if f.before != nil {
			if code := f.before(action, input); code != 0 {
				return reject(code)
			}
		}
		if action != "" {
			var id string
			if action == "rename" {
				if input["conflict_mode"] != "FAIL" {
					t.Fatal("rename does not reject conflicts")
				}
				id = valueString(input["fid"])
			} else {
				id = valueString(input["fid_list"].([]any)[0])
			}
			current := f.files.files[id]
			parent, name := current.Parent, valueString(input["file_name"])
			if action == "move" {
				parent, name = valueString(input["to_pdir_fid"]), current.Name
			}
			for _, other := range f.files.files {
				if other.ID != id && other.Parent == parent && other.Name == name {
					return reject(http.StatusConflict)
				}
			}
		}
		f.files.serve(w, r)
		if f.after != nil {
			if code := f.after(action, input); code != 0 {
				w = httptest.NewRecorder()
				return reject(code)
			}
		}
		return w.Result(), nil
	})}
	d := newQuarkDrive(q, nil, 1, "auto")
	d.retryWait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return f, d
}

func quarkMoveEntry(file quarkMockFile) davEntry {
	return davEntry{Item: Item{ID: file.ID, Name: file.Name, Size: int64(len(file.Content)), Directory: file.Directory}, Parent: file.Parent}
}

func TestQuarkCrossDirectoryRenameAvoidsIntermediateCollisions(t *testing.T) {
	for _, test := range []struct {
		name                                        string
		sourceCollision, targetCollision, directory bool
	}{
		{"clear", false, false, false},
		{"source_name", true, false, false},
		{"target_old_name", false, true, false},
		{"both_names", true, true, false},
		{"directory", true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, d := newQuarkMoveFixture(t)
			source := f.files.add("A", "old.txt", []byte("move me"), test.directory)
			var preserved []quarkMockFile
			if test.sourceCollision {
				preserved = append(preserved, f.files.add("A", "new.txt", []byte("source neighbour"), false))
			}
			if test.targetCollision {
				preserved = append(preserved, f.files.add("B", "old.txt", []byte("target neighbour"), false))
			}
			if err := d.Move(context.Background(), quarkMoveEntry(source), "B", "new.txt"); err != nil {
				t.Fatal(err)
			}
			got := f.files.files[source.ID]
			if got.Parent != "B" || got.Name != "new.txt" || !bytes.Equal(got.Content, source.Content) || got.Directory != source.Directory {
				t.Fatal("move changed object contents or destination", got)
			}
			for _, old := range preserved {
				got := f.files.files[old.ID]
				if got.Parent != old.Parent || got.Name != old.Name || !bytes.Equal(got.Content, old.Content) {
					t.Fatal("unrelated object changed", got)
				}
			}
		})
	}
}

func TestQuarkCrossDirectoryRenameRejectsOccupiedDestination(t *testing.T) {
	f, d := newQuarkMoveFixture(t)
	source := f.files.add("A", "old.txt", []byte("original"), false)
	target := f.files.add("B", "new.txt", []byte("keep"), false)
	err := d.Move(context.Background(), quarkMoveEntry(source), "B", "new.txt")
	if !errors.Is(err, os.ErrExist) || f.renames != 0 || f.moves != 0 {
		t.Fatalf("occupied target dispatched mutations: %v", err)
	}
	if f.files.files[source.ID].Name != source.Name || !bytes.Equal(f.files.files[target.ID].Content, target.Content) {
		t.Fatal("preflight changed a file")
	}
}

func TestQuarkCrossDirectoryRenameRestoresOnlyRejectedSteps(t *testing.T) {
	for _, step := range []string{"move", "final rename", "uncertain move", "occupied source"} {
		t.Run(step, func(t *testing.T) {
			f, d := newQuarkMoveFixture(t)
			source := f.files.add("A", "old.txt", []byte("original"), false)
			var neighbour quarkMockFile
			f.before = func(action string, input map[string]any) int {
				if step == "uncertain move" && action == "move" {
					return http.StatusInternalServerError
				}
				if step == "move" && action == "move" {
					return http.StatusUnauthorized
				}
				if (step == "final rename" || step == "occupied source") && action == "rename" && input["file_name"] == "new.txt" {
					if step == "occupied source" {
						neighbour = f.files.add("A", "old.txt", []byte("external"), false)
					}
					return http.StatusUnauthorized
				}
				return 0
			}
			err := d.Move(context.Background(), quarkMoveEntry(source), "B", "new.txt")
			var partial *davMutationError
			if !errors.As(err, &partial) || !partial.Partial {
				t.Fatal("staged failure not reported", err)
			}
			got := f.files.files[source.ID]
			if step == "move" || step == "final rename" {
				if got.Parent != "A" || got.Name != "old.txt" {
					t.Fatal("rejected step did not restore original", got, err)
				}
			} else {
				if !strings.HasPrefix(got.Name, ".cloudprobe-move-") || f.moves != 1 {
					t.Fatal("uncertain or conflicting state was replayed", got, f.moves)
				}
				if step == "occupied source" && !bytes.Equal(f.files.files[neighbour.ID].Content, []byte("external")) {
					t.Fatal("rollback overwrote external object")
				}
			}
		})
	}
}

func TestQuarkCrossDirectoryRenameReconcilesLostAcknowledgements(t *testing.T) {
	for _, step := range []string{"stage", "move", "final"} {
		t.Run(step, func(t *testing.T) {
			f, d := newQuarkMoveFixture(t)
			source := f.files.add("A", "old.txt", []byte("original"), false)
			f.after = func(action string, input map[string]any) int {
				if step == "move" && action == "move" || step == "stage" && action == "rename" && input["file_name"] != "new.txt" || step == "final" && action == "rename" && input["file_name"] == "new.txt" {
					return http.StatusInternalServerError
				}
				return 0
			}
			if err := d.Move(context.Background(), quarkMoveEntry(source), "B", "new.txt"); err != nil {
				t.Fatal(err)
			}
			got := f.files.files[source.ID]
			if got.Parent != "B" || got.Name != "new.txt" || f.moves != 1 || f.renames != 2 {
				t.Fatal("lost acknowledgement caused replay", got, f.moves, f.renames)
			}
		})
	}
}

func TestQuarkCrossDirectoryRenameCancellationStopsRequestedMove(t *testing.T) {
	f, d := newQuarkMoveFixture(t)
	source := f.files.add("A", "old.txt", []byte("original"), false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.after = func(action string, _ map[string]any) int {
		if action == "rename" {
			cancel()
		}
		return 0
	}
	err := d.Move(ctx, quarkMoveEntry(source), "B", "new.txt")
	if err == nil || f.moves != 0 || f.renames > 2 {
		t.Fatal("canceled staging continued mutation", err, f.moves, f.renames)
	}
	if !bytes.Equal(f.files.files[source.ID].Content, source.Content) {
		t.Fatal("cancel changed contents")
	}
}

func TestDAVMoveVerificationRejectsDuplicateDestinationNames(t *testing.T) {
	expected := davEntry{Item: Item{ID: "expected", Name: "target.txt", Size: 3}}
	other := davEntry{Item: Item{ID: "other", Name: expected.Name, Size: 3}}
	drive := &guiLifecycleDrive{list: func(context.Context, string) ([]davEntry, error) {
		return []davEntry{expected, other}, nil
	}}
	fs := &davFS{}
	if err := fs.verify(context.Background(), &davMount{Alias: "drive", drive: drive}, "parent", expected.Name, -1, expected); err == nil {
		t.Fatal("duplicate destination was accepted because expected ID appeared first")
	}
}

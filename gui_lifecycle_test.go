package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type guiLifecycleDrive struct {
	davDrive
	list func(context.Context, string) ([]davEntry, error)
	url  string
}

func (d *guiLifecycleDrive) List(ctx context.Context, parent string) ([]davEntry, error) {
	return d.list(ctx, parent)
}
func (d *guiLifecycleDrive) URL(context.Context, davEntry) (string, error) { return d.url, nil }

func guiLifecycleFixture(t *testing.T, drive davDrive) (*guiApp, *httptest.Server, *atomic.Int64) {
	t.Helper()
	account := guiAccount{Key: "a", Provider: "quark", Root: "0", credentials: map[string]any{"user_id": "fixture"}}
	var shutdowns atomic.Int64
	app := &guiApp{data: t.TempDir(), nonce: "fixture-nonce", accounts: map[string]guiAccount{"a": account}, downloads: map[string]guiDownload{}, shutdown: func() { shutdowns.Add(1) }}
	app.dav = &davManager{app: app, server: &http.Server{}, mounts: []*davMount{{Alias: "fixture", Identity: davIdentity(account), drive: drive}}}
	server := httptest.NewServer(app)
	app.host = strings.TrimPrefix(server.URL, "http://")
	t.Cleanup(server.Close)
	t.Cleanup(app.stopRequests)
	return app, server, &shutdowns
}

func guiLifecycleRequest(ctx context.Context, server *httptest.Server, path string, body any) (int, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/api/"+path, strings.NewReader(string(data)))
	if err != nil {
		return 0, err
	}
	request.Header.Set("X-CloudProbe-UI", "fixture-nonce")
	response, err := server.Client().Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	return response.StatusCode, err
}

func TestGUIQuitCancelsSlowQueriesAndDownloads(t *testing.T) {
	for _, operation := range []string{"list", "fetch"} {
		t.Run(operation, func(t *testing.T) {
			entered, canceled := make(chan struct{}), make(chan struct{})
			entry := davEntry{Item: Item{ID: "file", FID: "version-1", Name: "f.bin", Size: 3}}
			drive := &guiLifecycleDrive{}
			if operation == "list" {
				drive.list = func(ctx context.Context, _ string) ([]davEntry, error) {
					close(entered)
					<-ctx.Done()
					close(canceled)
					return nil, ctx.Err()
				}
			} else {
				drive.list = func(context.Context, string) ([]davEntry, error) { return []davEntry{entry}, nil }
				body := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Length", "3")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					close(entered)
					<-r.Context().Done()
					close(canceled)
				}))
				defer body.Close()
				drive.url = body.URL
			}
			app, server, shutdowns := guiLifecycleFixture(t, drive)
			input := map[string]any{"account": "a", "directory": "0"}
			if operation == "fetch" {
				snapshot, err := app.storeBrowseSnapshot("a", "0", []Item{entry.Item}, []davEntry{entry})
				if err != nil {
					t.Fatal(err)
				}
				input["snapshot"], input["id"], input["name"] = snapshot, entry.ID, entry.Name
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan int, 1)
			go func() { code, _ := guiLifecycleRequest(ctx, server, operation, input); done <- code }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("cloud operation was not reached")
			}
			quitCtx, cancelQuit := context.WithTimeout(context.Background(), time.Second)
			defer cancelQuit()
			for index := 0; index < 2; index++ {
				code, err := guiLifecycleRequest(quitCtx, server, "quit", map[string]any{})
				if err != nil || code != http.StatusOK {
					t.Fatalf("quit returned %d %v", code, err)
				}
			}
			select {
			case <-canceled:
			case <-ctx.Done():
				t.Fatal("quit did not cancel cloud I/O")
			}
			select {
			case code := <-done:
				if code == http.StatusOK {
					t.Fatal("canceled operation reported success")
				}
			case <-ctx.Done():
				t.Fatal("canceled request did not finish")
			}
			app.mu.Lock()
			published := app.lastResult != "" || len(app.downloads) != 0
			app.mu.Unlock()
			if published || shutdowns.Load() != 1 {
				t.Fatalf("published=%v shutdown calls=%d", published, shutdowns.Load())
			}
		})
	}
}

func TestGUICanceledQueryCannotPublishLateProviderSuccess(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	drive := &guiLifecycleDrive{list: func(ctx context.Context, _ string) ([]davEntry, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return []davEntry{}, nil
	}}
	app, server, _ := guiLifecycleFixture(t, drive)
	done := make(chan struct{})
	go func() {
		defer close(done)
		guiLifecycleRequest(context.Background(), server, "list", map[string]any{"account": "a", "directory": "0"})
	}()
	<-entered
	app.stopRequests()
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("late query did not finish")
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.lastResult != "" || len(app.browseSnapshots) != 0 {
		t.Fatal("canceled query published its late result")
	}
}

func TestGUIControlLockWaitCanBeCanceled(t *testing.T) {
	app := &guiApp{}
	app.op.Lock()
	defer app.op.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		done <- app.lockOperation(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/accounts", nil).WithContext(ctx))
	}()
	cancel()
	select {
	case acquired := <-done:
		if acquired {
			t.Fatal("canceled control operation acquired the lock")
		}
	case <-time.After(time.Second):
		t.Fatal(fmt.Errorf("canceled control operation stayed in the queue"))
	}
}

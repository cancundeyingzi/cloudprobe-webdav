package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEngineDiagnosticClassifiesActionableFailures(t *testing.T) {
	for _, tc := range []struct{ code, message string }{
		{"component_load", "DllNotFoundException: missing-native.dll"},
		{"component_load", "BadImageFormatException: wrong architecture"},
		{"initialization", "InvalidOperationException: 原版引擎初始化失败"},
		{"process_exit", "unknown native failure"},
	} {
		d := describeEngineError(errors.New(tc.message))
		if d.Code != tc.code || len(d.Steps) == 0 || !strings.Contains(d.Detail, tc.message) {
			t.Fatalf("missing useful diagnosis: %+v", d)
		}
	}
	for _, code := range []string{"component_missing", "runtime_missing", "runtime_version", "runtime_architecture", "runtime_probe", "data_unwritable", "timeout", "unsupported_platform"} {
		d := describeEngineError(engineProblem(code, "reason", "details"))
		if d.Code != code || d.Reason != "reason" || d.Detail != "details" || len(d.Steps) == 0 {
			t.Fatalf("typed diagnosis lost: %+v", d)
		}
	}
}

func TestEngineDiagnosticsCaptureRealExitWithoutLeakingCredentials(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestEngineDiagnosticChild$")
	cmd.Env = append(os.Environ(), "CLOUDPROBE_ENGINE_DIAGNOSTIC_CHILD=1")
	tail := &engineErrorTail{}
	cmd.Stderr = tail
	err := cmd.Run()
	if err == nil {
		t.Fatal("child did not fail")
	}
	bridge := &OriginalBridge{cmd: cmd, lastErr: err, stderr: tail, done: make(chan struct{}), exited: true, exitCode: cmd.ProcessState.ExitCode()}
	close(bridge.done)
	g := &guiApp{bridge: bridge, extraSecrets: map[string]struct{}{"private-unlabelled-value": {}}, enginePaths: enginePaths{SDK: "example/sdk", Bridge: "example/bridge/CloudProbe.Bridge.dll", Data: "example/data"}}
	status := g.publicStatus()
	d, ok := status["engine_diagnostic"].(*engineFailure)
	if !ok || status["engine_ready"] != false {
		t.Fatal("runtime exit not surfaced", status)
	}
	if !strings.Contains(d.Detail, "退出码：7") || !strings.Contains(d.Detail, "missing-native.dll") || d.Paths.SDK != "example/sdk" {
		t.Fatalf("lost exit detail: %+v", d)
	}
	b, _ := json.Marshal(status)
	for _, secret := range []string{"private-unlabelled-value", "never-show-this-token"} {
		if strings.Contains(string(b), secret) {
			t.Fatal("diagnostics leaked a credential")
		}
	}
	response := httptest.NewRecorder()
	g.fail(response, bridge.exitError())
	if strings.Contains(response.Body.String(), "private-unlabelled-value") || strings.Contains(response.Body.String(), "never-show-this-token") {
		t.Fatal("API failure response leaked diagnostic credentials")
	}
}

func TestEngineDiagnosticChild(t *testing.T) {
	if os.Getenv("CLOUDPROBE_ENGINE_DIAGNOSTIC_CHILD") != "1" {
		return
	}
	if os.Getenv("CLOUDPROBE_ENGINE_DIAGNOSTIC_CHILD_WAIT") == "1" {
		_, _ = io.Copy(io.Discard, os.Stdin)
	}
	fmt.Fprintln(os.Stderr, "DllNotFoundException: missing-native.dll\naccess_token=never-show-this-token\nprivate-unlabelled-value")
	os.Exit(7)
}

func TestEngineDiagnosticsAreConcurrentWithProcessExit(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestEngineDiagnosticChild$")
	cmd.Env = append(os.Environ(), "CLOUDPROBE_ENGINE_DIAGNOSTIC_CHILD=1", "CLOUDPROBE_ENGINE_DIAGNOSTIC_CHILD_WAIT=1")
	hideChildWindow(cmd)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	b := &OriginalBridge{cmd: cmd, done: make(chan struct{}), stopping: make(chan struct{})}
	close(b.stopping)
	go b.waitProcess()
	if b.Alive() || !strings.Contains(b.exitError().Error(), "尚未取得") {
		t.Fatal("stopping process was reported as already reaped")
	}
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-b.done:
					return
				default:
					_ = b.exitError()
				}
			}
		})
	}
	_ = in.Close()
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not exit")
	}
	readers.Wait()
	if !strings.Contains(b.exitError().Error(), "退出码：7") {
		t.Fatal("published exit code was lost")
	}
}

func TestEngineErrorTailIsBoundedAndConcurrent(t *testing.T) {
	tail := &engineErrorTail{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = tail.Write([]byte(strings.Repeat("x", 1024)))
				_ = tail.Text()
			}
		}()
	}
	wg.Wait()
	_, _ = tail.Write([]byte("last error"))
	got := tail.Text()
	if len(got) > engineDetailLimit || !strings.HasSuffix(got, "last error") {
		t.Fatal("stderr tail was lost or unbounded")
	}
}

func TestEngineFailureRemainsVisibleWithIndependentAccount(t *testing.T) {
	g := &guiApp{accounts: map[string]guiAccount{"q": {Key: "q", Provider: "quark", Name: "夸克"}}, accountOrder: []string{"q"}}
	g.recordEngineFailure(engineProblem("component_missing", "缺少 ntkernel.dll", "C:/example/sdk/ntkernel.dll"))
	for i := 0; i < 3; i++ {
		s := g.publicStatus()
		if s["engine_error"] != "缺少 ntkernel.dll" || s["has_accounts"] != true || s["engine_diagnostic"] == nil {
			t.Fatal("failure or independent account disappeared", s)
		}
	}
}

func TestEnginePreflightExplainsMissingComponentAndRuntime(t *testing.T) {
	if runtime.GOOS != "windows" || runtime.GOARCH != "amd64" {
		t.Skip("Windows SDK preflight")
	}
	sdk := t.TempDir()
	bridge := filepath.Join(sdk, "CloudProbe.Bridge.dll")
	_, err := startOriginalBridge(context.Background(), sdk, bridge, filepath.Join(sdk, "data"))
	d := describeEngineError(err)
	if d.Code != "component_missing" || !strings.Contains(d.Detail, filepath.Join(sdk, "ntkernel.dll")) {
		t.Fatalf("missing component path not reported: %+v", d)
	}
	for _, name := range []string{"ntkernel.dll", "BaoMiHua.dll", "QRCoder.dll", "CloudProbe.Bridge.dll", "CloudProbe.Bridge.runtimeconfig.json"} {
		if err := os.WriteFile(filepath.Join(sdk, name), []byte("placeholder"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", t.TempDir())
	_, err = startOriginalBridge(context.Background(), sdk, bridge, filepath.Join(sdk, "data"))
	d = describeEngineError(err)
	if d.Code != "runtime_missing" {
		t.Fatalf("runtime failure hidden: %+v", d)
	}
}

func TestEngineRuntimeVersionRequiresNet8(t *testing.T) {
	if runtime8Pattern.MatchString("Microsoft.NETCore.App 10.0.1 [C:/dotnet]\nMicrosoft.NETCore.App 7.0.7 [C:/dotnet]") {
		t.Fatal("accepted another major version")
	}
	if !runtime8Pattern.MatchString("Microsoft.NETCore.App 8.0.33 [C:/dotnet]\n") {
		t.Fatal("rejected NET 8")
	}
}

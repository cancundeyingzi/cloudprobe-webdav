package main

import (
	"context"
	"debug/pe"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

const engineDetailLimit = 8192

type enginePaths struct {
	Executable string `json:"executable"`
	SDK        string `json:"sdk"`
	Bridge     string `json:"bridge"`
	Data       string `json:"data"`
}

type engineFailure struct {
	Code   string       `json:"code"`
	Title  string       `json:"title"`
	Reason string       `json:"reason"`
	Steps  []string     `json:"steps"`
	Detail string       `json:"detail,omitempty"`
	Paths  *enginePaths `json:"paths,omitempty"`
}

func (e *engineFailure) Error() string {
	if e.Detail == "" {
		return e.Reason
	}
	return e.Reason + "\n" + e.Detail
}

func engineProblem(code, reason, detail string) *engineFailure {
	e := &engineFailure{Code: code, Title: "网易引擎不可用", Reason: reason, Detail: detail}
	switch code {
	case "component_missing", "component_unreadable":
		e.Title = "运行组件缺失或无法读取"
		e.Steps = []string{"将完整 Windows ZIP 解压到同一目录，保留 sdk 和 bridge 文件夹，不要只复制 EXE。", "按下方实际路径检查文件是否存在；若被安全软件隔离，先核实文件来源，再恢复或重新解压。"}
	case "runtime_missing", "runtime_version", "runtime_architecture":
		e.Title = ".NET 8 x64 运行环境不完整"
		e.Steps = []string{"安装 Microsoft .NET 8 的 Windows x64 运行时；只有 .NET 6、7、10 或 x86 版本不能替代。", "安装后关闭本程序并重新启动；若存在多套 dotnet，检查下方实际使用的 dotnet 路径。"}
	case "runtime_probe", "process_start":
		e.Title = "无法启动桥接运行环境"
		e.Steps = []string{"检查下方 dotnet 与桥接路径是否可访问，以及安全软件是否阻止运行。", "关闭程序后重新启动；若仍失败，复制诊断信息以进一步定位。"}
	case "component_load":
		e.Title = "运行组件加载失败"
		e.Steps = []string{"重新完整解压 Windows x64 包，避免混用其他版本的 SDK 或 Bridge。", "查看具体缺失或不兼容的 DLL 名称；确认使用 .NET 8 x64，且组件没有被安全软件隔离。"}
	case "data_unwritable":
		e.Title = "引擎资料目录无法写入"
		e.Steps = []string{"将整个软件目录移动到当前用户有写入权限的位置，再启动。", "保留软件目录中的 data；程序不会改存 AppData，也不需要删除账号资料。"}
	case "initialization":
		e.Title = "原版引擎初始化失败"
		e.Steps = []string{"先正常退出其他 CloudProbe 实例和网易爆米花，再启动本程序。", "若仍失败，查看下方引擎返回内容并复制诊断信息；不要删除 data 中的账号资料。"}
	case "timeout":
		e.Title = "原版引擎启动超时"
		e.Steps = []string{"关闭其他 CloudProbe 实例和网易爆米花，检查安全软件是否拦截组件，再重新启动。", "若再次超时，复制诊断信息；目前尚不能确定是组件阻塞还是其他原因。"}
	case "unsupported_platform":
		e.Title = "当前平台不支持原版引擎"
		e.Steps = []string{"爆米花原版 SDK 需要 Windows x64，请使用对应的完整 Windows 包。"}
	default:
		e.Title = "桥接进程已退出"
		e.Steps = []string{"查看退出信息和底层错误，关闭程序后重新启动。", "若再次发生，复制诊断信息并查看日志；原因尚未确认，不要反复删除账号资料。"}
	}
	return e
}

func describeEngineError(err error) *engineFailure {
	if err == nil {
		return nil
	}
	var known *engineFailure
	if errors.As(err, &known) {
		copy := *known
		return &copy
	}
	text := err.Error()
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "badimageformatexception"), strings.Contains(lower, "dllnotfoundexception"), strings.Contains(lower, "could not load file or assembly"), strings.Contains(lower, "filenotfoundexception"):
		return engineProblem("component_load", "桥接返回了组件加载错误。", text)
	case strings.Contains(text, "初始化失败"):
		return engineProblem("initialization", "原版 SDK 未能完成初始化；具体原因见引擎返回内容。", text)
	default:
		return engineProblem("process_exit", "桥接未能保持运行，原因尚未确认。", text)
	}
}

// Capture a bounded tail even if a broken child continuously writes stderr.
type engineErrorTail struct {
	mu   sync.Mutex
	data []byte
}

func (b *engineErrorTail) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if len(p) >= engineDetailLimit {
		b.data = append(b.data[:0], p[len(p)-engineDetailLimit:]...)
		return n, nil
	}
	if overflow := len(b.data) + len(p) - engineDetailLimit; overflow > 0 {
		b.data = b.data[overflow:]
	}
	b.data = append(b.data, p...)
	return n, nil
}
func (b *engineErrorTail) Text() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.ToValidUTF8(string(b.data), "�")
}

var runtime8Pattern = regexp.MustCompile(`(?m)^Microsoft\.NETCore\.App 8\.[0-9]+\.[0-9]+\s`)
var engineSensitiveLine = regexp.MustCompile(`(?im)^.*(?:token|secret|password|authorization|cookie|app[_-]?key)\s*["']?\s*[:=].*$`)

func redactEngineDetail(text string, redact func(string) string) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.Map(func(r rune) rune {
		if r < 32 && r != '\n' && r != '\t' && r != '\r' {
			return -1
		}
		return r
	}, text)
	text = redact(text)
	text = engineSensitiveLine.ReplaceAllString(text, "[含凭据的诊断行已隐藏]")
	if len(text) > engineDetailLimit {
		text = strings.ToValidUTF8(text[len(text)-engineDetailLimit:], "�")
	}
	return strings.TrimSpace(text)
}

func checkEngineRuntime(ctx context.Context, dotnet string) error {
	if runtime.GOOS == "windows" {
		binary, err := pe.Open(dotnet)
		if err != nil {
			return engineProblem("runtime_probe", "无法检查 dotnet 程序。", dotnet+"\n"+err.Error())
		}
		machine := binary.Machine
		binary.Close()
		if machine != pe.IMAGE_FILE_MACHINE_AMD64 {
			return engineProblem("runtime_architecture", "当前找到的 dotnet 不是 Windows x64 程序。", dotnet)
		}
	}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, dotnet, "--list-runtimes")
	cmd.WaitDelay = time.Second
	hideChildWindow(cmd)
	var output engineErrorTail
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		return engineProblem("runtime_probe", "dotnet 运行环境检查失败。", fmt.Sprintf("%s\n%v\n%s", dotnet, err, output.Text()))
	}
	if !runtime8Pattern.MatchString(output.Text()) {
		return engineProblem("runtime_version", "当前 dotnet 未找到 Microsoft.NETCore.App 8.x 运行时。", dotnet+"\n已安装运行时：\n"+output.Text())
	}
	return nil
}

func (g *guiApp) safeEngineDiagnostic(err error, paths enginePaths) *engineFailure {
	d := describeEngineError(err)
	if d == nil {
		return nil
	}
	d.Reason = redactEngineDetail(d.Reason, g.redactDAV)
	d.Detail = redactEngineDetail(d.Detail, g.redactDAV)
	paths.Executable = g.redactDAV(paths.Executable)
	paths.SDK = g.redactDAV(paths.SDK)
	paths.Bridge = g.redactDAV(paths.Bridge)
	paths.Data = g.redactDAV(paths.Data)
	d.Paths = &paths
	return d
}

func (g *guiApp) recordEngineFailure(err error) {
	g.mu.Lock()
	paths := g.enginePaths
	g.mu.Unlock()
	d := g.safeEngineDiagnostic(err, paths)
	g.mu.Lock()
	g.engineFailure = d
	g.engineError = d.Reason
	g.stage = "引擎启动失败"
	g.mu.Unlock()
	g.event(d.Title + "：" + d.Reason)
	g.runLog.add(runLogEntry{Operation: "启动原版引擎", Source: "本机", Result: "失败", Error: d.Reason, Detail: d.Detail})
}

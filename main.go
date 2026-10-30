package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

const version = "1.12.5"

type namesFlag []string

func (n *namesFlag) String() string     { return strings.Join(*n, ", ") }
func (n *namesFlag) Set(v string) error { *n = append(*n, v); return nil }
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	code, e := run(ctx, os.Args[1:], os.Stdout)
	if e != nil {
		fmt.Fprintln(os.Stderr, "错误：", e)
	}
	os.Exit(code)
}
func flags(name string, w io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(w)
	return f
}
func parseFlags(f *flag.FlagSet, args []string) error {
	if e := f.Parse(args); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("不识别的位置参数：%q；所有参数请使用 -名称 值", f.Args())
	}
	return nil
}
func run(ctx context.Context, args []string, w io.Writer) (int, error) {
	if len(args) == 0 {
		return guiCommand(ctx, nil, w)
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprint(w, help)
		return 0, nil
	}
	switch args[0] {
	case "gui":
		return guiCommand(ctx, args[1:], w)
	case "version":
		fmt.Fprintln(w, "CloudProbe", version)
		return 0, nil
	case "init":
		return initCommand(args[1:], w)
	case "list", "fetch", "doctor", "auth-probe":
		return onlineCommand(ctx, args[0], args[1:], w)
	case "filter":
		return filterCommand(args[1:], w)
	case "demo":
		return demoCommand(ctx, args[1:], w)
	default:
		return 2, fmt.Errorf("未知命令 %q；使用 cloudprobe help", args[0])
	}
}
func initCommand(args []string, w io.Writer) (int, error) {
	f := flags("init", w)
	p := f.String("provider", "mobile", "mobile 或 wopan")
	out := f.String("out", defaultLocalPath("config.mobile.json"), "写入配置文件，拒绝覆盖")
	transport := f.String("transport", "", "direct、netease 或 gateway")
	dll := f.String("native-dll", "", "可选：已分析版本的 ntkernel.dll 路径")
	if e := parseFlags(f, args); e != nil {
		return 2, e
	}
	c := defaultConfig(*p)
	if *transport != "" {
		c.Transport = *transport
		if *transport == "netease" {
			c.BaseURL = "https://api.filmly.netease.com"
		}
	}
	if *dll != "" {
		c.Netease.NativeDLL, _ = filepath.Abs(*dll)
	}
	if e := c.validate(); e != nil {
		return 2, e
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	if e := os.MkdirAll(filepath.Dir(*out), 0700); e != nil {
		return 1, e
	}
	if e := writeNew(*out, b); e != nil {
		return 1, e
	}
	fmt.Fprintf(w, "已创建 %s。填写授权字段后执行 doctor，再运行 list。\n", *out)
	return 0, nil
}
func onlineCommand(ctx context.Context, command string, args []string, w io.Writer) (int, error) {
	f := flags(command, w)
	config := f.String("config", defaultLocalPath("config.mobile.json"), "配置文件")
	dir := f.String("dir", "root", "查询目录 ID，不递归")
	out := f.String("out", defaultLocalPath("results"), "结果根目录，每次新建子目录")
	id := f.String("id", "", "文件 ID / contentID")
	fid := f.String("fid", "", "联通下载 fid，可省略并用 id 查询")
	name := f.String("name", "download.bin", "下载保存文件名")
	var expected namesFlag
	f.Var(&expected, "expect", "预期文件名，可重复指定")
	if e := parseFlags(f, args); e != nil {
		return 2, e
	}
	c, e := loadConfig(*config)
	if e != nil {
		return 2, e
	}
	folder, e := runFolder(*out)
	if e != nil {
		return 1, e
	}
	cl, e := newClient(c, folder)
	if e != nil {
		return 2, e
	}
	sanitize := func(e error) error {
		if e == nil {
			return nil
		}
		return errors.New(cl.redact.Text(e.Error()))
	}
	fmt.Fprintf(w, "结果目录：%s\n", folder)
	if c.Transport == "netease" {
		fmt.Fprintln(w, "网易兼容模式：已恢复路由/解密，但实网签名仍返回 10012；本模式当前仅用于诊断，不能作为联通实网已打通的证明。")
	}
	if command == "doctor" {
		if e := c.credentialsReady(); e != nil {
			fmt.Fprintln(w, "配置结构通过，但授权字段尚未就绪。未发网络请求。")
			return 2, e
		}
		fmt.Fprintf(w, "配置可解析；provider=%s，transport=%s；TLS 校验开启。\n", c.Provider, c.Transport)
		if c.Transport == "netease" {
			fmt.Fprintln(w, "网易加解密配置可加载；签名实现尚未通过实网校验。")
		}
		fmt.Fprintln(w, "此命令未发网络请求。请确认令牌、应用身份、设备标识已经填写。")
		return 0, nil
	}
	if command == "auth-probe" {
		if c.Transport != "netease" {
			return 2, errors.New("auth-probe 仅用于 netease 传输")
		}
		e = cl.ensureSID(ctx)
		if e != nil {
			return 1, sanitize(e)
		}
		fmt.Fprintln(w, "网易匿名会话握手成功：服务端已返回 sid。未验证账号登录或网盘权限；sid 不显示、不导出。")
		return 0, nil
	}
	if command == "fetch" {
		if e := c.credentialsReady(); e != nil {
			return 2, e
		}
		r, e := cl.Fetch(ctx, *id, *fid, *name, "live")
		if e != nil {
			return 1, sanitize(e)
		}
		b, _ := json.MarshalIndent(r, "", "  ")
		if e = writeNew(filepath.Join(folder, "download-receipt.json"), b); e != nil {
			return 1, e
		}
		fmt.Fprintf(w, "已保存指定文件 %s，%d 字节，SHA-256 %s\n", *name, r.Bytes, r.SHA256)
		return 0, nil
	}
	if strings.TrimSpace(*dir) == "" {
		return 2, errors.New("目录 ID 不能为空")
	}
	if e := c.credentialsReady(); e != nil {
		return 2, e
	}
	r, e := cl.List(ctx, *dir, "live", expected)
	if se := saveReport(folder, r, cl.redact); se != nil {
		return 1, se
	}
	printReport(w, r)
	if e != nil {
		return 1, sanitize(e)
	}
	if !r.Complete {
		return 4, nil
	}
	for _, x := range r.Expected {
		if !x.Returned {
			return 3, nil
		}
	}
	return 0, nil
}
func filterCommand(args []string, w io.Writer) (int, error) {
	f := flags("filter", w)
	input := f.String("input", "", "目录响应 JSON/XML，或本工具 report.json")
	p := f.String("provider", "", "mobile 或 wopan；报告可自动识别")
	out := f.String("out", defaultLocalPath("results"), "输出根目录")
	var expected namesFlag
	f.Var(&expected, "expect", "预期文件名，可重复")
	if e := parseFlags(f, args); e != nil {
		return 2, e
	}
	if *input == "" {
		return 2, errors.New("filter 需要 -input")
	}
	file, e := os.Open(*input)
	if e != nil {
		return 1, e
	}
	defer file.Close()
	b, e := readBounded(file, 16<<20)
	if e != nil {
		return 1, e
	}
	var r Report
	var marker map[string]json.RawMessage
	_ = json.Unmarshal(b, &marker)
	if wrapped, ok := marker["response_logical_body"]; ok {
		var response string
		if e = json.Unmarshal(wrapped, &response); e != nil {
			return 1, errors.New("HTTP 记录的 response_logical_body 不是字符串")
		}
		b = []byte(response)
		marker = nil
		_ = json.Unmarshal(b, &marker)
	}
	if _, ok := marker["items"]; ok {
		if e = json.Unmarshal(b, &r); e != nil {
			return 1, e
		}
		if *p != "" {
			r.Provider = *p
		}
		if r.Provider != "mobile" && r.Provider != "wopan" {
			return 2, errors.New("报告 provider 无效")
		}
		for i, it := range r.Items {
			r.Items[i] = classify(r.Provider, it)
		}
		r.Mode = "offline"
		r.Expected = nil
	} else {
		if *p != "mobile" && *p != "wopan" {
			return 2, errors.New("原始响应需要 -provider mobile 或 wopan")
		}
		page, e := parsePage(*p, b)
		if e != nil {
			return 1, e
		}
		r = newReport(*p, "offline", "")
		r.Items = page.Items
		r.Pages = 1
		r.StopReason = "只导入单页响应，不推断整个目录已经查完"
	}
	applyExpected(&r, expected)
	folder, e := runFolder(*out)
	if e != nil {
		return 1, e
	}
	if e = saveReport(folder, r, &Redactor{}); e != nil {
		return 1, e
	}
	printReport(w, r)
	fmt.Fprintf(w, "离线结果：%s\n", folder)
	return 0, nil
}
func demoCommand(ctx context.Context, args []string, w io.Writer) (int, error) {
	f := flags("demo", w)
	out := f.String("out", defaultLocalPath("results-demo"), "演示结果根目录")
	if e := parseFlags(f, args); e != nil {
		return 2, e
	}
	srv := demoServer()
	defer srv.Close()
	fmt.Fprintln(w, "本地模拟演示：不连接真实网盘，不需要账号。")
	for _, provider := range []string{"mobile", "wopan"} {
		c := defaultConfig(provider)
		c.BaseURL = srv.URL
		c.PageSize = 2
		c.Transport = "gateway"
		if provider == "mobile" {
			c.Transport = "direct"
		}
		c.Mobile.AppID = "demo-app"
		c.Mobile.AppSecret = "demo-application-secret"
		c.Mobile.AccessToken = "demo-mobile-user-token"
		c.Mobile.DeviceID = "demo-device"
		c.Wopan.AccessToken = "demo-wopan-user-token"
		folder, e := runFolder(filepath.Join(*out, provider))
		if e != nil {
			return 1, e
		}
		cl, e := newClient(c, folder)
		if e != nil {
			return 1, e
		}
		r, e := cl.List(ctx, "root", "demo", []string{"说明.txt"})
		if e != nil {
			return 1, e
		}
		r.Notes = append(r.Notes, "此报告来自本机模拟服务，不是实网验证。")
		if e = saveReport(folder, r, cl.redact); e != nil {
			return 1, e
		}
		printReport(w, r)
		receipt, e := cl.Fetch(ctx, "text-1", "", "说明.txt", "demo")
		if e != nil {
			return 1, e
		}
		b, _ := json.MarshalIndent(receipt, "", "  ")
		if e = writeNew(filepath.Join(folder, "download-receipt.json"), b); e != nil {
			return 1, e
		}
		fmt.Fprintf(w, "已在本地演示下载普通文本：%d 字节。报告：%s\n\n", receipt.Bytes, folder)
	}
	return 0, nil
}

const help = `CloudProbe 云盘文件测试工具 1.1.1

用法：cloudprobe <命令> [参数]（参数必须放在命令之后）

  gui        启动本地界面，复用原版网易扫码登录；双击程序默认进入此模式
  demo       无账号运行本地演示：两家目录分页、字幕过滤、指定文本下载
  init       创建配置：-provider mobile|wopan -out config.json
  doctor     检查本地配置：-config config.json
  auth-probe 验证网易匿名 sid 握手：-config config.wopan.json
  list       读取目录：-config config.json -dir 目录ID -expect 说明.txt
  filter     离线过滤：-provider mobile|wopan -input 原始响应.json
  fetch      下载指定文件：-config config.json -id 文件ID -name 说明.txt
  version    显示版本

list/fetch/filter/demo 支持 -out 结果根目录；每次运行创建独立子目录。
退出码：0 成功；1 调用/解析/I/O 失败；2 配置/用法错误；
        3 查询完成但预期文件未返回；4 分页未完成。

先运行：cloudprobe demo
真实测试请阅读 README.md，填写你已授权的令牌与应用身份。
`

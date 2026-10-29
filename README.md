# CloudProbe

CloudProbe 将网易爆米花绑定的联通/移动云盘及独立登录的夸克网盘挂载到统一 WebDAV 服务。项目使用 Go 编写，夸克原生桌面与网页通道支持受限内存流水上传；运行配置和账号资料留在本机。

## 功能

- 网易爆米花扫码与独立夸克 App、手机号、微信、开放平台登录。
- WebDAV 文件浏览、下载、Range、上传、覆盖、目录操作、移动、复制、锁与剩余空间查询。
- 网易接口、云盘原生接口与可选 OpenList 服务间进行目录和下载回退；写入使用原生接口，并核对版本及提交结果。OpenList 路径不能提供稳定对象身份，因此备用通道不执行写入。
- 可配置云端分片、并发、总体上传低速恢复和共享缓存预算。
- 严格纯内存、内存优先、磁盘兼容三种上传缓存方式。缓存上限约束文件正文，不代表整个进程的内存上限。

## 项目结构

- 根目录 Go 文件：客户端、驱动、WebDAV 服务和测试。
- `ui/`：嵌入的管理界面。
- `bridge/`：与原版 .NET SDK 通信的桥接源代码和构建脚本。
- `vendor/`：已固定的 Go 依赖；其中 WebDAV 配额和最终上传 ETag 的本地补丁见 `third_party/`。
- `third_party/`：协议说明与引用项目的许可。
- `docs/`：当前使用说明与安全/内存说明；旧版设计说明在 `docs/history/`。

## 构建

需要与 `go.mod` 兼容的 Go 工具链。离线构建：

```powershell
go test -mod=vendor ./...
go vet -mod=vendor ./...
go build -mod=vendor -trimpath -o CloudProbe.exe .
```

仓库包含依赖所需的 `publicsuffix/data` 嵌入文件。CI 从干净检出验证 Windows/Linux 测试、静态检查、Linux 竞态检测和 Windows 构建。WebDAV 本地修改的可重放补丁为 `third_party/webdav.patch`，维护步骤见 `third_party/webdav-quota.md`。

本轮审查修补的行为限制、资源预算和验证说明见 [发布审查修补说明](docs/发布审查修补说明.md)。Windows 凭据使用 DPAPI；其他平台目前仅使用私有文件权限保护，尚未提供凭据加密。

GUI 登录桥接还需要 Windows x64 与 .NET 8。出于本地部署和账号安全，原版 SDK 二进制不放在 GitHub 仓库中。取得可用 SDK 后生成桥接组件：

```powershell
.\bridge\build.ps1 -SdkPath C:\path\to\authorized-sdk
```

启动 GUI 时通过 `--sdk` 和 `--bridge` 指向 SDK 目录及 `bridge\build\CloudProbe.Bridge.dll`；通过 `--data` 指定本机资料目录。不要将资料目录、加密账号 vault 或运行日志提交到版本库。

## 运行实网测试

普通 `go test` 不访问真实云盘。名称以 `TestLive` 开头的测试须先显式选择本机隔离资料目录和专用远端测试目录，再设置相应 `CLOUDPROBE_LIVE_*` 变量。涉及目录写入的测试只用于执行人授权的独立测试目录；不要将账号、令牌、文件名或测试日志提交到仓库。

# CloudProbe 云盘文件测试工具

当前交付为 **CloudProbe 1.6.1（Go 图形管理界面与 WebDAV 服务）**。本版是维护修复，界面、默认参数和正常操作流程保持 1.6.0 的体验；修复范围见 `WEBDAV-1.6.1修复说明.md`。使用方式见 `WEBDAV-使用说明.md`，纯内存流水线、总体低速恢复和无限故障重试见 `WEBDAV-内存与总体低速.md`，容量扩展见 `WEBDAV-容量配额.md`。本版包含 vendored Go 依赖，图形登录桥接需要 .NET 8 x64 运行时和随包原版 SDK。离线编译使用 `-mod=vendor`；本地依赖补丁说明位于 `third_party/`。

以下保留早期命令行原型的说明与验收记录，表示原型阶段的状态；当前图形版、WebDAV、真实账号及性能验收以以上文档为准。

这是一个 Go 编写的 Windows 命令行工具，用于区分：**接口是否返回普通文件、爆米花的字幕后缀规则是否忽略它、指定文件能否下载**。

程序只使用 Go 标准库，提供 Windows x64 可执行文件，运行主程序不需要安装 Go、Python 或 .NET。

## 当前交付状态

| 能力 | 状态 |
|---|---|
| 本地演示：移动、联通各两页目录，包含视频、字幕、文本和无扩展名文件 | 已通过真实本机 HTTP 集成测试 |
| 模拟爆米花字幕后缀筛选，保留原始列表中的普通文本条目供对照 | 已验证 |
| 指定小文件下载、字节上限、SHA-256、拒绝覆盖已有文件 | 已验证 |
| 导入 XML/JSON 目录响应，或本工具生成的 HTTP 记录，离线分析 | 已验证 |
| 移动云盘官方接口直连 | 协议适配已实现；需要本地填写有效的应用身份、用户令牌和设备 ID；未实测个人账号 |
| 联通的网易兼容模式 | **诊断状态，尚未打通实网签名**；已确认 `/a` 路径前缀、响应解密，实网返回 `401 / 10012 签名错误` |
| 自行配置明文 JSON 网关（gateway） | 已用模拟服务验证；本包不提供一个可用的网易签名代理服务 |

本地演示通过不表示真实云盘已验证。网易模式会在命令行明确提示现有签名问题，错误也不会被当作空文件列表。

## 先运行不需要账号的演示

双击 `启动演示.cmd`，或者在 PowerShell 中执行：

```powershell
.\cloudprobe.exe demo
```

演示只监听并访问本机临时回环地址，不连接真实云盘、不使用真实账号。它会依次：

1. 查询模拟移动云盘和联通云盘，各读取两页。
2. 显示 `电影.mkv`、`电影.srt`、`说明.txt`、`无扩展文件` 的返回情况。
3. 标记只有字幕通过后缀候选筛选。
4. 单独下载模拟的 `说明.txt`，证明列目录、过滤和下载正文是三个步骤。

每次运行都会新建结果子目录。打开其中的 `report.html` 即可查看表格；`report.json` 便于程序分析，`summary.txt` 是纯文字说明。

## 移动云盘直连

先创建自己的配置文件：

```powershell
.\cloudprobe.exe init -provider mobile -out config.mobile.json
```

用文本编辑器填写 `mobile` 下的字段：

| 字段 | 含义 |
|---|---|
| `app_id` | 获授权的移动云盘应用 ID |
| `app_secret` | 对应应用密钥 |
| `access_token` | 通过该应用取得的用户访问令牌 |
| `device_id` | 与这份授权匹配的设备标识 |
| `channel_id` | 该应用有权限使用的渠道；示例默认 `10010`，需按实际授权修改 |

本包没有嵌入爆米花的应用密钥，也不包含用户令牌。配置值支持 `${环境变量名}`，例如 `"access_token":"${M139_ACCESS_TOKEN}"`。请把凭据放在本地配置或环境变量中。

检查配置，再指定目录执行测试：

```powershell
.\cloudprobe.exe doctor -config config.mobile.json
.\cloudprobe.exe list -config config.mobile.json -dir "实际目录ID" -expect "说明.txt"
```

`root` 是默认目录值；实际应用能访问的根目录/授权目录可能不同，必要时使用真实目录 ID。工具只查询指定目录，不递归全盘。

可以重复填写 `-expect`：

```powershell
.\cloudprobe.exe list -config config.mobile.json -dir "目录ID" -expect "说明.txt" -expect "电影.srt"
```

目录查询发送 `filterType=2`（只取文件条目）、`contentType=0`（全部类型）。工具保留普通文件用于对照；同时标注爆米花样本对缺少 ID、名称或 `contentSuffix` 的条目会做的解析过滤。

下载一个已经列出的指定文件：

```powershell
.\cloudprobe.exe fetch -config config.mobile.json -id "文件contentID" -name "说明.txt"
```

默认下载上限为 10 MiB，可在 `download_max_bytes` 中修改。列表检查不下载正文，只有执行 `fetch` 才下载指定文件。下载请求不携带应用密钥或网盘 API 请求头；媒体 URL 自身的访问参数由服务器提供。

## 联通的网易兼容模式：当前仅用于诊断

```powershell
.\cloudprobe.exe init -provider wopan -out config.wopan.json -native-dll "C:\你的爆米花安装目录\ntkernel.dll"
.\cloudprobe.exe auth-probe -config config.wopan.json
```

`native_dll` 用于静态读取已分析版本的协议数据，程序不加载或执行 DLL；只接受以下 SHA-256 的 `ntkernel.dll`：

```text
D30191FB2E2C016BBBF6B41752ACB23598BFF6284DBB806F1AA6701E2F871CBA
```

也可以用 `raw_key_base64` 显式配置协议密钥，二者选一个。程序不会在日志中导出密钥。

`auth-probe` 仅验证网易匿名 sid 握手，不需要填网盘令牌，也不证明爆米花账号已登录。当前版本的实网结果仍是 **HTTP 401，业务码 10012，签名错误**；这表示联通网易模式目前不能作为真实目录测试的可靠入口。

完整网盘请求还需要以下用户授权字段，即便填写它们，也不能视为当前签名问题已经解决：

- `netease.token`：爆米花已登录账号的会话令牌；
- `wopan.access_token`：目标联通云盘访问令牌；
- `netease.sid`：可选，未提供时尝试匿名握手。

联通的已知代理操作已经实现：`query_files`、`query_file_info`、`get_download_url`。`list` 会自动分页，不局限于原客户端字幕路径的第一页一百条；重复页和达到页数上限会标记为未完成。

如果你有自己的可用授权网关，可以配置 `transport="gateway"`、`base_url` 和 `headers`。联通网关需接受 `/v1/api/wopan/...` 的逻辑 JSON/查询参数格式，并返回对应 JSON；移动网关需接受 `/v1/api/m139` 的 `{method,path,headers,body}` 封装。此模式不自动解决网易签名或提供上游账号。

## 没有可用凭据，也可以分析已有响应

`filter` 支持：

- 移动目录接口的原始 XML；
- 原始 JSON，包括网易多层 `data` 封装；
- 本工具的 `http-*.json`，自动读取 `response_logical_body`；
- 本工具的 `report.json`。

```powershell
.\cloudprobe.exe filter -provider mobile -input samples\mobile-response.xml
.\cloudprobe.exe filter -provider wopan -input samples\wopan-response.json
```

单页响应无法证明整个目录已经查完，离线报告会明确保留这个限制。`filter` 仅作后缀/解析规则对照，不模拟爆米花后续的同名匹配、字幕智能匹配或资源库服务端索引。

## 如何理解结果

| 结果 | 可作出的判断 |
|---|---|
| 文件出现在原始条目中，字幕候选为“否” | 本次请求已返回元数据，样本规则会忽略它；不代表正文已被下载 |
| `fetch` 成功且有字节数和 SHA-256 | 这次确实取得了指定文件内容 |
| HTTP/业务码错误 | 请求失败；不能推断目录为空或没有文本 |
| `query_completed=false` | 分页未结束、重复页或调用失败；不能对缺失文件下确定结论 |
| 查询完成，但预期文件未返回 | 只说明本次请求未返回；还需核对目录、文件名、授权范围、上游筛选等 |

每次运行的结果目录包含：

```text
report.html / report.json / summary.txt   列表和过滤判断
http-001.json ...                        脱敏后的请求与响应
download-receipt.json                    指定下载的字节数与 SHA-256
指定的下载文件                           仅 fetch 或 demo 生成
```

令牌、密钥、Cookie 和 URL 查询参数会在诊断记录中隐藏；目录和文件名保留，方便对照。缓存、配置及原始网盘凭据不会被上传给第三方分析服务。

## 退出码

| 退出码 | 含义 |
|---|---|
| 0 | 当前命令成功 |
| 1 | 网络、业务状态、解析或文件写入失败 |
| 2 | 配置或命令参数错误 |
| 3 | 查询完成，但至少一个预期文件未返回 |
| 4 | 目录分页未完成 |

`filter` 完成单页分析时退出 0，但报告仍可能标为 `query_completed=false`，因为“成功解析一页”与“完整读取目录”不同。

## 编译与验证

源码位于 `source` 文件夹，需要 Go 1.24 或更新版本，无第三方模块依赖：

```powershell
cd source
go test ./...
go vet ./...
go build -trimpath -ldflags "-s -w" -o ..\cloudprobe.exe .
```

测试使用本机 HTTP 回环服务。受限制的沙箱可能禁止回环连接；真实本机环境应允许测试进程访问它自己创建的临时端口。

官方协议对照：[移动云盘开放平台帮助文档](https://open.yun.139.com/open/index.html#/help-document-new/index?type=API)。

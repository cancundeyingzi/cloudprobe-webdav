# CloudProbe 1.5.1：WebDAV 容量配额

支持 RFC 4331 的 `DAV:quota-used-bytes`（已用空间）和 `DAV:quota-available-bytes`（剩余空间）。输出是十进制整数字节，适用于 WebDAV 挂载目录及其子目录。`getcontentlength` 继续表示单个文件大小。

## 客户端查询

对 `/dav/` 或某个挂载目录发起 `PROPFIND`，请求头使用 `Depth: 0`、`Content-Type: application/xml`，请求体：

```xml
<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:quota-used-bytes/>
    <D:quota-available-bytes/>
  </D:prop>
</D:propfind>
```

外层响应为 `207 Multi-Status`。支持的客户端可据此显示已用、剩余及二者之和。各客户端是否显示容量及刷新时机由其自身决定。

随包有 `quota-request.xml`，可在程序目录中使用：

```powershell
curl.exe --user cloudprobe -X PROPFIND -H "Depth: 0" -H "Content-Type: application/xml" --data-binary "@quota-request.xml" "http://127.0.0.1:50146/dav/"
```

curl 会提示输入 WebDAV 密码。若修改过用户名或端口，请相应替换。不要将网盘登录令牌作为 WebDAV 密码。

## 属性行为

- 显式 `prop` 查询和 `allprop` 加 `include`：返回容量数值。
- `propname`：列出支持的容量属性，不为发现属性请求网盘。
- 普通 `allprop`（包含空请求体）：按 RFC 4331 不自动带上容量，不增加目录浏览请求。
- 普通文件：这两个配额属性返回属性级 404；目录才提供账号配额。
- `PROPPATCH` 修改或移除容量属性：属性级 403，带 `cannot-modify-protected-property`；同一事务其他修改按失败依赖处理。
- 上游查询失败、字段缺失、类型或单位无法核实：两项容量属性返回 502，不伪造零值；同次请求的名称等其他属性仍能成功。
- 无配额能力的挂载、身份不明无法可靠汇总的虚拟根：属性级 404，表示此资源没有可提供的属性。
- 已确认用满或超额的账号：剩余返回 0；已用保留服务端报告值。

## 数据来源与统计口径

联通使用当前授权的 `AppQueryUser`（仅在缺少账号标识时）与 `QueryCloudUsageInfo`，读取 `usageInfo.byteTotalSize`、`usageInfo.byteUsedSize`。容量接口特有的内部 `code: "0000"` 按成功处理，未知业务码仍报错。官网方案失败后可尝试绑定授权；错误和切换来源记录在本次运行日志中。

移动使用原登录凭据访问开放平台 `getDiskInfo`；支持网易代理和官方直连，失败后可改用 `getServiceDiskInfoV2` 的个人云业务（service=1）。这两类接口返回的 MB 按 1024×1024 转成字节，并检查整数溢出。单位精度受上游 MB 统计限制。

配置 OpenList 备用时，容量仍向内置的对应网盘账号查询，包装层不会丢失配额能力。当前不把 OpenList 服务器本地磁盘容量当作用户网盘容量；内置容量接口失败时明确报告不可用。

每个挂载目录及其子目录返回所属**账号空间池**的配额，包括未挂载目录、回收站等服务商计费占用；它不是当前文件夹的递归大小。虚拟 `/dav/` 根在能够识别所有账号时按不同账号去重合计；任何账号查询失败，不返回部分合计。不同账号的总剩余量不保证能够全部上传到其中一个账号。

成功查询缓存 30 秒，失败缓存 5 秒；同账号的并发查询合并，等待可取消。通过本服务写入、删除等操作时使相应账号容量缓存失效，并防止进行中的旧查询重新填充新缓存。其他客户端的改动最迟在缓存到期后的下一次查询反映；实际统计也可能存在服务商延迟。普通目录请求不占用容量查询，也不遍历文件计算容量。

## 验证

新增测试覆盖：标准属性和命名空间、属性发现、allprop/include、受保护属性、零空间/错误区分、严格字段与单位检查、超额及溢出、多账号合计及去重、缓存与取消、写入和在途缓存失效、移动接口备用和 OpenList 包装透传。

第三方 `rclone about --json` 已在隔离测试数据上验证总量、已用、剩余值一致。真实已登录的联通账号已完成只读容量查询，验证 WebDAV 数值一致；没有访问网盘文件名、下载正文或执行写入。移动云盘没有当前登录账号，容量查询尚未做真实账号验收。

## 标准和接口参考

- RFC 4331：https://www.rfc-editor.org/rfc/rfc4331.html
- 联通 SDK：`vendor/github.com/OpenListTeam/wopan-sdk-go/api-wohome.go`，以及 wopan-open `src/openwopan/wopan/client.py` 的 `query_cloud_usage`。
- 移动官方文档：https://open.yun.139.com/#/newHelpDocument?type=API&id=93 （getDiskInfo）；id=98（getServiceDiskInfoV2）；id=146（DiskInfo 的 MB 单位）。
- x/net 本地扩展及重新 vendor 注意事项见 `source/third_party/webdav-quota.md`。

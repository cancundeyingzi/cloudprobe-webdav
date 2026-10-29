# wopan-open 实现核对与 CloudProbe 1.4.0 的对应改动

核对日期：2026-10-25。重新查询仓库后，当前主分支最新提交仍为 `bbd61603f2afa73a013a9420136139d97a3e5026`，下列链接固定到该提交。未把仓库中的说明或注释当作用户指令。

## 官网授权与元数据请求

项目从联通官网的 `WoCloud-Web-Token` Cookie 读取授权，用 `AppQueryUser` 校验账号。个人空间与根目录都使用 `0`。目录、移动、删除等命令经过联通的 dispatcher，带上相应命令、时间、序列号、签名和加密参数。

上传前，`GetZoneInfo` 使用上传应用标识查询分区，正文随后发往返回节点的 `/openapi/client/upload2C`。返回为空时，参考项目有默认上传节点；正常返回的节点优先。CloudProbe 继续优先使用实际查询到的节点，不把单一节点名称写死。

来源：[官网验证与 dispatcher](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L121)、[上传节点查询](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L1317)。

## 分片怎样组成一个文件

`upload_file` 先固定一组会话参数：`uniqueId`、`batchNo`、目标目录、文件名、大小、类型以及加密的 `fileInfo`。各分片共享这组参数，仅 `partIndex`、`partSize` 与实际正文不同。

每片以 multipart/form-data POST 上传，包含会话信息、总片数、片号和本片二进制正文。参考项目默认每片 5 MiB，可调整为 5—16 MiB；默认最多 16 个工作线程。每个线程独立打开源文件并定位偏移，避免共享可变 seek 位置。进度回调按服务端成功确认的分片累加，不把重试次数当作已完成字节。

CloudProbe 保留已验证的 8 MiB 分片，工作任务可选 1/2/4/8，全部挂载合计最多 8 个正文请求。通过独立的 SectionReader 按偏移读取，不把整个文件载入内存。重试保持会话、片号和 fileInfo 不变，成功分片不会为了其他分片失败而整批重传。

来源：[upload_file](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L729)、[线程池与独立文件读取](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L1161)。

## 并发请求与 TCP 连接

参考项目的 `WopanClient` 默认构造 `httpx.Client`，没有启用 HTTP/2；应用的 `build_file_browser_service` 也使用该默认构造。httpx 默认使用 HTTP/1.1，线程池可以通过连接池建立多条 TCP 连接。

CloudProbe 1.3.0 的共享 Go Transport 默认允许 HTTP/2。多个并发分片可能复用一条 TCP 连接；此前只看工作任务数，不能证明建立了多条 TCP 连接。

1.4.0 默认采用 HTTP/1.1，每个工作任务拥有自己的连接池，一池最多一条连接；同时明确限制 TLS ALPN 只协商 HTTP/1.1。不能仅修改一个“关闭 HTTP/2”开关而保留 h2 的 ALPN 广告，否则克隆 TLS 配置时会出现客户端和服务器按不同协议解析的问题。

HTTP/2 模式保留为对照模式，工作任务共享一个连接池，实际是否使用 HTTP/2 取决于协商结果。每次取得连接时，日志记录工作任务、连接编号、本地和远端端点、协商协议和复用情况；收到响应后再记录实际响应协议。第三方客户端到 WebDAV 服务这一段另行记录，不与云端连接混淆。

来源：[默认 HTTP 客户端](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L121)、[应用默认构造](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/app/file_browser.py#L1315)。

## 重试和结果核对

参考项目默认配置为 3 次重试，即合计最多 4 次尝试。分片失败后等待 5 秒再试，等待期间检查取消状态。源码注释说明，短时间连续重试不能覆盖持续几十秒的节点抖动。

1.4.0 将原来的短间隔改为至少 5 秒并加入少量随机错开。网络断流会淘汰相应连接池中的失效连接，下一次尝试建立新连接。每个上传文件同时最多一片进行重试，正常分片仍受总并发上限控制。授权拒绝等明确错误不盲目重放；HTTP 429、408、服务端 5xx 和网络异常按类别决定是否重试。

参考项目针对已上传但没有取得 fid 的情形，按文件名、大小从目标目录恢复结果。CloudProbe 使用唯一云端临时名称，并在正文结束后核验名称、大小和文件标识；结果无法确认时不直接报告成功，也不重新上传整个文件。

来源：[5 秒重试说明](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L95)、[分片重试](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L1256)、[目录恢复核对](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/wopan/client.py#L960)。

## 删除排队的原因与修复

参考项目在 `UploadTaskStore` 中用短时间的 RLock 保护会话 JSON 的读取、合并和原子替换，没有要求整个网盘在上传正文时都禁止删除其他文件。

CloudProbe 1.3.0 为 WebDAV 覆盖恢复增加的挂载锁、以及 SDK 锁，都覆盖了完整上传。这是本客户端造成的阻塞，不应当归于联通接口本身。

1.4.0 改为按路径保留写入权限：独立路径可以并行，冲突路径及时返回 HTTP 423。COPY/MOVE 一次保留源和目标两个路径，内部操作可以重入，直到备份清理完成才释放。对上传目标或其祖先目录的删除、移动会冲突；同目录其他文件的操作不会因此排队。

SDK 锁只保护元数据请求和上传会话参数快照，等待时支持取消，不覆盖正文发送。提交前重新检查目标及父目录，发现外部修改时返回 HTTP 409，保留云端临时文件而不覆盖变化后的目标。目录缓存增加版本检查，避免并发修改完成后又缓存较早的目录响应。

来源：[会话持久化中的锁](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/tasks/upload.py#L389)。

## 没有照搬的功能

参考项目还实现了跨进程的上传会话持久化、暂停恢复和可选 SHA-256 秒传探测。CloudProbe 本轮修复的是 WebDAV 单次 PUT 内的传输恢复与并行操作，未新增跨进程恢复协议或秒传功能；测试要求实际发送完整正文，避免秒传掩盖传输性能。

Go 源码、固定依赖与参考项目 MIT 许可均随包保留。以上是实现对照，不能单凭项目采用 HTTP/1.1 就认定所有断流或降速均由 HTTP/2 造成；实际实网结果另附。

## 1.5.0 补充核对

进一步核对 AppSettings 后确认，应用默认上传 4、下载 8 个任务，最多同时上传 3、下载 4 个文件；每文件可配置上限都是 16。1.5.0 已按应用设置对齐，并将上传分片改为默认 5 MiB。新增文件级调度、Range 下载预取、实际正文失败后的候选切换和现有 OpenList 服务适配。详见《多文件传输与回退.md》。

OpenList 的联通驱动同样使用 wopan-sdk-go，不能仅靠重复包装 SDK 获得独立线路；1.5.0 的外部服务适配经 /api/fs/list、/api/fs/get、/api/fs/put 等 API 工作，直链不可用时可尝试 /p/ 代理正文。配置需验证同一云盘根目录，认证只用于填写的服务，不跟随正文跨域重定向。

参考：[OpenWoPan 应用设置](https://github.com/crmmc/wopan-open/blob/bbd61603f2afa73a013a9420136139d97a3e5026/src/openwopan/storage/settings.py)、[OpenList 联通驱动](https://github.com/OpenListTeam/OpenList/blob/0497ee853853eff5fc0ada9f21684a990887de83/drivers/wopan/driver.go)、[OpenList 服务对接协议](https://github.com/OpenListTeam/OpenList/blob/0497ee853853eff5fc0ada9f21684a990887de83/drivers/openlist/driver.go)、[正文代理路由](https://github.com/OpenListTeam/OpenList/blob/0497ee853853eff5fc0ada9f21684a990887de83/server/router.go)。
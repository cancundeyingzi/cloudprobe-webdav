# WebDAV 容量扩展的本地补丁

基于 `golang.org/x/net v0.61.0`，保留原 BSD 许可证。没有更改依赖版本。

- `vendor/golang.org/x/net/webdav/quota.go` 定义可选 `QuotaFileSystem`。
- `prop.go` 在显式属性查询时读取一次容量并按属性返回 200、404 或 502；`propname` 仅发现能力，不访问上游；普通 `allprop` 排除容量，`allprop/include` 可以显式加入。
- `webdav.go` 将已经解析的 `pf.Include` 传入 `allprop`，修正原实现误用 `pf.Prop` 导致忽略 `DAV:include` 的问题。
- 两项容量属性注册为受保护属性，`PROPPATCH` 在打开可写文件前拒绝修改，保持原有原子处理与 424 失败依赖逻辑。
- 应用只对目录返回配额；文件保留原有大小等属性。
- 1.6.1 补充：`webdav.go` 的 `handleLock` 传播创建资源时的 `File.Close()` 错误，使原有失败清理释放刚创建的锁。仅明确不存在时创建资源，并用 `O_EXCL` 避免查询错误或创建竞争截断已有内容。正常 LOCK、UNLOCK 与响应格式不变，回归见两组 maintenance 测试。
- 1.10.0 补充：`handlePut` 在成功 Close 后查询可选 `CommittedInfo()`，使用已经核验的最终云端对象生成 ETag；无法提供最终信息时省略响应 ETag。普通文件系统保持既有流程，避免将临时占位元数据作为已提交对象的验证器。

离线构建使用 `-mod=vendor`。不要直接重新执行 `go mod vendor` 后发布，否则会丢失此补丁。更新 x/net 后，重新应用本文件描述的三处源文件改动，并通过容量、锁和客户端互操作回归。随包 `source/vendor` 包含完整补丁。

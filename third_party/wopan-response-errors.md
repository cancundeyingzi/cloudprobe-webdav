# 联通 SDK 响应错误的本地补丁

基于 `github.com/OpenListTeam/wopan-sdk-go v0.2.2`，保留原有许可证和依赖版本。

`vendor/github.com/OpenListTeam/wopan-sdk-go/errors.go` 增加 `ResponseError`，携带 HTTP 状态、RSP 业务码和原有错误文本。`request.go` 的三个明确服务端失败出口使用此类型，错误字符串保持原样；请求参数、加密、签名、令牌刷新和重试逻辑不变。

应用据此区分明确拒绝、执行结果未知和已经部分执行，避免根据错误消息中是否出现“HTTP 403”等文本决定重放写入。只有操作尚未产生先前写入、且当前请求被明确拒绝时，才标记为 `davNotExecuted`。复合操作已部分成功时不会继承内部错误的重放许可。

离线构建使用 `-mod=vendor`。重新执行 `go mod vendor` 会覆盖本补丁；更新 SDK 时需重新应用并运行 `maintenance_regression_test.go`、`maintenance_edges_test.go` 及 OpenList 回退回归。

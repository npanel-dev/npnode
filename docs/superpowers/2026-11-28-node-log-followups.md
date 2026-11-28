# 节点日志上报：后续事项（审查中延后的次要问题）

来源：节点、面板后端、管理端三份计划在逐任务审查和整体审查中记录的 Minor 项。均已判定「不阻塞合并」。

## 建议优先处理
- 节点：面板客户端原有 `OnError` 日志会在控制台明文打印 `secret_key`（既有问题，上报内容已脱敏）。
- 管理端：英文界面的标题和处理建议是后端返回的中文；建议前端按错误码查 `nodeLogs.code.*`，查不到再用后端文案。
- 后端：inbound/cert 作用域的恢复与合并哈希仍区分协议名大小写。
- 节点：reload 内「获取服务端配置失败」未带错误码，同一次失败会产生两条事件。

## 节点（npnode）
- spool 解析失败直接删除，建议改名 `.corrupt` 保留。
- 缺「暂扣期间多次出现」的计数测试；`TestConcurrentAddAndFlush` 无断言；inbound/cert 恢复无单测。
- `sendSync` 阻塞时不响应 ctx（有 FlushFatal/replay 预算兜底）。
- `Add` 对每条 Warn+ 事件做一次 protojson 序列化估算大小，可改为组批时再算。
- 旧面板上每个进程首次 gRPC 发送仍要等满 15 秒 Ack（之后改走 HTTP 并退避）。
- Install 之前发生的 panic 不会上报；`TestReplaySpoolFailureDoesNotBlock` 固定耗时 5 秒。
- cert.go 续期失败但沿用旧证书（ok=true 带 error）时也发恢复信号，需补注释。
- 脱敏：JSON 转义引号内的值未处理；`(?i)` 使用 Unicode 大小写折叠（两端一致即可）。

## 面板后端（NPanel-backend）
- 乱序上报会把 `last_at` 改小，建议只增不减。
- Summary 全量读入内存聚合，服务器规模很大时改为 GROUP BY。
- `configuredInbounds` 仅 server 恢复需要，可按需查询；HTTP 路径重复查服务器。
- 清空全部是一条不分批的 DELETE，表大时建议分批。
- 清理任务：设置读取失败分支无测试；`DeleteBefore` 失败返回 nil 使 MaxRetry 无效。
- `registerNodeLogAdminRoutes` 的 logger 参数未用。

## 管理端（NPanel-frontend）
- 徽标没有键盘可达性（建议 `Badge asChild` 包 `<button>`）。
- 日志列表请求失败时抽屉内无提示，只有全局 toast。
- 相对时间不会随时间自动刷新；手写相对时间可换成通用工具。
- 设置 Popover 重开不重置草稿；保存中无 loading 文案。
- 原始日志着色：值里出现 ` k=v` 会被误切为 key（仅影响着色）。
- 文案测试的键列表是硬编码的。

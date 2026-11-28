# 节点日志上报（节点侧 npnode）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 节点把故障事件、恢复信号经 gRPC Sync（优先）或 HTTP（兜底）上报面板，因错误退出前必须先上报，发不出去落盘下次补发。

**Architecture:** 新包 `common/logreport` 负责错误码、归一化、脱敏、logrus Hook、环形缓冲、上报队列与落盘；`api/panel` 提供两种发送实现；`core`/`node`/`cmd` 在已知故障点包装错误码、在成功点发恢复信号，`cmd/server.go` 在所有错误退出路径调用 `FlushFatal` 并以非 0 退出。

**Tech Stack:** Go、logrus v1.9.3、protobuf（protoc + protoc-gen-go v1.34.2 + protoc-gen-go-grpc v1.5.1）、resty、protojson。

**Spec:** `docs/superpowers/specs/2026-11-28-node-log-report-design.md`

**配套计划：** 面板后端 `docs/superpowers/plans/2026-11-28-node-log-report-backend.md`、管理端 `docs/superpowers/plans/2026-11-28-node-log-report-frontend.md`。本计划可独立完成并测试（用 httptest / 假 Sender），不依赖面板先上线。

## Global Constraints

- proto 只追加：`SyncRequest.payload` 新增 `LogReport log = 6;`，`LogEvent` 字段 1–11、`LogResolve` 字段 1–5 与设计文档 4.1 完全一致（含 `uint32 count = 11`），面板仓库 `NPanel-backend/api/node/v1/node.proto` 必须逐字同步。
- HTTP 兜底：`POST {ApiHost}/v1/server/log?server_id=<id>&secret_key=<key>`，body 为 `protojson.MarshalOptions{UseProtoNames: true}` 序列化的 `LogReport`；2xx 视为面板已处理（含重复）。
- 事件采集规则：带 `code` 的 warn 及以上 → 事件；无 `code` 的仅 error/fatal/panic → `code=UNKNOWN`。
- 合并键：`code|protocol|port`；`code=UNKNOWN` 时为 `code|protocol|port|Normalize(message)`。节流窗口 60 秒，窗口内累计 `count`。
- 发送：每 10 秒或攒满 20 条事件发一批；内存队列上限 500 条（超出丢最旧）；恢复信号不节流，同一 `scope|protocol|port` 只保留最新。
- 环形缓冲 200 行；每个事件附最多 50 行上下文（事件发生时缓冲的最后 50 行）。
- 退出：`FlushFatal` 最多同步等待 5 秒，失败写 `<配置目录>/pending-logs.json`（上限 1 MB，超出丢最旧事件），随后 `os.Exit(1)`。
- 启动补发：读取配置后、拉取面板配置前补发 `pending-logs.json`，成功删除；失败保留，不阻塞启动。
- 脱敏：`secret_key` 值、x25519/Reality 私钥（43 位 base64url）、UUID 一律替换，上报的 `message` 与 `context` 都要脱敏。
- 代码注释用中文。

## Review Focus

- 面板完全不可达（或密钥错误）时节点重启循环：每次退出等待不超过 5 秒，`pending-logs.json` 不超过 1 MB，节点启动不被补发阻塞。→ Task 4 `TestFlushFatalTimeoutWritesSpoolWithinBudget`、`TestSpoolCappedAt1MB`、Task 7 `TestReplaySpoolFailureDoesNotBlock`。
- 日志里带出密钥或私钥：`secret_key=xxx` 出现在 resty 错误 URL 里。→ Task 2 `TestSanitizeQuerySecretAndKeys`。
- 同一错误在重启循环中反复出现：重启后节点内存清零，每次只报 `count=1`，面板靠合并键累加，节点侧合并键必须稳定（含变量的 UNKNOWN 文本）。→ Task 2 `TestMergeKeyStableAcrossVariables`。
- 调高 `Log.Level` 为 error 后，环形缓冲仍要有 info/debug 上下文，控制台输出却不能变多。→ Task 3 `TestOutputFilteredButRingKeepsDebug`。
- 并发：多个入站同时报错、恢复信号与事件交错、`FlushFatal` 与后台发送同时进行，不丢事件不 panic。→ Task 4 `TestConcurrentAddAndFlush`（`go test -race`）。

---

### Task 1: proto 新增 LogReport / LogEvent / LogResolve

**Files:**
- Modify: `proto/node/v1/node.proto`（`SyncRequest` oneof 与 `CertReport` 之后新增消息）
- Regenerate: `proto/node/v1/node.pb.go`、`proto/node/v1/node_grpc.pb.go`
- Test: `proto/node/v1/log_report_test.go`

**Interfaces:**
- Produces: `nodev1.LogReport{Events []*LogEvent; ReportId string; Resolves []*LogResolve}`、`nodev1.LogEvent{Level, Code, Stage, Protocol string; Port uint32; Message string; Context []string; OccurredAt int64; Version string; Exiting bool; Count uint32}`、`nodev1.LogResolve{Scope, Protocol string; Port uint32; ActiveInbounds []string; ResolvedAt int64}`、`nodev1.SyncRequest_Log{Log *LogReport}`。

- [ ] **Step 1: 写失败测试** `TestLogReportProtoJSONNames`：构造含 1 个 event、1 个 resolve 的 `LogReport`，用 `protojson.MarshalOptions{UseProtoNames: true}` 序列化，断言 JSON 含键 `report_id`、`occurred_at`、`active_inbounds`、`resolved_at`、`count`；再 `protojson.Unmarshal` 回来 `proto.Equal` 为真。
- [ ] **Step 2: 运行确认失败** `go test ./proto/node/v1/ -run TestLogReportProtoJSONNames` → 编译失败（类型未定义）。
- [ ] **Step 3: 按设计文档 4.1 修改 proto 并生成**（在 `proto/` 目录执行）：
  ```bash
  cd proto && protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative node/v1/node.proto
  ```
- [ ] **Step 4: 运行确认通过** `go test ./proto/... && go build ./...` → PASS。
- [ ] **Step 5: Commit** `feat(proto): 新增节点日志上报 LogReport`

### Task 2: logreport 基础——错误码、归一化、脱敏、合并键

**Files:**
- Create: `common/logreport/errors.go`、`common/logreport/codes.go`、`common/logreport/normalize.go`、`common/logreport/sanitize.go`
- Create: `common/logreport/testdata/normalize_cases.json`（面板后端计划会原样复制这份用例）
- Test: `common/logreport/errors_test.go`、`common/logreport/normalize_test.go`、`common/logreport/sanitize_test.go`

**Interfaces:**
- Produces:
  - `type Error struct{ Code, Stage, Protocol string; Port int; Err error }`，实现 `Error() string`（返回 `Err.Error()`）、`Unwrap() error`。
  - `func Wrap(code, stage string, err error) error`（err 为 nil 返回 nil；err 已是更内层 `*Error` 时保留内层 code/stage，仅补外层缺失字段）。
  - `func WithInbound(err error, protocol string, port int) error`（给链上最内层 `*Error` 补协议端口；链上没有 `*Error` 时包一层 `INBOUND_START_FAILED/inbound`）。
  - `func Classify(err error) (code, stage, protocol string, port int, ok bool)`（`errors.As` 取最内层 `*Error`）。
  - `func Normalize(msg string) string`、`func Sanitize(s string) string`、`func MergeKey(code, protocol string, port int, message string) string`。
  - `codes.go` 常量（stage 括号内）：`CodeConfigLoad="CONFIG_LOAD_FAILED"(config)`、`CodePanelConfig="PANEL_CONFIG_FETCH_FAILED"(config)`、`CodeCoreStart="CORE_START_FAILED"(core)`、`CodeInboundStart="INBOUND_START_FAILED"(inbound)`、`CodeVlessEncUnsupported="VLESS_ENCRYPTION_UNSUPPORTED"(inbound)`、`CodeVlessEncModeEmpty="VLESS_ENCRYPTION_MODE_EMPTY"(inbound)`、`CodeVlessEncKeyEmpty="VLESS_ENCRYPTION_KEY_EMPTY"(inbound)`、`CodeUserList="USER_LIST_FETCH_FAILED"(inbound)`、`CodeCertUnavailable="CERT_UNAVAILABLE"(cert)`、`CodeStatusReport="STATUS_REPORT_FAILED"(report)`、`CodeTrafficReport="TRAFFIC_REPORT_FAILED"(report)`、`CodeReload="RELOAD_FAILED"(runtime)`、`CodePanic="PANIC"(runtime)`、`CodeUnknown="UNKNOWN"`；stage 常量 `StageConfig/StageCore/StageInbound/StageCert/StageReport/StageRuntime`。

- [ ] **Step 1: 写失败测试**
  - `TestClassifyThroughFmtWrap`：`err := fmt.Errorf("启动节点失败: %w", fmt.Errorf("add new node error: %w", Wrap(CodeVlessEncModeEmpty, StageInbound, errors.New("vless encryption mode is empty"))))`，再 `WithInbound(err,"vless",443)`；`Classify` 得 `VLESS_ENCRYPTION_MODE_EMPTY, inbound, vless, 443, true`。
  - `TestWithInboundWithoutCode`：`WithInbound(errors.New("x"),"trojan",8443)` → `INBOUND_START_FAILED, inbound, trojan, 8443`。
  - `TestNormalizeCases`：读 `testdata/normalize_cases.json`（`[{"in":..., "out":...}]`），逐条断言。用例至少含：
    `{"in":"dial tcp 1.2.3.4:443: i/o timeout","out":"dial tcp <ip>: i/o timeout"}`、
    `{"in":"user 3f2b1c9e-1a2b-4c3d-9e8f-001122334455 not found","out":"user <uuid> not found"}`、
    `{"in":"got 12 users, version 345","out":"got <n> users, version <n>"}`、
    `{"in":"read \"abc def\" failed","out":"read \"?\" failed"}`、
    `{"in":"hash deadbeefcafe1234 mismatch","out":"hash <hex> mismatch"}`、
    `{"in":"  a   b  ","out":"a b"}`。
  - `TestMergeKeyStableAcrossVariables`：`MergeKey(CodeUnknown,"",0,"dial tcp 1.2.3.4:443: timeout")==MergeKey(CodeUnknown,"",0,"dial tcp 5.6.7.8:80: timeout")`；而 `MergeKey(CodeVlessEncModeEmpty,"vless",443,"a")==MergeKey(CodeVlessEncModeEmpty,"vless",443,"b")`（非 UNKNOWN 不含 message）。
  - `TestSanitizeQuerySecretAndKeys`：`Sanitize("Post https://p/v1/server/status?secret_key=abc123&server_id=1")` 含 `secret_key=<redacted>` 且不含 `abc123`；43 位 base64url 私钥 → `<key>`；UUID → `<uuid>`；JSON `"secret_key":"zzz"` → `"secret_key":"<redacted>"`。
- [ ] **Step 2: 运行确认失败** `go test ./common/logreport/` → 编译失败。
- [ ] **Step 3: 实现** 归一化替换顺序固定为：引号内容 → UUID → IPv6（方括号）/IPv4（含可选 `:port`）→ 8 位以上十六进制 → 十进制数字 → 合并空白并 trim（顺序本身是两端一致的契约，写进 `normalize.go` 注释）。
- [ ] **Step 4: 运行确认通过** `go test ./common/logreport/` → PASS。
- [ ] **Step 5: Commit** `feat(logreport): 错误码、归一化与脱敏`

### Task 3: logrus Hook、环形缓冲与输出级别过滤

**Files:**
- Create: `common/logreport/ring.go`、`common/logreport/hook.go`、`common/logreport/levelwriter.go`
- Test: `common/logreport/hook_test.go`

**Interfaces:**
- Consumes: Task 2 的 `Classify`、`Sanitize`、常量。
- Produces:
  - `type Ring struct` + `func NewRing(n int) *Ring`、`(*Ring).Add(line string)`、`(*Ring).Last(n int) []string`（并发安全）。
  - `type Hook struct` 实现 `logrus.Hook`；`func NewHook(ring *Ring, sink func(*nodev1.LogEvent)) *Hook`。`Levels()` 返回全部级别；`Fire` 先把格式化后的行（脱敏）写入 ring，再按 Global Constraints 的采集规则生成 `LogEvent`（`code/stage/protocol/port` 优先取 `entry.Data["err"]` 的 `Classify`，其次取 `entry.Data` 的 `code`/`stage`/`protocol`/`port` 字段；`message` = `entry.Message`，有 `err` 时为 `entry.Message + ": " + err.Error()`，脱敏；`Context = ring.Last(50)`；`OccurredAt` 毫秒；`Version = panel.NodeVersion` 由调用方通过 sink 填充，hook 不依赖 panel 包；`Exiting` 取 `entry.Data["exiting"]==true`；`Count=1`）。
  - `type LevelWriter struct{ Out io.Writer; Level logrus.Level }`：`Write` 解析行内 `level=xxx`，低于 `Level` 的丢弃。
  - `func Install(ring *Ring, sink func(*nodev1.LogEvent), outLevel logrus.Level, out io.Writer)`：`logrus.SetLevel(DebugLevel)`、`logrus.SetOutput(&LevelWriter{out, outLevel})`、`logrus.AddHook(NewHook(...))`。

- [ ] **Step 1: 写失败测试**
  - `TestCodedWarnBecomesEvent`：`log.WithField("err", Wrap(CodeCertUnavailable, StageCert, errors.New("acme"))).Warn("证书不可用")` → sink 收到 1 条，`Code=CERT_UNAVAILABLE`、`Level="warn"`。
  - `TestUncodedWarnIgnoredUncodedErrorUnknown`：无码 `Warn("证书申请成功")` → 0 条；无码 `Error("boom")` → 1 条 `Code=UNKNOWN`。
  - `TestEventContextFromRing`：先打 60 行 Debug，再 Error → `len(Context)==50` 且最后一行包含该 Error 的消息。
  - `TestOutputFilteredButRingKeepsDebug`：`Install(..., outLevel=WarnLevel, out=&buf)`，打 Debug + Warn → `buf` 只有 Warn 行，ring 两行都在。
  - `TestHookSanitizes`：`Error("Post https://p?secret_key=s3cr3t")` → 事件 Message 与 Context 都不含 `s3cr3t`。
- [ ] **Step 2: 运行确认失败** `go test ./common/logreport/ -run 'Hook|Event|Output|Uncoded|Coded'`。
- [ ] **Step 3: 实现上述类型。** Hook 内格式化用 `entry.String()`（与 TextFormatter 输出一致）。
- [ ] **Step 4: 运行确认通过**（含 `-race`）。
- [ ] **Step 5: Commit** `feat(logreport): logrus Hook 与环形缓冲`

### Task 4: Reporter——队列、节流、批量发送、FlushFatal 与落盘

**Files:**
- Create: `common/logreport/reporter.go`、`common/logreport/spool.go`、`common/logreport/default.go`
- Test: `common/logreport/reporter_test.go`、`common/logreport/spool_test.go`

**Interfaces:**
- Consumes: Task 1 proto 类型、Task 2 `MergeKey`。
- Produces:
  - `type Sender interface{ SendLogs(ctx context.Context, r *nodev1.LogReport) error }`
  - `type Reporter struct`；`func NewReporter(opts Options) *Reporter`，`Options{ Sender Sender; Version string; SpoolPath string; Now func() time.Time; NewID func() string }`（Now/NewID 为空用默认：`time.Now`、`bootID:seq`）。
  - `(*Reporter).SetSender(s Sender)`（启动中途才拿到客户端，热重载时替换）、`Add(e *nodev1.LogEvent)`（`e.Version` 为空时填 `Options.Version`）、`Resolve(r *nodev1.LogResolve)`、`Run(ctx context.Context)`（10 秒 ticker 或满 20 条触发 `flush`）、`FlushFatal(timeout time.Duration) error`、`ReplaySpool(ctx context.Context) error`。
  - 节流：`Add` 按 `MergeKey` 查 60 秒窗口；窗口内重复只把待发事件 `Count++` 并更新 `Message/Context/OccurredAt`；窗口内已发出过则新建待发事件 `Count=1` 并在窗口结束前不再新建（累加到这条）。
  - 发送失败：整批（同一 `ReportId`）保留，下次用同一 `ReportId` 重发；队列总事件数超过 500 丢最旧。
  - `default.go`：包级 `Default *Reporter` 与便捷函数 `Resolve(scope, protocol string, port int, active []string)`（Default 为 nil 时无操作）；scope 常量 `ScopeServer="server"`、`ScopeInbound="inbound"`、`ScopeCert="cert"`。
  - spool 文件格式：JSON 数组，每项为 `protojson(UseProtoNames)` 的 `LogReport`（保留原 `ReportId` 以便面板去重）。

- [ ] **Step 1: 写失败测试**（假 Sender 记录调用、可注入错误；假时钟）
  - `TestThrottleMergesWithinWindow`：同一 code/inbound 在 59 秒内 `Add` 3 次 → flush 后发出 1 条 `Count=3`；第 61 秒再 `Add` → 下一批 1 条 `Count=1`。
  - `TestUnknownDifferentMessagesNotMerged`：两条 UNKNOWN、归一化后不同 → 2 条。
  - `TestBatchOn20Events`：`Run` 中 `Add` 20 条不同事件 → 不等 10 秒立即发 1 批。
  - `TestRetrySameReportID`：Sender 第一次返回错误 → 第二次发送的 `ReportId` 与第一次相同、内容相同。
  - `TestQueueCap500DropsOldest`：Sender 永远失败，`Add` 600 条不同事件 → 待发总数 500，最早的 100 条不在其中。
  - `TestResolveKeepsLatestPerScope`：`Resolve(inbound vless 443)` 两次 → 一批中只有 1 条，`ResolvedAt` 为后一次。
  - `TestFlushFatalTimeoutWritesSpoolWithinBudget`：Sender 阻塞直到 ctx 取消 → `FlushFatal(5*time.Second)` 在 ≤5.5 秒返回错误，spool 文件存在且可解析出全部待发事件。
  - `TestSpoolCappedAt1MB`：待发事件总量 >1 MB → 文件 ≤1 MB 且保留最新事件。
  - `TestReplaySpoolDeletesOnSuccess` / `TestReplaySpoolKeepsOnFailure`。
  - `TestConcurrentAddAndFlush`：10 个 goroutine 各 `Add` 100 次同时 `FlushFatal`，`-race` 无报错。
- [ ] **Step 2: 运行确认失败** `go test -race ./common/logreport/`。
- [ ] **Step 3: 实现。**
- [ ] **Step 4: 运行确认通过** `go test -race ./common/logreport/` → PASS。
- [ ] **Step 5: Commit** `feat(logreport): 上报队列、节流与退出前落盘`

### Task 5: 发送实现——gRPC Sync（等 Ack）与 HTTP 兜底

**Files:**
- Modify: `api/panel/grpc.go`（把 `SendTraffic` 中等待 Ack 的逻辑抽为 `sendWithAck`；新增 `SendLogs`）
- Create: `api/panel/log.go`（HTTP `ReportLogs` 与路由发送器）
- Test: `api/panel/log_test.go`，并在 `api/panel/grpc_ack_test.go` 现有用例基础上新增 Sync 日志用例

**Interfaces:**
- Consumes: Task 4 `logreport.Sender`。
- Produces:
  - `func (g *GrpcClient) sendWithAck(reportID string, req *nodev1.SyncRequest) error`（`SendTraffic` 改为调用它，行为不变）。
  - `func (g *GrpcClient) SendLogs(ctx context.Context, r *nodev1.LogReport) error`：`r.ReportId` 为空时报错；走 `sendWithAck`。
  - `func (c *ClientV2) ReportLogs(ctx context.Context, r *nodev1.LogReport) error`：`POST /v1/server/log`，`server_id` 查询参数，body 为 `protojson(UseProtoNames)`，`Content-Type: application/json`；`>=400` 返回 `上报节点日志失败: HTTP %d`。
  - `type LogSender struct{ Grpc *GrpcClient; HTTP *ClientV2 }`，实现 `logreport.Sender`：`Grpc != nil && Grpc.IsSyncConnected()` 时走 gRPC，失败再走 HTTP；否则直接 HTTP。

- [ ] **Step 1: 写失败测试**
  - `TestReportLogsHTTP`：httptest 断言 `r.URL.Path=="/v1/server/log"`、`server_id=9`、`secret_key=k`、body `protojson.Unmarshal` 后 `ReportId=="b:1"` 且首个事件 `code=="VLESS_ENCRYPTION_MODE_EMPTY"`；返回 200 → `nil`；返回 403 → 错误含 `HTTP 403`。
  - `TestLogSenderFallsBackToHTTPWhenSyncDown`：`Grpc` 为未连接客户端 → HTTP 被调用 1 次。
  - `TestSendLogsWaitsForAck`（照 `grpc_ack_test.go` 中流量 Ack 用例的假流）：面板回 `Ack{report_id, ok:true}` → `nil`；回 `ok:false` → 错误。
  - 现有 `SendTraffic` 相关测试全部保持通过。
- [ ] **Step 2: 运行确认失败** `go test ./api/panel/ -run 'Logs|LogSender'`。
- [ ] **Step 3: 实现。**
- [ ] **Step 4: 运行确认通过** `go test -race ./api/panel/` → PASS。
- [ ] **Step 5: Commit** `feat(panel): 节点日志 gRPC 与 HTTP 发送`

### Task 6: 故障点打错误码、成功点发恢复信号

**Files:**
- Modify: `core/inbound.go:281-296`（`vlessDecryption` 三处错误分别 `logreport.Wrap(CodeVlessEncUnsupported|CodeVlessEncModeEmpty|CodeVlessEncKeyEmpty, StageInbound, ...)`）
- Modify: `core/node.go:17`（`%s` → `%w`）
- Modify: `node/controller.go:60-130`（`get user list error` 用 `Wrap(CodeUserList, StageInbound, …)`；`add new node error: %s` → `%w`；`Start()` 成功末尾 `logreport.Resolve(ScopeInbound, c.info.Type, c.info.Protocol.Port, nil)`）
- Modify: `node/node.go:94-117`（`启动节点 … 失败: %s` → `%w` 并 `WithInbound`；证书不可用的 `log.Error` 带上 `Wrap(CodeCertUnavailable, StageCert, err)` 作为 `err` 字段及 `protocol/port`；`Start()` 成功返回前 `logreport.Resolve(ScopeServer, "", 0, active)`，`active` 为已成功启动入站的 `"protocol:port"`；无协议分支 `active` 为空切片）
- Modify: `node/node.go:144-158`（`Report server status failed` 改为 `Warn` 并带 `Wrap(CodeStatusReport, StageReport, err)`）
- Modify: `node/cert.go:101-121`（`reportCert(ok=true, …)` 时 `logreport.Resolve(ScopeCert, type, port, nil)`）
- Modify: 流量上报失败日志处（`node/traffic.go` 中现有 `Report traffic`/重试失败日志）带 `Wrap(CodeTrafficReport, StageReport, err)`
- Test: `core/inbound_test.go`（新增用例）、`node/log_report_test.go`

**Interfaces:**
- Consumes: Task 2 `Wrap/WithInbound/Classify`、Task 4 `logreport.Resolve`、`logreport.Default`。

- [ ] **Step 1: 写失败测试**
  - `TestVlessDecryptionCodes`：`Encryption:"mlkem768x25519plus", EncryptionMode:""` → `Classify` 得 `VLESS_ENCRYPTION_MODE_EMPTY`；模式非空、私钥空 → `VLESS_ENCRYPTION_KEY_EMPTY`；`Encryption:"foo"` → `VLESS_ENCRYPTION_UNSUPPORTED`。
  - `TestNodeStartErrorCarriesInbound`（照 `node/empty_protocols_test.go` 的构造方式，一个 vless 入站、加密模式为空）→ `Node.Start()` 的错误 `Classify` 得 `VLESS_ENCRYPTION_MODE_EMPTY, inbound, vless, 443`。
  - `TestNodeStartEmitsServerResolve`：`logreport.Default` 换成带假 Sender 的 Reporter；无协议服务器 `Start()` 成功 → 待发中有 `Scope=server`、`ActiveInbounds` 为空的恢复信号。
- [ ] **Step 2: 运行确认失败** `go test ./core/ ./node/ -run 'VlessDecryptionCodes|NodeStartErrorCarriesInbound|NodeStartEmitsServerResolve'`。
- [ ] **Step 3: 按 Files 列表修改。**
- [ ] **Step 4: 运行确认通过** `go test -race ./...` → PASS（现有测试不得回归）。
- [ ] **Step 5: Commit** `feat(node): 故障点错误码与恢复信号`

### Task 7: cmd 接线——安装 Hook、补发、退出前上报、非 0 退出

**Files:**
- Modify: `cmd/server.go`（`serverHandle`、`reload`）
- Create: `cmd/fatal.go`
- Test: `cmd/fatal_test.go`

**Interfaces:**
- Consumes: Task 3 `Install`、Task 4 `NewReporter/SetSender/Run/FlushFatal/ReplaySpool/Default`、Task 5 `LogSender`、`ClientV2.ReportLogs`。
- Produces:
  - `func fatalExit(r *logreport.Reporter, exit func(int))`：`r.FlushFatal(5*time.Second)`，失败只记 Debug；随后 `exit(1)`。生产代码传 `os.Exit`。
  - `func replaySpool(ctx context.Context, r *logreport.Reporter)`：带 5 秒超时调用 `ReplaySpool`，失败只记 Debug。

- [ ] **Step 1: 写失败测试**
  - `TestFatalExitFlushesThenExits1`：假 Sender 记录调用，`fatalExit(r, func(code){got=code})` → Sender 先被调用，`got==1`。
  - `TestReplaySpoolFailureDoesNotBlock`：Sender 阻塞 → `replaySpool` 在 ≤5.5 秒返回，spool 文件仍在。
- [ ] **Step 2: 运行确认失败** `go test ./cmd/ -run 'FatalExit|ReplaySpool'`。
- [ ] **Step 3: 修改 `serverHandle`**：
  1. 进入后先建 `logreport.Default = NewReporter(Options{Version: version, SpoolPath: filepath.Join(filepath.Dir(config), "pending-logs.json")})`，保持现有 TextFormatter 设置后调用 `logreport.Install(ring(200), sink, 配置级别, 配置输出)`；sink 为 `Default.Add`。现有 `switch c.LogConfig.Level` / `Output` 分支改为计算 `outLevel`、`out` 传给 `Install`（配置读取失败时用 `WarnLevel` + stdout）。
  2. 配置读取失败：`log.WithField("err", logreport.Wrap(CodeConfigLoad, StageConfig, err)).WithField("exiting", true).Error(...)` 后 `fatalExit`（此时无 Sender，直接落盘）。
  3. 配置读取成功后：`Default.SetSender(panel.LogSender{HTTP: panel.NewClientV2(&c.ApiConfig)})`，`replaySpool(...)`，`go Default.Run(ctx)`。
  4. gRPC 客户端建好后 `SetSender(LogSender{Grpc: grpcClient, HTTP: p})`；`reload` 成功后同样替换。
  5. `获取服务端配置失败`、`启动Xray核心失败`、`获取节点配置失败`、`启动节点失败`、`start watch failed` 每处：错误分别包 `CodePanelConfig/CodeCoreStart/CodePanelConfig/（已带码，不再包）/CodeReload`，日志加 `exiting=true`，然后 `fatalExit` 代替 `return`。
  6. `serverHandle` 顶部 `defer` 恢复 panic：`log.WithField("err", logreport.Wrap(CodePanic, StageRuntime, fmt.Errorf("%v", rec))).WithField("exiting", true).Error("节点异常退出")` 后 `fatalExit`。
  7. 收到 SIGINT/SIGTERM 正常退出：`Default.FlushFatal(2*time.Second)` 尽力发送后正常返回（退出码 0）。
  8. `reload` 返回错误时外层 `重启失败` 日志带 `Wrap(CodeReload, StageRuntime, err)`（进程不退出）。
- [ ] **Step 4: 运行确认通过** `go test -race ./...` → PASS；`go build ./...` 成功。
- [ ] **Step 5: 本机手动验证**：临时目录写配置 `ApiHost: http://127.0.0.1:1`（不可达）、`ServerID: 1`、`SecretKey: test-secret`，运行 `go run . server -c <临时目录>/config.yml; echo $?` → 拉配置失败后 ≤6 秒退出、输出 `1`；`<临时目录>/pending-logs.json` 存在，含 `PANEL_CONFIG_FETCH_FAILED` 事件且不含 `test-secret`。再运行一次 → 补发失败，旧报告保留、新报告追加，文件仍 ≤1 MB。
- [ ] **Step 6: Commit** `feat(cmd): 退出前上报节点日志并以非 0 退出`

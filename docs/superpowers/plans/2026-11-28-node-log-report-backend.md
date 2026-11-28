# 节点日志上报（面板后端 NPanel-backend）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 接收节点经 gRPC Sync / HTTP 上报的故障事件与恢复信号，合并存储，按保留天数清理，并为管理端提供查询、汇总、清空、设置接口。

**Architecture:** 新包 `internal/nodelog` 负责归一化、合并键、提示文案、存储（upsert / resolve / list / summary / clear）与设置；`internal/server` 中 gRPC Sync 新 case 与 HTTP `/v1/server/log` 共用 `handleNodeLog`；管理接口为手写 khttp 路由，挂 `/api.admin.server.v1.ServerService/*` operation 复用服务器管理权限；清理为 asynq 定时任务。

**Tech Stack:** Go、kratos khttp、ent（sqlite 测试 / MySQL 生产）、Redis（miniredis 测试）、asynq、protobuf（`make api`）。

**Spec:** `/Users/mac/Project/Go/NP/npnode/docs/superpowers/specs/2026-11-28-node-log-report-design.md`（第 4.1、4.3 节）

**仓库根：** `B=/Users/mac/Project/Go/NP/NPanelPro/NPanel-backend`。前置：节点计划 Task 1（proto）与 Task 2（`normalize_cases.json`）已完成，可直接复制。

## Global Constraints

- `api/node/v1/node.proto` 的 `LogReport / LogEvent / LogResolve` 与 `SyncRequest.log = 6` 必须与 `npnode/proto/node/v1/node.proto` 逐字一致（仅 `go_package` 不同）。
- 节点 HTTP：`POST /v1/server/log`，鉴权与 `/v1/server/status` 相同（`CompatV1ServerSecretAllowedForServer`，失败 403 `Forbidden`）；body 用 `protojson.Unmarshal`（`DiscardUnknown: true`）解析为 `LogReport`；成功返回 `compatLegacyServerJSON(ctx, nil)`。
- 去重：Redis key `node:log:%d:%s`（serverID, report_id），处理流程与流量完全一致：`SetNX pending 2m` → 成功后 `Set done 24h`，失败 `Del`；已是 `done` 视为成功。gRPC 回 `Ack{report_id, ok}`。
- 合并键：`MergeKey(code, protocol, port, message)` = `code|protocol|port`，`code=="UNKNOWN"` 时追加 `|Normalize(message)`；入库 `merge_hash = hex(sha256(fmt.Sprintf("%d|%s", serverID, MergeKey(...))))`。`Normalize` 必须通过从节点仓库复制的 `normalize_cases.json` 全部用例。
- 恢复规则（只处理 `resolved_at IS NULL AND last_at <= resolve.resolved_at`）：`inbound` → 同 protocol+port 的 `stage=inbound`；`cert` → 同 protocol+port 的 `stage=cert`；`server` → `stage IN (config, core, report, runtime)`，以及 `stage IN (inbound, cert)` 中 `protocol:port` 既不在 `active_inbounds`、也不在该服务器当前协议配置里的事件。
- 复发：命中已有行时 `count += event.count`（`count` 为 0 按 1）、更新 `level/message/context/version/exiting/last_at`，`resolved_at` 置 NULL。
- 设置：`proxy_system` 中 `category="server"`、`key="ServerLogRetentionDays"`，默认 7，合法范围 1–180，越界 PUT 返回参数错误。
- 清理：每小时（cron `"@every 1h"`，`scheduled(..., time.Hour, ...)`）按 `last_at < now - days` 删除，按 id 分批每批 5000（先查 id 再 `DELETE … WHERE id IN`，sqlite 与 MySQL 通用）。
- 管理接口（时间字段统一 unix 毫秒）：`GET /v1/admin/server/log`、`DELETE /v1/admin/server/log`、`GET|PUT /v1/admin/server/log/setting`、`GET /v1/admin/server/log/summary`，返回结构见管理端计划 Global Constraints；operation 依次为 `ServerService/ListServerLog`、`ClearServerLog`、`GetServerLogSetting`、`UpdateServerLogSetting`、`GetServerLogSummary`。
- 单条事件上限：`message` 截断到 2000 字符，`context` 最多 50 行、每行截断 1000 字符（防御节点异常数据）。
- 删除服务器时同步删除其日志。
- 代码注释用中文。

## Review Focus

- 同一 report_id 在 gRPC 与 HTTP 各发一次（节点 Sync 发送失败后改走 HTTP）→ 只累加一次。→ Task 3 `TestNodeLogDedupAcrossGrpcAndHTTP`。
- 并发：两个请求同时插入同一合并键 → 不报唯一键错误、最终 `count` 为两者之和。→ Task 2 `TestUpsertConcurrentSameKey`。
- 迟到的恢复信号：节点补发的旧恢复信号（`resolved_at` 早于事件 `last_at`）不能把新故障标为已恢复。→ Task 2 `TestResolveIgnoresOlderSignal`。
- 旧节点 / 旧面板互通：未知字段、空 `events` 与空 `resolves` 的 LogReport → 正常返回成功不写库。→ Task 3 `TestNodeLogEmptyReport`。
- 入站在面板被删除后其旧故障应在下次 `server` 恢复时一并恢复。→ Task 2 `TestResolveServerClearsRemovedInbound`。

---

### Task 1: proto 同步与数据表

**Files:**
- Modify: `$B/api/node/v1/node.proto`（复制节点仓库新增的三条消息与 `log = 6`）
- Regenerate: `cd $B && make api`
- Create: `$B/ent/schema/proxyserverlog.go`；`go generate ./ent`
- Create: `$B/internal/migrate/legacy_sql/02201_server_log.up.sql`；Modify: `$B/internal/migrate/legacy_seed.go`（`legacySQLMigrations` 追加 `{version: 2201, path: "legacy_sql/02201_server_log.up.sql"}`）
- Test: `$B/internal/migrate/` 现有迁移测试保持通过

**Interfaces:**
- Produces: ent 实体 `ProxyServerLog`，表名 `server_log`，字段：`id int64`、`server_id int64`、`merge_hash string(64)`、`level string(8)`、`code string(64)`、`stage string(16)`、`protocol string(32)`、`port int`、`message text`、`context_json text default "[]"`、`version string(32)`、`exiting bool`、`count int64 default 1`、`first_at time`、`last_at time`、`resolved_at time optional nillable`、`created_at`、`updated_at`；索引 `(server_id, merge_hash)` Unique、`(server_id, last_at)`、`(last_at)`。

- [ ] **Step 1:** 照 `ent/schema/proxysshcredential.go` 写 schema（每字段 `.Comment`，`entsql.Annotation{Table: "server_log"}`），`go generate ./ent`。
- [ ] **Step 2:** 写 `02201_server_log.up.sql`（`CREATE TABLE IF NOT EXISTS`，索引名用 ent 风格 `proxyserverlog_server_id_merge_hash` 等），登记版本。
- [ ] **Step 3:** 同步 proto 并 `make api`。
- [ ] **Step 4: 运行** `cd $B && go build ./... && go test ./internal/migrate/...` → PASS。
- [ ] **Step 5: Commit** `feat(nodelog): 节点日志 proto 与数据表`

### Task 2: nodelog 存储——合并、恢复、查询、汇总、清空、设置

**Files:**
- Create: `$B/internal/nodelog/normalize.go`、`$B/internal/nodelog/hints.go`、`$B/internal/nodelog/store.go`、`$B/internal/nodelog/setting.go`
- Create: `$B/internal/nodelog/testdata/normalize_cases.json`（从 `npnode/common/logreport/testdata/` 原样复制）
- Test: `$B/internal/nodelog/normalize_test.go`、`$B/internal/nodelog/store_test.go`、`$B/internal/nodelog/setting_test.go`

**Interfaces:**
- Produces:
  - `func Normalize(msg string) string`、`func MergeKey(code, protocol string, port int, message string) string`（规则与节点一致）。
  - `type Hint struct{ Title, Hint string }`；`func HintFor(code string) Hint`（未知码返回零值）。首批条目覆盖节点 `codes.go` 全部 13 个码，例：`VLESS_ENCRYPTION_MODE_EMPTY` → `{"VLESS 加密模式未填写", "在协议设置中补全加密模式与私钥，或把加密改为 none"}`；`CERT_UNAVAILABLE` → `{"证书不可用", "检查域名解析是否指向本机、80 端口是否可访问，或改用 DNS 验证"}`；`PANEL_CONFIG_FETCH_FAILED` → `{"拉取面板配置失败", "检查 ApiHost、ServerID 与通讯密钥是否与面板一致"}`。
  - `type Store struct{ db *ent.Client }`、`func NewStore(db *ent.Client) *Store`
  - `(*Store).Apply(ctx, serverID int64, r *nodev1.LogReport, configured []string) error`：先逐条 upsert 事件，再逐条处理恢复（`configured` 为该服务器当前协议配置的 `"protocol:port"` 列表，由调用方提供）；整体在一个事务内。
  - `type ListQuery struct{ ServerID int64; Level, Status string; Page, Size int }`；`type LogView struct{ ID, ServerID int64; Level, Code, Stage, Protocol string; Port int; Title, Hint, Message string; Context []string; Version string; Exiting bool; Count, FirstAt, LastAt int64; ResolvedAt *int64 }`（json tag 用 snake_case，时间毫秒）
  - `(*Store).List(ctx, q ListQuery) ([]LogView, int, error)`（`Status` 空视为 `open`；`Size` 上限 100）
  - `type SummaryView struct{ ServerID int64; OpenErrors, OpenWarns int; LatestTitle, LatestMessage string; LatestAt int64 }`；`(*Store).Summary(ctx) ([]SummaryView, error)`（只返回有未恢复事件的服务器；`OpenErrors` 计 `error`+`fatal`）
  - `(*Store).Clear(ctx, serverID int64) (int, error)`（0 表示全部）、`(*Store).DeleteBefore(ctx, cutoff time.Time, batch int) (int, error)`
  - `func LoadRetentionDays(ctx, db *ent.Client) (int, error)`（未配置返回 7）、`func SaveRetentionDays(ctx, db *ent.Client, days int) error`（越界返回 `ErrInvalidRetention`）；按 category 参数化，写法照 `internal/data/cert_dns.go:70-110`。

- [ ] **Step 1: 写失败测试**（`enttest.Open(t,"sqlite3","file:nodelog_<Name>?mode=memory&cache=shared&_fk=1")`）
  - `TestNormalizeCases`：读复制来的 JSON，全部相等。
  - `TestApplyMergesAndCounts`：同 code/inbound 先后两批 `count=1`、`count=3` → 1 行，`count=4`，`first_at` 为第一批、`last_at` 为第二批。
  - `TestUnknownSplitByNormalizedMessage`：两条 UNKNOWN 归一化后相同 → 1 行；不同 → 2 行。
  - `TestApplyTruncatesOversized`：`message` 5000 字符 → 存 2000；`context` 80 行 → 存 50。
  - `TestResolveInboundOnlyInboundStage`：同 vless:443 有 inbound 与 cert 两行，`Resolve{scope:inbound}` → 只有 inbound 行 `resolved_at` 非空。
  - `TestResolveIgnoresOlderSignal`：事件 `last_at=2000`，恢复 `resolved_at=1000` → 不恢复。
  - `TestResolveServerClearsRemovedInbound`：trojan:8443 的 inbound 事件，`Resolve{scope:server, active_inbounds:["vless:443"]}` 且 `configured=["vless:443"]` → 被恢复；`configured` 含 `trojan:8443` → 不恢复。
  - `TestRecurrenceReopens`：恢复后再上报同键 → `resolved_at` 为 NULL，`count` 继续累加。
  - `TestUpsertConcurrentSameKey`：两个 goroutine 同时 `Apply` 同键各 `count=1` → 1 行 `count=2`，无错误。
  - `TestListFiltersAndOrder`、`TestSummaryCountsOpenOnly`、`TestClearServerAndAll`、`TestDeleteBeforeBatches`（7 条过期 batch=3 → 删 7 条，未过期保留）。
  - `TestRetentionDefaultAndBounds`：未配置 → 7；保存 0/181 → `ErrInvalidRetention`；保存 30 → 读回 30。
- [ ] **Step 2: 运行确认失败** `cd $B && go test ./internal/nodelog/`。
- [ ] **Step 3: 实现。** upsert：事务内按 `(server_id, merge_hash)` 查；无则 Create，遇唯一键冲突回退为 Update（实现 `TestUpsertConcurrentSameKey`）。
- [ ] **Step 4: 运行确认通过** `go test -race ./internal/nodelog/`。
- [ ] **Step 5: Commit** `feat(nodelog): 节点日志合并存储与恢复`

### Task 3: 节点上报入口——gRPC Sync 与 HTTP

**Files:**
- Create: `$B/internal/server/node_log.go`（`handleNodeLog`、去重 helper、取服务器已配置入站）
- Modify: `$B/internal/server/grpc_node_sync.go:160-177`（新增 `case *nodev1.SyncRequest_Log:`，回 Ack）
- Modify: `$B/internal/server/http_compat_server.go:440`（`registerLegacyServerCompatRoutes` 内注册 `r.POST("/v1/server/log", …)`）
- Modify: `$B/internal/data/admin_server.go:85-93`（`DeleteServer` 成功后删除该服务器的 `ProxyServerLog`）
- Test: `$B/internal/server/node_log_test.go`

**Interfaces:**
- Consumes: Task 2 `Store.Apply`。
- Produces:
  - `func (s *nodeGRPCService) handleNodeLog(ctx context.Context, serverID int64, r *nodev1.LogReport) *nodev1.Ack`（gRPC 与 HTTP 共用；`ReportId` 为空时不去重直接处理）。
  - `func nodeReportOnce(ctx context.Context, rdb redis.UniversalClient, key string, fn func() error) (done bool, err error)`：抽出流量去重流程（`SetNX pending 2m` / `done 24h` / 失败 `Del`）；流量处理 `handleSyncTraffic` 改用它（行为不变，现有流量测试须全部通过）。
  - `func configuredInbounds(ctx context.Context, db *ent.Client, serverID int64) ([]string, error)`：返回 `"protocol:port"`，复用节点 `GetConfig` 处理（`internal/server/grpc_node.go`）中解析服务器协议的同一函数，不另写解析。

- [ ] **Step 1: 写失败测试**（`startNodeGRPCTestEnv`、`openSync`、`recvHello`，HTTP 用 `newSessionRouteServer` 同样方式搭）
  - `TestNodeGRPCSyncLogAckAndStore`：Sync 发 `SyncRequest_Log{report_id:"b:1", 1 个 vless:443 事件}` → 收到 `Ack{report_id:"b:1", ok:true}`，库中 1 行。
  - `TestNodeLogDedupAcrossGrpcAndHTTP`：同一 `b:2` 先 gRPC 后 HTTP → `count==1`。
  - `TestNodeLogHTTPForbidden`：错误密钥 → 403。
  - `TestNodeLogHTTPProtojson`：HTTP body 为 `protojson(UseProtoNames)` 且含未知字段 `"future_field":1` → 200 且入库。
  - `TestNodeLogEmptyReport`：空事件空恢复 → 成功、库中 0 行。
  - `TestDeleteServerRemovesLogs`。
  - 现有 `TestNodeGRPCSyncTrafficAckAndDedup`、`TestNodeGRPCSyncTrafficFailureAllowsRetry` 保持通过。
- [ ] **Step 2: 运行确认失败** `go test ./internal/server/ -run 'NodeLog|NodeGRPCSyncLog|DeleteServerRemovesLogs'`。
- [ ] **Step 3: 实现。**
- [ ] **Step 4: 运行确认通过** `go test -race ./internal/server/... ./internal/data/...`。
- [ ] **Step 5: Commit** `feat(server): 接收节点日志上报`

### Task 4: 管理接口

**Files:**
- Create: `$B/internal/server/node_log_admin.go`（`registerNodeLogAdminRoutes(srv *khttp.Server, d *data.Data)`）
- Modify: `$B/internal/server/http.go:151` 附近（注册调用）
- Test: `$B/internal/server/node_log_admin_test.go`

**Interfaces:**
- Consumes: Task 2 `Store.List/Summary/Clear`、`LoadRetentionDays/SaveRetentionDays`、`HintFor`。
- Produces: 路由（全部用 `cert_credentials.go:74-88` 的 `serverRoute` 包装，operation 前缀 `/api.admin.server.v1.ServerService/`）：
  - `GET /v1/admin/server/log` → `ListServerLog` → `{list: []LogView, total}`（`LogView.Title/Hint` 由 `HintFor` 填）
  - `DELETE /v1/admin/server/log` → `ClearServerLog` → `{deleted}`
  - `GET /v1/admin/server/log/setting` → `GetServerLogSetting` → `{retention_days}`
  - `PUT /v1/admin/server/log/setting` → `UpdateServerLogSetting`（body `{retention_days}`）→ `{retention_days}`；越界返回参数错误
  - `GET /v1/admin/server/log/summary` → `GetServerLogSummary` → `{list: []SummaryView}`（`LatestTitle` 由 `HintFor` 填）

- [ ] **Step 1: 写失败测试**：管理员 token 调用五个接口各验证一次返回结构；`PUT retention_days=0` → 参数错误；无权限角色（菜单无 `server` 权限）调用 → 40008；`GET log` 未传 `server_id` → 参数错误。测试搭建照 `cert_credentials` 现有测试（若无，照 `diagnostic_test.go` 中 `call("PUT", …, adminToken, …)` 的写法）。
- [ ] **Step 2: 运行确认失败** `go test ./internal/server/ -run ServerLogAdmin`。
- [ ] **Step 3: 实现。**
- [ ] **Step 4: 运行确认通过。**
- [ ] **Step 5: Commit** `feat(server): 节点日志管理接口`

### Task 5: 保留清理定时任务

**Files:**
- Modify: `$B/internal/queue/types/task.go`（新增 `SchedulerServerLogRetention = "scheduler:server-log:retention"`）
- Create: `$B/internal/queue/handler/server_log_retention_handler.go`
- Modify: `$B/internal/queue/handler/routes.go:95` 附近（`mux.Handle(..., scheduled(..., time.Hour, NewServerLogRetentionHandler(db, logger)))`）
- Modify: `$B/internal/data/queue_scheduler.go:94-97` 附近（`scheduler.Register("@every 1h", …, asynq.MaxRetry(1))`）
- Test: `$B/internal/queue/handler/server_log_retention_handler_test.go`

**Interfaces:**
- Consumes: Task 2 `LoadRetentionDays`、`Store.DeleteBefore`。
- Produces: `type ServerLogRetentionHandler struct`；`func NewServerLogRetentionHandler(db *ent.Client, logger log.Logger) *ServerLogRetentionHandler`；`ProcessTask(ctx, *asynq.Task) error`（`context.WithTimeout(ctx, routingRetentionBudget)`；读设置失败只记日志返回 nil，不删数据；batch 5000）。

- [ ] **Step 1: 写失败测试**（sqlite，照 `subscribe_risk_retention_handler_test.go`）：保留 7 天，插入 `last_at` 为 8 天前与 1 天前各一条 → 处理后只剩 1 天前那条；设置为 30 → 两条都保留。
- [ ] **Step 2: 运行确认失败** `go test ./internal/queue/handler/ -run ServerLogRetention`。
- [ ] **Step 3: 实现并注册。**
- [ ] **Step 4: 运行确认通过** `cd $B && go test ./...` 全部通过。
- [ ] **Step 5: Commit** `feat(queue): 节点日志按保留天数清理`

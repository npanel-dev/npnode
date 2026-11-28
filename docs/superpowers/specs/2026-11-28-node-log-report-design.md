# 节点日志上报与面板展示 设计文档

日期：2026-11-28
涉及仓库：`npnode`（节点）、`NPanelPro/NPanel-backend`（面板后端）、`NPanelPro/NPanel-frontend`（管理端）

## 1. 背景与目的

线上事故：节点 1.0.15 部署后后台一直显示离线。原因是面板下发的 vless 协议 `encryption=mlkem768x25519plus` 但 `encryption_mode` 为空，
`nodes.Start()` 失败后进程直接 `return`（退出码 0），systemd 每 10 秒重启一次，状态上报任务从未运行。
排查只能 SSH 上机器看 `journalctl`。

目的：节点配置错误、无法启动、运行中出错时，管理员在「服务器管理」里直接看到**具体是什么问题、怎么处理**，不再需要登录服务器。

## 2. 目标 / 非目标 / 反目标

**目标**
- 节点把错误、警告作为「故障事件」上报，面板格式化展示（时间 / 级别 / 阶段 / 入站 / 原因 / 处理建议），每条附原始日志上下文。
- 节点因错误退出前，**必须先上报**；上报不出去时落盘，下次启动先补发。
- 面板可设置日志保留天数，可一键清空全部日志，也可清空单台服务器日志。
- 上报通道：gRPC Sync 为主，HTTP 兜底（gRPC 不可用的部署同样可用）。

**非目标**
- 不做完整的远程日志流（实时 tail 全部 info/debug 日志）。
- 不做告警推送（Telegram / 邮件），后续可基于本表扩展。
- 不处理节点连不上面板、密钥错误这类面板天然收不到的情况（只显示「最后上报时间」）。

**反目标（做了也算失败）**
- 重启循环把日志表刷爆：同一问题重启 90 次产生 90 条。
- 上报阻塞或拖慢节点正常运行，或上报失败导致节点无法启动。
- 原始日志里带出密钥（`SecretKey`、私钥、用户 UUID 等）。

## 3. 方案选择

| 方案 | 说明 | 取舍 |
|---|---|---|
| A. 仅 HTTP 新接口 | 节点 `POST /v1/server/log`，本地落盘补发 | 最简单，但与 gRPC 优先的现有架构不一致 |
| **B. gRPC Sync 新 payload + HTTP 兜底（已选）** | Sync 流已建立时走 `SyncRequest.log`；未建立（启动阶段、回退 HTTP）时走 HTTP | 与证书上报 `CertReport` 一致；需要维护两条入口，但面板侧共用同一处理函数 |
| C. 存 system 表 JSON | 照抄 `cert_node_status` | 无法按时间保留、分页、筛选，不采用 |

用户选择 B。注意：启动失败发生在 `StartSync` 之前，此时 Sync 尚未建立，**启动类故障实际都走 HTTP**；运行期故障走 Sync。

## 4. 设计

### 4.1 事件模型（proto，节点与面板两份 `node.proto` 同步修改）

```proto
message SyncRequest {
  oneof payload {
    ...
    LogReport log = 6;
  }
}

// LogReport 一批节点故障事件与恢复信号
message LogReport {
  repeated LogEvent events = 1;
  string report_id = 2;   // 面板按 report_id 去重并回 Ack，未 Ack 用同一 ID 重发（同流量上报）
  repeated LogResolve resolves = 3;
}

// LogResolve 恢复信号：某个范围内的问题已经好了，面板把对应的未恢复事件标记为已恢复
message LogResolve {
  string scope = 1;       // server：节点整体启动成功；inbound：某入站启动成功；cert：某入站证书可用
  string protocol = 2;    // scope 为 inbound / cert 时必填
  uint32 port = 3;
  repeated string active_inbounds = 4; // scope=server 时附当前已启动的入站 "protocol:port"
  int64 resolved_at = 5;  // unix 毫秒
}

message LogEvent {
  string level = 1;       // error / warn / fatal
  string code = 2;        // 稳定错误码，如 VLESS_ENCRYPTION_MODE_EMPTY；无法归类为 UNKNOWN
  string stage = 3;       // config / core / inbound / cert / report / runtime
  string protocol = 4;    // 关联入站，可空
  uint32 port = 5;
  string message = 6;     // 原始错误文本（已脱敏）
  repeated string context = 7; // 事件前后的原始日志行（已脱敏，最多 50 行）
  int64 occurred_at = 8;  // unix 毫秒
  string version = 9;     // 节点版本
  bool exiting = 10;      // 节点因此事件退出
  uint32 count = 11;      // 节点节流窗口内该事件累计次数（≥1），面板 count += count
}
```

HTTP 兜底：`POST /v1/server/log?server_id=&secret_key=`，body 为 `LogReport` 的 JSON 形式，鉴权方式与 `/v1/server/status` 一致。

### 4.2 节点侧（npnode）

1. **采集**：logrus Hook。
   - 环形缓冲保留最近 200 行，用作上下文。【实现决定】logger 内部级别设为 Debug 以便 Hook 收到全部日志，
     控制台 / 文件输出改由自定义 Writer 按 `Log.Level` 过滤，对外可见的日志输出与现在一致。
   - 【实现决定】**只有带 `code` 字段的 warn 及以上**生成事件；没有 `code` 的日志仅当级别为 error / fatal / panic 时以 `UNKNOWN` 上报。
     这样「证书申请成功」「gRPC 已恢复」这类被打成 Warn 的正常信息不会进入故障列表，也无需逐处降级。
   - `code/stage/protocol/port` 从日志字段读取（`log.WithFields` 加 `code`、`stage` 等）。
   - 已知故障点补上错误码：拉配置失败、Xray 核心启动失败、入站构建失败（vless 加密缺模式 / 私钥、不支持的加密方式等）、证书不可用、状态 / 流量上报失败。
2. **脱敏**：上报前替换 `SecretKey`、`encryption_private_key`、Reality 私钥、UUID 样式字符串。
3. **节流合并**：【实现决定】合并键 = `code+protocol+port`；`code=UNKNOWN` 时再加上**归一化后的 message**
   （去掉数字、IP、端口、十六进制串、引号内容）。同一合并键 60 秒内只上报一次，`count` 带上期间累计次数。
   节点与面板使用同一归一化函数规则（两边各实现一份，附相同测试用例）。
4. **发送**：后台队列每 10 秒或攒满 20 条发一批；Sync 已连接走 gRPC，否则走 HTTP。失败保留在内存队列（上限 500 条，超出丢最旧）。
5. **退出前必须上报**：`cmd/server.go` 中所有 `return` 的错误路径以及 `panic` 恢复处，统一调用 `logreport.FlushFatal(timeout=5s)`：
   - 同步走 HTTP 发送全部待发事件（此时一定没有 Sync）。
   - 失败或超时则写入 `<配置目录>/pending-logs.json`（上限 1MB）。
   - 进程以**非 0** 退出码退出（顺带修正现在错误退出码为 0 的问题）。
6. **补发**：启动时读取配置后、拉取面板配置前，先补发 `pending-logs.json`，成功后删除；失败保留，不阻塞启动。
7. **重启循环**：因合并键相同，面板只看到 1 条事件、次数不断增加。
8. **恢复上报点**（与事件走同一队列、同一通道）：
   - 某入站 `controller.Start()` 成功 → `LogResolve{scope:inbound, protocol, port}`。
   - 证书变为可用（`CertReport.ok=true` 的同一时机）→ `LogResolve{scope:cert, protocol, port}`。
   - `nodes.Start()` 返回成功（含「服务器还没有协议」分支）→ `LogResolve{scope:server, active_inbounds}`。
   - 热重载成功后同样发送 `scope:server`。
   - 恢复信号不节流，但同一 scope 连续重复的只保留最新一条。

### 4.3 面板后端（NPanel-backend）

1. **数据表** `proxy_server_log`（ent schema + 迁移）：
   `id, server_id, level, code, stage, protocol, port, message, context(text, JSON 数组), version, exiting, count, first_at, last_at, created_at`
   - 合并键：`server_id` + 节点合并键（见 4.2 第 3 点）的哈希。命中则 `count += event.count`、更新 `message/last_at/context/version`，否则插入。
   - 【实现决定】`report_id` 去重沿用流量上报的做法（Redis + TTL 24 小时），gRPC 重发与 HTTP 补发都先查重再累加。
   - 索引：`(server_id, last_at)`、合并键唯一索引。
   - 表增加 `resolved_at`（可空）。未恢复 = `resolved_at IS NULL`。
   - **恢复规则**（只处理 `resolved_at IS NULL` 且 `last_at <= resolve.resolved_at` 的事件，避免旧信号覆盖新故障）：
     - `scope=inbound`：恢复该服务器同 `protocol+port` 的 `stage=inbound` 事件。
     - `scope=cert`：恢复该服务器同 `protocol+port` 的 `stage=cert` 事件（入站启动成功不代表证书问题已解决）。
     - `scope=server`：该服务器 `stage in (config, core, report, runtime)` 的事件，以及**协议端口不在 `active_inbounds` 且已不在面板该服务器协议配置中**的入站事件（入站被删除后其旧故障随之恢复）。
     - 空 stage 或未知 stage 的事件按 runtime 处理（节点与面板两端都做此归一）。
   - **复发**：已恢复的事件再次上报时，`resolved_at` 置空、`count` 继续累加、`last_at` 更新，列表中重新出现在「未恢复」。
2. **入口**：gRPC `grpc_node_sync.go` 新增 `case *nodev1.SyncRequest_Log`，HTTP `http_compat_server.go` 新增 `/v1/server/log`，共用 `handleNodeLog`。
3. **错误码提示文案**：面板内置 `code → 中文标题 + 处理建议` 对照表（如 `VLESS_ENCRYPTION_MODE_EMPTY` → 「VLESS 加密模式未填写，请在协议设置中补全加密模式与私钥，或把加密改为 none」）。未知码显示原文。
4. **保留与清理**：
   - 设置项存 system 表：`ServerLogRetentionDays`，默认 7，范围 1–180。
   - 新增定时任务 `scheduler:server-log:retention`，每小时按 `last_at` 分批删除过期记录（参照拉订阅日志清理任务）。
   - 删除服务器时级联删除其日志。
5. **管理接口**（RBAC 复用服务器管理权限）：
   - `GET /v1/admin/server/log?server_id=&level=&status=open|resolved|all&page=&size=`，默认 `open`，按 `last_at` 倒序。
   - `DELETE /v1/admin/server/log?server_id=`：带 `server_id` 清空该服务器，不带则清空全部。
   - `GET/PUT /v1/admin/server/log/setting`：保留天数。
   - 每台服务器**未恢复**的 `error` 数、`warn` 数和最近一条未恢复事件摘要。【计划阶段调整】改为独立接口 `GET /v1/admin/server/log/summary`，
     前端按服务器 ID 合并（与证书状态 `cert_status` 做法一致），不改生成式的服务器列表 proto。

### 4.4 管理端（NPanel-frontend，`sections/servers`）

- 服务器列表：状态列旁显示红色徽标「N 个故障」（只计未恢复的 error，与现有 `CertIssueBadge` 同风格），悬停显示最近一条标题；全部恢复后徽标消失。
- 每行操作增加「日志」，打开抽屉：
  ```
  ┌ 服务器 #1 日志                     [未恢复|已恢复|全部] [级别▾] [清空本服务器] ┐
  │ ● 错误  入站启动失败 · vless:443       ×90  2 分钟前        │
  │   VLESS 加密模式未填写                                      │
  │   建议：在协议设置中补全加密模式与私钥，或把加密改为 none   │
  │   ▸ 原始日志（50 行）        首次 11:21:56 · 版本 1.0.15    │
  │ ● 警告  面板 gRPC 不可用，已改用 HTTP   ×90  2 分钟前       │
  └─────────────────────────────────────────────────────────────┘
  ```
  原始日志展开后用等宽字体，按 `level=` 着色，`msg/err` 字段高亮。
- 页面工具栏增加「日志设置」：保留天数输入框 +「清空所有服务器日志」（二次确认）。
- 文案走 i18n（zh-CN、en）。

## 5. 约束

- 两个仓库的 `node.proto` 必须保持一致；新增字段只追加、不改编号，旧节点、旧面板互不影响（旧面板忽略未知 payload，旧节点不发送）。
- HTTP 兜底接口要兼容走 `/api` 前缀剥离的部署。
- 节点上报全部异步、带上限；只有退出路径允许同步等待（≤5 秒）。

## 6. 留给实现者的事项

- 已知错误点的错误码清单在实现时补全，首批至少覆盖第 4.2 节第 1 点列出的场景。
- 抽屉的具体组件、分页方式沿用 `sections/servers` 现有写法。
- 迁移文件编号按 NPanel-backend 现有规则取下一个。

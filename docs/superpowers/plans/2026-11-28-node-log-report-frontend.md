# 节点日志上报（管理端 NPanel-frontend）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在「服务器管理」里显示未恢复故障徽标、按服务器查看格式化日志（可筛选、展开原始日志、清空），并提供保留天数设置与清空全部。

**Architecture:** 手写服务函数 `node-log.ts` 对接后端四个接口；`sections/servers/node-logs/` 下拆分徽标、抽屉、原始日志查看、设置四个组件，用 @tanstack/react-query 拉数据；原始日志着色用纯函数 `parseLogLine`。

**Tech Stack:** React + TS、@tanstack/react-query、shadcn（Sheet / Badge / ToggleGroup / Select / Collapsible / ScrollArea）、`ConfirmButton`、react-i18next、vitest + @testing-library/react、biome。

**Spec:** `/Users/mac/Project/Go/NP/npnode/docs/superpowers/specs/2026-11-28-node-log-report-design.md`（第 4.4 节）

**仓库根：** `R=/Users/mac/Project/Go/NP/NPanelPro/NPanel-frontend`，`A=$R/apps/admin`，`U=$R/packages/ui/src`。依赖后端计划 `2026-11-28-node-log-report-backend.md` 的接口；开发期可用 `vi.mock` 独立完成。

## Global Constraints

- 接口契约（与后端计划一致，时间均为 unix 毫秒）：
  - `GET /v1/admin/server/log?server_id=&level=&status=open|resolved|all&page=&size=` → `data: { list: ServerLog[]; total: number }`，默认 `status=open`，按 `last_at` 倒序。
  - `DELETE /v1/admin/server/log?server_id=`：带 `server_id` 清空该服务器，不带清空全部 → `data: { deleted: number }`。
  - `GET /v1/admin/server/log/setting` / `PUT`（body `{ retention_days }`）→ `data: { retention_days: number }`，范围 1–180，默认 7。
  - `GET /v1/admin/server/log/summary` → `data: { list: ServerLogSummary[] }`。
  - `ServerLog = { id; server_id; level: "error"|"warn"|"fatal"; code; stage; protocol; port; title; hint; message; context: string[]; version; exiting: boolean; count; first_at; last_at; resolved_at: number|null }`（`title/hint` 由后端按错误码生成，未知码时 `title` 为空，前端显示 `message`）。
  - `ServerLogSummary = { server_id; open_errors; open_warns; latest_title; latest_message; latest_at }`。
- 徽标只计 `open_errors`（未恢复的 error + fatal）；为 0 时不渲染。
- 文案全部走 `useTranslation("servers")` 的 `nodeLogs.*` 嵌套键，zh-CN 与 en-US 同步（`$A/public/assets/locales/{zh-CN,en-US}/servers.json`）。
- 服务请求统一用 `@workspace/ui/lib/request`，URL 前缀 `${import.meta.env.VITE_API_PREFIX || ""}`。
- 数据刷新：summary `refetchInterval: 60_000`（与证书状态一致）；抽屉打开时列表 `refetchInterval: 15_000`。

## Review Focus

- 某服务器从未上报过日志（summary 里没有它）→ 不显示徽标、抽屉显示「暂无日志」，不报错。→ Task 3 `renders nothing when server absent from summary`、Task 4 `shows empty state`。
- 原始日志行很长或含非 `key=value` 文本（Xray 自己的 `[Warning] core: ...` 行）→ 不崩、按原文显示且自动换行。→ Task 2 `parseLogLine keeps non-logfmt lines`。
- 清空后列表与徽标立即消失，不用等轮询。→ Task 4 `clear invalidates list and summary`。
- 保留天数输入 0、181、空、小数 → 保存按钮禁用。→ Task 5 `disables save for out-of-range values`。
- `resolved_at` 有值的事件在「全部」视图中应显示「已恢复 · 时间」，与未恢复的区分开。→ Task 4 `marks resolved events`。

---

### Task 1: 服务函数与类型

**Files:**
- Create: `$U/services/admin/node-log.ts`
- Test: `$U/services/admin/node-log.test.ts`

**Interfaces:**
- Produces（全部返回 `data.data`）：
  - `type ServerLog`、`type ServerLogSummary`、`type ServerLogStatus = "open"|"resolved"|"all"`（字段见 Global Constraints）。
  - `listServerLogs(q: { server_id: number; level?: string; status?: ServerLogStatus; page: number; size: number }): Promise<{ list: ServerLog[]; total: number }>`
  - `clearServerLogs(serverId?: number): Promise<{ deleted: number }>`
  - `getServerLogSetting(): Promise<{ retention_days: number }>`、`updateServerLogSetting(retentionDays: number): Promise<{ retention_days: number }>`
  - `getServerLogSummary(): Promise<ServerLogSummary[]>`

- [ ] **Step 1: 写失败测试**（`vi.mock("@workspace/ui/lib/request")`）：`listServerLogs({server_id:1,status:"open",page:1,size:20})` 调用 `GET …/v1/admin/server/log` 且 `params` 相等；`clearServerLogs()` 调用 `DELETE` 且无 `server_id`；`clearServerLogs(3)` 带 `params:{server_id:3}`；`updateServerLogSetting(14)` 发 `PUT` 且 `data:{retention_days:14}`。
- [ ] **Step 2: 运行确认失败** `cd $A && bunx vitest run ../../packages/ui/src/services/admin/node-log.test.ts`。
- [ ] **Step 3: 实现**，写法照 `$U/services/admin/diagnostic.ts:111-228` 的 `Envelope/Page/base` 模式。
- [ ] **Step 4: 运行确认通过。**
- [ ] **Step 5: Commit** `feat(admin): 节点日志服务函数`

### Task 2: 原始日志行解析与查看组件

**Files:**
- Create: `$A/src/sections/servers/node-logs/parse-log-line.ts`、`$A/src/sections/servers/node-logs/raw-log-view.tsx`
- Test: `$A/src/sections/servers/node-logs/parse-log-line.test.ts`

**Interfaces:**
- Produces:
  - `type LogToken = { text: string; kind: "level-error"|"level-warn"|"level-info"|"level-debug"|"key"|"msg"|"err"|"plain" }`
  - `parseLogLine(line: string): LogToken[]`：识别 `level=xxx`（按级别着色）、`msg=...`、`err=...`（高亮），其余 `key=` 为 `key`；整行不是 logfmt 时返回单个 `plain`。拼接所有 `text` 必须等于原行。
  - `<RawLogView lines={string[]} />`：`font-mono text-xs`，`<pre className="whitespace-pre-wrap break-words">`，每行按 token 着色（error 红、warn 琥珀、debug 灰、msg 前景加粗、err 红色下划线），外层 `ScrollArea` 最大高度 `max-h-80`。

- [ ] **Step 1: 写失败测试** `it.each`：
  - `level=error msg=启动节点失败 err=vless encryption mode is empty` → 含 `{kind:"level-error"}`、`{kind:"msg"}`、`{kind:"err"}`，且 `tokens.map(t=>t.text).join("")===line`。
  - `[Warning] core: Xray 26.10.31 started` → 单个 `plain`（`parseLogLine keeps non-logfmt lines`）。
  - 空字符串 → `[]`。
- [ ] **Step 2: 运行确认失败** `cd $A && bunx vitest run src/sections/servers/node-logs/parse-log-line.test.ts`。
- [ ] **Step 3: 实现。**
- [ ] **Step 4: 运行确认通过。**
- [ ] **Step 5: Commit** `feat(admin): 原始日志着色解析`

### Task 3: 故障徽标

**Files:**
- Create: `$A/src/sections/servers/node-logs/node-log-badge.tsx`
- Modify: `$A/src/sections/servers/index.tsx:240`（旁边加 `const { data: logSummary } = useServerLogSummary();`）、`index.tsx:378-405`（status 列 flex 内追加 `<NodeLogBadge summary={findLogSummary(logSummary, row.original.id)} onClick={() => openLogs(row.original)} />`）
- Test: `$A/src/sections/servers/node-logs/node-log-badge.test.tsx`

**Interfaces:**
- Consumes: Task 1 `getServerLogSummary`、`ServerLogSummary`。
- Produces: `useServerLogSummary()`（queryKey `["getServerLogSummary"]`，60 秒轮询）、`findLogSummary(list: ServerLogSummary[]|undefined, serverId: number): ServerLogSummary|undefined`、`<NodeLogBadge summary? onClick? />`：`open_errors>0` 时 `<Badge variant="destructive">` 文案 `nodeLogs.badge`（`{{count}} 个故障` / `{{count}} issues`），原生 `title` 为 `latest_title || latest_message` + `\n` + `formatDate(latest_at)`。

- [ ] **Step 1: 写失败测试**（`// @vitest-environment jsdom`）：`open_errors:2` → 文本含 `2`，`title` 含 latest_title；`open_errors:0` → 不渲染；`summary` 为 `undefined` → 不渲染（`renders nothing when server absent from summary`）；点击触发 `onClick`。
- [ ] **Step 2: 运行确认失败。**
- [ ] **Step 3: 实现并接入 index.tsx。**
- [ ] **Step 4: 运行确认通过** `bunx vitest run src/sections/servers`。
- [ ] **Step 5: Commit** `feat(admin): 服务器列表故障徽标`

### Task 4: 日志抽屉

**Files:**
- Create: `$A/src/sections/servers/node-logs/node-log-sheet.tsx`、`$A/src/sections/servers/node-logs/node-log-item.tsx`
- Modify: `$A/src/sections/servers/index.tsx:62-203`（`ServerRowActions` 菜单在「编辑」后加「日志」按钮，打开抽屉）；`index.tsx` 顶层持有 `logServer: API.Server | null` 状态，徽标点击与菜单按钮共用同一个 `<NodeLogSheet server={logServer} onOpenChange=… />`
- Test: `$A/src/sections/servers/node-logs/node-log-sheet.test.tsx`

**Interfaces:**
- Consumes: Task 1 `listServerLogs/clearServerLogs`、Task 2 `RawLogView`。
- Produces: `<NodeLogSheet server={API.Server|null} onOpenChange={(open:boolean)=>void} />`、`<NodeLogItem log={ServerLog} />`。

- [ ] **Step 1: 写失败测试**（`vi.hoisted` + `vi.mock("@workspace/ui/services/admin/node-log")`，QueryClientProvider 包裹，照 `client-diagnostics.test.tsx`）：
  - 默认请求 `status:"open", page:1, size:20`；切到「全部」后请求 `status:"all"`；级别选 `error` 后请求带 `level:"error"`。
  - 渲染一条 `{level:"error", title:"VLESS 加密模式未填写", hint:"…", protocol:"vless", port:443, count:90}` → 可见 `vless:443`、`×90`、标题与建议；点击「原始日志（N 行）」后可见 context 第一行。
  - `title:""` 时显示 `message`。
  - `resolved_at` 有值 → 显示 `nodeLogs.resolvedAt`（`marks resolved events`）。
  - 列表为空 → 显示 `nodeLogs.empty`（`shows empty state`）。
  - 点「清空本服务器」并确认 → 调用 `clearServerLogs(server.id)`，随后 `invalidateQueries` 了 `["serverLogs", id]` 与 `["getServerLogSummary"]`（`clear invalidates list and summary`）。
- [ ] **Step 2: 运行确认失败。**
- [ ] **Step 3: 实现。** 布局照设计文档 4.4 草图：头部为标题 +`ToggleGroup`（未恢复 / 已恢复 / 全部）+ 级别 `Select`（全部 / 错误 / 警告）+ `ConfirmButton confirmVariant="destructive"`；条目显示级别点、`title||message`、`stage` 中文名、`protocol:port`、`×count`、相对时间（`last_at`），次行 `hint`，底行 `Collapsible` 展开 `RawLogView` 与「首次 · 版本 · 退出标记」。分页用「加载更多」（`size=20` 递增 page）。Sheet 宽度 `sm:max-w-3xl`（同 client-diagnostics）。
- [ ] **Step 4: 运行确认通过。**
- [ ] **Step 5: Commit** `feat(admin): 服务器日志抽屉`

### Task 5: 日志设置（保留天数、清空全部）

**Files:**
- Create: `$A/src/sections/servers/node-logs/node-log-settings.tsx`
- Modify: `$A/src/sections/servers/index.tsx:461-488`（toolbar 里 `ServerForm` 前加 `<NodeLogSettings />`）
- Test: `$A/src/sections/servers/node-logs/node-log-settings.test.tsx`

**Interfaces:**
- Consumes: Task 1 `getServerLogSetting/updateServerLogSetting/clearServerLogs`。
- Produces: `<NodeLogSettings />`：按钮「日志设置」打开 Popover/Dialog，内含 `Input type="number" min=1 max=180 step=1 className="w-24"` + 保存；下方 destructive `ConfirmButton`「清空所有服务器日志」。

- [ ] **Step 1: 写失败测试**：加载后输入框为 `7`；输入 `0`、`181`、空、`1.5` 时保存禁用（`disables save for out-of-range values`）；输入 `30` 保存 → `updateServerLogSetting(30)`；确认清空全部 → `clearServerLogs()` 无参数，且失效 `["getServerLogSummary"]`。
- [ ] **Step 2: 运行确认失败。**
- [ ] **Step 3: 实现**，照 `$A/src/sections/user/client-diagnostics.tsx:825-930` 的 `DiagnosticPolicyEditor` 写法。
- [ ] **Step 4: 运行确认通过。**
- [ ] **Step 5: Commit** `feat(admin): 节点日志保留与清空设置`

### Task 6: 文案与整体检查

**Files:**
- Modify: `$A/public/assets/locales/zh-CN/servers.json`、`$A/public/assets/locales/en-US/servers.json`（新增 `nodeLogs` 对象）

**Interfaces:**
- `nodeLogs` 键：`badge`、`menu`、`title`、`statusOpen`、`statusResolved`、`statusAll`、`levelAll`、`levelError`、`levelWarn`、`clearServer`、`clearServerConfirm`、`clearAll`、`clearAllConfirm`、`rawLog`、`firstAt`、`version`、`exiting`、`resolvedAt`、`empty`、`loadMore`、`settings`、`retentionDays`、`retentionHint`、`save`、`stage.config|core|inbound|cert|report|runtime`。

- [ ] **Step 1: 补全两份 JSON**（中文例：`stage.inbound`=「入站启动」、`clearAllConfirm`=「将删除所有服务器的节点日志，无法恢复。」、`retentionHint`=「超过天数的日志会被自动清理（1–180 天）」）。
- [ ] **Step 2: 运行** `cd $A && bun run i18n:sync`，确认无缺失键告警。
- [ ] **Step 3: 全量检查** `cd $A && bunx vitest run src/sections/servers ../../packages/ui/src/services/admin/node-log.test.ts && bunx tsc && bun run check` → 全部通过。
- [ ] **Step 4: 在浏览器中手动验证**（`run` skill 启动 admin，连本地后端）：徽标、抽屉筛选、展开原始日志、清空、设置保存。
- [ ] **Step 5: Commit** `feat(admin): 节点日志文案`

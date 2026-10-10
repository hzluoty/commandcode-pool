# 上游同步记录（commandcode-proxy → commandcode-pool）

本文件记录每一次与上游 [`commandcode-proxy`](https://github.com/MAXeaglet/commandcode-proxy) 的
功能同步：同步点、逐条处置与**刻意不移植**的差异。号池是 Cloudflare Worker + D1 的多账号实现，
上游是单账号 Node 进程，因此同步按**功能/协议行为逐条移植**，绝不整文件搬运。

## 本次同步点

| 项 | 值 |
| --- | --- |
| 上游仓库 | `C:\Users\Xuyuxuan\Downloads\github\commandcode-proxy`（origin: MAXeaglet/commandcode-proxy） |
| 上游 HEAD | `972828a` — docs: remove Contributors section from README（2026-10-09） |
| 号池基线 | `9be2445` — fix: usage tab rolls forward and API responses are never cached（2026-09-27） |
| 权威协议来源 | Provider API 官方文档 <https://commandcode.ai/docs/provider>（端点、错误封套、ZDR、usage） |

## 逐条处置（窗口 2026-09-27 → 2026-10-09）

| 上游提交 | 内容 | 处置 |
| --- | --- | --- |
| `40b338e` | `tool_choice: none` 清空 tools 且不下发 tool_choice（#47） | **已移植**：`lib/api-adapters.js` 的 `disableTools()`（三条协议面通用） |
| `40b338e` | 释放 `respReq` 内存（#31） | **已移植（Worker 形式）**：`lib/generation-runner.js` 在 fetch 后 `prepared.body = null` |
| `aa4a3c1` / `a474cbc` | 零输出/非法 finishReason 不得谎报成功（#56/#61） | **部分移植**：① 零字节流以本协议错误帧收尾（见下）；② chat 流的失败族 `finish_reason`（`error`/`upstream_error`/`connection_error`）不再记账为成功。**不移植**：「上游发了标准终止事件但内容为空」按上游原样透传（那是 Provider 自己的权威终止语义） |
| `ce5a217` / `639fc6e` | 未吐字前的连接层闪断透明重试（#50/#55） | **不移植（刻意）**：与「单请求单账号单次尝试」冲突。号池的取舍是：闪断直接映射 502 返回客户端，账号状态不动（不冷却、不换号），由客户端自行重试 |
| `26fbae0` | CI：Node 22/24 矩阵 + Bun job | **部分移植**：新增 `.github/workflows/test.yml`（Node 22/24）。**不加 Bun**：本仓库产物是 Cloudflare Worker，Bun 不是运行目标 |
| `2eccdbf` / `7f6bf21` / `8c45a05` | Responses 图片槽位/工具截图的协议转换 | **不适用**：那是上游 CLI wire ↔ OpenAI 的转换层；号池原生透传 Provider API，图片由上游原生处理 |
| `92ead01` / `99b9786` | 兜底 `session_id` 从请求体派生以优化提示缓存 | **不适用**：只作用于 CLI 信封的 `threadId` / `x-session-id`（未公开 `/alpha/generate`）。号池的缓存命中靠 **sticky 调度**保证同账号连续命中 |
| `972828a` | README 文档 | 不适用 |

## 本次新增的号池侧行为（超出上游窗口、审计中发现的缺口）

| 项 | 位置 | 依据 |
| --- | --- | --- |
| 按协议面的错误封套 | `lib/failure-policy.js` `clientErrorBody()`；`worker.js` `surfaceError()` | 官方文档：`/v1/messages` 回 `{"type":"error","error":{...}}`，其余回 OpenAI 封套 |
| 零字节流的终止错误帧 | `lib/generation-runner.js` + `streamErrorFrame()` | `aa4a3c1` 的同类问题：状态码已提交时只能在线格上说真话 |
| 模型目录 5 分钟缓存 | `worker.js` `getModels()` | 上游 `modelRefreshIntervalMs`；减少拿账号 Key 打控制面的次数（封号风险面） |
| 入站 body 上限 `MAX_BODY_MB`（默认 32MB → 413） | `worker.js` `handleGenerate()` | 上游 `CC_MAX_BODY_MB`，但阈值按 Worker 的 128MB 内存收紧 |
| 413 前先取消入站流 | `worker.js` `handleGenerate()` | workerd 在入站 body 未读完时回响应会把连接重置（客户端只见 network connection lost）；`request.body.cancel()` 后回 413，实测同一连接上的后续请求正常 |
| 本地 mock：`empty` 形态 + 多线程 | `mock_server.py` | 零字节 SSE 用于手工验证新行为；`slow` 等阻塞应答不再卡住后续请求 |

## 刻意保留的差异（再次同步时不要「修掉」）

1. **绝不重试、绝不换号**：一个逻辑请求只选一个账号、只发一次上游请求；失败即映射返回并（仅对 401/402/429）更新账号健康。
2. **不伪造 CLI 身份**：只发 `Authorization` / `Content-Type` / `Accept` / 可选 `x-cmd-zdr`，不发送 CLI 版本头、设备指纹、生命周期事件。
3. **只走公开端点**：生成链路的路径白名单锁定 `/provider/v1/{chat/completions,messages,responses}`（`lib/upstream-client.js`）。
4. **不做空闲断连（idle kill）**：上游「一个 token 都不吐」的静默期可达数百秒，Worker 侧不设 idle timeout，避免误杀合法长思考；客户端中断会立即同步终止上游连接。
5. **403 不改账号健康**：官方文档把 403 定义为 `upgrade_required`（计划级），不是密钥失效；长期 403 的账号由管理员在面板里禁用。
6. **`prompt_cache_key` 一律剥离**：不属于 Provider 公开 schema，且不得影响账号选择（`lib/api-adapters.js`）。

## 残留风险（已知，未修）

- **管理端额度面板**：`POST /api/refresh` 会拿账号 Key 调 `/alpha/whoami`、`/alpha/billing/*`、`/alpha/usage/summary` 等未公开端点（来自 commandcode-usage 的口径）。生成链路不碰这些端点；如要彻底避免，别用刷新按钮。
- **计划级 403 的粘性**：若某账号在加入后被降级到无 API 权限的计划，sticky 调度会持续选中它并持续返回 403；需人工禁用该账号（或在面板观察 `last_error`）。
- **模型目录缓存是 isolate 级**：不同 isolate 会各自抓一次，缓存不跨节点共享（不影响正确性）。

## 下次同步怎么做

```bash
cd /mnt/c/Users/Xuyuxuan/Downloads/github/commandcode-proxy
git fetch origin && git log --oneline <上次同步点>..HEAD        # 本文件顶部的「上游 HEAD」即上次同步点
```

对每条提交先判断它作用在哪一层：

- **CLI 信封 / 设备身份 / 重试 / 协议转换** → 与号池架构冲突或不适用，记录到本文件即可；
- **Provider API 语义（错误封套、usage、零输出、tool_choice、体积上限、目录）** → 按 `lib/` 边界移植，并在 `test/test.mjs` 补断言。

移植完跑 `npm test`（自带 Provider mock，离线），本文件顶部的同步点更新为新的上游 HEAD。

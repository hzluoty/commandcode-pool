# commandcode-pool

Cloudflare Worker：把多个 Command Code 账号组成号池，对外提供 **OpenAI / Anthropic / Responses 兼容 API**：

- `POST /v1/chat/completions` — OpenAI Chat Completions（流式 + 非流式）
- `POST /v1/messages` — Anthropic Messages API（流式 + 非流式，含 thinking / tool_use）
- `POST /v1/responses` — OpenAI Responses API（流式 + 非流式，Codex 等客户端）
- `GET /v1/models` — 模型目录（10 分钟缓存，失败回退内置列表）

协议层逐字段对齐 [commandcode-proxy](https://github.com/MAXeaglet/commandcode-proxy)
（wire 协议版本 `1.53.1`）：9 键 CLI 信封、`User-Agent: cli`、`traceparent`、
`x-project-slug`、确定性设备指纹（`/alpha/fingerprint/record`）、生命周期事件
（`/alpha/lifecycle-events`）、每 key 会话（`x-session-id` / `threadId`）。
号池调度与多账号额度面板参考 [commandcode-usage](https://github.com/MAXeaglet/commandcode-usage)。

## 为什么默认「粘住单账号」（cache 优先）

上游的 prompt cache 是**按账号（API Key）维度**计费的：请求命中缓存时
`usage.prompt_tokens_details.cached_tokens` 会显著大于 0，额度消耗大幅下降。

如果号池每个请求都轮换账号，每个账号的缓存永远 miss，等于全部按原价烧额度。
因此本 Worker 默认调度策略为 **sticky**：

1. 每次请求优先复用「最近使用」的那个账号，持续命中它的 prompt cache；
2. 直到该账号额度耗尽（`429`/`402` 冷却，按上游 `Retry-After`）或密钥失效（`401` 自动拉黑），
   才切换到下一个可用账号；
3. 冷却结束后**不会立即切回去**——新账号会一直用到它也耗尽为止，缓存始终连续命中。

想均摊负载（牺牲缓存）设 `POOL_STRATEGY=round_robin`（最久未用优先）。

同一请求内的故障转移：`429`/`402`/`401`/`403`/`5xx`/网络错误/流不完整/零输出/空闲超时
自动换下一个账号重试；`400`/`422` 等请求本身的问题不重试。故障转移只发生在向客户端
写出任何字节之前，流式响应一旦开始不会在另一个账号上重放。

## 部署

```bash
./deploy.sh    # 自动确保 D1 database_id 并部署 Worker + 静态页面
```

或手动：

```bash
# 1. 创建 D1（存账号、额度缓存、用量与冷却状态）
npx wrangler d1 create commandcode-pool
#   → 把输出的 database_id 填进 wrangler.toml

# 2. 配置鉴权
npx wrangler secret put ADMIN_TOKEN   # /api/* 管理令牌
npx wrangler secret put API_KEYS      # /v1/* 客户端 Bearer Key（逗号分隔）

# 3. 部署
npx wrangler deploy
```

Worker 启动后自动建表，无需手动执行 `schema.sql`。

### 添加账号

管理 API（`ADMIN_TOKEN` 通过 `x-admin-token` 头传递）：

```bash
# 添加（先注册设备指纹，再经 /alpha/whoami 等验证，无效密钥会被拒绝）
curl -X POST https://<worker>/api/accounts \
  -H "x-admin-token: $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"key": "你的-command-code-api-key", "label": "go-1"}'

# 浏览器授权（可选）：面板「浏览器登录」会打开
#   https://commandcode.ai/studio/auth/cli?callback=http://127.0.0.1:5959/callback&state=…
# 授权页只接受 localhost 回调，自动回传仅限本地 CLI（pi / cmdc，见 pi-commandcode-provider）；
# 授权完成后页面会显示密钥，复制后粘贴到面板输入框走上面的 /api/accounts 即可。

# 列表（密钥打码，含额度缓存与用量）
curl https://<worker>/api/accounts -H "x-admin-token: $ADMIN_TOKEN"

# 刷新额度（全部，或 {"id": 1} 单个）
curl -X POST https://<worker>/api/refresh -H "x-admin-token: $ADMIN_TOKEN" -d '{}'

# 号池概览（当前粘住的账号、总用量、cache 命中统计）
curl https://<worker>/api/state -H "x-admin-token: $ADMIN_TOKEN"

# 用量统计（5m/1h/5h/1d/3d/7d/30d）
curl 'https://<worker>/api/usage/daily?range=1d' -H "x-admin-token: $ADMIN_TOKEN"

# 启用/禁用、删除
curl -X PATCH https://<worker>/api/accounts/1 -H "x-admin-token: $ADMIN_TOKEN" -d '{"enabled": false}'
curl -X DELETE https://<worker>/api/accounts/1 -H "x-admin-token: $ADMIN_TOKEN"
```

密钥从 commandcode.ai/settings 获取。也可以不绑 D1，用环境变量 `ACCOUNTS` 直接给号池
（JSON 数组 `[{"key":"...","label":"..."}]` 或逗号分隔 Key 列表；状态仅存内存，不持久）。

## 调用

```bash
curl https://<worker>/v1/chat/completions \
  -H "Authorization: Bearer $API_KEYS 中的一把" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek/deepseek-v4-flash",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

- **鉴权**：`Authorization: Bearer <key>` 或 `x-api-key: <key>`（Anthropic SDK 风格）。
- **模型 ID**：与 `/v1/models` 返回的一致；未提供时缺省 `deepseek/deepseek-v4-flash`。
- **流式**：OpenAI 返回标准 SSE（`chat.completion.chunk` + `data: [DONE]`），usage 附着在
  finish chunk（恒带 `prompt_tokens_details.cached_tokens`）；Anthropic 返回
  `message_start`/`content_block_*`/`message_delta`/`message_stop` 序列（含 thinking
  块与 `signature_delta`）；Responses 返回 `response.created` → `response.completed`
  具名事件（单调 `sequence_number`）。
- **推理内容**：`reasoning_content`（OpenAI）/ `thinking` 块（Anthropic）/ reasoning item（Responses）。
- **工具调用**：三协议双向转换；工具名按 CLI 规则重写别名（`bash_output`→`shell_output` 等）。
- **推理强度**：OpenAI `reasoning_effort` 透传；Anthropic `thinking.budget_tokens` 映射
  （≥10000→high，≥5000→medium，否则 low）。
- **缓存断点**：Anthropic `cache_control` 块透传；`prompt_cache_key` 自动转 system 末块
  `ephemeral` 断点。
- **system**：块数组上传；客户端未提供时默认发占位 `' '` 阻止上游注入约 7.5K token 的默认
  system prompt（`EMPTY_SYSTEM_PLACEHOLDER=false` 关闭）。
- **图片**：`image_url` 仅支持 `data:image/...;base64,...`。
- **usage**：Anthropic 端 `input_tokens` 只计非缓存部分（`cache_read_input_tokens` /
  `cache_creation_input_tokens` 与之相加为总输入）。
- `max_tokens` 缺省 64000、上限 200000；`max_completion_tokens` 亦可。
- Responses API 为无状态代理：`previous_response_id` 收到即 400。

## 环境变量

| 变量 | 说明 |
| --- | --- |
| `API_BASE` | 上游地址，默认 `https://api.commandcode.ai` |
| `POOL_STRATEGY` | `sticky`（默认，缓存优先）\| `round_robin` |
| `API_KEYS` | 逗号分隔的客户端 Key；未配置时 `/v1/*` 返回 503 |
| `ADMIN_TOKEN` | `/api/*` 管理令牌（`x-admin-token` 头）；未配置时管理 API 返回 503 |
| `ALLOW_ANONYMOUS` | 仅显式设为 `true` 时允许未配置鉴权，适合本地开发 |
| `ACCOUNTS` | 未绑定 D1 时的内存号池（见上） |
| `FINGERPRINT_SALT` | 设备指纹盐：非空时成批更换所有账号的伪造设备身份（逃生口） |
| `STREAM_IDLE_MS` | 流式上游读空闲超时（默认 30000）；超时按 429 退避并可换号 |
| `NONSTREAM_IDLE_MS` | 非流式上游读空闲超时（默认 90000） |
| `EMPTY_SYSTEM_PLACEHOLDER` | 客户端未提供 system 时是否发占位 `' '`（默认 `true`） |
| `CLI_MODE` / `CLI_SESSION_MODE` | 信封 `mode`（默认 `agent`）/ 生命周期 `mode`（默认 `interactive`） |
| `DEVICE_PROJECT_DIR` | 伪造项目目录（同时决定 `x-project-slug`） |
| `TIMEZONE_OFFSET` | `/api/usage/daily` 分桶时区（小时，默认 8） |

## 端点一览

| 方法 | 路径 | 鉴权 | 说明 |
| --- | --- | --- | --- |
| POST | `/v1/chat/completions` | API_KEYS | OpenAI 兼容 chat |
| POST | `/v1/messages` | API_KEYS | Anthropic Messages |
| POST | `/v1/responses` | API_KEYS | OpenAI Responses |
| GET | `/v1/models` | API_KEYS | 模型目录（10 分钟缓存） |
| GET | `/health` | 无 | 健康检查 |
| GET | `/admin` | 静态页 | 管理面板（数据仍走 `/api/*`） |
| GET | `/api/accounts` | ADMIN_TOKEN | 账号列表（打码） |
| POST | `/api/accounts` | ADMIN_TOKEN | 添加账号 |
| PATCH/DELETE | `/api/accounts/:id` | ADMIN_TOKEN | 改备注/启停 / 删除 |
| POST | `/api/refresh` | ADMIN_TOKEN | 刷新额度（`{"id":1}` 单个） |
| GET | `/api/state` | ADMIN_TOKEN | 号池概览 |
| GET | `/api/usage/daily?range=5m` | ADMIN_TOKEN | 用量统计 |
| POST | `/api/login` | 无 | 校验管理密码 |
## 本地开发

```bash
cp .dev.vars.example .dev.vars   # 指向本地 mock
python3 mock_server.py 18090 &   # 离线 mock 上游（形状按 key 里的形态名选择）
npx wrangler dev                 # http://127.0.0.1:8787
npm test                         # 本地单元 + mock 端到端测试（无需真实密钥，共 280+ 断言）
```

mock 形态：`cc-ok-*` 正常 \| `cc-zero-*` 零输出 \| `cc-partial-*` 流中断 \| `cc-tool-*`
工具调用 \| `cc-streamerr-*` 流内 429 \| `cc-slow-*` 慢速（触发空闲超时）\| `cc-snake-*`
snake_case \| `cc-exhausted-*` 402 \| `cc-limited-*` 429 \| `cc-badkey-*` 401 \|
`cc-forbidden-*` 403 \| `cc-badrequest-*` 400。GET 额度形态同
[commandcode-usage](https://github.com/MAXeaglet/commandcode-usage) 的 mock
（`sk-ok-*` / `sk-exhausted-*` / `sk-partial-*` / `sk-snake-*` / `sk-garbage-*` /
`sk-badkey-*` / `sk-cancel-*` / `sk-lowbal-*`）。

## 说明

- 管理前端源码为 `public/admin.html`（wrangler `[assets]` 托管），`/admin` 由 Worker
  改写为 `/admin.html` 交给 assets；不再有内嵌双份同步问题。
- 反检测层每个账号密钥确定性派生同一台伪造设备（Workers 多 isolate 下也一致）；
  会话按 key + 12h 时间桶派生，所有 isolate 呈现同一 `threadId`。
- 上游 `/alpha/*` 与 `/alpha/generate` 均为未公开接口，字段做了防御性兼容；
  Command Code 调整内部 API 时适配层可能需要更新（npm 版本漂移只会告警，不会自动改版本号）。
- 密钥明文只存你自己的 D1 / 环境变量，除转发给 commandcode.ai 外不发给任何第三方；API 返回一律打码。
- 全部账号都在冷却时，客户端收到 `429 rate_limit_error` 与最早恢复时间（`Retry-After`）。
- D1 新版不再写旧表 `usage_daily`（检测到旧数据时自动一次性迁移进 5 分钟桶，历史不丢）。
- 流式响应首字节后上游出错不会换号重试（协议固有），按原协议写错误帧后干净关闭。

## 致谢

本项目参考了以下开源项目（均为 [MIT](https://opensource.org/license/mit) 许可证）：

- [commandcode-proxy](https://github.com/MAXeaglet/commandcode-proxy)（MAXeaglet，MIT）— 协议层逐字段对齐其 wire 协议实现（CLI 信封、设备指纹、生命周期事件等）。
- [commandcode-usage](https://github.com/MAXeaglet/commandcode-usage)（MAXeaglet，MIT）— 号池调度思路、多账号额度面板与离线 mock 形态参考。
- [pi-commandcode-provider](https://github.com/patlux/pi-commandcode-provider)（patlux，MIT）— 浏览器授权（OAuth localhost 回调）流程参考。

## 社区

本项目在 [LINUX DO](https://linux.do) 社区发布与交流 — *Where possibility begins.*
欢迎佬友们到社区参与讨论、反馈问题与提出建议。

## 许可证

本项目基于 [MIT License](LICENSE) 发布。上述被参考项目各自的 MIT 许可证与版权声明归其原作者所有。

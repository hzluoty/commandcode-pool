# commandcode-pool

Cloudflare Worker：把多个 Command Code 账号组成号池，对外提供 **OpenAI / Anthropic / Responses 兼容 API**：

- `POST /v1/chat/completions` — OpenAI Chat Completions（流式 + 非流式）
- `POST /v1/messages` — Anthropic Messages API（流式 + 非流式）
- `POST /v1/responses` — OpenAI Responses API（流式 + 非流式）
- `GET /v1/models` — 模型目录
- `GET /admin` — 管理面板（数据走 `/api/*`）

本项目基于 Command Code 官方公开 **Provider API**（`https://api.commandcode.ai/provider/v1`），采用**单请求单账号单次尝试**原则，彻底消除跨账号 prompt 重放与未公开 CLI 身份伪造带来的封号风险。

## 核心设计与防封原则

1. **公开 Provider API 原生通信**：直接与官方 Provider 端点通信，不伪造 CLI 版本头（`x-command-code-version`）、不构造未公开的设备指纹与生命周期事件。
2. **严格单次调用，绝不跨账号重放**：一个逻辑请求只选择一个账号执行一次上游调用。若该账号返回 429、401 或网络超时，错误直接映射返回客户端并更新该账号健康状态，绝不拿客户端 prompt 重试其他账号（避免多账号因并发/重放被关联封禁）。
3. **粘性调度（Cache 优先）**：默认策略为 **sticky**，优先复用最近活跃账号，保证提示词缓存（Prompt Cache）持续命中；当账号遇到 429 或 401 时进入冷却/禁用，后续独立请求自动切换到下一个可用账号。
4. **单路径原生流式 Relay**：流式响应通过 `TransformStream` 直接向客户端透传字节，并在传输过程中就地解析终端状态与 Token 用量，杜绝内存无界缓冲与背压失真。客户端中断请求时，上游连接立刻同步终止。

## 部署

```bash
./deploy.sh    # 自动确保 D1 database_id 并部署 Worker + 静态页面
```

或手动：

```bash
# 1. 创建 D1（存账号、用量与冷却状态）
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
# 添加（通过 Provider /models 鉴权接口验证，无效密钥直接拒绝）
curl -X POST https://<worker>/api/accounts \
  -H "x-admin-token: $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d '{"key": "你的-command-code-api-key", "label": "go-1"}'

# 列表（密钥打码，含本地记录的真实用量）
curl https://<worker>/api/accounts -H "x-admin-token: $ADMIN_TOKEN"

# 刷新状态（验证密钥有效性并更新状态）
curl -X POST https://<worker>/api/refresh -H "x-admin-token: $ADMIN_TOKEN" -d '{}'

# 号池概览（当前活跃账号、总用量、分桶统计）
curl https://<worker>/api/state -H "x-admin-token: $ADMIN_TOKEN"

# 用量统计（5m/1h/6h/1d/7d/30d）
curl 'https://<worker>/api/usage/daily?range=1d' -H "x-admin-token: $ADMIN_TOKEN"

# 启用/禁用、删除
curl -X PATCH https://<worker>/api/accounts/1 -H "x-admin-token: $ADMIN_TOKEN" -d '{"enabled": false}'
curl -X DELETE https://<worker>/api/accounts/1 -H "x-admin-token: $ADMIN_TOKEN"
```

也可以不绑 D1，用环境变量 `ACCOUNTS` 直接给号池（JSON 数组 `[{"key":"...","label":"..."}]` 或逗号分隔 Key 列表；状态仅存内存，不持久）。

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
- **模型 ID**：与 `/v1/models` 返回的一致；未提供时缺省 `deepseek/deepseek-v4-flash`（Messages 缺省 `claude-sonnet-4-6`）。
- **流式**：OpenAI 返回标准 SSE（`chat.completion.chunk` + `data: [DONE]`）；Anthropic 返回 `message_start`/`content_block_*`/`message_delta`/`message_stop` 原生事件；Responses 返回 `response.created` → `response.completed` 原生事件。

## 环境变量

| 变量 | 说明 |
| --- | --- |
| `API_BASE` | 上游地址，默认 `https://api.commandcode.ai` |
| `POOL_STRATEGY` | `sticky`（默认，缓存优先）\| `round_robin`（初选调度，无跨账号重放） |
| `API_KEYS` | 逗号分隔的客户端 Key；未配置时 `/v1/*` 返回 503 |
| `ADMIN_TOKEN` | `/api/*` 管理令牌（`x-admin-token` 头）；未配置时管理 API 返回 503 |
| `ALLOW_ANONYMOUS` | 仅显式设为 `true` 时允许未配置鉴权，适合本地开发 |
| `ACCOUNTS` | 未绑定 D1 时的内存号池（见上） |
| `TIMEZONE_OFFSET` | `/api/usage/daily` 分桶时区（小时，默认 8） |

## 端点一览

| 方法 | 路径 | 鉴权 | 说明 |
| --- | --- | --- | --- |
| POST | `/v1/chat/completions` | API_KEYS | OpenAI 兼容 chat |
| POST | `/v1/messages` | API_KEYS | Anthropic Messages |
| POST | `/v1/responses` | API_KEYS | OpenAI Responses |
| GET | `/v1/models` | API_KEYS | 模型目录 |
| GET | `/health` | 无 | 健康检查 |
| GET | `/admin` | 静态页 | 管理面板（数据仍走 `/api/*`） |
| GET | `/api/accounts` | ADMIN_TOKEN | 账号列表（打码） |
| POST | `/api/accounts` | ADMIN_TOKEN | 添加账号 |
| PATCH/DELETE | `/api/accounts/:id` | ADMIN_TOKEN | 改备注/启停 / 删除 |
| POST | `/api/refresh` | ADMIN_TOKEN | 刷新状态 |
| GET | `/api/state` | ADMIN_TOKEN | 号池概览 |
| GET | `/api/usage/daily?range=5m` | ADMIN_TOKEN | 用量统计 |
| POST | `/api/login` | 无 | 校验管理密码 |

## 本地开发与测试

```bash
cp .dev.vars.example .dev.vars   # 指向本地 mock
python3 mock_server.py 18090 &   # 离线 mock 上游
npx wrangler dev                 # http://127.0.0.1:8787
npm test                         # 运行单测与端到端测试
```

## 许可证

本项目基于 [MIT License](LICENSE) 发布。

# Command Code Pool (Vercel 版)

> [English Documentation](README.md)

专为 **Command Code** ([commandcode.ai](https://commandcode.ai)) 打造的 **Vercel 云端部署多账号号池**，对外提供高兼容的 **OpenAI / Anthropic / Responses** 接口，并全面深度支持 **Go 套餐（`individual-go`）** 的登录鉴权、代理调用与用量记录。

参考与融合三大核心项目精髓：
- [MAXeaglet/commandcode-usage](https://github.com/MAXeaglet/commandcode-usage)：多账号额度矩阵、5 小时/周滚动窗口监控、套餐月度额度推导。
- [MAXeaglet/commandcode-proxy](https://github.com/MAXeaglet/commandcode-proxy)：高保真 1.53.1 CLI wire 协议 (`/alpha/generate`)、确定性设备指纹派生、会话管理、零输出防护与协议转换。
- [patlux/pi-commandcode-provider](https://github.com/patlux/pi-commandcode-provider)：浏览器辅助 Studio OAuth 登录流程、Go 套餐传输回退机制与 Quota 账单计算。

---

## 🌟 核心特性

1. **全面支持 Go 套餐 (`individual-go`)**：
   - **登录/添加**：调用 `/alpha/whoami` 与 `/alpha/billing/subscriptions` 自动识别 `individual-go` 套餐，显示独特的 Go 套餐标识。
   - **调用（解决 403 `upgrade_required`）**：Command Code 官方公开的 Provider API (`/provider/v1/*`) 对 Go 套餐账号报错 `403 upgrade_required`。本号池采用与官方 CLI 完全一致的 9 键信封传输协议（`/alpha/generate`），为 Go 套餐账号提供稳定无阻的 OpenAI / Anthropic 调用支持！
   - **用量记录**：自动按 Go 套餐的 10 Credits/月上限推导月度配额与进度条，实时监控 5 小时滚动窗口和周窗口的使用量与重置倒计时。
2. **多协议兼容网关**：
   - `POST /v1/chat/completions`：OpenAI Chat 接口（流式 SSE + 非流式，支持思维链 reasoning_content 与工具调用）。
   - `POST /v1/messages`：Anthropic Messages 接口（流式 SSE + 非流式，支持 thinking 思考块与伪造签名校验）。
   - `POST /v1/responses`：OpenAI Responses API 接口。
   - `GET /v1/models`：动态探测并缓存上游可用模型目录。
3. **粘性缓存与防封号调度（Sticky Routing）**：
   - 默认采用 `sticky` 策略优先复用账号，最大化 Prompt Cache 命中率，大幅节省 Token 并有效降低账号封号风险（封号风险）。
   - 遇 `429` 自动按 `Retry-After` 冷却换号；遇 `401` 自动拉黑禁用。
4. **Vercel 原生无痛部署**：
   - 基于 `vercel.json` 自动配置路由重写，零复杂配置。
   - 数据存储支持三级自适应：无外部依赖内存/环境变量模式 (`ACCOUNTS`)、本地/Serverless `/tmp` 模式、以及 Vercel KV / Upstash Redis 持久化。
   - 自带现代化 Web 管理控制台 (`/admin`) 与在线调用调试窗口 (Playground)。

---

## 🚀 Vercel 快速部署

### 1. 部署到 Vercel

1. 将本代码仓库推送至你的 GitHub；
2. 打开 [Vercel 控制台](https://vercel.com/new)，导入该仓库；
3. 配置如下环境变量：

| 环境变量 | 说明 | 示例 |
|---|---|---|
| `ADMIN_TOKEN` | 管理控制台密码（访问 `/admin` 时使用） | `your_secure_password` |
| `API_KEYS` | 客户端调用对外接口的 Bearer 密钥（逗号隔开） | `sk-pool-key-1,sk-pool-key-2` |
| `ACCOUNTS` | *(可选)* 初始号池账号列表（JSON 或逗号隔开的 Key 列表） | `[{"key":"user_xxx","label":"go-1"}]` |
| `POOL_STRATEGY` | 调度策略：`sticky`（默认，推荐）或 `round_robin` | `sticky` |
| `KV_REST_API_URL` | *(可选)* Vercel KV / Upstash Redis REST 地址 | `https://...upstash.io` |
| `KV_REST_API_TOKEN` | *(可选)* Vercel KV / Upstash Redis REST 令牌 | `...` |
| `REDIS_URL` | *(可选)* 标准 Redis 连接串（Redis Cloud / Vercel Redis 集成）；未同时配置上面两个 REST 变量时使用 | `rediss://default:...@host:port` |

4. 点击 **Deploy**，部署完成后访问 `https://<你的域名>.vercel.app/admin` 即可进入管理面板。

---

## 🔑 账号添加与登录（含 Go 套餐）

### 1. 网页管理面板 (`/admin`)
在浏览器访问 `/admin`，输入 `ADMIN_TOKEN` 登录：
- **手动添加账号**：输入 Command Code API Key（以 `user_` 开头），号池自动向上游请求 `/alpha/whoami` 验证、生成唯一硬件指纹、识别套餐（Go、GOAT、Pro 等），并拉取额度快照。
- **浏览器登录**：点击「浏览器登录」按钮打开官方授权页（`commandcode.ai/studio/auth/cli`），完成登录后复制页面显示的 API Key 粘贴回控制台即可。
- **在线测试**：控制台通过 `POST /api/test` 携带 `X-Admin-Token` 调用同一号池，支持流式与非流式并记录用量，不需要向浏览器提供 `API_KEYS`。若只配置了 `API_KEYS` 而未配置 `ADMIN_TOKEN`，此入口返回 403，需先设置管理密码；两者都未配置时可免密测试。对外 `/v1/*` 推理接口仍只接受 `API_KEYS`，不接受管理员令牌。

### 2. 支持套餐一览

| 套餐 ID | 套餐名称 | 月度 Credits | 线路模式 |
|---|---|---|---|
| `individual-go` | **Go** | **10** | **高保真 CLI Wire 协议 (`/alpha/generate`)** |
| `individual-goat` | GOAT | 70 | CLI Wire 协议 / Provider API |
| `individual-pro` | Pro | 30 | CLI Wire 协议 / Provider API |
| `individual-pro-v1` | Pro | 80 | CLI Wire 协议 / Provider API |
| `individual-provider`| Provider | 15 | CLI Wire 协议 / Provider API |
| `individual-max` | Max | 150 | CLI Wire 协议 / Provider API |
| `individual-ultra` | Ultra | 300 | CLI Wire 协议 / Provider API |
| `teams-pro` | Teams Pro | 40 | CLI Wire 协议 / Provider API |

---

## 📡 客户端调用示例

### OpenAI 兼容调用 (`/v1/chat/completions`)

```bash
curl https://<你的域名>.vercel.app/v1/chat/completions \
  -H "Authorization: Bearer <API_KEYS>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek/deepseek-v4-flash",
    "messages": [{"role": "user", "content": "你好！"}],
    "stream": true
  }'
```

### Anthropic 兼容调用 (`/v1/messages`)

```bash
curl https://<你的域名>.vercel.app/v1/messages \
  -H "x-api-key: <API_KEYS>" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "anthropic/claude-3-5-sonnet",
    "messages": [{"role": "user", "content": "你好 Claude！"}],
    "max_tokens": 1024
  }'
```

---

## 📊 用量记录与额度监控（含 Go 套餐）

控制台 `/admin` 提供全方位的额度监控看板：
1. **上游官方配额**：
   - **5 小时滚动窗口**：已用 / 上限、重置时间倒计时、超额封顶预警；
   - **周限额窗口**：已用 / 上限、重置倒计时；
   - **月度额度**：针对 Go 套餐准确按 10 credits 上限计算使用百分比与剩余额度；
   - **余额状态**：月度剩余、充值余额、免费额度。
2. **号池网关用量**：
   - 累计请求数、Prompt Tokens、Completion Tokens、Cache Read Tokens；
   - 聚合时序图表：支持按 `5m`、`1h`、`5h`、`1d`、`3d`、`7d` 查看调用趋势与缓存命中率。旧 `30d` 请求降级为 `1d`。
   - Redis 用量桶按最大查询范围加一天余量过期（当前为 8 天）；已有桶保持原 TTL，不删除账号键和累计用量。
   - 读取每批最多 256 个桶，通过 pipeline 批量传输；计数和元数据在 Redis 中原子合并，保持原 JSON 存储格式。
   - 仅记录完整成功的网关请求；上游 `finish-step` 不算整体完成，错误/截断/完全空响应不计成功。正文、思考或工具输出存在时允许缺失用量；只记录上游明确报告的 Tokens，缺失项按 0，不按分块数估算、不因输出为 0 清空输入/缓存数。流式调用先保存用量，再发送成功结束帧。
   - 「全部刷新」与在线测试结束会同步刷新账号和用量；统计 API 与浏览器请求禁止缓存。在线测试正文和思考过程分开展示，并显示请求模型、Tokens 和耗时（请求模型不代表已核验上游实际模型身份）。

### Vercel 用量排查

访问 `/health` 检查 `storage`：持久化部署应为 `RedisPool` 或 `UpstashKvPool`；`FilePool` 使用实例内 `/tmp`，无法跨实例共享或保证冷启动后保留。配置 `REDIS_URL` 或完整的 REST URL/Token 后重新部署。网关统计只包括通过本项目完成的调用，不会把账号过去在官方 CLI/其他客户端的调用导入；上游历史用量可在账号详情里查看。

---

## 🛠️ 本地运行与测试

```bash
# 启动本地服务
node api/index.js

# 运行自动化测试套件
node test/run-tests.mjs

# 可选真实 Redis 集成测试：先启动空的、可丢弃的本机测试 Redis
node test/redis-integration.mjs redis://127.0.0.1:16379
```

---

## 📄 开源许可

MIT License

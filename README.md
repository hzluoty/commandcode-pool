# Command Code Pool (Vercel Edition)

> [中文文档](README_zh.md)

A production-ready multi-account pool for **Command Code** ([commandcode.ai](https://commandcode.ai)) deployed on **Vercel**. Provides unified **OpenAI / Anthropic / Responses** compatible endpoints with full support for the **Go Plan (`individual-go`)**, including authentication, inference, and quota/usage tracking.

Referencing and integrating key architectural paradigms from:
- [MAXeaglet/commandcode-usage](https://github.com/MAXeaglet/commandcode-usage): Multi-account quota matrix, 5-hour & weekly rolling windows, monthly plan derivations.
- [MAXeaglet/commandcode-proxy](https://github.com/MAXeaglet/commandcode-proxy): High-fidelity 1.53.1 CLI wire envelope (`/alpha/generate`), deterministic device fingerprinting, session management, zero-output guard, and protocol translation.
- [patlux/pi-commandcode-provider](https://github.com/patlux/pi-commandcode-provider): Browser-assisted Studio OAuth authentication, Go plan transport fallback, and quota calculation.

---

## 🌟 Highlights

1. **Full Go Plan (`individual-go`) Support**:
   - **Login & Auth**: Automatically identifies the Go plan on registration via `/alpha/whoami` & `/alpha/billing/subscriptions`.
   - **Inference (Bypasses 403 `upgrade_required`)**: Command Code's public Provider API (`/provider/v1/*`) rejects Go plan accounts with `403 Forbidden: upgrade_required`. This pool proxies requests through the authenticated 9-key CLI wire protocol (`/alpha/generate`), allowing Go plan accounts to serve OpenAI and Anthropic clients smoothly.
   - **Quota & Usage**: Automatically maps the 10 Credits/month quota for Go plan accounts, derives monthly usage, and monitors 5-hour and weekly rolling limits.
2. **Multi-Protocol Gateway**:
   - `POST /v1/chat/completions`: OpenAI Chat Completions (streaming SSE + non-streaming, function calling, reasoning effort, thinking content).
   - `POST /v1/messages`: Anthropic Messages API (streaming SSE + non-streaming, `tool_use`, `thinking` blocks with simulated signatures).
   - `POST /v1/responses`: OpenAI Responses API.
   - `GET /v1/models`: Dynamic model discovery and catalog caching.
3. **Cache-Aware Sticky Scheduling**:
   - By default, requests sticky-route to the most recently used account to maximize upstream Prompt Cache hits and minimize account ban risks (封号风险).
   - Automatically fails over to the next available account on `429` (with `Retry-After` cooldown) or `401` (account disable).
4. **Vercel Native Deployment**:
   - Zero-configuration serverless deployment with `vercel.json` rewrites.
   - Pluggable persistence: In-memory/Environment variable (`ACCOUNTS`), local `/tmp` file, or Vercel KV / Upstash Redis.
   - Built-in Web dashboard at `/admin`.

---

## 🚀 Quick Start on Vercel

### 1. One-Click Deploy to Vercel

1. Push this repository to GitHub.
2. In the [Vercel Dashboard](https://vercel.com/new), import the repository.
3. Add the following Environment Variables:

| Variable | Description | Example |
|---|---|---|
| `ADMIN_TOKEN` | Password for the `/admin` dashboard | `my_secret_admin_token` |
| `API_KEYS` | Authorized Bearer keys for clients (comma-separated) | `sk-pool-key-1,sk-pool-key-2` |
| `ACCOUNTS` | *(Optional)* Initial accounts seed (JSON or comma-separated keys) | `[{"key":"user_xxx","label":"go-1"}]` |
| `POOL_STRATEGY` | `sticky` (default, cache-friendly) or `round_robin` | `sticky` |
| `KV_REST_API_URL` | *(Optional)* Upstash / Vercel KV URL for multi-region persistence | `https://...upstash.io` |
| `KV_REST_API_TOKEN` | *(Optional)* Upstash / Vercel KV Token | `...` |
| `REDIS_URL` | *(Optional)* Standard Redis connection string (Redis Cloud / Vercel Redis integration); used when the REST variables above are not both set | `rediss://default:...@host:port` |

4. Click **Deploy**. Your dashboard will be live at `https://<your-project>.vercel.app/admin`.

---

## 🔑 Login & Account Management

### 1. Web Dashboard (`/admin`)
Visit `/admin` in your browser. Enter your `ADMIN_TOKEN` to access the dashboard.
- **Add Account**: Enter a Command Code API key (`user_...` or `token_user_...`) with an optional label. The pool automatically verifies the key upstream, records device fingerprints, identifies the plan, and caches initial quota data.
- **Browser Login**: Click **浏览器登录** (Browser Login) to open the official Command Code Studio OAuth authentication page (`commandcode.ai/studio/auth/cli`). Once authenticated, copy the generated key into the dashboard.
- **Playground**: The dashboard sends `X-Admin-Token` to `POST /api/test`, reusing pool inference, streaming, and usage accounting without exposing `API_KEYS`. If `API_KEYS` is configured but `ADMIN_TOKEN` is not, this route returns 403 until an admin password is set. With neither configured, local tests remain password-free. Public `/v1/*` inference endpoints still require client `API_KEYS` and do not accept admin credentials.

### 2. Supported Plans

| Plan ID | Plan Name | Monthly Credits | Go Plan Wire Routing |
|---|---|---|---|
| `individual-go` | **Go** | **10** | **CLI Envelope (`/alpha/generate`)** |
| `individual-goat` | GOAT | 70 | CLI Envelope / Provider API |
| `individual-pro` | Pro | 30 | CLI Envelope / Provider API |
| `individual-pro-v1` | Pro | 80 | CLI Envelope / Provider API |
| `individual-provider`| Provider | 15 | CLI Envelope / Provider API |
| `individual-max` | Max | 150 | CLI Envelope / Provider API |
| `individual-ultra` | Ultra | 300 | CLI Envelope / Provider API |
| `teams-pro` | Teams Pro | 40 | CLI Envelope / Provider API |

---

## 📡 API Calling

### OpenAI Compatible (`/v1/chat/completions`)

```bash
curl https://<your-domain>.vercel.app/v1/chat/completions \
  -H "Authorization: Bearer <API_KEYS>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek/deepseek-v4-flash",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

### Anthropic Compatible (`/v1/messages`)

```bash
curl https://<your-domain>.vercel.app/v1/messages \
  -H "x-api-key: <API_KEYS>" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "anthropic/claude-3-5-sonnet",
    "messages": [{"role": "user", "content": "Hello Claude!"}],
    "max_tokens": 1024
  }'
```

---

## 📊 Quota & Usage Tracking

The dashboard at `/admin` and the API at `/api/usage/daily` track both upstream quota metrics and local pool consumption:
1. **Upstream Quota**:
   - **5-Hour Rolling Limit**: `used / cap`, `resetAt` countdown, and exceeded warnings.
   - **Weekly Limit**: `used / cap`, `resetAt` countdown.
   - **Monthly Quota**: Derived from plan mapping (e.g. 10 credits for Go plan, used = 10 - remaining).
   - **Credit Balances**: Monthly remaining, purchased, and free credits.
2. **Pool Consumption**:
   - Request counts, prompt tokens, completion tokens, and cache-read tokens.
   - Interactive breakdown charts across `5m`, `1h`, `5h`, `1d`, `3d`, and `7d`. Removed `30d` queries fall back to `1d`.
   - Redis usage buckets expire after the maximum query window plus one day (currently 8 days). Existing buckets keep their original TTL; account keys and lifetime totals are not deleted.
   - Reads use bounded pipelines of 256 buckets. Writes atomically merge JSON counters and metadata without changing the stored format.
   - Only fully completed gateway requests count as successes. A `finish-step` alone is not a completed request; upstream errors, truncated streams, and entirely empty responses are not counted. Real text, reasoning, or tool output is accepted even without usage. Tokens come only from upstream reports: missing fields count as 0, chunk counts are not estimates, and zero output tokens do not erase reported input/cache tokens. Streaming usage is saved before successful terminal frames are sent.
   - The console refreshes the visible tab every 5 seconds; Refresh All and completed Playground calls reload both accounts and usage without caching. The Playground separates the answer from collapsible reasoning and displays the requested model, tokens, and elapsed time (the requested model does not independently verify upstream model identity).

### Troubleshooting usage on Vercel

Check `storage` in `/health`: a persistent deployment should report `RedisPool` or `UpstashKvPool`. `FilePool` uses instance-local `/tmp`, which is neither shared across instances nor guaranteed after a cold start. Configure `REDIS_URL` or a complete REST URL/token pair and redeploy. Gateway statistics only include completed calls through this project, not historical calls made with the official CLI or other clients; upstream history is available in account details.

---

## 🛠️ Local Development & Testing

```bash
# Run local server
node api/index.js

# Run full test suite
node test/run-tests.mjs

# Optional real-Redis integration test: start an EMPTY disposable local Redis first
node test/redis-integration.mjs redis://127.0.0.1:16379
```

---

## 📄 License

MIT

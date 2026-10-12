// api/index.js
// Vercel Serverless Function 主入口 & 本地独立运行服务器
// 全功能支持：多账号号池、Go 套餐登录/调用/用量、OpenAI/Anthropic/Responses 兼容接口、Web 管理控制台
import http from 'node:http';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Readable } from 'node:stream';

import {
  DEFAULT_MODEL,
  MODEL_CACHE_TTL_MS,
  BUILTIN_MODELS,
  STUDIO_BASE_URL,
} from '../lib/constants.js';
import { getConfig } from '../lib/config.js';
import {
  json,
  readBody,
  safeEqual,
  maskKey,
  sanitizeApiKey,
  JSON_HEADERS,
} from '../lib/crypto-utils.js';
import { getPoolStore } from '../lib/pool-store.js';
import { fetchReport } from '../lib/quota-service.js';
import { forwardToCC } from '../lib/upstream-client.js';
import {
  buildCcRequest,
  convertAnthropicToOpenAI,
  convertResponsesToChat,
  createSseTranslator,
  createAnthropicSseTranslator,
  createResponsesSseTranslator,
  collectCcStream,
  collectedUsageOpenAI,
  buildAnthropicResponse,
  mapCcError,
  mapCcEventError,
  zeroOutputError,
  idleTimeoutError,
  transportError,
} from '../lib/api-adapters.js';
import {
  evaluateAccountAction,
  mapClientError,
  parseRetryAfter,
} from '../lib/failure-policy.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');

// 模型列表缓存
let cachedModels = null;
let modelsCacheExpiresAt = 0;

async function getModels(config, pool) {
  const now = Date.now();
  if (cachedModels && now < modelsCacheExpiresAt) {
    return cachedModels;
  }
  try {
    const resp = await fetch(`${config.apiBase}/provider/v1/models`, {
      headers: {
        'User-Agent': 'commandcode-pool/1.0',
        'Accept': 'application/json',
      },
    });
    if (resp.ok) {
      const data = await resp.json();
      if (Array.isArray(data?.data) && data.data.length > 0) {
        cachedModels = data.data;
        modelsCacheExpiresAt = now + MODEL_CACHE_TTL_MS;
        return cachedModels;
      }
    }
  } catch (e) {
    // 失败降级
  }
  cachedModels = BUILTIN_MODELS;
  modelsCacheExpiresAt = now + 60000;
  return cachedModels;
}

// 客户端鉴权（API_KEYS 保护对外 /v1 接口）
const CLI_CALLBACK_PORT = 8976;

function verifyClientAuth(req, config) {
  if (!config.apiKeys || config.apiKeys.length === 0) return true;
  const authHeader = req.headers.get('authorization') || '';
  const apiKeyHeader = req.headers.get('x-api-key') || '';
  let token = '';
  if (authHeader.startsWith('Bearer ')) token = authHeader.slice(7).trim();
  else if (apiKeyHeader) token = apiKeyHeader.trim();

  if (!token) return false;
  return config.apiKeys.some(k => safeEqual(token, k));
}

// 管理员鉴权（ADMIN_TOKEN 保护 /api 接口）
function verifyAdminAuth(req, config) {
  if (!config.adminToken) return true;
  const adminHeader = req.headers.get('x-admin-token') || '';
  return safeEqual(adminHeader, config.adminToken);
}

// 账号添加入库（先校验、后派生指纹与生命周期、再拉取额度快照）
async function provisionAccount(pool, config, key, label = '') {
  // 1. 验证上游有效性并获取用户信息
  const report = await fetchReport(config.apiBase, key);
  const accountLabel = label || report.account?.userName || maskKey(key);

  // 2. 添加到号池
  const id = await pool.add(key, accountLabel);

  // 3. 保存配额信息
  await pool.saveQuota(id, report);
  return id;
}

// ── 核心推理处理：带统一号池调度、故障转移、流式与非流式 ──
async function handleInference(request, config, pool, kind) {
  if (!verifyClientAuth(request, config)) {
    return json({ error: { message: 'Unauthorized: Invalid API Key', type: 'authentication_error' } }, 401);
  }

  let clientBody;
  try {
    clientBody = await request.json();
  } catch (e) {
    return json({ error: { message: 'Invalid JSON request body', type: 'invalid_request_error' } }, 400);
  }

  // 协议输入归一化为 OpenAI 格式
  let openaiReq;
  if (kind === 'messages') {
    openaiReq = convertAnthropicToOpenAI(clientBody);
  } else if (kind === 'responses') {
    openaiReq = convertResponsesToChat(clientBody);
  } else {
    openaiReq = clientBody;
  }

  const model = openaiReq.model || DEFAULT_MODEL;
  const wantsStream = Boolean(clientBody.stream);
  const excludeIds = [];

  // 故障转移循环：在向客户端发送任何数据前可安全换号
  while (true) {
    const account = await pool.pick(config.poolStrategy, excludeIds);
    if (!account) {
      const waitUntil = await pool.earliestRateLimitedUntil();
      const waitSec = Math.max(1, Math.ceil((waitUntil - Date.now()) / 1000));
      const retryAfter = waitUntil > 0 ? waitSec : 15;
      const mapped = mapClientError(kind, 429, '所有账号当前均在冷却或不可用，请稍后重试', 'rate_limit_error', retryAfter);
      return json(mapped.body, mapped.status, mapped.headers);
    }

    let upstreamResp;
    try {
      const ccBody = buildCcRequest(openaiReq, config);
      upstreamResp = await forwardToCC(ccBody, account.api_key, config, request.signal, openaiReq.prompt_cache_key, request.headers);
    } catch (err) {
      if (err.name === 'AbortError') throw err;
      // 网络级别错误换号重试
      await pool.cooldown(account.id, 10, err.message || '网络连接错误');
      excludeIds.push(account.id);
      continue;
    }

    // 上游 HTTP 状态码异常
    if (!upstreamResp.ok) {
      const errorText = await upstreamResp.text();
      const action = evaluateAccountAction(upstreamResp.status, errorText, upstreamResp.headers);

      if (action.action === 'disable') {
        await pool.disable(account.id, action.reason);
        excludeIds.push(account.id);
        continue;
      } else if (action.action === 'cooldown') {
        await pool.cooldown(account.id, action.seconds, action.message);
        excludeIds.push(account.id);
        continue;
      } else {
        // 客户端自身请求错误（400/422 等），直接返回给客户端，不换号也不惩罚账号
        const mapped = mapCcError(upstreamResp.status, errorText);
        const mappedClient = mapClientError(kind, mapped.status, mapped.body?.error?.message || 'Upstream Error', mapped.body?.error?.type);
        return json(mappedClient.body, mappedClient.status, mappedClient.headers);
      }
    }

    // ── 流式响应分支 ──
    if (wantsStream) {
      const completionId = `${kind === 'messages' ? 'msg_' : 'chatcmpl-'}${Date.now()}`;
      const created = Math.floor(Date.now() / 1000);

      let translator;
      if (kind === 'messages') translator = createAnthropicSseTranslator(model, completionId);
      else if (kind === 'responses') translator = createResponsesSseTranslator(model, completionId, created);
      else translator = createSseTranslator(model, completionId, created);

      const upstreamReader = upstreamResp.body.getReader();
      const decoder = new TextDecoder();
      let streamBuffer = '';
      let firstChunkSent = false;

      const readable = new ReadableStream({
        async start(controller) {
          const enc = new TextEncoder();
          const send = (str) => {
            if (!str) return;
            controller.enqueue(enc.encode(str));
            firstChunkSent = true;
          };

          if (typeof translator.preamble === 'function') {
            send(translator.preamble());
          }

          try {
            while (true) {
              const { done, value } = await upstreamReader.read();
              if (done) break;
              streamBuffer += decoder.decode(value, { stream: true });

              let idx;
              while ((idx = streamBuffer.indexOf('\n')) >= 0) {
                const line = streamBuffer.slice(0, idx).trim();
                streamBuffer = streamBuffer.slice(idx + 1);
                if (!line) continue;
                const translated = await translator.parseLine(line);
                if (translated) {
                  if (Array.isArray(translated)) translated.forEach(send);
                  else send(translated);
                }
              }
            }

            // 收尾
            if (typeof translator.finalize === 'function') {
              const fin = await translator.finalize();
              if (Array.isArray(fin)) fin.forEach(send);
              else if (fin) send(fin);
            }
            if (typeof translator.getDoneEvent === 'function') {
              send(translator.getDoneEvent());
            }

            // 成功记录用量
            await pool.recordSuccess(account.id, {
              prompt_tokens: translator.inputTokens || 0,
              completion_tokens: translator.outputTokens || 0,
              prompt_tokens_details: { cached_tokens: translator.cachedInputTokens || 0 },
            });
            controller.close();
          } catch (streamErr) {
            controller.error(streamErr);
          } finally {
            try { upstreamReader.releaseLock(); } catch {}
          }
        },
        cancel() {
          try { upstreamReader.cancel(); } catch {}
        },
      });

      return new Response(readable, {
        status: 200,
        headers: {
          ...JSON_HEADERS,
          'Content-Type': 'text/event-stream; charset=utf-8',
          'Cache-Control': 'no-cache, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
        },
      });
    }

    // ── 非流式响应分支 ──
    const collected = await collectCcStream(upstreamResp);

    if (collected.upstreamError) {
      const errAction = evaluateAccountAction(collected.upstreamError.reportedStatus || collected.upstreamError.status);
      if (errAction.action === 'cooldown' || errAction.action === 'disable') {
        if (errAction.action === 'disable') await pool.disable(account.id, errAction.reason);
        else await pool.cooldown(account.id, errAction.seconds, errAction.message);
        excludeIds.push(account.id);
        continue;
      }
      return json(collected.upstreamError.body, collected.upstreamError.status);
    }

    if (!collected.sawFinish && collected.finishReason === 'upstream_error') {
      await pool.cooldown(account.id, 10, '上游提前断开');
      excludeIds.push(account.id);
      continue;
    }

    const usage = collectedUsageOpenAI(collected.usage);
    await pool.recordSuccess(account.id, usage);

    if (kind === 'messages') {
      const anthropicResp = await buildAnthropicResponse(
        model,
        collected.fullText,
        collected.toolCalls,
        collected.finishReason,
        collected.usage,
        collected.thinkingText
      );
      return json(anthropicResp);
    }

    if (kind === 'responses') {
      const respId = `resp_${Date.now()}`;
      return json({
        id: respId,
        object: 'response',
        created_at: Math.floor(Date.now() / 1000),
        status: 'completed',
        model,
        output_text: collected.fullText,
        output: [
          {
            id: `msg_${Date.now()}`,
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text: collected.fullText, annotations: [] }],
          },
        ],
        usage: {
          input_tokens: usage.prompt_tokens,
          output_tokens: usage.completion_tokens,
          total_tokens: usage.total_tokens,
          input_tokens_details: { cached_tokens: usage.prompt_tokens_details?.cached_tokens || 0 },
        },
      });
    }

    // 默认 OpenAI Chat Completions 响应
    const choice = {
      index: 0,
      message: {
        role: 'assistant',
        content: collected.fullText,
      },
      finish_reason: collected.finishReason || 'stop',
    };
    if (collected.thinkingText) {
      choice.message.reasoning_content = collected.thinkingText;
    }
    if (collected.toolCalls?.length) {
      choice.message.tool_calls = collected.toolCalls;
    }

    return json({
      id: `chatcmpl-${Date.now()}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [choice],
      usage,
    });
  }
}

// 静态文件读取助手
function serveStaticFile(filePath, contentType = 'text/html; charset=utf-8') {
  try {
    if (fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath);
      return new Response(content, {
        status: 200,
        headers: { 'Content-Type': contentType },
      });
    }
  } catch (e) {}
  return null;
}

// ── 统一 Web Request 路由调度器 ──
export async function handleRequest(request) {
  const config = getConfig();
  const pool = getPoolStore(config);
  const url = new URL(request.url);
  const pathName = url.pathname;

  // CORS 预检
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: JSON_HEADERS });
  }

  // 1. 静态前端页面
  if (pathName === '/' || pathName === '/index.html') {
    const fileResp = serveStaticFile(path.join(PROJECT_ROOT, 'public', 'index.html'));
    if (fileResp) return fileResp;
  }
  if (pathName === '/admin' || pathName === '/admin.html') {
    const fileResp = serveStaticFile(path.join(PROJECT_ROOT, 'public', 'admin.html'));
    if (fileResp) return fileResp;
  }

  // 2. 健康检查
  if (pathName === '/health') {
    const count = await pool.enabledCount();
    return json({ ok: true, enabledAccounts: count, version: '1.0.0' });
  }

  // 3. 模型列表
  if (pathName === '/v1/models' && request.method === 'GET') {
    const models = await getModels(config, pool);
    return json({ object: 'list', data: models });
  }

  // 4. 推理端点
  if (pathName === '/v1/chat/completions' && request.method === 'POST') {
    return handleInference(request, config, pool, 'chat');
  }
  if (pathName === '/v1/messages' && request.method === 'POST') {
    return handleInference(request, config, pool, 'messages');
  }
  if (pathName === '/v1/responses' && request.method === 'POST') {
    return handleInference(request, config, pool, 'responses');
  }

  // 5. 管理端 API (/api/*)
  if (pathName === '/api/login' && request.method === 'POST') {
    if (!config.adminToken) return json({ ok: true });
    const body = await readBody(request) || {};
    if (safeEqual(String(body.password || ''), config.adminToken)) return json({ ok: true });
    return json({ error: '管理员密码错误' }, 401);
  }

  // 浏览器辅助登录：生成 Command Code Studio 授权 URL
  if (pathName === '/api/auth/browser-url' && request.method === 'GET') {
    if (!verifyAdminAuth(request, config)) return json({ error: '未授权' }, 401);
    // Studio 仅允许 localhost 回调；本机无监听，Studio 的 POST 会失败并自动跳到“复制 API 密钥”页(fallback)。
    // 不能加 mode=redirect：该模式把密钥放在表单 POST 里，浏览器只会停在空白的 /callback 页。
    const callbackUrl = `http://127.0.0.1:${CLI_CALLBACK_PORT}/callback`;
    const state = crypto.randomUUID();
    const authUrl = `${STUDIO_BASE_URL}/studio/auth/cli?callback=${encodeURIComponent(callbackUrl)}&state=${encodeURIComponent(state)}`;
    return json({ authUrl, callbackUrl, state });
  }

  // 后续 /api 路由需要管理员鉴权
  if (pathName.startsWith('/api/')) {
    if (!verifyAdminAuth(request, config)) {
      return json({ error: '未授权: 请提供有效的 X-Admin-Token' }, 401);
    }

    if (pathName === '/api/accounts' && request.method === 'GET') {
      const accounts = await pool.list();
      return json({ accounts });
    }

    if (pathName === '/api/accounts' && request.method === 'POST') {
      const body = (await readBody(request)) || {};
      const key = sanitizeApiKey(body.key);
      const label = String(body.label || '').trim();
      if (!key) return json({ error: 'Key 不能为空' }, 400);

      try {
        const id = await provisionAccount(pool, config, key, label);
        const list = await pool.list();
        const created = list.find(a => a.id === id);
        return json({ ok: true, account: created }, 201);
      } catch (e) {
        return json({ error: e.message || '账号校验或添加失败' }, e.status || 500);
      }
    }

    const accountMatch = pathName.match(/^\/api\/accounts\/(\d+)$/);
    if (accountMatch) {
      const id = Number(accountMatch[1]);
      if (request.method === 'PATCH') {
        const body = (await readBody(request)) || {};
        await pool.patch(id, { label: body.label, enabled: body.enabled });
        const list = await pool.list();
        return json({ ok: true, account: list.find(a => a.id === id) });
      }
      if (request.method === 'DELETE') {
        await pool.remove(id);
        return json({ ok: true });
      }
    }

    if (pathName === '/api/refresh' && request.method === 'POST') {
      const body = (await readBody(request)) || {};
      const accounts = await pool.list();
      const targets = body.id != null ? accounts.filter(a => a.id === Number(body.id)) : accounts;
      if (body.id != null && !targets.length) return json({ error: '账号不存在' }, 404);

      const refreshed = await Promise.all(
        targets.slice(0, 12).map(async (a) => {
          const raw = await pool.getRaw(a.id);
          if (!raw?.api_key) return a;
          try {
            const report = await fetchReport(config.apiBase, raw.api_key);
            await pool.saveQuota(a.id, report);
            const freshList = await pool.list();
            return freshList.find(x => x.id === a.id) || a;
          } catch (e) {
            await pool.markQuotaError(a.id, e.message);
            const freshList = await pool.list();
            return freshList.find(x => x.id === a.id) || a;
          }
        })
      );
      return json({ ok: true, accounts: refreshed, skipped: Math.max(0, targets.length - 12) });
    }

    if (pathName === '/api/usage/daily' && request.method === 'GET') {
      const range = url.searchParams.get('range') || '1d';
      const usage = await pool.dailyUsage(range);
      return json(usage);
    }

    if (pathName === '/api/state' && request.method === 'GET') {
      const accounts = await pool.list();
      const enabled = accounts.filter(a => a.enabled);
      const stickyAccount = await pool.pick('sticky');
      return json({
        totalAccounts: accounts.length,
        enabledAccounts: enabled.length,
        activeStickyAccount: stickyAccount ? rowToAccount(stickyAccount) : null,
        poolStrategy: config.poolStrategy,
      });
    }
  }

  return json({ error: 'Endpoint Not Found' }, 404);
}

// ── Vercel Node Serverless Function 通用适配器 ──
export default async function handler(req, res) {
  // 1. 如果是在 Vercel Node 运行时下被调用（存在 res 对象）
  if (res && typeof res.writeHead === 'function') {
    const protocol = req.headers['x-forwarded-proto'] || 'http';
    const host = req.headers['x-forwarded-host'] || req.headers.host || 'localhost';
    const url = new URL(req.url, `${protocol}://${host}`);

    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (Array.isArray(v)) v.forEach(item => headers.append(k, item));
      else if (v) headers.set(k, v);
    }

    const abortController = new AbortController();
    req.on('close', () => abortController.abort());

    let body = null;
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      body = Readable.toWeb(req);
    }

    const webRequest = new Request(url, {
      method: req.method,
      headers,
      body,
      signal: abortController.signal,
      duplex: 'half',
    });

    try {
      const webResponse = await handleRequest(webRequest);
      res.statusCode = webResponse.status;
      for (const [key, value] of webResponse.headers.entries()) {
        res.setHeader(key, value);
      }

      if (webResponse.body) {
        const reader = webResponse.body.getReader();
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            res.write(value);
          }
        } finally {
          res.end();
        }
      } else {
        res.end();
      }
    } catch (err) {
      if (!res.headersSent) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: { message: err.message || 'Internal Server Error' } }));
      }
    }
    return;
  }

  // 2. 如果是在 Edge 运行时或标准 Web Request 下调用
  return handleRequest(req);
}

// ── 本地直接执行模式 (node api/index.js) ──
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const config = getConfig();
  const server = http.createServer((req, res) => handler(req, res));
  server.listen(config.port, config.host, () => {
    console.log(`[CommandCode-Pool] Server running at http://${config.host}:${config.port}`);
    console.log(`[CommandCode-Pool] Admin console: http://localhost:${config.port}/admin`);
    console.log(`[CommandCode-Pool] OpenAI Chat API: http://localhost:${config.port}/v1/chat/completions`);
  });
}

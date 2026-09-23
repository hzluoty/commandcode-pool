// Command Code Pool — Cloudflare Worker gateway for Command Code Provider API
// Native protocol relay: Chat Completions, Anthropic Messages, and Responses.
// One logical request -> one selected account -> single upstream attempt.
// No prompt replay, no CLI emulation, no synthetic device fingerprints.

import { prepareGenerationRequest } from './lib/api-adapters.js';
import { createProviderFailurePolicy, ClientInputError } from './lib/failure-policy.js';
import { runSingleAccountGeneration } from './lib/generation-runner.js';
import { sendGeneration } from './lib/upstream-client.js';
import { selectSingleAccount, unavailableAccountResponse, persistAccountHealth } from './lib/pool-store.js';
import { planInfo, buildDetail, fetchAccountQuota } from './lib/quota-client.js';

// ============================ 常量与配置 ============================

const DEFAULT_BASE = 'https://api.commandcode.ai';
const DEFAULT_TZ_OFFSET = 8;
const DEFAULT_CONTROL_PLANE_TIMEOUT_MS = 15000;
const D1_MIGRATION_LOCK_MS = 120000;
const D1_MIGRATION_BATCH_ROWS = 200;
const D1_MIGRATION_WAIT_MS = 250;
const USAGE_BUCKET_SECONDS = 300; // 5 分钟分桶
const MAX_USAGE_RETENTION_SECONDS = 30 * 86400; // 最多保留 30 天分桶

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, x-api-key, x-admin-token, x-cmd-zdr',
};

function isRecord(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function str(v) {
  return typeof v === 'string' ? v : (v == null ? '' : String(v));
}

function maskKey(key) {
  if (!key) return '';
  if (key.length <= 8) return key.slice(0, 2) + '***' + key.slice(-2);
  return key.slice(0, 4) + '***' + key.slice(-4);
}

async function safeEqual(a, b) {
  const enc = new TextEncoder();
  const ha = await crypto.subtle.digest('SHA-256', enc.encode(a || ''));
  const hb = await crypto.subtle.digest('SHA-256', enc.encode(b || ''));
  const ab = new Uint8Array(ha);
  const bb = new Uint8Array(hb);
  let diff = 0;
  for (let i = 0; i < ab.length; i++) diff |= ab[i] ^ bb[i];
  return diff === 0;
}

function bearerToken(request) {
  const auth = request.headers.get('Authorization') || '';
  const match = auth.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function randomHex(len = 16) {
  const bytes = new Uint8Array(len);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...JSON_HEADERS, ...headers },
  });
}

function errorResponse(status, type, message, code = null, headers = {}) {
  return json(
    {
      error: {
        message,
        type,
        ...(code ? { code } : {}),
      },
    },
    status,
    headers,
  );
}

async function readBody(request) {
  const type = request.headers.get('content-type') || '';
  if (type.includes('application/json')) {
    try {
      return await request.json();
    } catch {
      return null;
    }
  }
  return null;
}

function selectionStrategy(env = {}) {
  const s = String(env.POOL_STRATEGY || '').trim().toLowerCase();
  return s === 'round_robin' ? 'round_robin' : 'sticky';
}

function getConfig(env = {}) {
  return {
    apiBase: env.API_BASE || DEFAULT_BASE,
    adminToken: env.ADMIN_TOKEN || '',
    apiKeys: env.API_KEY || '',
    allowAnonymous: env.ALLOW_ANONYMOUS === 'true' || env.ALLOW_ANONYMOUS === '1',
    tzOffset: Number.isFinite(Number(env.TIMEZONE_OFFSET)) ? Number(env.TIMEZONE_OFFSET) : DEFAULT_TZ_OFFSET,
    controlPlaneTimeoutMs: DEFAULT_CONTROL_PLANE_TIMEOUT_MS,
    defaultModel: env.DEFAULT_MODEL || 'deepseek/deepseek-v4-flash',
    messagesDefaultModel: env.MESSAGES_DEFAULT_MODEL || 'claude-sonnet-4-6',
    defaultMaxTokens: Number.isFinite(Number(env.DEFAULT_MAX_TOKENS)) ? Number(env.DEFAULT_MAX_TOKENS) : 4096,
    selectionStrategy: selectionStrategy(env),
  };
}

function configuredClientKeys(env) {
  const raw = env.API_KEY || '';
  return raw.split(',').map((k) => k.trim()).filter(Boolean);
}

function clientAuthConfigured(env) {
  return configuredClientKeys(env).length > 0;
}

function isAnonymousAllowed(env) {
  return env.ALLOW_ANONYMOUS === 'true' || env.ALLOW_ANONYMOUS === '1';
}

async function checkClientAuth(request, env) {
  const keys = configuredClientKeys(env);
  if (!keys.length) return isAnonymousAllowed(env);
  const token = bearerToken(request) || (request.headers.get('x-api-key') || '').trim();
  for (const key of keys) {
    if (await safeEqual(token, key)) return true;
  }
  return false;
}

function configuredAdminToken(env) {
  return String(env.ADMIN_TOKEN || '');
}

function adminAuthConfigured(env) {
  return !!configuredAdminToken(env);
}

async function checkAdminAuth(request, env) {
  const token = configuredAdminToken(env);
  if (!token) return isAnonymousAllowed(env);
  const given = request.headers.get('x-admin-token') || bearerToken(request);
  return await safeEqual(given || '', token);
}

// ============================ §U 用量统计辅助 ============================

function dayStartEpoch(dayStr, tzOffset = DEFAULT_TZ_OFFSET) {
  const match = String(dayStr || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return 0;
  const year = Number(match[1]);
  const month = Number(match[2]) - 1;
  const day = Number(match[3]);
  return Math.floor(Date.UTC(year, month, day, -tzOffset, 0, 0, 0) / 1000);
}

function usageBucketStart(timeMs, tzOffset = DEFAULT_TZ_OFFSET) {
  const sec = Math.floor(timeMs / 1000);
  const local = sec + tzOffset * 3600;
  const bucketLocal = Math.floor(local / USAGE_BUCKET_SECONDS) * USAGE_BUCKET_SECONDS;
  return bucketLocal - tzOffset * 3600;
}

function normalizeUsageRange(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (s === '5m' || s === '5min') return { key: '5m', label: '最近 5 分钟', seconds: 300, intervalSeconds: 300 };
  if (s === '1h') return { key: '1h', label: '最近 1 小时', seconds: 3600, intervalSeconds: 300 };
  if (s === '6h') return { key: '6h', label: '最近 6 小时', seconds: 6 * 3600, intervalSeconds: 300 };
  if (s === '24h' || s === '1d') return { key: '1d', label: '最近 24 小时', seconds: 86400, intervalSeconds: 300 };
  if (s === '7d') return { key: '7d', label: '最近 7 天', seconds: 7 * 86400, intervalSeconds: 3600 };
  if (s === '30d') return { key: '30d', label: '最近 30 天', seconds: 30 * 86400, intervalSeconds: 86400 };
  return { key: '1d', label: '最近 24 小时', seconds: 86400, intervalSeconds: 300 };
}

function buildUsage(bucketRows, labels, info, tzOffset) {
  const totals = { requests: 0, promptTokens: 0, completionTokens: 0, cacheReadTokens: 0 };
  const accounts = {};
  const bucketsMap = new Map();

  for (const row of bucketRows) {
    const accId = row.account_id;
    if (!accounts[accId]) {
      accounts[accId] = {
        id: accId,
        label: labels[accId] || `Account #${accId}`,
        requests: 0,
        promptTokens: 0,
        completionTokens: 0,
        cacheReadTokens: 0,
      };
    }
    const a = accounts[accId];
    a.requests += row.requests || 0;
    a.promptTokens += row.prompt_tokens || 0;
    a.completionTokens += row.completion_tokens || 0;
    a.cacheReadTokens += row.cache_read_tokens || 0;

    totals.requests += row.requests || 0;
    totals.promptTokens += row.prompt_tokens || 0;
    totals.completionTokens += row.completion_tokens || 0;
    totals.cacheReadTokens += row.cache_read_tokens || 0;

    const intervalStart = Math.floor(row.bucket_start / info.intervalSeconds) * info.intervalSeconds;
    let b = bucketsMap.get(intervalStart);
    if (!b) {
      b = { timestamp: intervalStart, requests: 0, promptTokens: 0, completionTokens: 0, cacheReadTokens: 0 };
      bucketsMap.set(intervalStart, b);
    }
    b.requests += row.requests || 0;
    b.promptTokens += row.prompt_tokens || 0;
    b.completionTokens += row.completion_tokens || 0;
    b.cacheReadTokens += row.cache_read_tokens || 0;
  }

  const timeline = Array.from(bucketsMap.values()).sort((a, b) => a.timestamp - b.timestamp);
  return {
    totals,
    accounts: Object.values(accounts),
    timeline,
  };
}

// ============================ §M 上游与模型 ============================

async function upstreamFetch(url, options = {}, { signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  if (signal) signal.addEventListener('abort', () => controller.abort(signal.reason), { once: true });
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchModels(base, apiKey, signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS) {
  return upstreamFetch(`${base}/provider/v1/models`, {
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Accept': 'application/json',
    },
  }, { signal, timeoutMs });
}

async function getModels(env, base, signal) {
  const pool = getPool(env);
  const key = await pool.anyKey();
  const fallback = [
    { id: 'deepseek/deepseek-v4-flash', object: 'model' },
    { id: 'deepseek/deepseek-v4-pro', object: 'model' },
    { id: 'claude-sonnet-4-6', object: 'model' },
    { id: 'gpt-5.5', object: 'model' },
  ];
  if (!key) return fallback;
  try {
    const res = await fetchModels(base, key, signal);
    if (!res.ok) return fallback;
    const data = await res.json();
    return Array.isArray(data?.data) ? data.data : (Array.isArray(data) ? data : fallback);
  } catch {
    return fallback;
  }
}

// ============================ §L 账号池 ============================

const ACCOUNT_ROW_FIELDS = `id, api_key, label, enabled, user_name, plan_id, plan_name,
  monthly_left, purchased, free,
  five_hour_used, five_hour_cap, five_hour_exceeded, five_hour_reset,
  weekly_used, weekly_cap, weekly_exceeded, weekly_reset,
  rate_limited_until, disabled_reason, last_error, last_used_at,
  requests, prompt_tokens, completion_tokens, cache_read_tokens, created_at, detail`;

function parseDetailValue(raw) {
  if (isRecord(raw)) return { ...raw };
  if (typeof raw !== 'string' || !raw) return {};
  try {
    const value = JSON.parse(raw);
    return isRecord(value) ? value : {};
  } catch {
    return {};
  }
}

function publicDetail(raw) {
  const detail = parseDetailValue(raw);
  return Object.keys(detail).length ? detail : null;
}

function rowToAccount(row) {
  return {
    id: row.id,
    maskedKey: maskKey(row.api_key),
    label: row.label || '',
    enabled: !!row.enabled,
    user_name: row.user_name || '',
    plan: row.plan_id || row.plan_name ? {
      planId: row.plan_id,
      name: row.plan_name || row.plan_id,
      monthlyCredits: planInfo(row.plan_id)?.monthlyCredits ?? null,
    } : null,
    credits: {
      monthlyCredits: row.monthly_left,
      purchasedCredits: row.purchased,
      freeCredits: row.free,
      fiveHour: row.five_hour_cap != null ? {
        used: row.five_hour_used,
        cap: row.five_hour_cap,
        exceeded: !!row.five_hour_exceeded,
        resetAt: row.five_hour_reset,
      } : null,
      weekly: row.weekly_cap != null ? {
        used: row.weekly_used,
        cap: row.weekly_cap,
        exceeded: !!row.weekly_exceeded,
        resetAt: row.weekly_reset,
      } : null,
    },
    pool: {
      rateLimited: (row.rate_limited_until || 0) > Date.now(),
      rateLimitedUntil: row.rate_limited_until || 0,
      disabledReason: row.disabled_reason || '',
    },
    lastError: row.last_error || '',
    lastUsedAt: row.last_used_at || 0,
    usage: {
      requests: row.requests || 0,
      promptTokens: row.prompt_tokens || 0,
      completionTokens: row.completion_tokens || 0,
      cacheReadTokens: row.cache_read_tokens || 0,
    },
    detail: publicDetail(row.detail),
    createdAt: row.created_at,
  };
}

class D1Pool {
  constructor(db) {
    this.db = db;
    this._schemaReady = false;
    this._schemaPromise = null;
    this._seedPromise = null;
    this.tzOffset = DEFAULT_TZ_OFFSET;
  }

  async ensureSchema() {
    if (this._schemaReady) return;
    if (this._schemaPromise) return this._schemaPromise;
    this._schemaPromise = (async () => {
      for (let attempt = 0; attempt < 8; attempt++) {
        const ready = await this._ensureSchema();
        if (ready !== false) return;
        await new Promise((resolve) => setTimeout(resolve, D1_MIGRATION_WAIT_MS));
      }
      const error = new Error('D1 usage migration is still in progress');
      error.code = 'D1_MIGRATION_IN_PROGRESS';
      error.status = 503;
      throw error;
    })();
    try {
      await this._schemaPromise;
      this._schemaReady = true;
    } finally {
      this._schemaPromise = null;
    }
  }

  async _ensureSchema() {
    if (this._schemaReady) return;
    await this.db.batch([
      this.db.prepare(`CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_key TEXT NOT NULL UNIQUE,
        label TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        user_name TEXT NOT NULL DEFAULT '',
        plan_id TEXT NOT NULL DEFAULT '',
        plan_name TEXT NOT NULL DEFAULT '',
        monthly_left REAL, purchased REAL, free REAL,
        five_hour_used REAL, five_hour_cap REAL, five_hour_exceeded INTEGER NOT NULL DEFAULT 0,
        five_hour_reset INTEGER NOT NULL DEFAULT 0,
        weekly_used REAL, weekly_cap REAL, weekly_exceeded INTEGER NOT NULL DEFAULT 0,
        weekly_reset INTEGER NOT NULL DEFAULT 0,
        rate_limited_until INTEGER NOT NULL DEFAULT 0,
        disabled_reason TEXT NOT NULL DEFAULT '',
        last_error TEXT NOT NULL DEFAULT '',
        last_used_at INTEGER NOT NULL DEFAULT 0,
        requests INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        detail TEXT
      )`),
      this.db.prepare(`CREATE TABLE IF NOT EXISTS usage_buckets (
        bucket_start INTEGER NOT NULL,
        account_id INTEGER NOT NULL,
        requests INTEGER NOT NULL DEFAULT 0,
        prompt_tokens INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (bucket_start, account_id),
        FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
      )`),
      this.db.prepare(`CREATE TABLE IF NOT EXISTS usage_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL DEFAULT ''
      )`),
      this.db.prepare(`CREATE TABLE IF NOT EXISTS usage_migration_rows (
        migration_key TEXT NOT NULL,
        day TEXT NOT NULL,
        account_id INTEGER NOT NULL,
        PRIMARY KEY (migration_key, day, account_id)
      )`),
    ]);

    const { results: accountInfo } = await this.db.prepare('PRAGMA table_info(accounts)').all();
    const accountColumns = new Set((accountInfo || []).map((row) => row.name));
    const accountMigrations = {
      label: "TEXT NOT NULL DEFAULT ''",
      enabled: 'INTEGER NOT NULL DEFAULT 1',
      user_name: "TEXT NOT NULL DEFAULT ''",
      plan_id: "TEXT NOT NULL DEFAULT ''",
      plan_name: "TEXT NOT NULL DEFAULT ''",
      monthly_left: 'REAL',
      purchased: 'REAL',
      free: 'REAL',
      five_hour_used: 'REAL',
      five_hour_cap: 'REAL',
      five_hour_exceeded: 'INTEGER NOT NULL DEFAULT 0',
      five_hour_reset: 'INTEGER NOT NULL DEFAULT 0',
      weekly_used: 'REAL',
      weekly_cap: 'REAL',
      weekly_exceeded: 'INTEGER NOT NULL DEFAULT 0',
      weekly_reset: 'INTEGER NOT NULL DEFAULT 0',
      rate_limited_until: 'INTEGER NOT NULL DEFAULT 0',
      disabled_reason: "TEXT NOT NULL DEFAULT ''",
      last_error: "TEXT NOT NULL DEFAULT ''",
      last_used_at: 'INTEGER NOT NULL DEFAULT 0',
      requests: 'INTEGER NOT NULL DEFAULT 0',
      prompt_tokens: 'INTEGER NOT NULL DEFAULT 0',
      completion_tokens: 'INTEGER NOT NULL DEFAULT 0',
      cache_read_tokens: 'INTEGER NOT NULL DEFAULT 0',
      created_at: 'INTEGER NOT NULL DEFAULT 0',
      detail: 'TEXT',
    };
    for (const [name, definition] of Object.entries(accountMigrations)) {
      if (!accountColumns.has(name)) {
        try {
          await this.db.prepare(`ALTER TABLE accounts ADD COLUMN ${name} ${definition}`).run();
        } catch (e) {
          if (!/duplicate column/i.test(String(e?.message || e))) throw e;
        }
      }
    }

    await this.db.batch([
      this.db.prepare('CREATE INDEX IF NOT EXISTS idx_accounts_enabled ON accounts(enabled, rate_limited_until)'),
      this.db.prepare('CREATE INDEX IF NOT EXISTS idx_usage_buckets_start ON usage_buckets(bucket_start)'),
    ]);
    this._schemaReady = true;
  }

  async pick(strategy = 'sticky') {
    await this.ensureSchema();
    const order = strategy === 'round_robin' ? 'ASC' : 'DESC';
    const row = await this.db.prepare(
      `SELECT ${ACCOUNT_ROW_FIELDS} FROM accounts
       WHERE enabled = 1 AND rate_limited_until <= ?
       ORDER BY last_used_at ${order}, id ASC LIMIT 1`
    ).bind(Date.now()).first();
    return row || null;
  }

  async enabledCount() {
    await this.ensureSchema();
    const row = await this.db.prepare('SELECT COUNT(*) AS n FROM accounts WHERE enabled = 1').first();
    return row ? row.n : 0;
  }

  async availableKeys() {
    await this.ensureSchema();
    const { results } = await this.db.prepare(
      'SELECT api_key FROM accounts WHERE enabled = 1 AND rate_limited_until <= ? ORDER BY last_used_at DESC, id ASC'
    ).bind(Date.now()).all();
    return (results || []).map((row) => row.api_key).filter(Boolean);
  }

  async anyKey() {
    const keys = await this.availableKeys();
    return keys[0] || null;
  }

  async list() {
    await this.ensureSchema();
    const { results } = await this.db.prepare(`SELECT ${ACCOUNT_ROW_FIELDS} FROM accounts ORDER BY id ASC`).all();
    return (results || []).map(rowToAccount);
  }

  async add(key, label) {
    await this.ensureSchema();
    let r;
    try {
      r = await this.db.prepare(
        'INSERT INTO accounts (api_key, label, created_at) VALUES (?, ?, ?)'
      ).bind(key, label || '', Date.now()).run();
    } catch (e) {
      if (/UNIQUE|constraint/i.test(String(e?.message || e))) {
        const error = new Error('该密钥已存在');
        error.status = 409;
        throw error;
      }
      throw e;
    }
    return r.meta && r.meta.last_row_id;
  }

  async getCredential(id) {
    await this.ensureSchema();
    const row = await this.db.prepare('SELECT api_key FROM accounts WHERE id = ?').bind(id).first();
    return row?.api_key || null;
  }

  async patch(id, { label, enabled }) {
    await this.ensureSchema();
    const sets = [];
    const params = [];
    if (label !== undefined) {
      sets.push('label = ?');
      params.push(String(label).slice(0, 60));
    }
    if (enabled !== undefined) {
      sets.push('enabled = ?');
      params.push(enabled ? 1 : 0);
      if (enabled) {
        sets.push('disabled_reason = ?');
        params.push('');
      }
    }
    if (!sets.length) return false;
    params.push(id);
    const result = await this.db.prepare(`UPDATE accounts SET ${sets.join(', ')} WHERE id = ?`).bind(...params).run();
    return Number(result?.meta?.changes || 0) > 0;
  }

  async remove(id) {
    await this.ensureSchema();
    const result = await this.db.prepare('DELETE FROM accounts WHERE id = ?').bind(id).run();
    if (Number(result?.meta?.changes || 0) > 0) {
      await this.db.prepare('DELETE FROM usage_buckets WHERE account_id = ?').bind(id).run();
      return true;
    }
    return false;
  }

  async recordSuccess(id, usage) {
    await this.ensureSchema();
    const cacheRead = usage.prompt_tokens_details?.cached_tokens || 0;
    await this.db.batch([
      this.db.prepare(
        `UPDATE accounts SET requests = requests + 1,
           prompt_tokens = prompt_tokens + ?, completion_tokens = completion_tokens + ?,
           cache_read_tokens = cache_read_tokens + ?,
           last_used_at = ?, last_error = ''
         WHERE id = ?`
      ).bind(usage.prompt_tokens || 0, usage.completion_tokens || 0, cacheRead, Date.now(), id),
      this.db.prepare(
        `INSERT INTO usage_buckets (bucket_start, account_id, requests, prompt_tokens, completion_tokens, cache_read_tokens)
         VALUES (?, ?, 1, ?, ?, ?)
         ON CONFLICT(bucket_start, account_id) DO UPDATE SET
           requests = requests + 1,
           prompt_tokens = prompt_tokens + excluded.prompt_tokens,
           completion_tokens = completion_tokens + excluded.completion_tokens,
           cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens`
      ).bind(usageBucketStart(Date.now(), this.tzOffset), id, usage.prompt_tokens || 0, usage.completion_tokens || 0, cacheRead),
    ]);
  }

  async dailyUsage(range = '1d') {
    await this.ensureSchema();
    const info = normalizeUsageRange(range);
    const now = Math.floor(Date.now() / 1000);
    const since = now - info.seconds;
    const { results: bucketRows } = await this.db.prepare(
      `SELECT bucket_start, account_id, requests, prompt_tokens, completion_tokens, cache_read_tokens
       FROM usage_buckets WHERE bucket_start >= ? ORDER BY bucket_start ASC`
    ).bind(since).all();
    const accounts = await this.list();
    const labels = {};
    for (const a of accounts) labels[a.id] = a.label || a.user_name || '';
    return buildUsage(bucketRows || [], labels, info, this.tzOffset);
  }

  async cooldown(id, seconds, lastError) {
    await this.ensureSchema();
    await this.db.prepare(
      'UPDATE accounts SET rate_limited_until = ?, last_error = ? WHERE id = ?'
    ).bind(Date.now() + seconds * 1000, lastError || '', id).run();
  }

  async disable(id, reason) {
    await this.ensureSchema();
    await this.db.prepare(
      'UPDATE accounts SET enabled = 0, disabled_reason = ?, last_error = ? WHERE id = ?'
    ).bind(reason, reason, id).run();
  }

  async earliestRateLimitedUntil() {
    await this.ensureSchema();
    const row = await this.db.prepare(
      'SELECT MIN(rate_limited_until) AS t FROM accounts WHERE enabled = 1 AND rate_limited_until > ?'
    ).bind(Date.now()).first();
    return row && row.t ? row.t : 0;
  }

  async markQuotaError(id, message) {
    await this.ensureSchema();
    await this.db.prepare('UPDATE accounts SET last_error = ? WHERE id = ?').bind(message, id).run();
  }

  async saveQuota(id, report) {
    await this.ensureSchema();
    const c = report.credits || {};
    const fh = c.fiveHour || {}, wk = c.weekly || {};
    const p = report.plan || {};
    const uName = report.account?.userName || '';
    const pId = p.planId || '';
    const pName = p.name || '';
    const detail = buildDetail(report);
    await this.db.prepare(
      `UPDATE accounts SET
         user_name = COALESCE(NULLIF(?, ''), user_name),
         plan_id = COALESCE(NULLIF(?, ''), plan_id),
         plan_name = COALESCE(NULLIF(?, ''), plan_name),
         monthly_left = ?, purchased = ?, free = ?,
         five_hour_used = ?, five_hour_cap = ?, five_hour_exceeded = ?, five_hour_reset = ?,
         weekly_used = ?, weekly_cap = ?, weekly_exceeded = ?, weekly_reset = ?,
         detail = ?, last_error = ''
       WHERE id = ?`
    ).bind(
      uName, pId, pName,
      c.monthlyCredits ?? null, c.purchasedCredits ?? null, c.freeCredits ?? null,
      fh.used ?? null, fh.cap ?? null, fh.exceeded ? 1 : 0, fh.resetAt ?? 0,
      wk.used ?? null, wk.cap ?? null, wk.exceeded ? 1 : 0, wk.resetAt ?? 0,
      detail, id,
    ).run();
  }

  async seedIfEmpty(keys) {
    const normalized = (keys || []).filter((item) => item && item.key).map((item) => ({ key: String(item.key), label: String(item.label || '') }));
    if (this._seedPromise) {
      try {
        await this._seedPromise;
      } catch {
        /* ignore */
      }
    }
    if (!normalized.length) return 0;
    this._seedPromise = (async () => {
      await this.ensureSchema();
      let added = 0;
      for (const item of normalized) {
        const result = await this.db.prepare('INSERT OR IGNORE INTO accounts (api_key, label, created_at) VALUES (?, ?, ?)')
          .bind(item.key, item.label, Date.now()).run();
        if (Number(result?.meta?.changes || 0) > 0) added++;
      }
      return added;
    })();
    try {
      return await this._seedPromise;
    } finally {
      this._seedPromise = null;
    }
  }
}

class MemPool {
  constructor(items) {
    this.seq = 1;
    this.tzOffset = DEFAULT_TZ_OFFSET;
    this.buckets = new Map();
    this.accounts = items.map((it) => ({
      id: this.seq++,
      api_key: it.key,
      label: it.label || '',
      enabled: true,
      user_name: '',
      plan_id: '',
      plan_name: '',
      monthly_left: null,
      purchased: null,
      free: null,
      five_hour_used: null,
      five_hour_cap: null,
      five_hour_exceeded: 0,
      five_hour_reset: 0,
      weekly_used: null,
      weekly_cap: null,
      weekly_exceeded: 0,
      weekly_reset: 0,
      rate_limited_until: 0,
      disabled_reason: '',
      last_error: '',
      last_used_at: 0,
      requests: 0,
      prompt_tokens: 0,
      completion_tokens: 0,
      cache_read_tokens: 0,
      created_at: Date.now(),
      detail: null,
    }));
  }

  async pick(strategy = 'sticky') {
    const now = Date.now();
    const avail = this.accounts
      .filter((a) => a.enabled && a.rate_limited_until <= now)
      .sort((a, b) => (strategy === 'round_robin' ? a.last_used_at - b.last_used_at : b.last_used_at - a.last_used_at) || a.id - b.id);
    return avail[0] || null;
  }

  async enabledCount() {
    return this.accounts.filter((a) => a.enabled).length;
  }

  async availableKeys() {
    const now = Date.now();
    return this.accounts
      .filter((a) => a.enabled && a.rate_limited_until <= now)
      .sort((a, b) => b.last_used_at - a.last_used_at || a.id - b.id)
      .map((a) => a.api_key)
      .filter(Boolean);
  }

  async anyKey() {
    const keys = await this.availableKeys();
    return keys[0] || null;
  }

  async list() {
    return this.accounts.map(rowToAccount);
  }

  async add(key, label) {
    if (this.accounts.some((a) => a.api_key === key)) {
      const e = new Error('该密钥已存在');
      e.status = 409;
      throw e;
    }
    const id = this.seq++;
    this.accounts.push({
      id,
      api_key: key,
      label: label || '',
      enabled: true,
      user_name: '',
      plan_id: '',
      plan_name: '',
      monthly_left: null,
      purchased: null,
      free: null,
      five_hour_used: null,
      five_hour_cap: null,
      five_hour_exceeded: 0,
      five_hour_reset: 0,
      weekly_used: null,
      weekly_cap: null,
      weekly_exceeded: 0,
      weekly_reset: 0,
      rate_limited_until: 0,
      disabled_reason: '',
      last_error: '',
      last_used_at: 0,
      requests: 0,
      prompt_tokens: 0,
      completion_tokens: 0,
      cache_read_tokens: 0,
      created_at: Date.now(),
      detail: null,
    });
    return id;
  }

  async getCredential(id) {
    return this.accounts.find((a) => a.id === id)?.api_key || null;
  }

  async patch(id, { label, enabled }) {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return false;
    if (label !== undefined) a.label = String(label).slice(0, 60);
    if (enabled !== undefined) {
      a.enabled = !!enabled;
      if (a.enabled) a.disabled_reason = '';
    }
    return true;
  }

  async remove(id) {
    const i = this.accounts.findIndex((a) => a.id === id);
    if (i < 0) return false;
    this.accounts.splice(i, 1);
    return true;
  }

  async recordSuccess(id, usage) {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return;
    a.requests++;
    a.prompt_tokens += usage.prompt_tokens || 0;
    a.completion_tokens += usage.completion_tokens || 0;
    a.cache_read_tokens += usage.prompt_tokens_details?.cached_tokens || 0;
    a.last_used_at = Date.now();
    a.last_error = '';
    this.pruneBuckets(Math.floor(Date.now() / 1000));
    const bucket = usageBucketStart(Date.now(), this.tzOffset);
    let perAcct = this.buckets.get(bucket);
    if (!perAcct) {
      perAcct = new Map();
      this.buckets.set(bucket, perAcct);
    }
    const cur = perAcct.get(id) || { requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0 };
    cur.requests++;
    cur.prompt_tokens += usage.prompt_tokens || 0;
    cur.completion_tokens += usage.completion_tokens || 0;
    cur.cache_read_tokens += usage.prompt_tokens_details?.cached_tokens || 0;
    perAcct.set(id, cur);
  }

  pruneBuckets(nowSeconds = Math.floor(Date.now() / 1000)) {
    const cutoff = nowSeconds - MAX_USAGE_RETENTION_SECONDS;
    for (const bucketStart of this.buckets.keys()) {
      if (bucketStart < cutoff) this.buckets.delete(bucketStart);
    }
    while (this.buckets.size > Math.ceil(MAX_USAGE_RETENTION_SECONDS / USAGE_BUCKET_SECONDS) + 1) {
      this.buckets.delete(this.buckets.keys().next().value);
    }
  }

  async dailyUsage(range = '1d') {
    const info = normalizeUsageRange(range);
    this.pruneBuckets(Math.floor(Date.now() / 1000));
    const cutoff = Math.floor(Date.now() / 1000) - info.seconds;
    const labels = {};
    for (const a of this.accounts) labels[a.id] = a.label || a.user_name || '';
    const rows = [];
    for (const [bucket_start, perAcct] of this.buckets) {
      if (bucket_start < cutoff) continue;
      for (const [account_id, v] of perAcct) rows.push({ bucket_start, account_id, ...v });
    }
    return buildUsage(rows, labels, info, this.tzOffset);
  }

  async cooldown(id, seconds, lastError) {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return;
    a.rate_limited_until = Date.now() + seconds * 1000;
    a.last_error = lastError || '';
  }

  async disable(id, reason) {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return;
    a.enabled = false;
    a.disabled_reason = reason;
    a.last_error = reason;
  }

  async earliestRateLimitedUntil() {
    const now = Date.now();
    const times = this.accounts.filter((a) => a.enabled && a.rate_limited_until > now).map((a) => a.rate_limited_until);
    return times.length ? Math.min(...times) : 0;
  }

  async markQuotaError(id, message) {
    const a = this.accounts.find((a) => a.id === id);
    if (a) a.last_error = message;
  }

  async saveQuota(id, report) {
    const a = this.accounts.find((x) => x.id === id);
    if (!a) return false;
    const c = report.credits || {};
    const fh = c.fiveHour || {}, wk = c.weekly || {};
    const p = report.plan || {};
    if (report.account?.userName) a.user_name = report.account.userName;
    if (p.planId) a.plan_id = p.planId;
    if (p.name) a.plan_name = p.name;
    a.monthly_left = c.monthlyCredits ?? null;
    a.purchased = c.purchasedCredits ?? null;
    a.free = c.freeCredits ?? null;
    a.five_hour_used = fh.used ?? null;
    a.five_hour_cap = fh.cap ?? null;
    a.five_hour_exceeded = fh.exceeded ? 1 : 0;
    a.five_hour_reset = fh.resetAt ?? 0;
    a.weekly_used = wk.used ?? null;
    a.weekly_cap = wk.cap ?? null;
    a.weekly_exceeded = wk.exceeded ? 1 : 0;
    a.weekly_reset = wk.resetAt ?? 0;
    a.detail = buildDetail(report);
    a.last_error = '';
    return true;
  }

  async seedIfEmpty() {
    return 0;
  }
}

function parseAccountsEnv(raw) {
  if (!raw) return [];
  const trimmed = raw.trim();
  if (trimmed.startsWith('[')) {
    try {
      const arr = JSON.parse(trimmed);
      return (Array.isArray(arr) ? arr : [])
        .map((it) => isRecord(it) ? { key: str(it.key) || '', label: str(it.label) || '' } : { key: String(it), label: '' })
        .filter((it) => it.key);
    } catch {
      return [];
    }
  }
  return trimmed.split(',').map((s) => s.trim()).filter(Boolean).map((key) => ({ key, label: '' }));
}

const d1Pools = new WeakMap();
const memPools = new WeakMap();

function getPool(env) {
  const tz = Number.isFinite(Number(env.TIMEZONE_OFFSET)) ? Number(env.TIMEZONE_OFFSET) : DEFAULT_TZ_OFFSET;
  if (env.DB) {
    let p = d1Pools.get(env.DB);
    if (!p) {
      p = new D1Pool(env.DB);
      d1Pools.set(env.DB, p);
    }
    p.tzOffset = tz;
    return p;
  }
  let p = memPools.get(env);
  if (!p) {
    p = new MemPool(parseAccountsEnv(env.ACCOUNTS));
    memPools.set(env, p);
  }
  p.tzOffset = tz;
  return p;
}

// ============================ §A 管理端业务 ============================

async function buildPoolState(env, pool, accountList) {
  const strategy = selectionStrategy(env);
  const accounts = accountList || await pool.list();
  const enabled = accounts.filter((a) => a.enabled);
  const active = await pool.pick(strategy);
  return {
    strategy,
    d1: !!env.DB,
    tzOffset: pool.tzOffset,
    totals: {
      accounts: accounts.length,
      enabled: enabled.length,
      rateLimited: enabled.filter((a) => a.pool.rateLimited).length,
      requests: accounts.reduce((s, a) => s + a.usage.requests, 0),
      promptTokens: accounts.reduce((s, a) => s + a.usage.promptTokens, 0),
      completionTokens: accounts.reduce((s, a) => s + a.usage.completionTokens, 0),
      cacheReadTokens: accounts.reduce((s, a) => s + a.usage.cacheReadTokens, 0),
    },
    activeAccount: active ? {
      id: active.id,
      label: active.label,
      maskedKey: maskKey(active.api_key),
    } : null,
  };
}

async function provisionAccount(env, key, label, signal) {
  const cfg = getConfig(env);
  const pool = getPool(env);
  if (!/^[\x21-\x7e]+$/.test(key)) {
    const e = new Error('密钥格式不对：检测到非 ASCII 字符。密钥应是纯 ASCII 字符串，请重新复制粘贴。');
    e.status = 400;
    throw e;
  }
  let res;
  try {
    res = await fetchModels(cfg.apiBase, key, signal, cfg.controlPlaneTimeoutMs);
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError') throw e;
    const err = new Error(e.message || '上游验证连接失败');
    err.status = 502;
    throw err;
  }
  if (res.status === 401 || res.status === 403) {
    const err = new Error(`密钥无效或已失效 (HTTP ${res.status})`);
    err.status = 400;
    throw err;
  }
  if (!res.ok) {
    const err = new Error(`上游验证失败 (HTTP ${res.status})`);
    err.status = res.status >= 500 ? 502 : res.status;
    throw err;
  }
  return pool.add(key, label || maskKey(key));
}

async function handleAdmin(request, env, ctx, url) {
  const base = getConfig(env).apiBase;
  const path = url.pathname;
  const pool = getPool(env);

  if (path === '/api/login' && request.method === 'POST') {
    if (!adminAuthConfigured(env)) {
      if (isAnonymousAllowed(env)) return json({ ok: true });
      return json({ error: 'admin authentication is not configured', code: 'auth_not_configured' }, 503);
    }
    const body = await readBody(request) || {};
    if (await safeEqual(String(body.password || ''), configuredAdminToken(env))) return json({ ok: true });
    return json({ error: '密码不对' }, 401);
  }

  if (path === '/api/accounts' && request.method === 'GET') {
    return json({ accounts: await pool.list() });
  }

  if (path === '/api/accounts' && request.method === 'POST') {
    const body = await readBody(request) || {};
    const key = String(body.key || '').trim();
    const label = String(body.label || '').trim().slice(0, 60);
    if (!key) return json({ error: 'missing key' }, 400);
    let id;
    try {
      id = await provisionAccount(env, key, label, request.signal);
    } catch (e) {
      return json({ error: e.message || '验证失败' }, e.status || 502);
    }
    const accounts = await pool.list();
    return json({ account: accounts.find((a) => a.id === id) || null }, 201);
  }

  const m = path.match(/^\/api\/accounts\/(\d+)$/);
  if (m) {
    const id = Number(m[1]);
    const all = await pool.list();
    const exists = all.some((a) => a.id === id);
    if (!exists) return json({ error: '账号不存在' }, 404);

    if (request.method === 'PATCH') {
      const body = await readBody(request) || {};
      await pool.patch(id, { label: body.label, enabled: body.enabled });
      const fresh = (await pool.list()).find((a) => a.id === id);
      return json({ account: fresh });
    }
    if (request.method === 'DELETE') {
      await pool.remove(id);
      return json({ ok: true });
    }
    return json({ error: 'method not allowed' }, 405);
  }

  if (path === '/api/refresh' && request.method === 'POST') {
    const body = await readBody(request) || {};
    const accounts = await pool.list();
    const targets = body.id != null ? accounts.filter((a) => a.id === Number(body.id)) : accounts;
    if (body.id != null && !targets.length) return json({ error: '账号不存在' }, 404);

    const refreshOne = async (a) => {
      const apiKey = await pool.getCredential(a.id);
      if (!apiKey) return { ...a, lastError: 'account not found' };
      try {
        const res = await fetchModels(base, apiKey, request.signal, getConfig(env).controlPlaneTimeoutMs);
        if (res.status === 401) {
          await pool.disable(a.id, 'key_invalid (401)');
        } else if (res.status === 429) {
          const retryAfter = Number(res.headers.get('retry-after')) || 300;
          await pool.cooldown(a.id, retryAfter, 'rate_limited (429)');
        } else if (res.ok) {
          try {
            const report = await fetchAccountQuota(base, apiKey, {
              signal: request.signal,
              timeoutMs: getConfig(env).controlPlaneTimeoutMs,
            });
            if (report) await pool.saveQuota(a.id, report);
          } catch (qe) {
            if (request.signal.aborted || qe?.name === 'AbortError') throw qe;
            if (qe?.status === 401) {
              await pool.disable(a.id, 'key_invalid (401)');
            } else if (qe?.status === 429) {
              const retryAfter = Number(qe.retryAfter) || 300;
              await pool.cooldown(a.id, retryAfter, 'rate_limited (429)');
            } else {
              await pool.markQuotaError(a.id, qe.message || String(qe));
              console.warn(JSON.stringify({ message: 'quota fetch transient error', accountId: a.id, error: qe.message }));
            }
          }
        } else {
          await pool.markQuotaError(a.id, `upstream models HTTP ${res.status}`);
        }
        return (await pool.list()).find((x) => x.id === a.id) || a;
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError') throw e;
        await pool.markQuotaError(a.id, e.message || String(e));
        return (await pool.list()).find((x) => x.id === a.id) || a;
      }
    };

    const refreshed = await Promise.all(targets.slice(0, 12).map(refreshOne));
    return json({ accounts: refreshed, skipped: Math.max(0, targets.length - 12) });
  }

  if (path === '/api/usage/daily' && request.method === 'GET') {
    const requested = url.searchParams.get('range') || url.searchParams.get('days') || '1d';
    const range = normalizeUsageRange(requested);
    return json({
      range: range.key,
      rangeLabel: range.label,
      seconds: range.seconds,
      intervalSeconds: range.intervalSeconds,
      tzOffset: pool.tzOffset,
      ...(await pool.dailyUsage(range)),
    });
  }

  if (path === '/api/state' && request.method === 'GET') {
    return json(await buildPoolState(env, pool));
  }

  return json({ error: 'not found' }, 404);
}

// ============================ §G 生成调用 ============================

async function handleGenerate(request, env, ctx, kind) {
  const cfg = getConfig(env);
  const pool = getPool(env);
  const requestId = crypto.randomUUID();
  const url = new URL(request.url);

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return errorResponse(400, 'invalid_request_error', 'Invalid JSON body: ' + (e?.message || 'parse error'));
  }

  let prepared;
  try {
    prepared = prepareGenerationRequest(kind, body, {
      request,
      defaultModel: cfg.defaultModel,
      messagesDefaultModel: cfg.messagesDefaultModel,
      defaultMaxTokens: cfg.defaultMaxTokens,
    });
  } catch (error) {
    const status = error.status || 400;
    const code = error.code || 'invalid_request_error';
    return errorResponse(status, 'invalid_request_error', error.message, code);
  }

  const policy = createProviderFailurePolicy();

  return runSingleAccountGeneration({
    request,
    ctx,
    kind: prepared.protocol,
    cfg,
    pool,
    strategy: selectionStrategy(env),
    prepared,
    requestId,
    sendGeneration,
    readErrorText: async (res) => {
      try {
        return await res.text();
      } catch {
        return '';
      }
    },
    policy,
    renderFailure: (k, mapped) => {
      const headers = new Headers();
      headers.set('content-type', 'application/json; charset=utf-8');
      if (mapped.retryAfter) headers.set('retry-after', String(mapped.retryAfter));
      return new Response(JSON.stringify(mapped.body), { status: mapped.status, headers });
    },
  });
}

// ============================ §Q 路由分发 ============================

function serviceDescriptor() {
  return json({
    service: 'commandcode-pool',
    endpoints: [
      '/v1/chat/completions',
      '/v1/messages',
      '/v1/responses',
      '/v1/models',
      '/health',
      '/admin',
      '/api/accounts',
      '/api/state',
      '/api/refresh',
      '/api/usage/daily',
    ],
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const base = getConfig(env).apiBase;
    const needsPool = path.startsWith('/v1/') ||
      path.startsWith('/provider/v1/') ||
      path === '/api/accounts' ||
      path.startsWith('/api/accounts/') ||
      path === '/api/refresh' ||
      path === '/api/state' ||
      path === '/api/usage/daily' ||
      path === '/v1/models' ||
      path === '/provider/v1/models';

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: JSON_HEADERS });

    if (path === '/health') return json({ status: 'ok' });

    // ---------- /v1/* 及 /provider/v1/* 生成端点 ----------
    const isChat = path === '/v1/chat/completions' || path === '/provider/v1/chat/completions' || path === '/chat/completions';
    const isMessages = path === '/v1/messages' || path === '/provider/v1/messages' || path === '/messages';
    const isResponses = path === '/v1/responses' || path === '/provider/v1/responses' || path === '/responses';

    if (isChat || isMessages || isResponses) {
      if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
      if (!clientAuthConfigured(env) && !isAnonymousAllowed(env)) {
        return errorResponse(503, 'server_error', 'client authentication is not configured', 'auth_not_configured');
      }
      if (!(await checkClientAuth(request, env))) return errorResponse(401, 'authentication_error', 'invalid api key');
      if (needsPool && env.DB && env.ACCOUNTS) await getPool(env).seedIfEmpty(parseAccountsEnv(env.ACCOUNTS));
      const kind = isChat ? 'chat' : (isMessages ? 'messages' : 'responses');
      try {
        return await handleGenerate(request, env, ctx, kind);
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError') throw e;
        console.error(JSON.stringify({ message: 'generate request failed', error: e instanceof Error ? e.message : String(e), kind }));
        return errorResponse(500, 'server_error', 'internal server error', 'internal_error');
      }
    }

    if ((path === '/v1/models' || path === '/provider/v1/models') && request.method === 'GET') {
      if (!clientAuthConfigured(env) && !isAnonymousAllowed(env)) {
        return errorResponse(503, 'server_error', 'client authentication is not configured', 'auth_not_configured');
      }
      if (!(await checkClientAuth(request, env))) return errorResponse(401, 'authentication_error', 'invalid api key');
      if (needsPool && env.DB && env.ACCOUNTS) await getPool(env).seedIfEmpty(parseAccountsEnv(env.ACCOUNTS));
      let data;
      try {
        data = await getModels(env, base, request.signal);
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError') throw e;
        console.error(JSON.stringify({ message: 'model catalog request failed', error: e instanceof Error ? e.message : String(e) }));
        return errorResponse(502, 'server_error', 'model catalog unavailable', 'model_catalog_error');
      }
      return json({ object: 'list', data });
    }

    // ---------- /api/*（管理） ----------
    if (path.startsWith('/api/')) {
      if (path !== '/api/login' && !adminAuthConfigured(env) && !isAnonymousAllowed(env)) {
        return json({ error: 'admin authentication is not configured', code: 'auth_not_configured' }, 503);
      }
      if (path !== '/api/login' && !(await checkAdminAuth(request, env))) {
        return json({ error: 'admin-required', message: '需要访问令牌' }, 401);
      }
      try {
        if (needsPool && env.DB && env.ACCOUNTS) await getPool(env).seedIfEmpty(parseAccountsEnv(env.ACCOUNTS));
        return await handleAdmin(request, env, ctx, url);
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError') throw e;
        console.error(JSON.stringify({ message: 'admin request failed', error: e instanceof Error ? e.message : String(e), path }));
        return json(
          { error: e.status && e.status >= 400 && e.status < 600 ? (e.message || 'request failed') : 'internal server error' },
          e.status && e.status >= 400 && e.status < 600 ? e.status : 500,
        );
      }
    }

    // ---------- 静态页面（wrangler [assets] 托管 public/） ----------
    if (path === '/admin' || path === '/admin/') {
      if (env.ASSETS) {
        return env.ASSETS.fetch(new Request(new URL('/admin.html', request.url), request));
      }
      return json({ error: 'not found' }, 404);
    }

    if (path === '/') {
      if (env.ASSETS) return env.ASSETS.fetch(request);
      return serviceDescriptor();
    }

    if (env.ASSETS) return env.ASSETS.fetch(request);

    return json({ error: 'not found' }, 404);
  },
};

export {
  parseAccountsEnv,
  D1Pool,
  MemPool,
  getPool,
  getConfig,
  selectionStrategy,
  provisionAccount,
  fetchModels,
  maskKey,
  safeEqual,
  rowToAccount,
  handleGenerate,
  buildPoolState,
  normalizeUsageRange,
  usageBucketStart,
};

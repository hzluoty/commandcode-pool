// lib/pool-store.js
// 号池存储与调度引擎：支持 Memory、Local JSON / tmp、Vercel KV / Upstash Redis(REST)、以及标准 Redis(REDIS_URL)
import fs from 'node:fs';
import path from 'node:path';
import { DEFAULT_TZ_OFFSET, USAGE_RANGES, USAGE_BUCKET_SECONDS, planInfo } from './constants.js';
import { isRecord, str, num, maskKey, usageBucketStart } from './crypto-utils.js';

export function normalizeUsageRange(value = '1d') {
  if (value && typeof value === 'object' && value.key && value.seconds) return value;
  let key = String(value || '1d').trim().toLowerCase();
  if (/^\d+$/.test(key)) key += 'd';
  if (USAGE_RANGES[key]) return USAGE_RANGES[key];
  const days = Number.parseInt(key, 10);
  if (Number.isFinite(days) && days > 0 && days <= 90) {
    return { key: `${days}d`, seconds: days * 86400, label: `${days}天`, intervalSeconds: 86400 };
  }
  return USAGE_RANGES['1d'];
}

function usagePeriodStart(bucketStart, intervalSeconds, tzOffset) {
  const offset = tzOffset * 3600;
  return Math.floor((Number(bucketStart) + offset) / intervalSeconds) * intervalSeconds - offset;
}

function usagePeriodLabel(periodStart, intervalSeconds, tzOffset) {
  const d = new Date((Number(periodStart) + tzOffset * 3600) * 1000);
  const pad = (n) => String(n).padStart(2, '0');
  const date = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
  if (intervalSeconds >= 86400) return date;
  if (intervalSeconds >= 3600) return `${date} ${pad(d.getUTCHours())}:00`;
  return `${date} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}

export function buildUsage(rows, labels, range, tzOffset) {
  const info = normalizeUsageRange(range);
  const byPeriod = new Map();
  for (const r of rows) {
    const rawStart = r.bucket_start != null ? r.bucket_start : 0;
    const periodStart = usagePeriodStart(rawStart, info.intervalSeconds, tzOffset);
    const period = String(periodStart);
    let d = byPeriod.get(period);
    if (!d) {
      d = {
        day: usagePeriodLabel(periodStart, info.intervalSeconds, tzOffset),
        period,
        requests: 0,
        promptTokens: 0,
        completionTokens: 0,
        cacheReadTokens: 0,
        accountMap: new Map(),
      };
      byPeriod.set(period, d);
    }
    d.requests += r.requests || 0;
    d.promptTokens += r.prompt_tokens || 0;
    d.completionTokens += r.completion_tokens || 0;
    d.cacheReadTokens += r.cache_read_tokens || 0;

    let account = d.accountMap.get(r.account_id);
    if (!account) {
      account = {
        id: r.account_id,
        label: (labels && labels[r.account_id]) || '#' + r.account_id,
        requests: 0,
        promptTokens: 0,
        completionTokens: 0,
        cacheReadTokens: 0,
      };
      d.accountMap.set(r.account_id, account);
    }
    account.requests += r.requests || 0;
    account.promptTokens += r.prompt_tokens || 0;
    account.completionTokens += r.completion_tokens || 0;
    account.cacheReadTokens += r.cache_read_tokens || 0;
  }

  const daily = [...byPeriod.values()]
    .map((d) => {
      const { accountMap, ...row } = d;
      row.accounts = [...accountMap.values()];
      return row;
    })
    .sort((a, b) => Number(a.period) - Number(b.period));

  const totals = daily.reduce((s, d) => ({
    requests: s.requests + d.requests,
    promptTokens: s.promptTokens + d.promptTokens,
    completionTokens: s.completionTokens + d.completionTokens,
    cacheReadTokens: s.cacheReadTokens + d.cacheReadTokens,
  }), { requests: 0, promptTokens: 0, completionTokens: 0, cacheReadTokens: 0 });

  return { daily, totals };
}

export function parseAccountsEnv(raw) {
  if (!raw) return [];
  const trimmed = raw.trim();
  if (trimmed.startsWith('[')) {
    try {
      const arr = JSON.parse(trimmed);
      return (Array.isArray(arr) ? arr : [])
        .map((it) => isRecord(it) ? { key: str(it.key) || '', label: str(it.label) || '' } : { key: String(it), label: '' })
        .filter((it) => it.key);
    } catch (e) { return []; }
  }
  return trimmed.split(',').map((s) => s.trim()).filter(Boolean).map((key) => ({ key, label: '' }));
}

export function rowToAccount(row) {
  const isGo = (row.plan_id || '').toLowerCase().includes('go') && !(row.plan_id || '').toLowerCase().includes('goat');
  return {
    id: row.id,
    maskedKey: maskKey(row.api_key),
    label: row.label || '',
    enabled: !!row.enabled,
    user_name: row.user_name || '',
    plan: row.plan_id || row.plan_name ? {
      planId: row.plan_id,
      name: row.plan_name || row.plan_id,
      monthlyCredits: planInfo(row.plan_id)?.monthlyCredits ?? (isGo ? 10 : null),
      isGoPlan: isGo,
    } : null,
    credits: {
      monthlyCredits: row.monthly_left,
      purchasedCredits: row.purchased,
      freeCredits: row.free,
      monthlyCap: row.monthly_cap ?? (isGo ? 10 : (planInfo(row.plan_id)?.monthlyCredits ?? null)),
      monthlyUsed: row.monthly_used ?? (isGo && row.monthly_left != null ? Math.max(0, 10 - row.monthly_left) : null),
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
    detail: row.detail ? (typeof row.detail === 'string' ? JSON.parse(row.detail) : row.detail) : null,
    createdAt: row.created_at,
  };
}

export function mergeQuotaState(previous, report) {
  const old = previous || {};
  const pick = (next, fallback) => (next !== undefined && next !== null && next !== '' ? next : (fallback ?? null));
  const boolInt = (next, fallback) => typeof next === 'boolean' ? (next ? 1 : 0) : (fallback ?? 0);
  const c = isRecord(report?.credits) ? report.credits : null;
  const p = isRecord(report?.plan) ? report.plan : null;
  const fh = isRecord(report?.fiveHour) ? report.fiveHour : null;
  const wk = isRecord(report?.weekly) ? report.weekly : null;
  const isGo = p?.isGoPlan || (p?.planId || '').toLowerCase().includes('go') && !(p?.planId || '').toLowerCase().includes('goat');

  return {
    user_name: pick(report?.account?.userName, old.user_name || ''),
    plan_id: pick(p?.planId, old.plan_id || ''),
    plan_name: pick(p?.planName, old.plan_name || ''),
    monthly_left: c ? pick(c.monthlyLeft, old.monthly_left) : (old.monthly_left ?? null),
    monthly_cap: c ? pick(c.monthlyCap, old.monthly_cap ?? (isGo ? 10 : null)) : (old.monthly_cap ?? (isGo ? 10 : null)),
    monthly_used: c ? pick(c.monthlyUsed, old.monthly_used) : (old.monthly_used ?? null),
    purchased: c ? pick(c.purchased, old.purchased) : (old.purchased ?? null),
    free: c ? pick(c.free, old.free) : (old.free ?? null),
    five_hour_used: fh ? pick(fh.used, old.five_hour_used) : (old.five_hour_used ?? null),
    five_hour_cap: fh ? pick(fh.cap, old.five_hour_cap) : (old.five_hour_cap ?? null),
    five_hour_exceeded: fh ? boolInt(fh.exceeded, old.five_hour_exceeded) : (old.five_hour_exceeded ?? 0),
    five_hour_reset: fh ? pick(fh.resetAt, old.five_hour_reset) : (old.five_hour_reset ?? 0),
    weekly_used: wk ? pick(wk.used, old.weekly_used) : (old.weekly_used ?? null),
    weekly_cap: wk ? pick(wk.cap, old.weekly_cap) : (old.weekly_cap ?? null),
    weekly_exceeded: wk ? boolInt(wk.exceeded, old.weekly_exceeded) : (old.weekly_exceeded ?? 0),
    weekly_reset: wk ? pick(wk.resetAt, old.weekly_reset) : (old.weekly_reset ?? 0),
    last_error: '',
    detail: report ? JSON.stringify(report) : (old.detail || null),
  };
}

// ── 内存号池实现 ──
export class MemPool {
  constructor(items = [], tzOffset = DEFAULT_TZ_OFFSET) {
    this.seq = 1;
    this.tzOffset = tzOffset;
    this.buckets = new Map(); // bucket_start -> Map(account_id -> usage)
    this.accounts = items.map((it) => ({
      id: this.seq++,
      api_key: it.key,
      label: it.label || '',
      enabled: true,
      user_name: '', plan_id: '', plan_name: '',
      monthly_left: null, monthly_cap: null, monthly_used: null,
      purchased: null, free: null,
      five_hour_used: null, five_hour_cap: null, five_hour_exceeded: 0, five_hour_reset: 0,
      weekly_used: null, weekly_cap: null, weekly_exceeded: 0, weekly_reset: 0,
      rate_limited_until: 0, disabled_reason: '', last_error: '', last_used_at: 0,
      requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0,
      created_at: Date.now(), detail: null,
    }));
  }

  async pick(strategy = 'sticky', excludeIds = []) {
    const now = Date.now();
    const avail = this.accounts
      .filter((a) => a.enabled && a.rate_limited_until <= now && !excludeIds.includes(a.id))
      .sort((a, b) => (strategy === 'round_robin' ? a.last_used_at - b.last_used_at : b.last_used_at - a.last_used_at) || a.id - b.id);
    return avail[0] || null;
  }

  async enabledCount() {
    return this.accounts.filter((a) => a.enabled).length;
  }

  async list() {
    return this.accounts.map(rowToAccount);
  }

  async getRaw(id) {
    return this.accounts.find((a) => a.id === id) || null;
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
      user_name: '', plan_id: '', plan_name: '',
      monthly_left: null, monthly_cap: null, monthly_used: null,
      purchased: null, free: null,
      five_hour_used: null, five_hour_cap: null, five_hour_exceeded: 0, five_hour_reset: 0,
      weekly_used: null, weekly_cap: null, weekly_exceeded: 0, weekly_reset: 0,
      rate_limited_until: 0, disabled_reason: '', last_error: '', last_used_at: 0,
      requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0,
      created_at: Date.now(), detail: null,
    });
    return id;
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
    for (const perAcct of this.buckets.values()) {
      perAcct.delete(id);
    }
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

  async dailyUsage(range = '1d') {
    const info = normalizeUsageRange(range);
    const cutoff = Math.floor(Date.now() / 1000) - info.seconds;
    const labels = {};
    for (const a of this.accounts) labels[a.id] = a.label || a.user_name || '';
    const rows = [];
    for (const [bucket_start, perAcct] of this.buckets) {
      if (bucket_start < cutoff) continue;
      for (const [account_id, v] of perAcct) {
        rows.push({ bucket_start, account_id, ...v });
      }
    }
    return buildUsage(rows, labels, info, this.tzOffset);
  }

  async cooldown(id, seconds, lastError = '') {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return;
    a.rate_limited_until = Date.now() + seconds * 1000;
    a.last_error = lastError || '';
  }

  async disable(id, reason = '') {
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

  async saveQuota(id, report) {
    const a = this.accounts.find((x) => x.id === id);
    if (!a) return;
    const state = mergeQuotaState(a, report);
    Object.assign(a, state);
  }

  async markQuotaError(id, message) {
    const a = this.accounts.find((a) => a.id === id);
    if (a) a.last_error = message;
  }

  async seedIfEmpty(items) {
    if (this.accounts.length > 0) return 0;
    let added = 0;
    for (const it of items) {
      if (!it.key) continue;
      await this.add(it.key, it.label);
      added++;
    }
    return added;
  }
}

// ── 文件持久化号池（适用于 Serverless /tmp 或本地运行） ──
export class FilePool extends MemPool {
  constructor(filePath, items = [], tzOffset = DEFAULT_TZ_OFFSET) {
    super(items, tzOffset);
    this.filePath = filePath;
    this.load();
  }

  load() {
    try {
      if (fs.existsSync(this.filePath)) {
        const raw = fs.readFileSync(this.filePath, 'utf-8');
        const data = JSON.parse(raw);
        if (Array.isArray(data.accounts)) {
          this.accounts = data.accounts;
          this.seq = Math.max(1, ...this.accounts.map(a => a.id + 1));
        }
        if (Array.isArray(data.buckets)) {
          this.buckets = new Map();
          for (const [k, v] of data.buckets) {
            this.buckets.set(Number(k), new Map(v));
          }
        }
      }
    } catch (e) {
      // 容错
    }
  }

  save() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const serialized = {
        accounts: this.accounts,
        buckets: [...this.buckets.entries()].map(([k, v]) => [k, [...v.entries()]]),
      };
      fs.writeFileSync(this.filePath, JSON.stringify(serialized, null, 2), 'utf-8');
    } catch (e) {
      // 容错
    }
  }

  async add(key, label) {
    const id = await super.add(key, label);
    this.save();
    return id;
  }

  async patch(id, updates) {
    const res = await super.patch(id, updates);
    this.save();
    return res;
  }

  async remove(id) {
    const res = await super.remove(id);
    this.save();
    return res;
  }

  async recordSuccess(id, usage) {
    await super.recordSuccess(id, usage);
    this.save();
  }

  async cooldown(id, seconds, lastError) {
    await super.cooldown(id, seconds, lastError);
    this.save();
  }

  async disable(id, reason) {
    await super.disable(id, reason);
    this.save();
  }

  async saveQuota(id, report) {
    await super.saveQuota(id, report);
    this.save();
  }
}

// ── Upstash Redis / Vercel KV REST 号池（原生适配 Vercel） ──
export class UpstashKvPool {
  constructor(url, token, tzOffset = DEFAULT_TZ_OFFSET) {
    this.url = url.replace(/\/+$/, '');
    this.token = token;
    this.tzOffset = tzOffset;
  }

  async cmd(command, ...args) {
    const resp = await fetch(`${this.url}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify([command, ...args]),
    });
    if (!resp.ok) {
      throw new Error(`KV command ${command} failed with status ${resp.status}`);
    }
    const data = await resp.json();
    return data.result;
  }

  async list() {
    const res = await this.cmd('HGETALL', 'cc_pool:accounts');
    if (!res) return [];
    const accounts = [];
    if (Array.isArray(res)) {
      for (let i = 1; i < res.length; i += 2) {
        try { accounts.push(JSON.parse(res[i])); } catch {}
      }
    } else if (typeof res === 'object') {
      for (const val of Object.values(res)) {
        try { accounts.push(JSON.parse(val)); } catch {}
      }
    }
    return accounts.sort((a, b) => a.id - b.id).map(rowToAccount);
  }

  async getRaw(id) {
    const res = await this.cmd('HGET', 'cc_pool:accounts', String(id));
    if (!res) return null;
    try { return JSON.parse(res); } catch { return null; }
  }

  async pick(strategy = 'sticky', excludeIds = []) {
    const accounts = await this.list();
    const now = Date.now();
    const avail = accounts
      .filter((a) => a.enabled && (!a.pool.rateLimited) && !excludeIds.includes(a.id))
      .sort((a, b) => (strategy === 'round_robin' ? a.lastUsedAt - b.lastUsedAt : b.lastUsedAt - a.lastUsedAt) || a.id - b.id);
    if (!avail[0]) return null;
    return this.getRaw(avail[0].id);
  }

  async enabledCount() {
    const accounts = await this.list();
    return accounts.filter((a) => a.enabled).length;
  }

  async add(key, label) {
    const accounts = await this.list();
    if (accounts.some(a => a.maskedKey === maskKey(key))) {
      const e = new Error('该密钥已存在');
      e.status = 409;
      throw e;
    }
    const id = Number(await this.cmd('INCR', 'cc_pool:seq')) || Date.now();
    const raw = {
      id,
      api_key: key,
      label: label || '',
      enabled: true,
      user_name: '', plan_id: '', plan_name: '',
      monthly_left: null, monthly_cap: null, monthly_used: null,
      purchased: null, free: null,
      five_hour_used: null, five_hour_cap: null, five_hour_exceeded: 0, five_hour_reset: 0,
      weekly_used: null, weekly_cap: null, weekly_exceeded: 0, weekly_reset: 0,
      rate_limited_until: 0, disabled_reason: '', last_error: '', last_used_at: 0,
      requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0,
      created_at: Date.now(), detail: null,
    };
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
    return id;
  }

  async patch(id, { label, enabled }) {
    const raw = await this.getRaw(id);
    if (!raw) return false;
    if (label !== undefined) raw.label = String(label).slice(0, 60);
    if (enabled !== undefined) {
      raw.enabled = !!enabled;
      if (raw.enabled) raw.disabled_reason = '';
    }
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
    return true;
  }

  async remove(id) {
    await this.cmd('HDEL', 'cc_pool:accounts', String(id));
    return true;
  }

  async recordSuccess(id, usage) {
    const raw = await this.getRaw(id);
    if (!raw) return;
    raw.requests = (raw.requests || 0) + 1;
    raw.prompt_tokens = (raw.prompt_tokens || 0) + (usage.prompt_tokens || 0);
    raw.completion_tokens = (raw.completion_tokens || 0) + (usage.completion_tokens || 0);
    raw.cache_read_tokens = (raw.cache_read_tokens || 0) + (usage.prompt_tokens_details?.cached_tokens || 0);
    raw.last_used_at = Date.now();
    raw.last_error = '';
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));

    const bucket = usageBucketStart(Date.now(), this.tzOffset);
    const bucketKey = `cc_pool:bucket:${bucket}`;
    const rawBucket = await this.cmd('HGET', bucketKey, String(id));
    const cur = rawBucket ? JSON.parse(rawBucket) : { requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0 };
    cur.requests += 1;
    cur.prompt_tokens += (usage.prompt_tokens || 0);
    cur.completion_tokens += (usage.completion_tokens || 0);
    cur.cache_read_tokens += (usage.prompt_tokens_details?.cached_tokens || 0);
    await this.cmd('HSET', bucketKey, String(id), JSON.stringify(cur));
    // 自动过期 31 天
    await this.cmd('EXPIRE', bucketKey, 31 * 86400);
  }

  async cooldown(id, seconds, lastError = '') {
    const raw = await this.getRaw(id);
    if (!raw) return;
    raw.rate_limited_until = Date.now() + seconds * 1000;
    raw.last_error = lastError || '';
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
  }

  async disable(id, reason = '') {
    const raw = await this.getRaw(id);
    if (!raw) return;
    raw.enabled = false;
    raw.disabled_reason = reason;
    raw.last_error = reason;
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
  }

  async earliestRateLimitedUntil() {
    const accounts = await this.list();
    const now = Date.now();
    const times = accounts.filter(a => a.enabled && a.pool.rateLimitedUntil > now).map(a => a.pool.rateLimitedUntil);
    return times.length ? Math.min(...times) : 0;
  }

  async saveQuota(id, report) {
    const raw = await this.getRaw(id);
    if (!raw) return;
    const state = mergeQuotaState(raw, report);
    Object.assign(raw, state);
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
  }

  async markQuotaError(id, message) {
    const raw = await this.getRaw(id);
    if (!raw) return;
    raw.last_error = message;
    await this.cmd('HSET', 'cc_pool:accounts', String(id), JSON.stringify(raw));
  }

  async dailyUsage(range = '1d') {
    const info = normalizeUsageRange(range);
    const accounts = await this.list();
    const labels = {};
    for (const a of accounts) labels[a.id] = a.label || a.user_name || '';
    // 简易聚合：从当前往前计算桶
    const now = Date.now();
    const count = Math.ceil(info.seconds / USAGE_BUCKET_SECONDS);
    const rows = [];
    for (let i = 0; i < count; i++) {
      const bucket = usageBucketStart(now - i * USAGE_BUCKET_SECONDS * 1000, this.tzOffset);
      const bres = await this.cmd('HGETALL', `cc_pool:bucket:${bucket}`);
      if (bres) {
        const pairs = Array.isArray(bres)
          ? bres
          : Object.entries(bres).flat();
        for (let j = 0; j < pairs.length; j += 2) {
          try {
            const accId = Number(pairs[j]);
            const val = JSON.parse(pairs[j + 1]);
            rows.push({ bucket_start: bucket, account_id: accId, ...val });
          } catch {}
        }
      }
    }
    return buildUsage(rows, labels, info, this.tzOffset);
  }

  async seedIfEmpty(items) {
    const accounts = await this.list();
    if (accounts.length > 0) return 0;
    let added = 0;
    for (const it of items) {
      if (!it.key) continue;
      await this.add(it.key, it.label);
      added++;
    }
    return added;
  }
}

// ── 标准 Redis (TCP, redis:// 或 rediss://) 号池：复用 UpstashKvPool 的全部逻辑，只替换命令传输 ──
export class RedisPool extends UpstashKvPool {
  // client 仅供测试注入：需提供 sendCommand(argv)
  constructor(url, tzOffset = DEFAULT_TZ_OFFSET, client = null) {
    super('http://unused', '', tzOffset);
    this.redisUrl = url;
    this.client = client;
    this.connecting = null;
  }

  async getClient() {
    if (this.client) return this.client;
    // 并发请求共用同一次连接；失败时清空，下次请求重试
    this.connecting ||= (async () => {
      const { createClient } = await import('redis');
      const c = createClient({ url: this.redisUrl });
      c.on('error', () => {}); // 断线由 node-redis 自动重连，命令失败会在 cmd() 中抛出
      await c.connect();
      return c;
    })().catch((e) => { this.connecting = null; throw e; });
    this.client = await this.connecting;
    return this.client;
  }

  async cmd(command, ...args) {
    const c = await this.getClient();
    return c.sendCommand([command, ...args.map(String)]);
  }
}

// 单例缓存
let globalPool = null;

export function getPoolStore(config) {
  if (globalPool) return globalPool;

  const tz = config.tzOffset ?? DEFAULT_TZ_OFFSET;

  // 1. 若配置了 Upstash / Vercel KV，使用 UpstashKvPool
  if (config.kvUrl && config.kvToken) {
    globalPool = new UpstashKvPool(config.kvUrl, config.kvToken, tz);
  }
  // 2. 标准 Redis 连接串 (REDIS_URL)
  else if (config.redisUrl) {
    globalPool = new RedisPool(config.redisUrl, tz);
  }
  // 3. 本地持久化或 /tmp 持久化
  else {
    const tmpFile = process.env.VERCEL ? '/tmp/commandcode-pool.json' : path.resolve(process.cwd(), '.data', 'pool.json');
    const seedItems = parseAccountsEnv(config.accounts);
    globalPool = new FilePool(tmpFile, seedItems, tz);
  }

  return globalPool;
}

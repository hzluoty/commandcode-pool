// commandcode-pool — Cloudflare Worker
// 把多个 Command Code（Go 套餐等）账号组成号池，对外提供 OpenAI / Anthropic / Responses
// 兼容 API（协议层对齐 commandcode-proxy，wire 协议版本 1.53.1）。
//
// 端点：
//   POST /v1/chat/completions   OpenAI Chat Completions（流式 + 非流式）
//   POST /v1/messages           Anthropic Messages API（流式 + 非流式）
//   POST /v1/responses          OpenAI Responses API（流式 + 非流式）
//   GET  /v1/models             模型目录（10 分钟缓存）
//   GET  /health                健康检查
//   /admin                      管理面板（public/ 静态资源，数据走 /api/*）
//   /api/*                      管理 API（x-admin-token 头）
//
// 号池调度：默认 sticky（粘住单账号直至 429/401，最大化上游 prompt cache 命中）；
// POOL_STRATEGY=round_robin 时最久未用优先。429/401/403/5xx/网络错误自动换号重试，
// 400/422 等请求本身的问题不重试。故障转移只发生在向客户端写出任何字节之前。
//
// 反检测：每个账号密钥确定性派生一台伪造设备（指纹 / 会话 / 生命周期事件与 CLI 1.53.1
// 逐字段对齐），配置 FINGERPRINT_SALT 可成批更换身份。
//
// 存储：D1（账号、额度缓存、用量与冷却状态，跨 isolate 一致）；未绑定 D1 时可用
// ACCOUNTS 环境变量提供内存号池（状态不持久）。

// ============================ §A 常量与配置 ============================

const DEFAULT_BASE = 'https://api.commandcode.ai';

// 本 Worker 实际实现的 wire 协议版本（对齐 command-code@1.53.1 源码）。
// 版本号必须与实际实现的形状自洽：npm 上有新版本只告警、不自动改。
const CC_PROTOCOL_VERSION = '1.53.1';

const DEFAULT_MAX_TOKENS = 64000;
const MAX_MAX_TOKENS = 200000;
const DEFAULT_MODEL = 'deepseek/deepseek-v4-flash';
const DEFAULT_429_COOLDOWN_S = 60;
const AUTH_403_COOLDOWN_S = 600;
const ZERO_OUTPUT_COOLDOWN_S = 10;
const MODEL_CACHE_TTL_MS = 10 * 60 * 1000;
const MAX_REQUEST_BODY_BYTES = 8 * 1024 * 1024;
const MAX_UPSTREAM_JSON_BYTES = 2 * 1024 * 1024;
const MAX_SSE_EVENT_CHARS = 2 * 1024 * 1024;

// 上游读空闲超时（只计 reader.read() 的等待，每收到 chunk 重置，不是总时长）。
const DEFAULT_STREAM_IDLE_MS = 30000;
const DEFAULT_NONSTREAM_IDLE_MS = 90000;
const DEFAULT_CONTROL_PLANE_TIMEOUT_MS = 10000;
const FINGERPRINT_CACHE_TTL_MS = 30 * 60 * 1000;
const KEY_STATE_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_FINGERPRINT_CACHE_ENTRIES = 256;
const MAX_KEY_STATE_ENTRIES = 256;
const MAX_MODEL_CACHE_ENTRIES = 128;
const MAX_USAGE_RETENTION_SECONDS = 90 * 24 * 60 * 60;
const D1_MIGRATION_LOCK_MS = 5 * 60 * 1000;
const D1_MIGRATION_WAIT_MS = 250;
const D1_MIGRATION_BATCH_ROWS = 100;

// 按天用量统计的时区偏移（小时）。默认 UTC+8，可用环境变量 TIMEZONE_OFFSET 覆盖。
const DEFAULT_TZ_OFFSET = 8;
const USAGE_BUCKET_SECONDS = 5 * 60;
const USAGE_RANGES = {
  '5m': { key: '5m', seconds: 5 * 60, label: '5分钟', intervalSeconds: 5 * 60 },
  '1h': { key: '1h', seconds: 60 * 60, label: '1小时', intervalSeconds: 5 * 60 },
  '5h': { key: '5h', seconds: 5 * 60 * 60, label: '5小时', intervalSeconds: 5 * 60 },
  '1d': { key: '1d', seconds: 24 * 60 * 60, label: '1天', intervalSeconds: 60 * 60 },
  '3d': { key: '3d', seconds: 3 * 24 * 60 * 60, label: '3天', intervalSeconds: 6 * 60 * 60 },
  '7d': { key: '7d', seconds: 7 * 24 * 60 * 60, label: '7天', intervalSeconds: 24 * 60 * 60 },
  '30d': { key: '30d', seconds: 30 * 24 * 60 * 60, label: '30天', intervalSeconds: 24 * 60 * 60 },
};

function normalizeUsageRange(value = '1d') {
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

function dayKey(ts = Date.now(), tzOffset = DEFAULT_TZ_OFFSET) {
  return new Date(ts + tzOffset * 3600e3).toISOString().slice(0, 10);
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

function usageBucketStart(ts = Date.now(), tzOffset = DEFAULT_TZ_OFFSET) {
  const offset = tzOffset * 3600;
  return Math.floor((Math.floor(ts / 1000) + offset) / USAGE_BUCKET_SECONDS) * USAGE_BUCKET_SECONDS - offset;
}

function dayStartEpoch(day, tzOffset = DEFAULT_TZ_OFFSET) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(day || ''));
  if (!m) return 0;
  return Math.floor(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) / 1000) - tzOffset * 3600;
}

// 把 5 分钟桶（以及旧的按天记录）聚合成前端友好的结构。
function buildUsage(rows, labels, range, tzOffset) {
  const info = normalizeUsageRange(range);
  const byDay = new Map();
  for (const r of rows) {
    const rawStart = r.bucket_start != null ? r.bucket_start : dayStartEpoch(r.day, tzOffset);
    const periodStart = usagePeriodStart(rawStart, info.intervalSeconds, tzOffset);
    const period = String(periodStart);
    let d = byDay.get(period);
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
      byDay.set(period, d);
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
  const daily = [...byDay.values()]
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

// 已知套餐的月度信用额（来自社区 CLI 的 planId 映射；未知套餐返回 null）
const KNOWN_PLANS = {
  'individual-go':       { name: 'Go',         monthlyCredits: 10 },
  'individual-goat':     { name: 'GOAT',       monthlyCredits: 70 },
  'individual-pro':      { name: 'Pro',        monthlyCredits: 30 },
  'individual-pro-v1':   { name: 'Pro',        monthlyCredits: 80 },
  'individual-provider': { name: 'Provider',   monthlyCredits: 15 },
  'individual-max':      { name: 'Max',        monthlyCredits: 150 },
  'individual-ultra':    { name: 'Ultra',      monthlyCredits: 300 },
  'teams-pro':           { name: 'Teams Pro',  monthlyCredits: 40 },
};
const PLAN_PREFIXES = Object.keys(KNOWN_PLANS).sort((a, b) => b.length - a.length);

function planInfo(planId) {
  if (!planId) return undefined;
  const norm = String(planId).toLowerCase().replace(/_/g, '-');
  const prefix = PLAN_PREFIXES.find((p) => norm.startsWith(p));
  return prefix ? KNOWN_PLANS[prefix] : undefined;
}

// 每请求从 env 解析配置（Workers 无 process.env；测试会构造不同 env）
function positiveInt(v, fallback) {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function getConfig(env = {}) {
  return {
    apiBase: env.API_BASE || DEFAULT_BASE,
    fingerprintSalt: env.FINGERPRINT_SALT || '',
    streamIdleMs: positiveInt(env.STREAM_IDLE_MS, DEFAULT_STREAM_IDLE_MS),
    nonStreamIdleMs: positiveInt(env.NONSTREAM_IDLE_MS, DEFAULT_NONSTREAM_IDLE_MS),
    controlPlaneTimeoutMs: positiveInt(env.CONTROL_PLANE_TIMEOUT_MS, DEFAULT_CONTROL_PLANE_TIMEOUT_MS),
    emptySystemPlaceholder: String(env.EMPTY_SYSTEM_PLACEHOLDER ?? 'true').toLowerCase() !== 'false',
    cliMode: env.CLI_MODE || 'agent',
    cliSessionMode: env.CLI_SESSION_MODE || 'interactive',
    deviceProjectDir: env.DEVICE_PROJECT_DIR || 'C:\\Users\\dev\\projects\\app',
  };
}

function nowUnix() { return Math.floor(Date.now() / 1000); }
function getDateStr() { return new Date().toISOString().slice(0, 10); }

// ============================ §B 加密与设备指纹 ============================
// 形态与哈希逐字对齐官方 CLI 1.53.1。信号值由 API key 确定性派生（同一账号永远
// 上报同一台设备：重启、多 isolate、额度恢复后都不变）；FINGERPRINT_SALT 是成批
// 更换身份的逃生口。注意哈希阶段用 CLI 的固定盐 FP_SALT，salt 只影响「伪造哪台机器」。

const encoder = new TextEncoder();

async function sha256Bytes(data) {
  return new Uint8Array(await crypto.subtle.digest('SHA-256', data));
}
function sha256Text(text) { return sha256Bytes(encoder.encode(text)); }

function bytesToHex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function bytesToBase64(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
}

// 字典序比较（替代 Node 的 Buffer.compare）
function lexCompare(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

const FINGERPRINT_CPUS = [
  { model: '12th Gen Intel(R) Core(TM) i7-12650H', cores: 10 },
  { model: '12th Gen Intel(R) Core(TM) i5-12400F', cores: 6 },
  { model: '12th Gen Intel(R) Core(TM) i9-12900K', cores: 16 },
  { model: '13th Gen Intel(R) Core(TM) i7-13700K', cores: 16 },
  { model: '13th Gen Intel(R) Core(TM) i5-13600K', cores: 14 },
  { model: '13th Gen Intel(R) Core(TM) i9-13900K', cores: 24 },
  { model: 'Intel(R) Core(TM) Ultra 7 155H', cores: 16 },
  { model: 'Intel(R) Core(TM) Ultra 9 285H', cores: 16 },
  { model: 'Intel(R) Core(TM) i9-14900K', cores: 24 },
  { model: 'Intel(R) Core(TM) i7-14700K', cores: 20 },
  { model: 'AMD Ryzen 7 7800X3D', cores: 8 },
  { model: 'AMD Ryzen 9 7950X', cores: 16 },
  { model: 'AMD Ryzen 5 7600', cores: 6 },
  { model: 'AMD Ryzen 9 7900X', cores: 12 },
  { model: 'AMD Ryzen 7 5800X3D', cores: 8 },
];
const FINGERPRINT_MEMS = [8, 16, 24, 32, 48, 64];
const FINGERPRINT_TZS = [
  'America/New_York', 'America/Chicago', 'America/Los_Angeles', 'America/Toronto',
  'Europe/London', 'Europe/Berlin', 'Europe/Paris', 'Europe/Moscow',
  'Asia/Shanghai', 'Asia/Tokyo', 'Asia/Singapore', 'Asia/Seoul', 'Asia/Hong_Kong',
  'Australia/Sydney', 'Pacific/Auckland',
];
const FINGERPRINT_MAC_COUNT_RANGE = [2, 3, 4, 5];
const FP_SALT = 'command-code:device-fingerprint:v1';
const FP_OS_USERS = ['dev', 'user', 'admin', 'coder', 'engineer', 'work'];
const FP_MAIL_DOMAINS = ['gmail.com', 'outlook.com', 'qq.com', '163.com'];

// 设备档案：指纹 / config.environment / config.workingDir / x-project-slug / lifecycle.os
// 共用同一份，避免「指纹说 win32、环境说 linux」这类自相矛盾。
const DEVICE_PLATFORM = 'win32';
const DEVICE_ARCH = 'x64';
const DEVICE_OS_RELEASE = '10.0.22631';

function fpDigest(apiKey, field, salt) {
  return sha256Text(`${salt || ''}\0${apiKey}\0${field}`);
}

// 从候选池确定性地挑一项：打分取最大。以后往池里加候选只影响「新候选恰好胜出」的 key。
async function fpPickIndex(apiKey, field, items, labelOf, salt) {
  let bestIdx = 0;
  let bestScore = null;
  for (let i = 0; i < items.length; i++) {
    const score = await fpDigest(apiKey, `${field}\0${labelOf(i)}`, salt);
    if (bestScore === null || lexCompare(score, bestScore) > 0) { bestScore = score; bestIdx = i; }
  }
  return bestIdx;
}

// CLI 的 hashSignal：sha256(FP_SALT + "\0" + value.toLowerCase())，空值返回 undefined（JSON 里被丢掉）
async function fingerprintHash(value) {
  const v = String(value ?? '').trim();
  if (!v) return undefined;
  return bytesToHex(await sha256Text(`${FP_SALT}\0${v.toLowerCase()}`));
}

// Isolate 内共享缓存：持久值只使用不可逆 scope key，统一 TTL 和容量；
// 正在计算的 Promise 只在短暂 single-flight 期间按嵌套 scope 保存。
class IsolateCache {
  constructor(maxEntries, defaultTtlMs) {
    this.maxEntries = maxEntries;
    this.defaultTtlMs = defaultTtlMs;
    this.items = new Map();
  }
  get(key, now = Date.now()) {
    const entry = this.items.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= now) {
      this.items.delete(key);
      return undefined;
    }
    this.items.delete(key);
    this.items.set(key, entry);
    return entry.value;
  }
  set(key, value, ttlMs = this.defaultTtlMs) {
    this.items.delete(key);
    this.items.set(key, { value, expiresAt: Date.now() + ttlMs });
    while (this.items.size > this.maxEntries) {
      this.items.delete(this.items.keys().next().value);
    }
  }
  delete(key) { this.items.delete(key); }
  get size() { return this.items.size; }
}
function structuredScopeKey(...parts) {
  return parts.map((part) => {
    const value = String(part ?? '');
    return `${value.length}:${value}`;
  }).join('|');
}
async function digestCacheKey(scope, value) {
  return bytesToHex(await sha256Text(`command-code-cache-v1\0${scope}\0${value}`));
}

const fingerprintCache = new IsolateCache(MAX_FINGERPRINT_CACHE_ENTRIES, FINGERPRINT_CACHE_TTL_MS);
const fingerprintFlights = new Map(); // 仅短暂保存原始 credential，避免长期明文 key
function getNestedFlight(map, scope, credential) {
  let scoped = map.get(scope);
  if (!scoped) { scoped = new Map(); map.set(scope, scoped); }
  let promise = scoped.get(credential);
  return { scoped, promise };
}
function clearNestedFlight(map, scope, credential, promise) {
  const scoped = map.get(scope);
  if (!scoped || scoped.get(credential) !== promise) return;
  scoped.delete(credential);
  if (!scoped.size) map.delete(scope);
}
function getFingerprint(apiKey, salt) {
  const scope = structuredScopeKey(salt);
  const active = getNestedFlight(fingerprintFlights, scope, apiKey);
  if (active.promise) return active.promise;
  const promise = (async () => {
    const cacheKey = await digestCacheKey('fingerprint', structuredScopeKey(salt, apiKey));
    const cached = fingerprintCache.get(cacheKey);
    if (cached) return cached;
    const value = await generateFingerprint(apiKey, salt);
    fingerprintCache.set(cacheKey, value);
    return value;
  })();
  active.scoped.set(apiKey, promise);
  promise.then(
    () => clearNestedFlight(fingerprintFlights, scope, apiKey, promise),
    () => clearNestedFlight(fingerprintFlights, scope, apiKey, promise),
  );
  return promise;
}
async function generateFingerprint(apiKey, salt) {
  const cpuEntry = FINGERPRINT_CPUS[await fpPickIndex(apiKey, 'cpu', FINGERPRINT_CPUS, (i) => `${FINGERPRINT_CPUS[i].model}|${FINGERPRINT_CPUS[i].cores}`, salt)];
  const memGiB = FINGERPRINT_MEMS[await fpPickIndex(apiKey, 'mem', FINGERPRINT_MEMS, (i) => String(FINGERPRINT_MEMS[i]), salt)];
  const tz = FINGERPRINT_TZS[await fpPickIndex(apiKey, 'timezone', FINGERPRINT_TZS, (i) => FINGERPRINT_TZS[i], salt)];
  const macCount = FINGERPRINT_MAC_COUNT_RANGE[await fpPickIndex(apiKey, 'macCount', FINGERPRINT_MAC_COUNT_RANGE, (i) => String(FINGERPRINT_MAC_COUNT_RANGE[i]), salt)];
  const osUser = FP_OS_USERS[await fpPickIndex(apiKey, 'osUser', FP_OS_USERS, (i) => FP_OS_USERS[i], salt)];
  const mailDomain = FP_MAIL_DOMAINS[await fpPickIndex(apiKey, 'mailDomain', FP_MAIL_DOMAINS, (i) => FP_MAIL_DOMAINS[i], salt)];
  const hex = async (field, bytes) => bytesToHex((await fpDigest(apiKey, field, salt)).slice(0, bytes));
  // Windows MachineGuid 形状：8-4-4-4-12
  const mid = await hex('machineId', 16);
  const machineId = `${mid.slice(0, 8)}-${mid.slice(8, 12)}-${mid.slice(12, 16)}-${mid.slice(16, 20)}-${mid.slice(20, 32)}`;
  const macs = [];
  for (let i = 0; i < macCount; i++) {
    const b = (await fpDigest(apiKey, `mac${i}`, salt)).slice(0, 6);
    macs.push([...b].map((x) => x.toString(16).padStart(2, '0')).join(':'));
  }
  macs.sort(); // CLI 对 MAC 去重后排序
  const hostname = `DESKTOP-${(await hex('hostname', 4)).toUpperCase()}`;
  const gitEmail = `${osUser}.${await hex('gitEmail', 3)}@${mailDomain}`;

  const machineIdHash = await fingerprintHash(machineId);
  const macHashes = (await Promise.all(macs.map((m) => fingerprintHash(m)))).filter(Boolean);
  const osUserHash = await fingerprintHash(osUser);
  const hostnameHash = await fingerprintHash(hostname);
  const gitEmailHash = await fingerprintHash(gitEmail);

  // CLI 的 thumbmark：主盐 + "\0machine\0" + join([machineId, macs.join(",")])
  const thumbSeed = [machineId.trim(), macs.join(','), machineId.trim() ? '' : hostname, machineId.trim() ? '' : cpuEntry.model].filter(Boolean);
  const thumbmark = bytesToHex(await sha256Text(`${FP_SALT}\0machine\0${thumbSeed.join('|') || 'unknown'}`));

  return {
    thumbmark,
    components: {
      machineIdHash,
      macHashes,
      osUserHash,
      hostnameHash,
      gitEmailHash,
      platform: DEVICE_PLATFORM,
      arch: DEVICE_ARCH,
      osRelease: DEVICE_OS_RELEASE,
      cpuModel: cpuEntry.model,
      cpuCount: cpuEntry.cores,
      memGiB,
      isContainer: false,
      timezone: tz,
      runtime: 'cli',
      collectorVersion: 1,
    },
  };
}

// 协议漂移检测（只告警，不改版本号）。每个 isolate 惰性检查一次。
let driftChecked = false;
async function checkProtocolDrift() {
  try {
    const res = await fetch('https://registry.npmjs.org/command-code/latest', { signal: AbortSignal.timeout(10000) });
    if (!res.ok) return;
    const pkg = await res.json();
    const latest = typeof pkg?.version === 'string' ? pkg.version : null;
    if (latest && latest !== CC_PROTOCOL_VERSION) {
      console.warn(JSON.stringify({ message: 'CC CLI version drift: protocol may have changed, re-align from the npm package', implemented: CC_PROTOCOL_VERSION, latest }));
    }
  } catch { /* log-only */ }
}
function maybeCheckProtocolDrift() {
  if (driftChecked) return;
  driftChecked = true;
  checkProtocolDrift();
}

// ============================ §C 会话与每 Key 初始化 ============================

// 每个账号密钥一个 session（12h 一桶 + key 派生抖动）。与 proxy.mjs 的随机 UUID 不同，
// 这里按 key 确定性派生：Workers 是多 isolate 运行时，随机会话会让同一账号在不同
// isolate 呈现不同 threadId；确定性派生保证全局一致，语义等同「一台设备的会话」。
const SESSION_DURATION_MS = 12 * 60 * 60 * 1000;
const SESSION_JITTER_MS = 60 * 60 * 1000;

async function sessionForKey(apiKey) {
  const bucket = Math.floor(Date.now() / SESSION_DURATION_MS);
  const digest = await sha256Text(`cc-session\0${apiKey}\0${bucket}`);
  const h = bytesToHex(digest).slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

function incomingSessionHeaders(request) {
  return {
    'x-session-id': request.headers.get('x-session-id') || '',
    'x-claude-code-session-id': request.headers.get('x-claude-code-session-id') || '',
    'session_id': request.headers.get('session_id') || '',
  };
}

function getSessionId(incomingHeaders, apiKey, promptCacheKey) {
  const candidates = [
    incomingHeaders['x-session-id'],
    incomingHeaders['x-claude-code-session-id'],
    incomingHeaders['session_id'],
    promptCacheKey,
  ];
  for (const id of candidates) {
    if (id && typeof id === 'string' && id.length >= 8) return id;
  }
  return sessionForKey(apiKey); // async —— 调用方需 await
}

// ── 每 Key 初始化（fingerprint + lifecycle，首次 + 每 8h+2h 抖动） ────
const INIT_REFRESH_MS = 8 * 60 * 60 * 1000;
const INIT_JITTER_MS = 2 * 60 * 60 * 1000;

const keyStateCache = new IsolateCache(MAX_KEY_STATE_ENTRIES, KEY_STATE_TTL_MS);
const keyStateFlights = new Map(); // 仅 pending 阶段短暂按 raw credential single-flight
const keyStateScope = (cfg) => structuredScopeKey(cfg.apiBase, cfg.fingerprintSalt, cfg.cliSessionMode);

function cliHeaders(cfg) {
  return {
    'Content-Type': 'application/json',
    'User-Agent': 'cli',
    'x-command-code-version': CC_PROTOCOL_VERSION,
    'x-cli-environment': 'production',
  };
}

async function sendInitializationEvent(url, headers, body, cfg, signal, label) {
  await upstreamFetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  }, {
    signal,
    timeoutMs: cfg.controlPlaneTimeoutMs,
    consume: async (response, bodySignal) => {
      await readLimitedText(response, MAX_UPSTREAM_JSON_BYTES, bodySignal);
      if (!response.ok) {
        const error = new Error(`${label} HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
    },
  });
}
function waitForSharedFlight(promise, signal) {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(abortErrorFrom(signal));
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener('abort', onAbort);
    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn(value);
    };
    const onAbort = () => finish(reject, abortErrorFrom(signal));
    signal.addEventListener('abort', onAbort, { once: true });
    if (signal.aborted) onAbort();
    promise.then((value) => finish(resolve, value), (error) => finish(reject, error));
  });
}

function ensureInitialized(apiKey, env, signal) {
  const cfg = getConfig(env);
  const scope = keyStateScope(cfg);
  const active = getNestedFlight(keyStateFlights, scope, apiKey);
  if (active.promise) return waitForSharedFlight(active.promise, signal);
  let promise;
  promise = (async () => {
    const sharedSignal = new AbortController();
    const sharedTimer = setTimeout(() => sharedSignal.abort(controlPlaneTimeoutError(`${cfg.apiBase}/init`)), cfg.controlPlaneTimeoutMs);
    try {
      const cacheKey = await digestCacheKey('init-state', structuredScopeKey(cfg.apiBase, cfg.fingerprintSalt, cfg.cliSessionMode, apiKey));
      let state = keyStateCache.get(cacheKey);
      if (!state) {
        state = { nextInitAt: 0, inFlight: null };
        keyStateCache.set(cacheKey, state);
      }
      if (state.inFlight && state.inFlight !== promise) return state.inFlight;
      if (Date.now() < state.nextInitAt) return;
      const work = (async () => {
        const fingerprint = await getFingerprint(apiKey, cfg.fingerprintSalt);
        const headers = { ...cliHeaders(cfg), 'Authorization': `Bearer ${apiKey}` };
        await Promise.all([
          sendInitializationEvent(`${cfg.apiBase}/alpha/fingerprint/record`, headers, fingerprint, cfg, sharedSignal.signal, 'fingerprint record'),
          sendInitializationEvent(`${cfg.apiBase}/alpha/lifecycle-events`, headers, {
            eventType: 'cli_session_exists',
            metadata: {
              sessionId: `sess_${randomHex(8)}`,
              cliVersion: CC_PROTOCOL_VERSION,
              mode: cfg.cliSessionMode,
              os: `${fingerprint.components.platform}-${fingerprint.components.arch}`,
            },
          }, cfg, sharedSignal.signal, 'lifecycle event'),
        ]);
        const j = (await fpDigest(apiKey, 'init-jitter', cfg.fingerprintSalt)).slice(0, 4);
        const jitter = ((j[0] << 24 | j[1] << 16 | j[2] << 8 | j[3]) >>> 0) % INIT_JITTER_MS;
        state.nextInitAt = Date.now() + INIT_REFRESH_MS + jitter;
      })();
      state.inFlight = work;
      try {
        await work;
      } finally {
        if (state.inFlight === work) state.inFlight = null;
      }
    } finally {
      clearTimeout(sharedTimer);
      if (!sharedSignal.signal.aborted) sharedSignal.abort();
    }
  })();
  active.scoped.set(apiKey, promise);
  promise.then(
    () => clearNestedFlight(keyStateFlights, scope, apiKey, promise),
    () => clearNestedFlight(keyStateFlights, scope, apiKey, promise),
  );
  return waitForSharedFlight(promise, signal);
}

// ============================ §D 小工具 ============================

const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Cache-Control': 'no-store',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Admin-Token, X-Api-Key, X-Session-Id, X-Cmd-Zdr',
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: JSON_HEADERS });
}

// OpenAI 形错误响应。retryAfterSec 同时进 Retry-After 头。
function errorResponse(status, type, message, code, retryAfterSec) {
  const body = { error: { message, type, param: null } };
  if (code) body.error.code = code;
  const headers = { ...JSON_HEADERS };
  if (retryAfterSec) headers['Retry-After'] = String(Math.max(1, Math.ceil(retryAfterSec)));
  return new Response(JSON.stringify(body), { status, headers });
}

function isRecord(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
function num(v) { return typeof v === 'number' && Number.isFinite(v) ? v : null; }
function str(v) { return typeof v === 'string' ? v : null; }

function toEpochMs(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v < 1e12 ? v * 1000 : v;
  if (typeof v === 'string' && v) {
    const t = Date.parse(v);
    if (!Number.isNaN(t)) return t;
  }
  return null;
}

function maskKey(key) {
  if (!key) return '';
  return key.length <= 8 ? key.slice(0, 2) + '…' : key.slice(0, 3) + '…' + key.slice(-4);
}

async function safeEqual(a, b) {
  const [left, right] = await Promise.all([
    crypto.subtle.digest('SHA-256', encoder.encode(String(a))),
    crypto.subtle.digest('SHA-256', encoder.encode(String(b))),
  ]);
  const leftBytes = new Uint8Array(left);
  const rightBytes = new Uint8Array(right);
  let diff = 0;
  for (let i = 0; i < leftBytes.length; i++) diff |= leftBytes[i] ^ rightBytes[i];
  return diff === 0;
}

function randomHex(nBytes) {
  const buf = new Uint8Array(nBytes);
  crypto.getRandomValues(buf);
  return [...buf].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function bearerToken(request) {
  const h = request.headers.get('Authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(h.trim());
  return m ? m[1].trim() : '';
}

async function readBody(request) {
  const declared = Number(request.headers.get('content-length'));
  if (Number.isFinite(declared) && declared > MAX_REQUEST_BODY_BYTES) {
    throw new ClientError('request body is too large', 413);
  }
  if (!request.body) return null;

  const reader = request.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_REQUEST_BODY_BYTES) {
        try { await reader.cancel(); } catch (e) { /* client body already closed */ }
        throw new ClientError('request body is too large', 413);
      }
      chunks.push(value);
    }
  } finally {
    try { reader.releaseLock(); } catch (e) { /* already released */ }
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try { return JSON.parse(new TextDecoder().decode(bytes)); } catch (e) { return null; }
}

async function abortableRead(reader, signal) {
  if (!signal) return reader.read();
  if (signal.aborted) throw abortErrorFrom(signal);
  return new Promise((resolve, reject) => {
    let settled = false;
    const onAbort = () => {
      if (settled) return;
      settled = true;
      try { reader.cancel(); } catch { /* body already closed */ }
      reject(abortErrorFrom(signal));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    reader.read().then((value) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener('abort', onAbort);
      resolve(value);
    }, (error) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener('abort', onAbort);
      reject(error);
    });
  });
}

function abortErrorFrom(signal) {
  if (signal?.reason instanceof Error) return signal.reason;
  const error = new Error('The operation was aborted');
  error.name = 'AbortError';
  return error;
}

async function readLimitedText(response, maxBytes = MAX_UPSTREAM_JSON_BYTES, signal) {
  if (!response.body) return '';
  const reader = response.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await abortableRead(reader, signal);
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        try { await reader.cancel(); } catch { /* upstream body already closed */ }
        const error = new Error('upstream response is too large');
        error.status = 502;
        throw error;
      }
      chunks.push(value);
    }
  } finally {
    try { reader.releaseLock(); } catch { /* already released */ }
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

function controlPlaneTimeoutError(url) {
  const error = new Error(`upstream request timed out: ${url}`);
  error.name = 'TimeoutError';
  error.status = 504;
  return error;
}

// 控制面唯一的 fetch owner：deadline 同时覆盖 fetch 与响应体读取，并在 finally 释放 signal/timer。
async function upstreamFetch(url, options = {}, { signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS, consume } = {}) {
  if (typeof consume !== 'function') throw new TypeError('upstreamFetch requires a consume callback');
  const controller = new AbortController();
  let timedOut = false;
  const timeoutError = controlPlaneTimeoutError(url);
  const onAbort = () => controller.abort(abortErrorFrom(signal));
  if (signal) {
    if (signal.aborted) throw abortErrorFrom(signal);
    signal.addEventListener('abort', onAbort, { once: true });
  }
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort(timeoutError);
  }, timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return consume ? await consume(response, controller.signal) : response;
  } catch (error) {
    if (signal?.aborted) throw abortErrorFrom(signal);
    if (timedOut || controller.signal.aborted && controller.signal.reason === timeoutError) throw timeoutError;
    throw error;
  } finally {
    clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', onAbort);
    if (!controller.signal.aborted) controller.abort();
  }
}

// ============================ 错误模型 ============================

// 客户端请求本身的问题（400/422 等），不参与账号故障转移。
class ClientError extends Error {
  constructor(message, status = 400) { super(message); this.status = status; }
}

// 号池内部失败分类：disable=密钥失效永久拉黑 / cooldown=按秒冷却 / retry=直接换号 /
// request=请求本身的问题（不重试，直接返回 mapped）。
// mapped: { status, body, retryAfter? } 为客户端可见响应（按 kind 包装）。
class UpstreamFailure extends Error {
  constructor(kind, mapped, cooldownSec, message) {
    super(message || mapped?.body?.error?.message || 'upstream failure');
    this.kind = kind;
    this.mapped = mapped || null;
    this.cooldownSec = cooldownSec || 0;
  }
}

// 从消息文本 "resets at <RFC3339>" 中提取恢复时间（上游 429 无 Retry-After 时的兜底）。
function retryAfterFromMessage(message, nowMs) {
  const idx = message.toLowerCase().indexOf('resets at ');
  if (idx < 0) return 0;
  const fields = message.slice(idx + 10).trim().split(/\s+/);
  if (!fields.length) return 0;
  const t = Date.parse(fields[0].replace(/[.,;)\]]+$/, ''));
  if (Number.isNaN(t)) return 0;
  return Math.max(0, Math.ceil((t - nowMs) / 1000));
}

// 从上游 429 响应提取冷却秒数（Retry-After 头 → rateLimit.reset → "resets at" 文案）。
function extractRetryAfterSec(status, bodyText, headers, nowMs = Date.now()) {
  if (status !== 429) return 0;
  const ra = headers ? headers.get('Retry-After') : null;
  if (ra) {
    const asInt = parseInt(ra, 10);
    if (Number.isFinite(asInt) && asInt > 0) return asInt;
    const t = Date.parse(ra);
    if (!Number.isNaN(t)) return Math.max(0, Math.ceil((t - nowMs) / 1000));
  }
  let payload = null;
  try { payload = JSON.parse(bodyText); } catch { /* 纯文本 */ }
  payload = isRecord(payload) ? payload : {};
  let reset = isRecord(payload.rateLimit) ? num(payload.rateLimit.reset) : null;
  const inner = isRecord(payload.error) ? payload.error : null;
  if (isRecord(inner?.rateLimit)) reset = num(inner.rateLimit.reset) ?? reset;
  if (reset && reset > 0) return Math.max(0, Math.ceil(reset - nowMs / 1000));
  const message = str(payload.error?.message) || str(payload.message) || bodyText || '';
  return retryAfterFromMessage(message, nowMs);
}

// ============================ §E 错误映射（对齐 proxy.mjs） ============================

// normalize CC usage: outputTokens=0 → zero everything（反异常计费）
function normalizeUsage(u) {
  if (!u) return;
  const ot = Number(u.outputTokens);
  if (!ot) {
    u.inputTokens = 0;
    u.cachedInputTokens = 0;
  }
}

// CC 的 inputTokens 是「总数」（含缓存命中），Anthropic 的 input_tokens 只计非缓存部分。
// 优先用上游的 noCacheTokens；缺失时回退减法（issue #25）。
function anthropicInputTokens(usage, noCacheOverride) {
  const u = usage || {};
  if (typeof noCacheOverride === 'number' && noCacheOverride >= 0) return noCacheOverride;
  const noCache = u.inputTokenDetails && u.inputTokenDetails.noCacheTokens;
  if (typeof noCache === 'number' && noCache >= 0) return noCache;
  const cacheRead = u.cachedInputTokens || (u.inputTokenDetails && u.inputTokenDetails.cacheReadTokens) || 0;
  const cacheWrite = (u.inputTokenDetails && u.inputTokenDetails.cacheWriteTokens) || 0;
  return Math.max(0, (u.inputTokens || 0) - cacheRead - cacheWrite);
}

// 上游 finishReason → 规范化。对齐 CLI 的 normalizeStopReason2：
//   tool_use|tool-calls|tool_calls → tool_calls；length 家族 → length；
//   network/connection/upstream error → upstream_error；未知值原样透出。
function mapFinishReason(reason) {
  const r = String(reason ?? '').trim().toLowerCase();
  if (!r) return 'stop';
  if (r === 'tool-calls' || r === 'tool_calls' || r === 'tool_use') return 'tool_calls';
  if (r === 'length' || r === 'max_tokens' || r === 'max_output_tokens' || r === 'model_context_window_exceeded') return 'length';
  if (/^(?:network|connection|upstream)[-_\s]?error$/.test(r)) return 'upstream_error';
  return r;
}

// OpenAI 的 finish_reason 没有 pause_turn：折成 'length' 至少如实表达「输出不完整」。
function toOpenAIFinishReason(finishReason) {
  return finishReason === 'pause_turn' ? 'length' : finishReason;
}

// 上游「没有正常走完」的两种情形（无 finish / provider 报连接失败）→ 可重试 502。
function incompleteUpstreamDetail(sawFinish, finishReason) {
  if (!sawFinish) return 'no finish event';
  if (finishReason === 'upstream_error') return 'provider reported an upstream connection failure';
  return null;
}

function incompleteUpstreamError(detail) {
  return {
    status: 502,
    retryAfter: 10,
    body: {
      error: {
        message: `Upstream stream ended without a completion finish (${detail}) — response was truncated`,
        type: 'upstream_error',
      },
      retry_after: 10,
    },
  };
}

// 零输出 → 429（反异常计费，SDK 自动重试）
function zeroOutputError() {
  return {
    status: 429,
    retryAfter: 10,
    body: {
      error: { message: 'Empty response from upstream (zero output tokens)', type: 'rate_limit_error' },
      retry_after: 10,
    },
  };
}

// 空闲超时 → 429 + retry_after 5
function idleTimeoutError() {
  return {
    status: 429,
    retryAfter: 5,
    body: {
      error: { message: 'Response timeout - request timed out', type: 'rate_limit_error' },
      retry_after: 5,
    },
  };
}

function transportError(message) {
  return {
    status: 502,
    retryAfter: 10,
    body: { error: { message: `Upstream error: ${message}`, type: 'proxy_error' }, retry_after: 10 },
  };
}

const CC_STATUS_MAP = {
  400: { status: 400, type: 'invalid_request_error' },
  401: { status: 401, type: 'authentication_error' },
  402: { status: 429, type: 'rate_limit_error' },
  403: { status: 401, type: 'authentication_error' },
  404: { status: 404, type: 'not_found' },
  422: { status: 400, type: 'invalid_request_error' },
  429: { status: 429, type: 'rate_limit_error' },
  500: { status: 502, type: 'upstream_error' },
  502: { status: 502, type: 'upstream_error' },
  503: { status: 503, type: 'temporarily_unavailable' },
};

function mapCcError(ccStatus, ccBody) {
  const mapped = CC_STATUS_MAP[ccStatus] || { status: 502, type: 'upstream_error' };
  let message = `CC API error (${ccStatus})`;
  let code = null;

  if (ccBody) {
    try {
      const parsed = JSON.parse(ccBody);
      message = parsed.error?.message || parsed.message || message;
      code = parsed.error?.code || parsed.code || null;
    } catch {
      message = String(ccBody).slice(0, 200) || message;
    }
  }

  if (ccStatus === 429) {
    return {
      status: 429,
      code,
      retryAfter: 30,
      body: {
        error: { message, type: 'rate_limit_error', ...(code ? { code } : {}) },
        retry_after: 30,
      },
    };
  }

  return { status: mapped.status, code, body: { error: { message, type: mapped.type, ...(code ? { code } : {}) } } };
}

// 流内 error 事件：取值链 = message 的 "<NNN>" 前缀 > error.statusCode > 502。
// 返回带上 reportedStatus 供号池按「上游真实状态」分类（429/401/403 冷却或拉黑）。
function mapCcEventError(event) {
  const message = event.error?.message || event.message || 'Unknown CC error';
  const code = event.error?.code || event.code || null;
  const statusMatch = String(message).match(/^<(\d{3})>/);
  const reportedStatus = statusMatch
    ? Number(statusMatch[1])
    : (Number.isInteger(event.error?.statusCode) ? event.error.statusCode : null);
  const ccStatus = reportedStatus ?? 502;
  const mapped = CC_STATUS_MAP[ccStatus] || { status: 502, type: 'upstream_error' };

  if (mapped.status === 429) {
    return {
      status: 429,
      code,
      reportedStatus,
      retryAfter: 30,
      body: { error: { message, type: 'rate_limit_error', ...(code ? { code } : {}) }, retry_after: 30 },
    };
  }

  return { status: mapped.status, code, reportedStatus, body: { error: { message, type: mapped.type, ...(code ? { code } : {}) } } };
}

// ── 号池失败分类 ──
// HTTP 状态按「原始状态」分类（客户端可见 body 是另一层映射）。
function classifyHttpFailure(rawStatus, mapped, retryAfterSec) {
  if (rawStatus === 401) return new UpstreamFailure('disable', mapped, 0, 'auth failed (401)');
  if (rawStatus === 403) return new UpstreamFailure('cooldown', mapped, AUTH_403_COOLDOWN_S, 'forbidden (403)');
  if (rawStatus === 402 || rawStatus === 429) {
    return new UpstreamFailure('cooldown', mapped, retryAfterSec > 0 ? retryAfterSec : DEFAULT_429_COOLDOWN_S);
  }
  if (rawStatus >= 500) return new UpstreamFailure('retry', mapped);
  return new UpstreamFailure('request', mapped); // 400/404/422 等：请求本身问题
}

// 流内 error 事件分类：优先 reportedStatus（上游真实状态），其次映射后状态。
function classifyEventFailure(mappedEventError) {
  const st = mappedEventError.reportedStatus;
  if (st === 401) return new UpstreamFailure('disable', mappedEventError, 0, 'auth failed (401, in-stream)');
  if (st === 403) return new UpstreamFailure('cooldown', mappedEventError, AUTH_403_COOLDOWN_S, 'forbidden (403, in-stream)');
  if (st === 402 || st === 429 || (st == null && mappedEventError.status === 429)) {
    return new UpstreamFailure('cooldown', mappedEventError, mappedEventError.retryAfter || 30);
  }
  if (mappedEventError.status >= 500) return new UpstreamFailure('retry', mappedEventError);
  return new UpstreamFailure('request', mappedEventError);
}

// ============================ §F CC 请求体构建（对齐 proxy.mjs buildCcRequest） ============================

// CLI 发送前会重写部分工具名（resolveToolNameAlias）
const TOOL_NAME_ALIASES = {
  bash_output: 'shell_output',
  task_output: 'shell_output',
  tool_search: 'search_tools',
  read_multiple_files: 'read_file',
};
function toWireToolName(name) { return TOOL_NAME_ALIASES[name] || name; }

// CLI 的 toWireToolOutput：只取文本块，用 '\n' 拼接
function toWireToolOutputValue(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.filter((c) => c && c.type === 'text').map((c) => c.text ?? '').join('\n');
  }
  return content == null ? '' : String(content);
}

function tryParseJSON(s) {
  try { return JSON.parse(s); } catch { return {}; }
}

// CLI 的 slug 规则：slug = slugify(workingDir)
function slugifyProjectPath(p) {
  const s = String(p || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return s || 'root';
}

function generateTraceparent() {
  return `00-${randomHex(16)}-${randomHex(8)}-01`;
}

function buildCcRequest(openaiReq, env) {
  const cfg = getConfig(env);
  const { model, messages, max_tokens, max_completion_tokens, temperature, tools, reasoning_effort, tool_choice, parallel_tool_calls, prompt_cache_key } = openaiReq;
  const msgs = Array.isArray(messages) ? messages : [];
  if (!msgs.length) throw new ClientError('messages is required');

  // system / developer → 块数组（非最后一块补 \n，cache_control 逐块保留）
  const systemMsgs = msgs.filter((m) => m.role === 'system' || m.role === 'developer');
  const systemBlocks = [];
  for (const m of systemMsgs) {
    if (typeof m.content === 'string') {
      if (m.content) systemBlocks.push({ type: 'text', text: m.content });
    } else if (Array.isArray(m.content)) {
      for (const c of m.content) {
        const text = c?.text ?? c?.content ?? '';
        if (text === '' && !c?.cache_control) continue;
        const block = { type: 'text', text: String(text) };
        if (c?.cache_control) block.cache_control = c.cache_control;
        systemBlocks.push(block);
      }
    } else if (m.content != null) {
      systemBlocks.push({ type: 'text', text: String(m.content) });
    }
  }
  for (let i = 0; i < systemBlocks.length - 1; i++) systemBlocks[i].text += '\n';
  const chatMessages = msgs.filter((m) => m.role !== 'system' && m.role !== 'developer');

  // tool_call_id → tool_name 反查表
  const toolNameMap = {};
  for (const msg of chatMessages) {
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) {
        if (tc.id) toolNameMap[tc.id] = tc.function?.name || '';
      }
    }
  }

  const ccMessages = chatMessages.map((msg) => {
    if (msg.role === 'user') {
      if (typeof msg.content === 'string') {
        return { role: 'user', content: [{ type: 'text', text: msg.content }] };
      }
      if (Array.isArray(msg.content)) {
        const parts = msg.content.map((part) => {
          if (part && part.type === 'image_url') {
            const url = part.image_url?.url || '';
            const mediaType = /^data:([^;,]+)/.exec(url)?.[1];
            const imagePart = { type: 'image', image: url };
            if (mediaType) imagePart.mimeType = mediaType;
            return imagePart;
          }
          return part;
        }).filter(Boolean);
        return { role: 'user', content: parts };
      }
      return { role: 'user', content: [{ type: 'text', text: String(msg.content) }] };
    }
    if (msg.role === 'assistant') {
      const parts = [];
      // 思考内容必须回传（CC thinking 模式校验），次序 [reasoning, text, tool-call]
      if (msg.reasoning_content) parts.push({ type: 'reasoning', text: msg.reasoning_content });
      if (msg.content && typeof msg.content === 'string') {
        if (msg.content) parts.push({ type: 'text', text: msg.content });
      } else if (msg.content && Array.isArray(msg.content)) {
        for (const part of msg.content) {
          if (!part) continue;
          if (part.type === 'text') parts.push(part);
          else if (part.type === 'reasoning' && !msg.reasoning_content) parts.push(part);
        }
      }
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          parts.push({
            type: 'tool-call',
            toolCallId: tc.id,
            toolName: tc.function?.name || '',
            input: (typeof tc.function?.arguments === 'string' ? tryParseJSON(tc.function.arguments) : (tc.function?.arguments || {})),
          });
        }
      }
      return { role: 'assistant', content: parts };
    }
    if (msg.role === 'tool') {
      return {
        role: 'tool',
        content: [{
          type: 'tool-result',
          toolCallId: msg.tool_call_id,
          toolName: toolNameMap[msg.tool_call_id] || msg.name || '',
          output: { type: 'text', value: toWireToolOutputValue(msg.content) },
        }],
      };
    }
    return { role: 'user', content: [{ type: 'text', text: String(msg.content ?? '') }] };
  });

  // 缓存断点：客户端已打过就保留；否则 prompt_cache_key → system 最后一块 ephemeral
  const hasCacheMarker = systemBlocks.some((b) => b.cache_control) || ccMessages.some((msg) =>
    Array.isArray(msg.content) && msg.content.some((part) => part?.cache_control));
  if (prompt_cache_key && !hasCacheMarker && systemBlocks.length) {
    systemBlocks[systemBlocks.length - 1].cache_control = { type: 'ephemeral' };
  }

  const maxTok = num(max_completion_tokens) ?? num(max_tokens) ?? DEFAULT_MAX_TOKENS;

  const body = {
    config: {
      workingDir: cfg.deviceProjectDir,
      date: getDateStr(),
      environment: DEVICE_PLATFORM,
      structure: [],
      isGitRepo: false,
      currentBranch: '',
      mainBranch: '',
      gitStatus: '',
      recentCommits: [],
    },
    memory: null,
    taste: null,
    skills: null,
    permissionMode: 'standard',
    mode: cfg.cliMode,
    // threadId 需为合法 UUID，否则整键省略（在 forwardToCC 拿到 sessionId 后补）
    params: {
      model: model || DEFAULT_MODEL,
      messages: ccMessages,
      max_tokens: Math.min(maxTok, MAX_MAX_TOKENS),
      stream: true, // CC API 只支持流式
    },
  };

  if (systemBlocks.length) {
    body.params.system = systemBlocks;
  } else if (cfg.emptySystemPlaceholder) {
    // 上游在 params.system 缺省时会注入约 7.5K token 的默认提示词；空格占位绕过
    body.params.system = [{ type: 'text', text: ' ' }];
  }
  if (temperature !== undefined) body.params.temperature = temperature;
  if (reasoning_effort !== undefined) body.params.reasoning_effort = reasoning_effort;
  // CLI 总是下发 tools（空数组与缺键在 wire 上可观测）；线格式只有 name/description/input_schema
  body.params.tools = (tools || []).map((t) => ({
    name: toWireToolName(t.function?.name || t.name || ''),
    description: t.function?.description || t.description || '',
    input_schema: t.function?.parameters || t.input_schema || { type: 'object', properties: {} },
  }));
  if (tool_choice !== undefined) {
    if (typeof tool_choice === 'string') {
      const map = { 'auto': 'auto', 'none': 'none', 'required': 'any' };
      body.params.tool_choice = { type: map[tool_choice] || 'auto' };
    } else if (tool_choice.type === 'function') {
      body.params.tool_choice = { type: 'tool', name: tool_choice.function?.name };
    } else {
      body.params.tool_choice = tool_choice;
    }
  }
  if (parallel_tool_calls !== undefined) body.params.parallel_tool_calls = parallel_tool_calls;

  return body;
}

// ============================ §G SSE 行解析 ============================

async function* parseSSE(body) {
  if (!body) throw new Error('upstream response has no body');
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  let dataLines = [];
  let dataChars = 0;
  const dispatch = function* () {
    if (!dataLines.length) return;
    const payload = dataLines.join('\n');
    dataLines = [];
    dataChars = 0;
    if (payload.trim()) yield payload;
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      if (buf.length > MAX_SSE_EVENT_CHARS) throw new Error('upstream SSE line is too large');
      let idx;
      while ((idx = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, idx).replace(/\r$/, '');
        buf = buf.slice(idx + 1);
        if (line === '') yield* dispatch();
        else if (line.startsWith(':')) { /* keep-alive 注释 */ }
        else if (line.startsWith('data:')) {
          const data = line.slice(5).replace(/^ /, '');
          dataChars += data.length;
          if (dataChars > MAX_SSE_EVENT_CHARS) throw new Error('upstream SSE event is too large');
          dataLines.push(data);
        }
        // event:/id:/retry: 字段不携带 JSON 载荷，忽略
      }
    }
    buf += decoder.decode();
    if (buf.trim()) {
      // 兼容以裸 JSON 行收尾且无空行的上游
      const data = buf.startsWith('data:') ? buf.slice(5).replace(/^ /, '') : buf.trim();
      dataChars += data.length;
      if (dataChars > MAX_SSE_EVENT_CHARS) throw new Error('upstream SSE event is too large');
      dataLines.push(data);
    }
    yield* dispatch();
  } finally {
    try { reader.releaseLock(); } catch (e) { /* already released */ }
  }
}

// ============================ §H 流式翻译器（CC NDJSON → 各协议 SSE） ============================

function makeChunk(id, created, model, delta, finishReason, usage) {
  const chunk = {
    id,
    object: 'chat.completion.chunk',
    created,
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason || null }],
  };
  if (usage) chunk.usage = usage;
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

// CC NDJSON → OpenAI chat.completion.chunk
function createSseTranslator(model, completionId, created) {
  let sawFinish = false;
  let chunkIndex = 0;
  let finishReason = null;
  let usage = null;

  return {
    lastCcEvent: '',
    upstreamError: null,
    inputTokens: 0,
    outputTokens: 0,
    cachedInputTokens: 0,
    parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) return null;

      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;

      const out = [];

      switch (event.type) {
        case 'text-start':
        case 'reasoning-start':
        case 'start':
        case 'start-step':
          break;

        case 'text-delta': {
          const text = event.text || event.delta || '';
          if (!text) break;
          const delta = chunkIndex === 0 ? { role: 'assistant', content: text } : { content: text };
          chunkIndex++;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          const delta = chunkIndex === 0
            ? { role: 'assistant', reasoning_content: text }
            : { reasoning_content: text };
          chunkIndex++;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'tool-call': {
          const tcEntry = {
            index: 0,
            id: event.toolCallId || `call_${Date.now()}_${chunkIndex}`,
            type: 'function',
            function: {
              name: event.toolName || '',
              arguments: typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {}),
            },
          };
          const delta = chunkIndex === 0
            ? { role: 'assistant', content: null, tool_calls: [tcEntry] }
            : { tool_calls: [tcEntry] };
          chunkIndex++;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'finish-step': {
          sawFinish = true;
          if (event.finishReason) finishReason = mapFinishReason(event.finishReason);
          if (event.usage) {
            usage = event.usage;
            this.inputTokens = event.usage.inputTokens ?? 0;
            this.outputTokens = event.usage.outputTokens ?? 0;
            this.cachedInputTokens = event.usage.cachedInputTokens ?? 0;
          }
          break;
        }

        case 'finish': {
          sawFinish = true;
          const fr = toOpenAIFinishReason(finishReason || mapFinishReason(event.finishReason || 'stop'));
          const u = event.totalUsage || usage || {};
          normalizeUsage(u);
          this.inputTokens = u.inputTokens ?? 0;
          this.outputTokens = u.outputTokens ?? 0;
          this.cachedInputTokens = u.cachedInputTokens ?? 0;
          const openaiUsage = {
            prompt_tokens: u.inputTokens ?? 0,
            completion_tokens: u.outputTokens ?? 0,
            total_tokens: (u.inputTokens ?? 0) + (u.outputTokens ?? 0),
            prompt_tokens_details: { cached_tokens: u.cachedInputTokens ?? 0 },
          };
          out.push(makeChunk(completionId, created, model, {}, fr, openaiUsage));
          break;
        }

        case 'error': {
          this.upstreamError = mapCcEventError(event);
          // 不发 finish chunk：让流自然终止，避免下游 agent 循环提前停
          break;
        }

        case 'reasoning-end': case 'provider-metadata':
        case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end':
        case 'tool-error': case 'text-end':
          break;
        default:
          break;
      }

      return out.length > 0 ? out : null;
    },

    incompleteDetail() {
      return incompleteUpstreamDetail(sawFinish, finishReason);
    },

    getDoneEvent() {
      return 'data: [DONE]\n\n';
    },
  };
}

// Claude 格式假签名：Claude Code 只做浅校验（base64 首字符 E/R + payload 首字节 0x12）。
async function fakeThinkingSignature(thinkingText) {
  const digest = await sha256Text(thinkingText || 'dsh-proxy-thinking');
  const seed = digest.slice(0, 64);
  const raw = new Uint8Array(2 + seed.length);
  raw[0] = 0x12;
  raw[1] = seed.length;
  raw.set(seed, 2);
  return bytesToBase64(raw);
}

// CC NDJSON → Anthropic Messages SSE。parseLine 同步产出事件帧；finalize() 异步
// （thinking 签名需要 sha256）产出收尾事件：message_delta + message_stop 或 error。
function createAnthropicSseTranslator(model, messageId) {
  let nextBlockIndex = 0;
  let currentBlockIndex = -1;
  let currentBlockType = null;
  let blockStarted = false;
  let inputTokens = 0;
  let outputTokens = 0;
  let cachedInputTokens = 0;
  let cacheWriteTokens = 0;
  let noCacheTokens = -1;
  let stopReason = null;
  let finishNorm = null;
  let sawFinish = false;
  let hasError = false;
  let currentThinkingText = '';

  const frame = (type, data) => `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;

  async function closeBlock() {
    if (!blockStarted) return '';
    const idx = currentBlockIndex;
    const type = currentBlockType;
    let out = '';
    if (type === 'thinking') {
      out += frame('content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'signature_delta', signature: await fakeThinkingSignature(currentThinkingText) } });
      currentThinkingText = '';
    }
    blockStarted = false;
    currentBlockType = null;
    return out + frame('content_block_stop', { type: 'content_block_stop', index: idx });
  }

  async function startBlock(type, contentBlock) {
    if (!blockStarted || currentBlockType !== type) {
      const close = await closeBlock();
      currentBlockIndex = nextBlockIndex++;
      currentBlockType = type;
      blockStarted = true;
      return close + frame('content_block_start', { type: 'content_block_start', index: currentBlockIndex, content_block: contentBlock });
    }
    return '';
  }

  return {
    lastCcEvent: '',
    upstreamError: null,
    inputTokens: 0,
    outputTokens: 0,
    cachedInputTokens: 0,

    preamble() {
      return frame('message_start', {
        type: 'message_start',
        message: {
          id: messageId,
          type: 'message',
          role: 'assistant',
          content: [],
          model,
          usage: { input_tokens: 0, output_tokens: 0 },
        },
      });
    },

    // 终态判定（finalize 之外的同步版本，供故障转移分类）
    incompleteDetail() {
      return incompleteUpstreamDetail(sawFinish, finishNorm);
    },
    terminalKind() {
      if (this.upstreamError) return 'upstreamError';
      if (incompleteUpstreamDetail(sawFinish, finishNorm)) return 'incomplete';
      if (outputTokens === 0) return 'zero';
      return 'ok';
    },

    async parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]') return null;
      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;

      const out = [];

      switch (event.type) {
        case 'start': case 'start-step': case 'text-start': case 'reasoning-start':
          break;

        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          out.push(await startBlock('thinking', { type: 'thinking', thinking: '' }));
          currentThinkingText += text;
          out.push(frame('content_block_delta', { type: 'content_block_delta', index: currentBlockIndex, delta: { type: 'thinking_delta', thinking: text } }));
          break;
        }

        case 'text-delta': {
          const text = event.text || '';
          out.push(await startBlock('text', { type: 'text', text: '' }));
          out.push(frame('content_block_delta', { type: 'content_block_delta', index: currentBlockIndex, delta: { type: 'text_delta', text } }));
          outputTokens += 1;
          break;
        }

        case 'tool-call': {
          const close = await closeBlock();
          if (close) out.push(close);
          const id = event.toolCallId || `toolu_${randomHex(12)}`;
          const name = event.toolName || '';
          const input = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          const tcIndex = nextBlockIndex++;
          out.push(frame('content_block_start', { type: 'content_block_start', index: tcIndex, content_block: { type: 'tool_use', id, name, input: {} } }));
          out.push(frame('content_block_delta', { type: 'content_block_delta', index: tcIndex, delta: { type: 'input_json_delta', partial_json: input } }));
          out.push(frame('content_block_stop', { type: 'content_block_stop', index: tcIndex }));
          outputTokens += 20;
          break;
        }

        case 'finish-step':
        case 'finish': {
          sawFinish = true;
          if (event.finishReason) {
            finishNorm = mapFinishReason(event.finishReason);
            stopReason = mapAnthropicStopReason(finishNorm);
          }
          const u = event.totalUsage || event.usage;
          if (u) {
            normalizeUsage(u);
            inputTokens = u.inputTokens ?? inputTokens;
            outputTokens = u.outputTokens ?? outputTokens;
            cachedInputTokens = u.cachedInputTokens ?? cachedInputTokens;
            cacheWriteTokens = u.inputTokenDetails?.cacheWriteTokens ?? cacheWriteTokens;
            if (typeof u.inputTokenDetails?.noCacheTokens === 'number') noCacheTokens = u.inputTokenDetails.noCacheTokens;
          }
          break;
        }

        case 'error': {
          hasError = true;
          this.upstreamError = mapCcEventError(event);
          out.push(frame('error', { type: 'error', error: this.upstreamError.body.error }));
          break;
        }

        case 'reasoning-end': case 'provider-metadata':
        case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end':
        case 'tool-error': case 'text-end':
          break;
        default:
          break;
      }

      // 同步计数进 ctx 供零输出判定
      this.inputTokens = inputTokens;
      this.outputTokens = outputTokens;
      this.cachedInputTokens = cachedInputTokens;

      return out.length ? out : null;
    },

    // 流正常读完后调用：closeBlock（补签名）+ 终态事件
    async finalize() {
      const out = [];
      if (hasError) return out;

      if (blockStarted) {
        const idx = currentBlockIndex;
        const type = currentBlockType;
        if (type === 'thinking') {
          out.push(frame('content_block_delta', { type: 'content_block_delta', index: idx, delta: { type: 'signature_delta', signature: await fakeThinkingSignature(currentThinkingText) } }));
          currentThinkingText = '';
        }
        blockStarted = false;
        currentBlockType = null;
        out.push(frame('content_block_stop', { type: 'content_block_stop', index: idx }));
      }

      const incomplete = incompleteUpstreamDetail(sawFinish, finishNorm);
      if (incomplete) {
        out.push(frame('error', { type: 'error', error: incompleteUpstreamError(incomplete).body.error }));
      } else if (outputTokens === 0) {
        out.push(frame('error', { type: 'error', error: { type: 'rate_limit_error', message: 'Empty response from upstream (zero output tokens)' }, retry_after: 10 }));
      } else {
        out.push(frame('message_delta', {
          type: 'message_delta',
          delta: { stop_reason: stopReason || 'end_turn' },
          usage: {
            output_tokens: outputTokens,
            cache_read_input_tokens: cachedInputTokens,
            cache_creation_input_tokens: cacheWriteTokens || 0,
            input_tokens: noCacheTokens >= 0
              ? noCacheTokens
              : Math.max(0, inputTokens - cachedInputTokens - (cacheWriteTokens || 0)),
          },
        }));
        out.push(frame('message_stop', { type: 'message_stop' }));
      }
      return out;
    },
  };
}

function mapAnthropicStopReason(finishReason) {
  switch (finishReason) {
    case 'tool_calls': return 'tool_use';
    case 'length': return 'max_tokens';
    case 'stop': return 'end_turn';
    case 'pause_turn': return 'pause_turn';
    case 'refusal': return 'refusal';
    default: return 'end_turn';
  }
}

// ============================ §H-3 Responses 翻译器 ============================

function newResponsesId(prefix) {
  return prefix + randomHex(12);
}

function responsesTextOf(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.map((p) => (p && typeof p === 'object' ? (p.text || '') : '')).join('');
}

function responsesReasoningOf(item) {
  if (!item) return '';
  if (Array.isArray(item.summary) && item.summary.length) return item.summary.map((p) => (p && p.text) || '').join('');
  if (Array.isArray(item.content) && item.content.length) return item.content.map((p) => (p && p.text) || '').join('');
  return typeof item.text === 'string' ? item.text : '';
}

function buildResponsesUsage(usage, fallbackOutputTokens) {
  const u = usage || {};
  normalizeUsage(u);
  const inTok = u.inputTokens || 0;
  const outTok = u.outputTokens || fallbackOutputTokens || 0;
  return {
    input_tokens: inTok,
    input_tokens_details: {
      cached_tokens: u.cachedInputTokens || 0,
      cache_write_tokens: (u.inputTokenDetails && u.inputTokenDetails.cacheWriteTokens) || 0,
    },
    output_tokens: outTok,
    output_tokens_details: { reasoning_tokens: 0 },
    total_tokens: inTok + outTok,
  };
}

// CC NDJSON → Responses 具名 SSE（每个事件带递增 sequence_number）
function createResponsesSseTranslator(model, responseId, created) {
  let seq = 0;
  const sse = (type, data) => 'event: ' + type + '\ndata: ' + JSON.stringify(Object.assign({ type, sequence_number: seq++ }, data)) + '\n\n';
  let createdSent = false;
  let current = null;
  let outputIndex = 0;
  const doneItems = [];
  let usage = null;
  let textAcc = '';
  let finishReason = null;
  let sawFinish = false;

  const baseResponse = (status, output) => ({
    id: responseId, object: 'response', created_at: created, status,
    output: output || [], output_text: '', model, error: null, incomplete_details: null,
    parallel_tool_calls: true, previous_response_id: null, store: false, tools: [], metadata: {},
  });

  function startResponse() {
    createdSent = true;
    return [
      sse('response.created', { response: baseResponse('in_progress') }),
      sse('response.in_progress', { response: baseResponse('in_progress') }),
    ];
  }

  function closeItem() {
    if (!current) return [];
    const out = [];
    const item = current.item;
    const idx = current.index;
    if (current.kind === 'message') {
      out.push(sse('response.output_text.done', { item_id: item.id, output_index: idx, content_index: 0, text: current.textBuf, logprobs: [] }));
      out.push(sse('response.content_part.done', {
        item_id: item.id, output_index: idx, content_index: 0,
        part: { type: 'output_text', text: current.textBuf, annotations: [] },
      }));
      item.content = [{ type: 'output_text', text: current.textBuf, annotations: [] }];
      item.status = 'completed';
    } else if (current.kind === 'function_call') {
      out.push(sse('response.function_call_arguments.done', { item_id: item.id, output_index: idx, arguments: item.arguments }));
      item.status = 'completed';
    } else if (current.kind === 'reasoning') {
      out.push(sse('response.reasoning_summary_text.done', { item_id: item.id, output_index: idx, summary_index: 0, text: current.textBuf }));
      out.push(sse('response.reasoning_summary_part.done', {
        item_id: item.id, output_index: idx, summary_index: 0,
        part: { type: 'summary_text', text: current.textBuf },
      }));
      item.summary = [{ type: 'summary_text', text: current.textBuf }];
      item.status = 'completed';
    }
    out.push(sse('response.output_item.done', { output_index: idx, item }));
    doneItems.push(item);
    current = null;
    return out;
  }

  function openItem(kind, item) {
    const out = closeItem();
    current = { kind, index: outputIndex++, item, textBuf: '' };
    out.push(sse('response.output_item.added', { output_index: current.index, item }));
    if (kind === 'message') {
      out.push(sse('response.content_part.added', {
        item_id: item.id, output_index: current.index, content_index: 0,
        part: { type: 'output_text', text: '', annotations: [] },
      }));
    } else if (kind === 'reasoning') {
      out.push(sse('response.reasoning_summary_part.added', {
        item_id: item.id, output_index: current.index, summary_index: 0,
        part: { type: 'summary_text', text: '' },
      }));
    }
    return out;
  }

  return {
    lastCcEvent: '',
    upstreamError: null,
    inputTokens: 0,
    outputTokens: 0,
    cachedInputTokens: 0,
    get started() { return createdSent; },
    get sawFinish() { return sawFinish; },
    get stopReason() { return finishReason; },

    parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) return null;
      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;
      const out = [];

      switch (event.type) {
        case 'text-start': case 'reasoning-start': case 'start': case 'start-step':
          break;

        case 'text-delta': {
          const text = event.text || event.delta || '';
          if (!text) break;
          if (!createdSent) out.push.apply(out, startResponse());
          if (!current || current.kind !== 'message') {
            out.push.apply(out, openItem('message', { type: 'message', id: newResponsesId('msg_'), status: 'in_progress', role: 'assistant', content: [] }));
          }
          current.textBuf += text;
          textAcc += text;
          out.push(sse('response.output_text.delta', { item_id: current.item.id, output_index: current.index, content_index: 0, delta: text, logprobs: [] }));
          break;
        }

        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          if (!createdSent) out.push.apply(out, startResponse());
          if (!current || current.kind !== 'reasoning') {
            out.push.apply(out, openItem('reasoning', { type: 'reasoning', id: newResponsesId('rs_'), summary: [], status: 'in_progress' }));
          }
          current.textBuf += text;
          out.push(sse('response.reasoning_summary_text.delta', {
            item_id: current.item.id, output_index: current.index, summary_index: 0, delta: text,
          }));
          break;
        }

        case 'tool-call': {
          if (!createdSent) out.push.apply(out, startResponse());
          const callId = event.toolCallId || newResponsesId('call_');
          const args = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          out.push.apply(out, openItem('function_call', {
            type: 'function_call', id: newResponsesId('fc_'), call_id: callId,
            name: event.toolName || '', arguments: '', status: 'in_progress',
          }));
          current.item.arguments = args;
          out.push(sse('response.function_call_arguments.delta', { item_id: current.item.id, output_index: current.index, delta: args }));
          break;
        }

        case 'finish': {
          sawFinish = true;
          finishReason = event.finishReason ? mapFinishReason(event.finishReason) : null;
          const u = event.totalUsage || event.usage || null;
          if (u) {
            normalizeUsage(u);
            usage = u;
            this.inputTokens = u.inputTokens || 0;
            this.outputTokens = u.outputTokens || 0;
            this.cachedInputTokens = u.cachedInputTokens || 0;
          }
          break;
        }

        case 'error': {
          this.upstreamError = mapCcEventError(event);
          break;
        }

        default: break;
      }
      return out.length ? out : null;
    },

    finish() {
      if (!createdSent) return [];
      const out = closeItem();
      const incomplete = incompleteUpstreamDetail(sawFinish, finishReason);
      if (incomplete) {
        out.push(sse('response.failed', {
          response: Object.assign(baseResponse('failed'), {
            error: { code: 'upstream_error', message: incompleteUpstreamError(incomplete).body.error.message },
          }),
        }));
        return out;
      }
      const truncated = finishReason === 'length';
      const paused = finishReason === 'pause_turn';
      out.push(sse(truncated || paused ? 'response.incomplete' : 'response.completed', {
        response: Object.assign(baseResponse(truncated || paused ? 'incomplete' : 'completed', doneItems.slice()), {
          output_text: textAcc,
          incomplete_details: truncated ? { reason: 'max_output_tokens' }
            : paused ? { reason: 'pause_turn' } : null,
          usage: buildResponsesUsage(usage, this.outputTokens),
        }),
      }));
      return out;
    },

    fail(message) {
      if (!createdSent) return [];
      return [sse('response.failed', {
        response: Object.assign(baseResponse('failed'), {
          error: { code: 'upstream_error', message: message || 'Upstream error' },
        }),
      })];
    },

    errorEvent(message) {
      return sse('error', { code: null, message: message || 'Upstream error', param: null });
    },
  };
}

// ============================ §I 非流收集器 ============================
// 三条协议共用的 NDJSON 收集：读全流 → 事件累积 → 终态判定。
// 抛出 STREAM_IDLE_TIMEOUT（Symbol 标记）；流内 error 事件存 upstreamError 不抛出。

const STREAM_IDLE_TIMEOUT = Symbol('STREAM_IDLE_TIMEOUT');

async function readWithIdle(reader, idleMs) {
  let timer = null;
  try {
    return await Promise.race([
      reader.read(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(STREAM_IDLE_TIMEOUT), idleMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function collectCcStream(upstreamResp, idleMs) {
  const reader = upstreamResp.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  let fullText = '';
  let thinkingText = '';
  let finishReason = 'stop';
  let sawFinish = false;
  let usage = null;
  const toolCalls = [];
  let upstreamError = null;
  let lastCcEvent = '';

  const processLines = () => {
    const lines = buf.split('\n');
    buf = lines.pop() || '';
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) continue;
      let event;
      try { event = JSON.parse(trimmed); } catch { continue; }
      if (!event.type) continue;
      lastCcEvent = event.type;
      switch (event.type) {
        case 'text-delta':
          fullText += event.text || '';
          break;
        case 'reasoning-delta':
          thinkingText += event.text || '';
          break;
        case 'tool-call':
          toolCalls.push({
            id: event.toolCallId || ('call_' + randomHex(8)),
            type: 'function',
            function: {
              name: event.toolName || '',
              arguments: typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {}),
            },
          });
          break;
        case 'finish-step':
        case 'finish':
          sawFinish = true;
          finishReason = mapFinishReason(event.finishReason || 'stop');
          if (event.totalUsage || event.usage) usage = event.totalUsage || event.usage;
          break;
        case 'error':
          upstreamError = mapCcEventError(event);
          break;
        case 'text-start': case 'text-end': case 'start': case 'start-step':
        case 'reasoning-start': case 'reasoning-end':
        case 'provider-metadata': case 'tool-input-start': case 'tool-input-delta':
        case 'tool-input-end': case 'tool-error':
          break;
        default:
          break;
      }
    }
  };

  try {
    while (true) {
      const { done, value } = await readWithIdle(reader, idleMs);
      if (done) break;
      const chunkText = decoder.decode(value, { stream: true });
      buf += chunkText;
      if (buf.length > MAX_SSE_EVENT_CHARS) throw new Error('upstream SSE line is too large');
      if (chunkText.indexOf('\n') !== -1) processLines();
    }
  } finally {
    try { reader.releaseLock(); } catch { /* already released */ }
  }

  // 尾部：补齐解码器缓冲并处理最后一轮（无尾换行的半截行丢弃，与 proxy 行为一致）
  buf += decoder.decode();
  processLines();

  return {
    fullText,
    thinkingText,
    toolCalls,
    usage,
    finishReason,
    sawFinish,
    upstreamError,
    lastCcEvent,
  };
}

function collectedUsageOpenAI(usage) {
  const u = usage || {};
  normalizeUsage(u);
  return {
    prompt_tokens: u.inputTokens ?? 0,
    completion_tokens: u.outputTokens ?? 0,
    total_tokens: (u.inputTokens ?? 0) + (u.outputTokens ?? 0),
    prompt_tokens_details: { cached_tokens: u.cachedInputTokens ?? 0 },
  };
}

// ============================ §J Anthropic / Responses 构建与转换 ============================

async function buildAnthropicResponse(model, fullText, toolCalls, finishReason, usage, thinkingText) {
  const content = [];
  if (thinkingText) {
    content.push({ type: 'thinking', thinking: thinkingText, signature: await fakeThinkingSignature(thinkingText) });
  }
  if (fullText) content.push({ type: 'text', text: fullText });
  for (const tc of toolCalls || []) {
    let input = {};
    try { input = JSON.parse(tc.function.arguments); } catch { input = {}; }
    content.push({ type: 'tool_use', id: tc.id, name: tc.function.name, input });
  }
  const u = usage || {};
  normalizeUsage(u);
  const estOut = Math.max(1,
    Math.ceil(((fullText || '').length + (thinkingText || '').length) / 4) + (toolCalls ? toolCalls.length * 20 : 0));
  return {
    id: `msg_${randomHex(12)}`,
    type: 'message',
    role: 'assistant',
    model,
    content,
    stop_reason: mapAnthropicStopReason(finishReason || 'stop'),
    stop_sequence: null,
    usage: {
      input_tokens: anthropicInputTokens(u),
      output_tokens: u.outputTokens || estOut,
      cache_creation_input_tokens: u.inputTokenDetails?.cacheWriteTokens ?? 0,
      cache_read_input_tokens: u.cachedInputTokens ?? 0,
    },
  };
}

function convertAnthropicToOpenAI(anthropicReq) {
  // 1. system（顶层）→ system 消息（块数组保留 cache_control）
  let systemPrompt = '';
  let systemBlocks = null;
  if (anthropicReq.system) {
    if (typeof anthropicReq.system === 'string') {
      systemPrompt = anthropicReq.system;
    } else if (Array.isArray(anthropicReq.system)) {
      systemBlocks = anthropicReq.system
        .filter((b) => b && b.type === 'text')
        .map((b) => {
          const blk = { type: 'text', text: b.text ?? '' };
          if (b.cache_control) blk.cache_control = b.cache_control;
          return blk;
        });
      systemPrompt = systemBlocks.map((b) => b.text).join('\n');
    }
  }

  // 2. messages 转换
  const toolNameFromId = {};
  const openaiMessages = [];
  if (systemPrompt) {
    openaiMessages.push({ role: 'system', content: systemBlocks && systemBlocks.length ? systemBlocks : systemPrompt });
  }

  const messages = anthropicReq.messages || [];
  for (const msg of messages) {
    if (msg.role === 'assistant') {
      let textContent = '';
      let thinkingContent = '';
      const textParts = [];
      let textHasCache = false;
      const toolCalls = [];
      const blocks = Array.isArray(msg.content) ? msg.content : [{ type: 'text', text: msg.content || '' }];
      for (const block of blocks) {
        if (block.type === 'text') {
          textContent += block.text || '';
          const part = { type: 'text', text: block.text || '' };
          if (block.cache_control) { part.cache_control = block.cache_control; textHasCache = true; }
          textParts.push(part);
        } else if (block.type === 'thinking') {
          thinkingContent += block.thinking || '';
        } else if (block.type === 'tool_use') {
          toolNameFromId[block.id] = block.name;
          toolCalls.push({
            id: block.id,
            type: 'function',
            function: { name: block.name, arguments: JSON.stringify(block.input || {}) },
          });
        }
      }
      const assistantMsg = { role: 'assistant', content: (textParts.length > 1 || textHasCache) ? textParts : (textContent || null) };
      if (thinkingContent) assistantMsg.reasoning_content = thinkingContent;
      if (toolCalls.length > 0) assistantMsg.tool_calls = toolCalls;
      openaiMessages.push(assistantMsg);
    } else if (msg.role === 'user') {
      let textContent = '';
      const parts = [];
      let textHasCache = false;
      const toolResults = [];
      if (typeof msg.content === 'string') {
        textContent = msg.content;
      } else if (Array.isArray(msg.content)) {
        for (const block of msg.content) {
          if (block.type === 'text') {
            textContent += block.text || '';
            const part = { type: 'text', text: block.text || '' };
            if (block.cache_control) { part.cache_control = block.cache_control; textHasCache = true; }
            parts.push(part);
          } else if (block.type === 'image') {
            const s = block.source || {};
            const url = s.type === 'base64' && s.data
              ? `data:${s.media_type || 'image/png'};base64,${s.data}`
              : (s.url || '');
            if (url) parts.push({ type: 'image_url', image_url: { url } });
          } else if (block.type === 'tool_result') {
            toolResults.push(block);
          }
        }
      }
      // tool_result 优先入队（OpenAI 语义：tool 消息紧跟 assistant tool_calls）
      for (const tr of toolResults) {
        const toolContent = typeof tr.content === 'string' ? tr.content
          : Array.isArray(tr.content) ? tr.content.map((c) => c.text || '').join('\n')
            : String(tr.content || '');
        const toolMsg = { role: 'tool', tool_call_id: tr.tool_use_id, content: toolContent };
        if (toolNameFromId[tr.tool_use_id]) toolMsg.name = toolNameFromId[tr.tool_use_id];
        openaiMessages.push(toolMsg);
      }
      if (parts.length || textContent) {
        const singleText = parts.length <= 1 && (parts.length === 0 || parts[0].type === 'text') && !textHasCache;
        openaiMessages.push({ role: 'user', content: singleText ? textContent : parts });
      }
    }
  }

  // 3. OpenAI 请求骨架
  const openaiReq = {
    model: anthropicReq.model || DEFAULT_MODEL,
    messages: openaiMessages,
    max_tokens: anthropicReq.max_tokens || DEFAULT_MAX_TOKENS,
    stream: anthropicReq.stream === true,
  };

  if (anthropicReq.tools && anthropicReq.tools.length > 0) {
    openaiReq.tools = anthropicReq.tools.map((t) => ({
      type: 'function',
      function: {
        name: t.name,
        description: t.description || '',
        parameters: t.input_schema || { type: 'object', properties: {} },
      },
    }));
  }

  if (anthropicReq.tool_choice) {
    const tc = anthropicReq.tool_choice;
    if (tc.type === 'auto' || tc.type === undefined) openaiReq.tool_choice = 'auto';
    else if (tc.type === 'any') openaiReq.tool_choice = 'required';
    else if (tc.type === 'tool') openaiReq.tool_choice = { type: 'function', function: { name: tc.name } };
    else if (tc.type === 'none') openaiReq.tool_choice = 'none';
  }

  if (anthropicReq.temperature !== undefined) openaiReq.temperature = anthropicReq.temperature;
  if (anthropicReq.top_p !== undefined) openaiReq.top_p = anthropicReq.top_p;
  if (anthropicReq.stop_sequences) openaiReq.stop = anthropicReq.stop_sequences;
  if (anthropicReq.metadata?.user_id) openaiReq.user = anthropicReq.metadata.user_id;

  // thinking → reasoning_effort（budget_tokens 映射）
  if (anthropicReq.thinking) {
    const t = anthropicReq.thinking;
    if (t.type === 'disabled' || t.type === 'none') {
      // 不发送 reasoning_effort
    } else if (t.type === 'adaptive') {
      openaiReq.reasoning_effort = t.effort ?? 'medium';
    } else if (t.budget_tokens !== undefined) {
      if (t.budget_tokens >= 10000) openaiReq.reasoning_effort = 'high';
      else if (t.budget_tokens >= 5000) openaiReq.reasoning_effort = 'medium';
      else openaiReq.reasoning_effort = 'low';
    }
  }

  return openaiReq;
}

// Responses API → 内部 Chat 格式（无状态代理：不支持 previous_response_id，收到即 400）
function convertResponsesToChat(respReq) {
  const messages = [];

  if (respReq.instructions !== undefined && respReq.instructions !== null) {
    const sys = responsesTextOf(respReq.instructions);
    if (sys) messages.push({ role: 'system', content: sys });
  }

  // reasoning / message / function_call 拆成并列 item；Chat 要求挂同一条 assistant 消息，先累积再冲刷
  let pending = null;
  const ensurePending = () => (pending = pending || { role: 'assistant', content: null, tool_calls: [] });
  const flushPending = () => {
    if (!pending) return;
    if (!pending.tool_calls.length) delete pending.tool_calls;
    if (!pending.reasoning_content) delete pending.reasoning_content;
    if (pending.content === null && !pending.tool_calls) { pending = null; return; }
    messages.push(pending);
    pending = null;
  };

  const input = respReq.input;
  if (typeof input === 'string') {
    messages.push({ role: 'user', content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (!item || typeof item !== 'object') continue;
      switch (item.type ?? (item.role ? 'message' : undefined)) {
        case 'reasoning': {
          const t = responsesReasoningOf(item);
          if (t) ensurePending().reasoning_content = t;
          break;
        }
        case 'message': {
          const text = responsesTextOf(item.content);
          if (item.role === 'assistant') {
            if (text) ensurePending().content = text;
          } else if (item.role === 'system' || item.role === 'developer') {
            flushPending();
            messages.push({ role: 'system', content: text });
          } else {
            flushPending();
            messages.push({ role: 'user', content: text });
          }
          break;
        }
        case 'function_call': {
          ensurePending().tool_calls.push({
            id: item.call_id || item.id || ('call_' + randomHex(8)),
            type: 'function',
            function: { name: item.name || '', arguments: item.arguments || '{}' },
          });
          break;
        }
        case 'function_call_output': {
          flushPending();
          messages.push({
            role: 'tool',
            tool_call_id: item.call_id || '',
            content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output === undefined ? '' : item.output),
          });
          break;
        }
        default:
          break;
      }
    }
  }
  flushPending();

  let tools;
  if (Array.isArray(respReq.tools) && respReq.tools.length) {
    tools = respReq.tools.filter((t) => t && (t.type === 'function' || t.name)).map((t) => ({
      type: 'function',
      function: {
        name: t.name || '',
        description: t.description || '',
        parameters: t.parameters || { type: 'object', properties: {} },
      },
    }));
    if (!tools.length) tools = undefined;
  }

  let toolChoice;
  const tc = respReq.tool_choice;
  if (typeof tc === 'string') toolChoice = tc;
  else if (tc && typeof tc === 'object' && tc.name) toolChoice = { type: 'function', function: { name: tc.name } };

  const out = { model: respReq.model, messages, stream: respReq.stream === true };
  if (tools) out.tools = tools;
  if (toolChoice) out.tool_choice = toolChoice;
  if (respReq.max_output_tokens !== undefined) out.max_tokens = respReq.max_output_tokens;
  if (respReq.temperature !== undefined) out.temperature = respReq.temperature;
  if (respReq.top_p !== undefined) out.top_p = respReq.top_p;
  if (respReq.parallel_tool_calls !== undefined) out.parallel_tool_calls = respReq.parallel_tool_calls;
  const eff = respReq.reasoning && typeof respReq.reasoning === 'object' ? respReq.reasoning.effort : undefined;
  if (eff) out.reasoning_effort = eff;
  return out;
}

function buildResponsesOutput(fullText, thinkingText, toolCalls) {
  const output = [];
  if (thinkingText) {
    output.push({ type: 'reasoning', id: newResponsesId('rs_'), summary: [{ type: 'summary_text', text: thinkingText }] });
  }
  if (fullText) {
    output.push({
      type: 'message', id: newResponsesId('msg_'), status: 'completed', role: 'assistant',
      content: [{ type: 'output_text', text: fullText, annotations: [] }],
    });
  }
  for (const tc of (toolCalls || [])) {
    const rawArgs = tc.function ? tc.function.arguments : '{}';
    output.push({
      type: 'function_call', id: newResponsesId('fc_'), call_id: tc.id,
      name: tc.function ? (tc.function.name || '') : '',
      arguments: typeof rawArgs === 'string' ? rawArgs : JSON.stringify(rawArgs || {}),
      status: 'completed',
    });
  }
  return output;
}

function buildResponsesObject(responseId, model, created, fullText, thinkingText, toolCalls, usage, opts) {
  const o = opts || {};
  const truncated = o.finishReason === 'length';
  const paused = o.finishReason === 'pause_turn';
  return {
    id: responseId,
    object: 'response',
    created_at: created,
    status: (truncated || paused) ? 'incomplete' : 'completed',
    completed_at: nowUnix(),
    error: null,
    incomplete_details: truncated ? { reason: 'max_output_tokens' }
      : paused ? { reason: 'pause_turn' } : null,
    input: o.input || [],
    instructions: o.instructions === undefined ? null : o.instructions,
    max_output_tokens: o.max_output_tokens === undefined ? null : o.max_output_tokens,
    model,
    output: buildResponsesOutput(fullText, thinkingText, toolCalls),
    output_text: fullText || '',
    parallel_tool_calls: true,
    previous_response_id: null,
    reasoning: o.reasoning || null,
    store: false,
    temperature: o.temperature === undefined ? 1 : o.temperature,
    text: { format: { type: 'text' } },
    tool_choice: o.tool_choice || 'auto',
    tools: o.tools || [],
    top_p: o.top_p === undefined ? 1 : o.top_p,
    truncation: 'disabled',
    usage: buildResponsesUsage(usage, 0),
    user: null,
    metadata: {},
  };
}

// ============================ §K 额度查询（/alpha/*） ============================

async function getJson(base, path, key, signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS) {
  const url = base + path;
  return upstreamFetch(url, {
    headers: {
      'Authorization': 'Bearer ' + key,
      'Accept': 'application/json',
      'User-Agent': 'cli',
      'x-command-code-version': CC_PROTOCOL_VERSION,
      'x-cli-environment': 'production',
    },
  }, {
    signal,
    timeoutMs,
    consume: async (response, bodySignal) => {
      const text = await readLimitedText(response, MAX_UPSTREAM_JSON_BYTES, bodySignal);
      if (!response.ok) {
        const error = new Error('HTTP ' + response.status);
        error.status = response.status;
        error.body = text.slice(0, 300);
        throw error;
      }
      try { return JSON.parse(text); }
      catch {
        const error = new Error('non-JSON response');
        error.status = 502;
        error.body = text.slice(0, 200);
        throw error;
      }
    },
  });
}

function pickWindow(wl, names) {
  for (const n of names) if (isRecord(wl[n])) return wl[n];
  return undefined;
}

function normalizeWindow(raw) {
  if (!isRecord(raw)) return undefined;
  const used = num(raw.used) ?? num(raw.usage) ?? num(raw.usedCredits) ?? num(raw.used_credits);
  const cap = num(raw.cap) ?? num(raw.limit) ?? num(raw.capCredits);
  const exceeded = raw.exceeded === true || raw.exceeded === 'true' ||
    (used != null && cap != null && cap > 0 && used >= cap);
  return {
    used: used ?? 0,
    cap: cap ?? 0,
    exceeded,
    resetAt: toEpochMs(raw.resetAt ?? raw.reset_at ?? raw.resetsAt) ?? 0,
  };
}

async function fetchReport(base, key, signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS) {
  const failures = [];
  const report = { sections: {} };
  let orgId;
  let planIdFallback;

  try {
    const who = await getJson(base, '/alpha/whoami', key, signal, timeoutMs);
    const user = isRecord(who.user) ? who.user : (isRecord(who.data) && isRecord(who.data.user) ? who.data.user : undefined);
    if (user) {
      report.account = {
        id: str(user.id) ?? '',
        name: str(user.name) ?? '',
        userName: str(user.userName) ?? str(user.username) ?? '',
      };
      report.sections.account = true;
    }
    const org = isRecord(who.org) ? who.org : undefined;
    orgId = org ? str(org.id) : undefined;
  } catch (e) {
    if (e.status === 401 || e.status === 403) {
    if (signal?.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError') throw e;
      const err = new Error('密钥被拒（HTTP ' + e.status + '）——请确认密钥有效，从 commandcode.ai/settings 获取。');
      err.status = e.status;
      throw err;
    }
    failures.push('whoami: ' + e.message);
  }

  try {
    const cr = await getJson(base, '/alpha/billing/credits', key, signal, timeoutMs);
    const credits = isRecord(cr.credits) ? cr.credits :
      (isRecord(cr.data) && isRecord(cr.data.credits) ? cr.data.credits : undefined);
    const wl = isRecord(cr.windowLimits) ? cr.windowLimits :
      (isRecord(cr.data) && isRecord(cr.data.windowLimits) ? cr.data.windowLimits : undefined);
    if (credits || wl) {
      report.credits = {
        monthlyCredits: num(credits?.monthlyCredits) ?? num(credits?.monthly_credits) ?? null,
        purchasedCredits: num(credits?.purchasedCredits) ?? num(credits?.purchased_credits) ?? null,
        freeCredits: num(credits?.freeCredits) ?? num(credits?.free_credits) ?? null,
        limited: wl?.limited === true,
        exceeded: str(wl?.exceeded) ?? '',
        belowThreshold: credits?.belowThreshold === true,
        creditThreshold: num(credits?.creditThreshold) ?? null,
        fiveHour: normalizeWindow(pickWindow(wl || {}, ['fiveHour', 'five_hour', 'rolling5h', '5h'])),
        weekly: normalizeWindow(pickWindow(wl || {}, ['weekly', 'week'])),
      };
      report.sections.credits = true;
      if (credits) planIdFallback = str(credits.planId) ?? str(credits.plan_id);
    }
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError') throw e;
    failures.push('billing/credits: ' + e.message);
  }

  try {
    const sub = await getJson(base, orgId
      ? '/alpha/billing/subscriptions?orgId=' + encodeURIComponent(orgId)
      : '/alpha/billing/subscriptions', key, signal, timeoutMs);
    const data = isRecord(sub.data) ? sub.data : (isRecord(sub.subscription) ? sub.subscription : undefined);
    const planId = str(data?.planId) ?? str(data?.plan_id) ?? planIdFallback;
    if (data || planId) {
      const info = planInfo(planId);
      report.plan = {
        planId: planId ?? '',
        name: info?.name ?? planId ?? '',
        status: str(data?.status) ?? '',
        monthlyCredits: info ? info.monthlyCredits : null,
        currentPeriodEnd: toEpochMs(data?.currentPeriodEnd ?? data?.current_period_end) ?? 0,
        cancelAtPeriodEnd: data?.cancelAtPeriodEnd === true,
        currentPeriodStart: toEpochMs(data?.currentPeriodStart ?? data?.current_period_start) ?? 0,
        pendingPhase: data?.pendingPhase ?? null,
      };
      report.planSource = 'subscription';
      report.sections.plan = true;
    }
  } catch (e) {
    if (planIdFallback) {
      const info = planInfo(planIdFallback);
      report.plan = {
        planId: planIdFallback, name: info?.name ?? planIdFallback, status: '',
        monthlyCredits: info ? info.monthlyCredits : null, currentPeriodEnd: 0,
        cancelAtPeriodEnd: false, currentPeriodStart: 0, pendingPhase: null,
      };
      report.planSource = 'credits';
      report.sections.plan = true;
    }
    if (signal?.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError') throw e;
    failures.push('billing/subscriptions: ' + e.message);
  }

  try {
    const us = await getJson(base, '/alpha/usage/summary', key, signal, timeoutMs);
    const u = isRecord(us.data) ? us.data : us;
    if (isRecord(u)) {
      report.usage = {
        totalCount: num(u.totalCount) ?? 0,
        totalCost: num(u.totalCost) ?? 0,
        averageCost: num(u.averageCost) ?? null,
        successRate: num(u.successRate) ?? 0,
        completedCount: num(u.completedCount) ?? 0,
        failedCount: num(u.failedCount) ?? 0,
        totalTokensIn: num(u.totalTokensIn) ?? 0,
        totalTokensOut: num(u.totalTokensOut) ?? 0,
        totalCredits: num(u.totalCredits) ?? 0,
        periodBasis: str(u.periodBasis) ?? '',
      };
      report.sections.usage = true;
    }
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError') throw e;
    failures.push('usage/summary: ' + e.message);
  }

  if (failures.length) report.failures = failures;
  if (!report.account && !report.credits && !report.plan) {
    const err = new Error('所有 commandcode.ai 端点均无法访问：' + failures.join('; '));
    err.status = 502;
    throw err;
  }
  return report;
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
  } catch (e) { return {}; }
}

function publicDetail(raw) {
  const detail = parseDetailValue(raw);
  return Object.keys(detail).length ? detail : null;
}

// Merge quota reports without allowing a partial upstream response to erase
// the last known-good account/plan/window snapshot. Both pool backends use this
// same projection so the D1 and in-memory fallbacks cannot drift.
function mergeQuotaState(previous, report) {
  const old = previous || {};
  const pick = (next, fallback) => (next !== undefined && next !== null && next !== '' ? next : (fallback ?? null));
  const boolInt = (next, fallback) => typeof next === 'boolean' ? (next ? 1 : 0) : (fallback ?? 0);
  const c = isRecord(report?.credits) ? report.credits : null;
  const p = isRecord(report?.plan) ? report.plan : null;
  const fh = c && isRecord(c.fiveHour) ? c.fiveHour : null;
  const wk = c && isRecord(c.weekly) ? c.weekly : null;
  const windowField = (next, field, fallback) => next && next[field] !== undefined && next[field] !== null
    ? next[field] : (fallback ?? null);
  const failures = Array.isArray(report?.failures) ? report.failures.filter(Boolean).map(String) : [];
  return {
    user_name: pick(report?.account?.userName, old.user_name || ''),
    plan_id: pick(p?.planId, old.plan_id || ''),
    plan_name: pick(p?.name, old.plan_name || ''),
    monthly_left: c ? pick(c.monthlyCredits, old.monthly_left) : (old.monthly_left ?? null),
    purchased: c ? pick(c.purchasedCredits, old.purchased) : (old.purchased ?? null),
    free: c ? pick(c.freeCredits, old.free) : (old.free ?? null),
    five_hour_used: windowField(fh, 'used', old.five_hour_used),
    five_hour_cap: windowField(fh, 'cap', old.five_hour_cap),
    five_hour_exceeded: fh ? boolInt(fh.exceeded, old.five_hour_exceeded) : (old.five_hour_exceeded ?? 0),
    five_hour_reset: windowField(fh, 'resetAt', old.five_hour_reset) ?? 0,
    weekly_used: windowField(wk, 'used', old.weekly_used),
    weekly_cap: windowField(wk, 'cap', old.weekly_cap),
    weekly_exceeded: wk ? boolInt(wk.exceeded, old.weekly_exceeded) : (old.weekly_exceeded ?? 0),
    weekly_reset: windowField(wk, 'resetAt', old.weekly_reset) ?? 0,
    last_error: failures.length ? failures.join('; ') : '',
    detail: mergeQuotaDetail(old.detail, report),
  };
}

function mergeQuotaDetail(previousRaw, report) {
  const detail = parseDetailValue(previousRaw);
  const c = isRecord(report?.credits) ? report.credits : null;
  const p = isRecord(report?.plan) ? report.plan : null;
  if (isRecord(report?.usage)) detail.usage = report.usage;
  if (c) {
    if (c.limited !== undefined) detail.limited = c.limited;
    if (c.exceeded !== undefined && c.exceeded !== '') detail.exceeded = c.exceeded;
    if (c.belowThreshold !== undefined) detail.belowThreshold = c.belowThreshold;
    if (c.creditThreshold !== undefined && c.creditThreshold !== null) detail.creditThreshold = c.creditThreshold;
  }
  if (p && report?.planSource === 'subscription') {
    if (p.status !== undefined && p.status !== '') detail.planStatus = p.status;
    if (p.currentPeriodStart) detail.currentPeriodStart = p.currentPeriodStart;
    if (p.currentPeriodEnd) detail.currentPeriodEnd = p.currentPeriodEnd;
    if (p.cancelAtPeriodEnd !== undefined) detail.cancelAtPeriodEnd = p.cancelAtPeriodEnd;
    if (p.pendingPhase !== undefined) detail.pendingPhase = p.pendingPhase;
  }
  const failures = Array.isArray(report?.failures) ? report.failures.filter(Boolean).map(String) : [];
  detail.partial = failures.length > 0;
  detail.failures = failures;
  detail.sections = isRecord(report?.sections) ? report.sections : (detail.sections || null);
  detail.lastChecked = Date.now();
  return JSON.stringify(detail);
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
        used: row.five_hour_used, cap: row.five_hour_cap,
        exceeded: !!row.five_hour_exceeded, resetAt: row.five_hour_reset,
      } : null,
      weekly: row.weekly_cap != null ? {
        used: row.weekly_used, cap: row.weekly_cap,
        exceeded: !!row.weekly_exceeded, resetAt: row.weekly_reset,
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

// D1 账号池：状态（冷却、用量、粘性选择）全部落库，跨 isolate 一致。
class D1Pool {
  constructor(db) { this.db = db; this._schemaReady = false; this._schemaPromise = null; this._seedPromise = null; this.tzOffset = DEFAULT_TZ_OFFSET; }

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
      )`)
    ]);

    // CREATE TABLE IF NOT EXISTS does not upgrade an existing D1 table. Add
    // columns introduced by newer versions so old installations keep working.
    // ALTER 包 try/catch：两个冷 isolate 并发迁移时，后到的会撞「duplicate column」，
    // 忽略即可（幂等）；迁移标记用 INSERT OR IGNORE 同理。
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

    // usage_daily 为旧版遗留表：新库不再创建；旧库检测到则补列并做一次迁移到 5 分钟桶。
    const { results: tables } = await this.db.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'usage_daily'"
    ).all();
    if (tables && tables.length) {
      const { results: dailyInfo } = await this.db.prepare('PRAGMA table_info(usage_daily)').all();
      const dailyColumns = new Set((dailyInfo || []).map((row) => row.name));
      const dailyMigrations = {
        requests: 'INTEGER NOT NULL DEFAULT 0',
        prompt_tokens: 'INTEGER NOT NULL DEFAULT 0',
        completion_tokens: 'INTEGER NOT NULL DEFAULT 0',
        cache_read_tokens: 'INTEGER NOT NULL DEFAULT 0',
      };
      for (const [name, definition] of Object.entries(dailyMigrations)) {
        if (!dailyColumns.has(name)) {
          try {
            await this.db.prepare(`ALTER TABLE usage_daily ADD COLUMN ${name} ${definition}`).run();
          } catch (e) {
            if (!/duplicate column/i.test(String(e?.message || e))) throw e;
          }
        }
      }
      await this.db.prepare('CREATE INDEX IF NOT EXISTS idx_usage_daily_day ON usage_daily(day)').run();

      const migrated = await this.db.prepare("SELECT value FROM usage_meta WHERE key = 'daily_to_bucket_v1'").first();
      const markerValue = String(migrated?.value || '');
      const running = markerValue.match(/^running:(\d+):([^:]+)$/);
      const runningAt = running ? Number(running[1]) : 0;
      if (migrated?.value !== '1') {
        const claimRecoverable = !migrated || (!running || !Number.isFinite(runningAt) || runningAt <= 0 || Date.now() - runningAt > D1_MIGRATION_LOCK_MS);
        if (!claimRecoverable) return false;
        const now = Date.now();
        const owner = randomHex(12);
        const claimValue = `running:${now}:${owner}`;
        const claim = migrated
          ? await this.db.prepare("UPDATE usage_meta SET value = ? WHERE key = 'daily_to_bucket_v1' AND value = ?").bind(claimValue, migrated.value).run()
          : await this.db.prepare("INSERT OR IGNORE INTO usage_meta (key, value) VALUES ('daily_to_bucket_v1', ?)").bind(claimValue).run();
        if (Number(claim?.meta?.changes || 0) === 0) return false;
        try {
          const legacyRows = (await this.db.prepare(
            'SELECT day, account_id, requests, prompt_tokens, completion_tokens, cache_read_tokens FROM usage_daily ORDER BY day, account_id'
          ).all()).results || [];
          for (let i = 0; i < legacyRows.length; i += D1_MIGRATION_BATCH_ROWS) {
            const batchRows = legacyRows.slice(i, i + D1_MIGRATION_BATCH_ROWS);
            const statements = batchRows.flatMap((row) => {
              const migrationKey = `daily_to_bucket_v1:${row.day}:${row.account_id}`;
              return [
                this.db.prepare('INSERT OR IGNORE INTO usage_migration_rows (migration_key, day, account_id) VALUES (?, ?, ?)')
                  .bind(migrationKey, row.day, row.account_id),
                this.db.prepare(
                  `INSERT INTO usage_buckets (bucket_start, account_id, requests, prompt_tokens, completion_tokens, cache_read_tokens)
                   SELECT ?, ?, ?, ?, ?, ? WHERE changes() = 1
                   ON CONFLICT(bucket_start, account_id) DO UPDATE SET
                     requests = usage_buckets.requests + excluded.requests,
                     prompt_tokens = usage_buckets.prompt_tokens + excluded.prompt_tokens,
                     completion_tokens = usage_buckets.completion_tokens + excluded.completion_tokens,
                     cache_read_tokens = usage_buckets.cache_read_tokens + excluded.cache_read_tokens`
                ).bind(dayStartEpoch(row.day, this.tzOffset), row.account_id, row.requests || 0,
                  row.prompt_tokens || 0, row.completion_tokens || 0, row.cache_read_tokens || 0),
              ];
            });
            if (statements.length) await this.db.batch(statements);
          }
          const completed = await this.db.prepare("UPDATE usage_meta SET value = '1' WHERE key = 'daily_to_bucket_v1' AND value = ?").bind(claimValue).run();
          if (Number(completed?.meta?.changes || 0) === 0) return false;
        } catch (error) {
          try {
            await this.db.prepare("UPDATE usage_meta SET value = ? WHERE key = 'daily_to_bucket_v1' AND value = ?").bind(`failed:${Date.now()}:${owner}`, claimValue).run();
          } catch { /* expiry-based recovery remains available */ }
          throw error;
        }
      }
    }

    await this.db.batch([
      this.db.prepare('CREATE INDEX IF NOT EXISTS idx_accounts_enabled ON accounts(enabled, rate_limited_until)'),
      this.db.prepare('CREATE INDEX IF NOT EXISTS idx_usage_buckets_start ON usage_buckets(bucket_start)'),
    ]);
    this._schemaReady = true;
  }

  // sticky: 取“最近使用”的可用账号 → 粘住单账号直至耗尽（缓存友好，默认）；
  // round_robin: 取“最久未用”的可用账号 → 均摊负载。
  async pick(strategy = 'sticky', excludeIds = []) {
    await this.ensureSchema();
    const params = [Date.now()];
    let excludeSql = '';
    if (excludeIds.length) {
      excludeSql = ' AND id NOT IN (' + excludeIds.map(() => '?').join(',') + ')';
      params.push(...excludeIds);
    }
    const order = strategy === 'round_robin' ? 'ASC' : 'DESC';
    const row = await this.db.prepare(
      `SELECT ${ACCOUNT_ROW_FIELDS} FROM accounts
       WHERE enabled = 1 AND rate_limited_until <= ?${excludeSql}
       ORDER BY last_used_at ${order}, id ASC LIMIT 1`
    ).bind(...params).first();
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
  async addWithQuota(key, label, report) {
    await this.ensureSchema();
    const state = mergeQuotaState({}, report);
    const insert = this.db.prepare(
      'INSERT INTO accounts (api_key, label, created_at) VALUES (?, ?, ?)'
    ).bind(key, label || '', Date.now());
    const update = this.db.prepare(
      `UPDATE accounts SET
         user_name = ?, plan_id = ?, plan_name = ?,
         monthly_left = ?, purchased = ?, free = ?,
         five_hour_used = ?, five_hour_cap = ?, five_hour_exceeded = ?, five_hour_reset = ?,
         weekly_used = ?, weekly_cap = ?, weekly_exceeded = ?, weekly_reset = ?,
         last_error = ?, detail = ?
       WHERE api_key = ?`
    ).bind(
      state.user_name, state.plan_id, state.plan_name,
      state.monthly_left, state.purchased, state.free,
      state.five_hour_used, state.five_hour_cap, state.five_hour_exceeded, state.five_hour_reset,
      state.weekly_used, state.weekly_cap, state.weekly_exceeded, state.weekly_reset,
      state.last_error, state.detail, key,
    );
    try {
      await this.db.batch([insert, update]);
    } catch (e) {
      if (/UNIQUE|constraint/i.test(String(e?.message || e))) {
        const error = new Error('该密钥已存在');
        error.status = 409;
        throw error;
      }
      throw e;
    }
    const row = await this.db.prepare('SELECT id FROM accounts WHERE api_key = ?').bind(key).first();
    return row?.id || null;
  }

  async patch(id, { label, enabled }) {
    await this.ensureSchema();
    const sets = [];
    const params = [];
    if (label !== undefined) { sets.push('label = ?'); params.push(String(label).slice(0, 60)); }
    if (enabled !== undefined) {
      sets.push('enabled = ?'); params.push(enabled ? 1 : 0);
      if (enabled) { sets.push('disabled_reason = ?'); params.push(''); }
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
      ).bind(usage.prompt_tokens || 0, usage.completion_tokens || 0, cacheRead,
        Date.now(), id),
      this.db.prepare(
        `INSERT INTO usage_buckets (bucket_start, account_id, requests, prompt_tokens, completion_tokens, cache_read_tokens)
         VALUES (?, ?, 1, ?, ?, ?)
         ON CONFLICT(bucket_start, account_id) DO UPDATE SET
           requests = requests + 1,
           prompt_tokens = prompt_tokens + excluded.prompt_tokens,
           completion_tokens = completion_tokens + excluded.completion_tokens,
           cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens`
      ).bind(usageBucketStart(Date.now(), this.tzOffset), id, usage.prompt_tokens || 0,
        usage.completion_tokens || 0, cacheRead),
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
      "UPDATE accounts SET enabled = 0, disabled_reason = ?, last_error = ? WHERE id = ?"
    ).bind(reason, reason, id).run();
  }

  async earliestRateLimitedUntil() {
    await this.ensureSchema();
    const row = await this.db.prepare(
      'SELECT MIN(rate_limited_until) AS t FROM accounts WHERE enabled = 1 AND rate_limited_until > ?'
    ).bind(Date.now()).first();
    return row && row.t ? row.t : 0;
  }

  async saveQuota(id, report) {
    await this.ensureSchema();
    const old = await this.db.prepare(`SELECT ${ACCOUNT_ROW_FIELDS} FROM accounts WHERE id = ?`).bind(id).first();
    if (!old) return;
    const state = mergeQuotaState(old, report);
    await this.db.prepare(
      `UPDATE accounts SET
         user_name = ?, plan_id = ?, plan_name = ?,
         monthly_left = ?, purchased = ?, free = ?,
         five_hour_used = ?, five_hour_cap = ?, five_hour_exceeded = ?, five_hour_reset = ?,
         weekly_used = ?, weekly_cap = ?, weekly_exceeded = ?, weekly_reset = ?,
         last_error = ?, detail = ?
       WHERE id = ?`
    ).bind(
      state.user_name, state.plan_id, state.plan_name,
      state.monthly_left, state.purchased, state.free,
      state.five_hour_used, state.five_hour_cap, state.five_hour_exceeded, state.five_hour_reset,
      state.weekly_used, state.weekly_cap, state.weekly_exceeded, state.weekly_reset,
      state.last_error, state.detail, id,
    ).run();
  }

  async markQuotaError(id, message) {
    await this.ensureSchema();
    await this.db.prepare('UPDATE accounts SET last_error = ? WHERE id = ?').bind(message, id).run();
  }

  async seedIfEmpty(keys) {
    const normalized = (keys || []).filter((item) => item && item.key).map((item) => ({ key: String(item.key), label: String(item.label || '') }));
    if (this._seedPromise) {
      try { await this._seedPromise; } catch { /* current caller still performs its own idempotent fill */ }
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

// 内存账号池：未绑定 D1 时的回退（ACCOUNTS 环境变量），状态不跨 isolate 共享。
class MemPool {
  constructor(items) {
    this.seq = 1;
    this.tzOffset = DEFAULT_TZ_OFFSET;
    this.buckets = new Map(); // 5 分钟桶：bucket_start -> Map(account_id -> usage)
    this.accounts = items.map((it) => ({
      id: this.seq++,
      api_key: it.key,
      label: it.label || '',
      enabled: true,
      user_name: '', plan_id: '', plan_name: '',
      monthly_left: null, purchased: null, free: null,
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
  async enabledCount() { return this.accounts.filter((a) => a.enabled).length; }
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
  async list() { return this.accounts.map(rowToAccount); }
  async add(key, label) {
    if (this.accounts.some((a) => a.api_key === key)) { const e = new Error('该密钥已存在'); e.status = 409; throw e; }
    const id = this.seq++;
    this.accounts.push({
      id, api_key: key, label: label || '', enabled: true, user_name: '', plan_id: '', plan_name: '',
      monthly_left: null, purchased: null, free: null,
      five_hour_used: null, five_hour_cap: null, five_hour_exceeded: 0, five_hour_reset: 0,
      weekly_used: null, weekly_cap: null, weekly_exceeded: 0, weekly_reset: 0,
      rate_limited_until: 0, disabled_reason: '', last_error: '', last_used_at: 0,
      requests: 0, prompt_tokens: 0, completion_tokens: 0, cache_read_tokens: 0,
      created_at: Date.now(), detail: null,
    });
    return id;
  }
  async getCredential(id) {
    return this.accounts.find((a) => a.id === id)?.api_key || null;
  }
  async addWithQuota(key, label, report) {
    const id = await this.add(key, label);
    try {
      await this.saveQuota(id, report);
      return id;
    } catch (error) {
      const account = this.accounts.find((a) => a.id === id);
      if (account?.api_key === key) await this.remove(id);
      throw error;
    }
  }
  async patch(id, { label, enabled }) {
    const a = this.accounts.find((a) => a.id === id);
    if (!a) return false;
    if (label !== undefined) a.label = String(label).slice(0, 60);
    if (enabled !== undefined) { a.enabled = !!enabled; if (a.enabled) a.disabled_reason = ''; }
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
    a.requests++; a.prompt_tokens += usage.prompt_tokens || 0;
    a.completion_tokens += usage.completion_tokens || 0;
    a.cache_read_tokens += usage.prompt_tokens_details?.cached_tokens || 0;
    a.last_used_at = Date.now(); a.last_error = '';
    this.pruneBuckets(Math.floor(Date.now() / 1000));
    const bucket = usageBucketStart(Date.now(), this.tzOffset);
    let perAcct = this.buckets.get(bucket);
    if (!perAcct) { perAcct = new Map(); this.buckets.set(bucket, perAcct); }
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
    a.enabled = false; a.disabled_reason = reason; a.last_error = reason;
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
  async seedIfEmpty() { return 0; }
}

// 解析 ACCOUNTS 环境变量：JSON 数组 [{key,label}] 或逗号分隔 Key 列表。
function parseAccountsEnv(raw) {
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

// 号池按 env（内存池）/ env.DB（D1 池）缓存实例：内存池的冷却与用量状态在
// 同一 isolate 的多次请求间保持；D1 池避免每个请求重复建表。
const d1Pools = new WeakMap();
const memPools = new WeakMap();
function getPool(env) {
  const tz = Number.isFinite(Number(env.TIMEZONE_OFFSET)) ? Number(env.TIMEZONE_OFFSET) : DEFAULT_TZ_OFFSET;
  if (env.DB) {
    let p = d1Pools.get(env.DB);
    if (!p) { p = new D1Pool(env.DB); d1Pools.set(env.DB, p); }
    p.tzOffset = tz;
    return p;
  }
  let p = memPools.get(env);
  if (!p) { p = new MemPool(parseAccountsEnv(env.ACCOUNTS)); memPools.set(env, p); }
  p.tzOffset = tz;
  return p;
}

// ============================ §M 上游调用 ============================

async function forwardToCC(env, body, apiKey, request, signal, promptCacheKey) {
  const cfg = getConfig(env);
  const url = `${cfg.apiBase}/alpha/generate`;
  const sessionId = await getSessionId(incomingSessionHeaders(request), apiKey, promptCacheKey);
  // CLI 的 toWireThreadId：只有合法 UUID 才放进信封，否则整键省略；
  // 同时按 CLI 的键顺序重排：config, memory, taste, skills, permissionMode, threadId, mode, promptCache, params
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(sessionId))) {
    const ordered = {};
    for (const k of ['config', 'memory', 'taste', 'skills', 'permissionMode']) ordered[k] = body[k];
    ordered.threadId = sessionId;
    for (const k of ['mode', 'promptCache', 'params']) if (k in body) ordered[k] = body[k];
    body = ordered;
  }

  // 与 CLI 的 buildCommandAuthHeaders 对齐：User-Agent 固定 "cli"
  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'cli',
    'x-command-code-version': CC_PROTOCOL_VERSION,
    'x-cli-environment': 'production',
    'x-project-slug': slugifyProjectPath(cfg.deviceProjectDir),
    'x-taste-learning': 'false',
    'x-session-id': sessionId,
    'Authorization': `Bearer ${apiKey}`,
    'traceparent': generateTraceparent(),
  };
  if (request.headers.get('x-cmd-zdr') === '1') headers['x-cmd-zdr'] = '1';

  return fetch(url, { method: 'POST', headers, body: JSON.stringify(body), signal });
}

// ============================ §N 统一故障转移循环 ============================

function pickClientStrategy(env) {
  const s = String(env.POOL_STRATEGY || 'sticky').toLowerCase();
  return s === 'round_robin' ? 'round_robin' : 'sticky';
}

const SSE_HEADERS = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-cache',
  'Connection': 'keep-alive',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Access-Control-Allow-Origin': '*',
};

// 客户端可见错误响应（按 kind 包装 mapped body）
function mappedErrorResponse(kind, mapped) {
  const retryAfter = mapped.retryAfter || mapped.body?.retry_after;
  const headers = { ...JSON_HEADERS };
  if (retryAfter) headers['Retry-After'] = String(Math.max(1, Math.ceil(retryAfter)));
  let body;
  if (kind === 'messages') {
    body = { type: 'error', error: { type: mapped.body.error.type, message: mapped.body.error.message } };
    if (retryAfter) body.retry_after = retryAfter;
  } else if (kind === 'responses') {
    body = { error: { message: mapped.body.error.message, type: mapped.body.error.type, code: mapped.code ?? null, param: null } };
    if (retryAfter) body.retry_after = retryAfter;
  } else {
    body = mapped.body;
  }
  return new Response(JSON.stringify(body), { status: mapped.status, headers });
}

function kindError(kind, status, type, message, retryAfter) {
  return mappedErrorResponse(kind, { status, retryAfter, body: { error: { message, type } } });
}

async function applyFailure(pool, acct, failure) {
  const reason = (failure.message || '').slice(0, 200);
  try {
    if (failure.kind === 'disable') await pool.disable(acct.id, reason || 'auth failed (401)');
    else if (failure.kind === 'cooldown') await pool.cooldown(acct.id, failure.cooldownSec, reason);
  } catch (e) {
    console.error(JSON.stringify({ message: 'failed to persist pool failure', error: e instanceof Error ? e.message : String(e) }));
  }
}

// ── 流式：TransformStream 写出端 ──
// preBuf 缓冲 preamble 帧（Anthropic 的 message_start），首个可见帧到达才认为 started，
// 因此 started 之前失败仍可向客户端返回 JSON 错误并换号重试。
function makeSink(kind) {
  const ts = new TransformStream();
  const writer = ts.writable.getWriter();
  const enc = new TextEncoder();
  const state = { started: false, preBuf: [], closed: false, lastWriteAt: 0 };
  let resolveOutcome;
  const outcome = new Promise((r) => { resolveOutcome = r; });
  let settled = false;
  const settle = (v) => { if (!settled) { settled = true; resolveOutcome(v); } };
  const isPreamble = (f) => kind === 'messages' && f.startsWith('event: message_start');

  const writeFrames = async (frames) => {
    if (state.closed || !frames || !frames.length) return true;
    if (!state.started) {
      state.preBuf.push(...frames);
      if (!frames.some((f) => !isPreamble(f))) return true;
      state.started = true;
      settle({ started: true });
      frames = state.preBuf.splice(0);
    }
    state.lastWriteAt = Date.now();
    try {
      for (const f of frames) await writer.write(enc.encode(f));
      return true;
    } catch {
      return false; // 客户端已断开
    }
  };

  return {
    ts,
    outcome,
    isStarted: () => state.started,
    lastWriteAt: () => state.lastWriteAt,
    writeFrames,
    // 终态：写入收尾帧并关闭流
    async finish(frames) {
      state.closed = true;
      try {
        if (frames && frames.length) {
          state.lastWriteAt = Date.now();
          for (const f of frames) await writer.write(enc.encode(f));
        }
        await writer.close();
      } catch {
        try { await writer.abort(); } catch { /* client gone */ }
      }
    },
    // 终态（未 started）：以失败收尾，Response 永不创建
    async fail(failure) {
      state.closed = true;
      settle({ started: false, failure });
      try { await writer.abort(); } catch { /* nothing to clean */ }
    },
  };
}

function createTranslatorFor(kind, meta) {
  if (kind === 'chat') return createSseTranslator(meta.model, meta.completionId, meta.created);
  if (kind === 'messages') return createAnthropicSseTranslator(meta.model, meta.messageId);
  return createResponsesSseTranslator(meta.model, meta.responseId, meta.created);
}

function determineTerminal(kind, translator) {
  if (translator.upstreamError) return { type: 'upstreamError' };
  if (kind === 'chat') {
    const d = translator.incompleteDetail();
    if (d) return { type: 'incomplete', detail: d };
    if (translator.outputTokens === 0) return { type: 'zero' };
    return { type: 'ok' };
  }
  if (kind === 'messages') {
    const tk = translator.terminalKind();
    if (tk === 'incomplete') return { type: 'incomplete', detail: translator.incompleteDetail() };
    if (tk === 'zero') return { type: 'zero' };
    return { type: 'ok' };
  }
  // responses
  const d = incompleteUpstreamDetail(translator.sawFinish, translator.stopReason);
  if (d) return { type: 'incomplete', detail: d };
  if (translator.outputTokens === 0 && !translator.started) return { type: 'zero' };
  return { type: 'ok' };
}

async function buildTerminalFrames(kind, terminal, translator) {
  switch (terminal.type) {
    case 'ok':
      if (kind === 'chat') return { frames: [translator.getDoneEvent()], record: true };
      if (kind === 'messages') return { frames: await translator.finalize(), record: translator.terminalKind() === 'ok' };
      return { frames: translator.finish(), record: true };
    case 'upstreamError':
      // 流内 error 事件已按各自协议在事件流里表达；这里补终态帧
      if (kind === 'chat') return { frames: [`data: ${JSON.stringify(translator.upstreamError.body)}\n\n`] };
      if (kind === 'messages') return { frames: [] };
      return { frames: translator.fail(translator.upstreamError.body.error.message) };
    case 'incomplete': {
      if (kind === 'chat') return { frames: [`data: ${JSON.stringify(incompleteUpstreamError(terminal.detail).body)}\n\n`] };
      if (kind === 'messages') return { frames: await translator.finalize() };
      return { frames: translator.finish() };
    }
    case 'zero':
      if (kind === 'chat') return { frames: [`data: ${JSON.stringify(zeroOutputError().body)}\n\n`] };
      if (kind === 'messages') return { frames: await translator.finalize() };
      return { frames: translator.finish() };
    case 'timeout': {
      const err = idleTimeoutError();
      if (kind === 'chat') return { frames: [`data: ${JSON.stringify(err.body)}\n\n`] };
      if (kind === 'messages') return { frames: [`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'rate_limit_error', message: err.body.error.message }, retry_after: 5 })}\n\n`] };
      return { frames: [translator.errorEvent(err.body.error.message)] };
    }
    case 'streamError': {
      const msg = (terminal.error && terminal.error.message) || 'stream error';
      if (kind === 'chat') return { frames: [`data: ${JSON.stringify({ error: { message: msg, type: 'proxy_error' } })}\n\n`] };
      if (kind === 'messages') return { frames: [`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'internal_error', message: msg } })}\n\n`] };
      return { frames: [translator.errorEvent(msg)] };
    }
    default:
      return { frames: [] };
  }
}

function failureForTerminal(terminal, translator) {
  switch (terminal.type) {
    case 'upstreamError': return classifyEventFailure(translator.upstreamError);
    case 'incomplete': return new UpstreamFailure('retry', incompleteUpstreamError(terminal.detail));
    case 'zero': return new UpstreamFailure('cooldown', zeroOutputError(), ZERO_OUTPUT_COOLDOWN_S);
    case 'timeout': return new UpstreamFailure('retry', idleTimeoutError());
    case 'streamError': return new UpstreamFailure('retry', transportError((terminal.error && terminal.error.message) || 'stream error'));
    default: return new UpstreamFailure('retry', transportError('unknown stream failure'));
  }
}

function usageFromTranslator(translator) {
  const prompt = translator.inputTokens || 0;
  const completion = translator.outputTokens || 0;
  return {
    prompt_tokens: prompt,
    completion_tokens: completion,
    total_tokens: prompt + completion,
    prompt_tokens_details: { cached_tokens: translator.cachedInputTokens || 0 },
  };
}

// 流式泵：读上游 → 翻译 → 写 sink。返回 {failure?}（未 started）或 {recordUsage, usage}。
async function pumpStream(kind, upstreamResp, translator, cfg, sink, abortController, onUsage) {
  const reader = upstreamResp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let clientGone = false;
  let terminal = null;

  // Anthropic 先缓冲 message_start（preamble），首个内容帧到达才真正 started
  if (kind === 'messages') {
    const ok = await sink.writeFrames([translator.preamble()]);
    if (!ok) {
      clientGone = true;
      terminal = { type: 'aborted' };
    }
  }

  // Anthropic 心跳：静默 > 15s 发 ping（官方 SDK 忽略）；只在 started 后。
  const heartbeat = kind === 'messages' ? setInterval(() => {
    if (sink.isStarted() && !clientGone && Date.now() - sink.lastWriteAt() > 15000) {
      sink.writeFrames(['event: ping\ndata: {"type":"ping"}\n\n']).then((ok) => { if (!ok) clientGone = true; });
    }
  }, 5000) : null;

  try {
    while (!terminal) {
      let chunk;
      try {
        chunk = await readWithIdle(reader, cfg.streamIdleMs);
      } catch (e) {
        if (e === STREAM_IDLE_TIMEOUT) { terminal = { type: 'timeout' }; }
        else terminal = { type: 'streamError', error: e };
        break;
      }
      if (chunk.done) break;
      if (clientGone) { terminal = { type: 'aborted' }; break; }

      const chunkText = decoder.decode(chunk.value, { stream: true });
      buffer += chunkText;
      if (buffer.length > MAX_SSE_EVENT_CHARS) { terminal = { type: 'streamError', error: new Error('upstream SSE line is too large') }; break; }
      let lines = [];
      if (chunkText.indexOf('\n') !== -1) {
        lines = buffer.split('\n');
        buffer = lines.pop() || '';
      }

      let hadVisible = false;
      for (const line of lines) {
        const frames = await translator.parseLine(line);
        if (frames && frames.length) {
          const ok = await sink.writeFrames(frames);
          if (!ok) { clientGone = true; terminal = { type: 'aborted' }; break; }
          hadVisible = true;
        }
      }
      if (terminal) break;

      if ((kind === 'chat' || kind === 'responses') && sink.isStarted() && !hadVisible) {
        const ok = await sink.writeFrames([': keepalive\n\n']);
        if (!ok) { clientGone = true; terminal = { type: 'aborted' }; break; }
      }
    }

    if (!terminal && buffer.trim()) {
      const frames = await translator.parseLine(buffer.trim());
      if (frames && frames.length) {
        const ok = await sink.writeFrames(frames);
        if (!ok) terminal = { type: 'aborted' };
      }
    }
    if (!terminal) terminal = determineTerminal(kind, translator);
  } finally {
    if (heartbeat) clearInterval(heartbeat);
    try { reader.releaseLock(); } catch { /* already released */ }
  }

  if (terminal.type === 'aborted') {
    try { abortController.abort(); } catch { /* already aborted */ }
    return { recordUsage: false };
  }
  if (terminal.type === 'timeout') {
    try { abortController.abort(); } catch { /* 打断上游，避免浪费 token */ }
  }

  if (!sink.isStarted()) {
    const failure = failureForTerminal(terminal, translator);
    await sink.fail(failure);
    return { failure };
  }

  const built = await buildTerminalFrames(kind, terminal, translator);
  await sink.finish(built.frames);
  const usage = usageFromTranslator(translator);
  if (built.record && onUsage) {
    try { await onUsage(usage); } catch { /* 记录失败不影响响应 */ }
  }
  return { recordUsage: built.record, usage };
}

function startStreamAttempt(kind, upstreamResp, meta, cfg, abortController, onUsage) {
  const translator = createTranslatorFor(kind, meta);
  const sink = makeSink(kind);
  const pumpPromise = pumpStream(kind, upstreamResp, translator, cfg, sink, abortController, onUsage);
  const outcome = sink.outcome.then(async (o) => {
    if (o.started) {
      // 泵继续在后台运行至完成（含用量记录）
      const done = pumpPromise.catch((e) => {
        console.error(JSON.stringify({ message: 'stream pump failed', error: e instanceof Error ? e.message : String(e) }));
      });
      return { started: true, done };
    }
    // 未 started：泵已结束，拿失败分类
    const result = await pumpPromise.catch(() => null);
    return { started: false, failure: o.failure || (result && result.failure) || new UpstreamFailure('retry', transportError('stream failed before start')) };
  });
  return { readable: sink.ts.readable, outcome };
}

// 非流收集（三条协议共用）
async function runCollectAttempt(kind, upstreamResp, cfg) {
  let collected;
  try {
    collected = await collectCcStream(upstreamResp, cfg.nonStreamIdleMs);
  } catch (e) {
    if (e === STREAM_IDLE_TIMEOUT) return { failure: new UpstreamFailure('retry', idleTimeoutError()) };
    return { failure: new UpstreamFailure('retry', transportError(e.message || String(e))) };
  }
  if (collected.upstreamError) return { failure: classifyEventFailure(collected.upstreamError) };
  const incomplete = incompleteUpstreamDetail(collected.sawFinish, collected.finishReason);
  if (incomplete) return { failure: new UpstreamFailure('retry', incompleteUpstreamError(incomplete)) };
  if (kind === 'chat') {
    const u = collected.usage || {};
    normalizeUsage(u);
    if ((u.outputTokens ?? 0) === 0) return { failure: new UpstreamFailure('cooldown', zeroOutputError(), ZERO_OUTPUT_COOLDOWN_S) };
  } else if (!collected.fullText && !collected.thinkingText && !collected.toolCalls.length) {
    return { failure: new UpstreamFailure('cooldown', zeroOutputError(), ZERO_OUTPUT_COOLDOWN_S) };
  }
  return { collected };
}

async function buildNonStreamResponse(kind, meta, collected) {
  const { model, created, completionId, messageId, responseId, echoOpts } = meta;
  const { fullText, thinkingText, toolCalls, usage, finishReason } = collected;
  if (kind === 'chat') {
    const message = { role: 'assistant', content: fullText || null };
    if (toolCalls.length) message.tool_calls = toolCalls;
    if (thinkingText) message.reasoning_content = thinkingText;
    return {
      id: completionId,
      object: 'chat.completion',
      created,
      model,
      choices: [{ index: 0, message, finish_reason: toOpenAIFinishReason(finishReason) }],
      usage: collectedUsageOpenAI(usage),
    };
  }
  if (kind === 'messages') {
    return buildAnthropicResponse(model, fullText, toolCalls, finishReason, usage, thinkingText);
  }
  const opts = Object.assign({}, echoOpts, { finishReason });
  return buildResponsesObject(responseId, model, created, fullText, thinkingText, toolCalls, usage, opts);
}

async function handleGenerate(request, env, ctx, kind) {
  const cfg = getConfig(env);
  const strategy = pickClientStrategy(env);
  const pool = getPool(env);

  let body;
  try { body = await readBody(request); }
  catch (e) {
    if (e instanceof ClientError) return kindError(kind, e.status, 'invalid_request_error', e.message);
    throw e;
  }
  if (body === null || !isRecord(body)) return kindError(kind, 400, 'invalid_request_error', 'bad request body');

  let ccBody;
  let model;
  let stream;
  let promptCacheKey;
  let echoOpts;
  try {
    if (kind === 'chat') {
      ccBody = buildCcRequest(body, env);
      model = body.model || DEFAULT_MODEL;
      stream = body.stream === true;
      promptCacheKey = typeof body.prompt_cache_key === 'string' ? body.prompt_cache_key : undefined;
    } else if (kind === 'messages') {
      const openaiReq = convertAnthropicToOpenAI(body);
      ccBody = buildCcRequest(openaiReq, env);
      model = body.model || 'claude-sonnet-4-6';
      stream = body.stream === true;
    } else {
      if (body.previous_response_id) {
        return kindError(kind, 400, 'invalid_request_error',
          'previous_response_id is not supported (this proxy is stateless); send the full input each turn');
      }
      const chatReq = convertResponsesToChat(body);
      if (!chatReq.messages.length) return kindError(kind, 400, 'invalid_request_error', 'input is required');
      ccBody = buildCcRequest(chatReq, env);
      model = chatReq.model || DEFAULT_MODEL;
      stream = chatReq.stream === true;
      echoOpts = {
        instructions: body.instructions === undefined ? null : body.instructions,
        max_output_tokens: body.max_output_tokens === undefined ? null : body.max_output_tokens,
        temperature: body.temperature,
        top_p: body.top_p,
        reasoning: body.reasoning || null,
        tool_choice: typeof body.tool_choice === 'string' ? body.tool_choice : 'auto',
        tools: body.tools || [],
        input: typeof body.input === 'string' ? [{ role: 'user', content: [{ type: 'input_text', text: body.input }] }] : (body.input || []),
      };
    }
  } catch (e) {
    if (e instanceof ClientError) return kindError(kind, e.status, 'invalid_request_error', e.message);
    return kindError(kind, 400, 'invalid_request_error', String(e.message || e));
  }

  const enabledCount = await pool.enabledCount();
  if (!enabledCount) {
    return kindError(kind, 503, 'server_error', 'no enabled Command Code accounts');
  }

  const meta = {
    model,
    created: nowUnix(),
    completionId: 'chatcmpl-' + randomHex(12),
    messageId: 'msg_' + randomHex(12),
    responseId: newResponsesId('resp_'),
    echoOpts,
  };

  const exclude = [];
  let lastFailure = null;

  for (let i = 0; i < enabledCount; i++) {
    const acct = await pool.pick(strategy, exclude);
    if (!acct) break;
    exclude.push(acct.id);

    const abortController = new AbortController();
    const onClientAbort = () => abortController.abort(request.signal.reason);
    if (request.signal.aborted) return kindError(kind, 499, 'server_error', 'client closed request');
    request.signal.addEventListener('abort', onClientAbort, { once: true });
    let resp;
    let streamHandedOff = false;
    try {
      await ensureInitialized(acct.api_key, env, abortController.signal);
      resp = await forwardToCC(env, ccBody, acct.api_key, request, abortController.signal, promptCacheKey);
      if (!resp.ok) {
        let errText = '';
        try { errText = await readLimitedText(resp, MAX_UPSTREAM_JSON_BYTES, abortController.signal); }
        catch { lastFailure = new UpstreamFailure('retry', transportError('upstream error response is too large')); continue; }
        const mapped = mapCcError(resp.status, errText);
        const failure = classifyHttpFailure(resp.status, mapped, extractRetryAfterSec(resp.status, errText, resp.headers));
        if (failure.kind === 'request') return mappedErrorResponse(kind, mapped);
        await applyFailure(pool, acct, failure);
        lastFailure = failure;
        continue;
      }
      if (stream) {
        const streamAttempt = startStreamAttempt(kind, resp, meta, cfg, abortController, async (usage) => {
          const persist = pool.recordSuccess(acct.id, usage).catch((e) => {
            console.error(JSON.stringify({ message: 'failed to record request usage', error: e instanceof Error ? e.message : String(e) }));
          });
          if (ctx && ctx.waitUntil) ctx.waitUntil(persist);
        });
        const outcome = await streamAttempt.outcome;
        if (outcome.started) {
          streamHandedOff = true;
          const cleanup = () => request.signal.removeEventListener('abort', onClientAbort);
          const done = outcome.done.then(cleanup, cleanup);
          if (ctx && ctx.waitUntil) ctx.waitUntil(done);
          else void done;
          return new Response(streamAttempt.readable, { status: 200, headers: SSE_HEADERS });
        }
        const failure = outcome.failure;
        if (failure.kind === 'request') return mappedErrorResponse(kind, failure.mapped);
        await applyFailure(pool, acct, failure);
        lastFailure = failure;
        continue;
      }
      const attempt = await runCollectAttempt(kind, resp, cfg);
      if (attempt.failure) {
        const failure = attempt.failure;
        if (failure.kind === 'request') return mappedErrorResponse(kind, failure.mapped);
        await applyFailure(pool, acct, failure);
        lastFailure = failure;
        continue;
      }
      const usage = collectedUsageOpenAI(attempt.collected.usage);
      const record = pool.recordSuccess(acct.id, usage).catch((e) => {
        console.error(JSON.stringify({ message: 'failed to record request usage', error: e instanceof Error ? e.message : String(e) }));
      });
      if (ctx && ctx.waitUntil) ctx.waitUntil(record);
      else await record;
      return json(await buildNonStreamResponse(kind, meta, attempt.collected));
    } catch (e) {
      if (e && e.name === 'AbortError') {
        if (request.signal.aborted || abortController.signal.aborted) throw e;
        continue;
      }
      lastFailure = new UpstreamFailure('retry', transportError((e && e.message) || String(e)));
      continue;
    } finally {
      if (!streamHandedOff) request.signal.removeEventListener('abort', onClientAbort);
    }
  }

  if (lastFailure && lastFailure.mapped) return mappedErrorResponse(kind, lastFailure.mapped);

  // 全部可用账号都在冷却
  const earliest = await pool.earliestRateLimitedUntil();
  const waitSec = earliest > 0 ? Math.ceil((earliest - Date.now()) / 1000) : 0;
  let message = 'all Command Code accounts are rate limited';
  if (waitSec > 0) message += `; next account available in ${waitSec}s`;
  return kindError(kind, 429, 'rate_limit_error', message, waitSec || undefined);
}

// ============================ §O 模型目录 ============================

// 动态拉取失败时的兜底目录（对齐 proxy.mjs 硬编码列表）
const MODELS = [
  { id: 'claude-sonnet-4-6', name: 'Claude Sonnet 4.6' },
  { id: 'claude-opus-4-8', name: 'Claude Opus 4.8' },
  { id: 'claude-opus-4-7', name: 'Claude Opus 4.7' },
  { id: 'claude-haiku-4-5-20251001', name: 'Claude Haiku 4.5' },
  { id: 'gpt-5.5', name: 'GPT-5.5' },
  { id: 'gpt-5.4', name: 'GPT-5.4' },
  { id: 'gpt-5.4-mini', name: 'GPT-5.4 Mini' },
  { id: 'gpt-5.3-codex', name: 'GPT-5.3 Codex' },
  { id: 'deepseek/deepseek-v4-pro', name: 'DeepSeek V4 Pro' },
  { id: 'deepseek/deepseek-v4-flash', name: 'DeepSeek V4 Flash' },
  { id: 'moonshotai/Kimi-K2.6', name: 'Kimi K2.6' },
  { id: 'moonshotai/Kimi-K2.5', name: 'Kimi K2.5' },
  { id: 'zai-org/GLM-5.1', name: 'GLM 5.1' },
  { id: 'zai-org/GLM-5', name: 'GLM 5' },
  { id: 'MiniMaxAI/MiniMax-M3', name: 'MiniMax M3' },
  { id: 'MiniMaxAI/MiniMax-M2.7', name: 'MiniMax M2.7' },
  { id: 'MiniMaxAI/MiniMax-M2.5', name: 'MiniMax M2.5' },
  { id: 'Qwen/Qwen3.6-Max-Preview', name: 'Qwen 3.6 Max Preview' },
  { id: 'Qwen/Qwen3.6-Plus', name: 'Qwen 3.6 Plus' },
  { id: 'Qwen/Qwen3.7-Max', name: 'Qwen 3.7 Max' },
  { id: 'stepfun/Step-3.7-Flash', name: 'Step 3.7 Flash' },
  { id: 'stepfun/Step-3.5-Flash', name: 'Step 3.5 Flash' },
  { id: 'xiaomi/mimo-v2.5-pro', name: 'MiMo V2.5 Pro' },
  { id: 'xiaomi/mimo-v2.5', name: 'MiMo V2.5' },
  { id: 'google/gemini-3.5-flash', name: 'Gemini 3.5 Flash' },
  { id: 'google/gemini-3.1-flash-lite', name: 'Gemini 3.1 Flash Lite' },
];

const modelCatalogCache = new IsolateCache(MAX_MODEL_CACHE_ENTRIES, MODEL_CACHE_TTL_MS);
const modelCatalogFlights = new Map();
function toModelEntry(m) {
  return {
    id: m.id,
    object: 'model',
    created: 1700000000,
    owned_by: 'commandcode',
    context_window: m.context_length,
  };
}

async function fetchModels(base, apiKey, signal, timeoutMs = DEFAULT_CONTROL_PLANE_TIMEOUT_MS) {
  return upstreamFetch(base + '/provider/v1/models', {
    headers: {
      'Authorization': 'Bearer ' + apiKey,
      'x-cli-environment': 'production',
      'x-command-code-version': CC_PROTOCOL_VERSION,
    },
  }, {
    signal,
    timeoutMs,
    consume: async (response, bodySignal) => {
      const text = await readLimitedText(response, MAX_UPSTREAM_JSON_BYTES, bodySignal);
      if (!response.ok) {
        const error = new Error('models: HTTP ' + response.status);
        error.status = response.status;
        error.body = text.slice(0, 300);
        throw error;
      }
      let list;
      try { list = JSON.parse(text); }
      catch {
        const error = new Error('models: invalid JSON response');
        error.status = 502;
        error.body = text.slice(0, 200);
        throw error;
      }
      const data = Array.isArray(list?.data) ? list.data : [];
      return data.map((m) => ({
        id: m.id,
        object: 'model',
        created: 1700000000,
        owned_by: 'commandcode',
        context_window: m.context_length,
      }));
    },
  });
}

async function getModels(env, base, signal) {
  const cfg = getConfig(env);
  const pool = getPool(env);
  const keys = await pool.availableKeys();
  for (const key of keys) {
    const flightKey = structuredScopeKey(base, key);
    const active = modelCatalogFlights.get(flightKey);
    if (active) {
      try { return await waitForSharedFlight(active, signal); } catch (e) {
        if (signal?.aborted || e?.name === 'AbortError') throw e;
        continue;
      }
    }
    const promise = (async () => {
      const sharedController = new AbortController();
      const timer = setTimeout(() => sharedController.abort(controlPlaneTimeoutError(`${base}/models`)), cfg.controlPlaneTimeoutMs);
      try {
        const cacheKey = await digestCacheKey('models', structuredScopeKey(base, key));
        const cached = modelCatalogCache.get(cacheKey);
        if (cached) return cached;
        const data = await fetchModels(base, key, sharedController.signal, cfg.controlPlaneTimeoutMs);
        modelCatalogCache.set(cacheKey, data);
        return data;
      } finally {
        clearTimeout(timer);
        if (!sharedController.signal.aborted) sharedController.abort();
      }
    })();
    modelCatalogFlights.set(flightKey, promise);
    promise.finally(() => { if (modelCatalogFlights.get(flightKey) === promise) modelCatalogFlights.delete(flightKey); }).catch(() => {});
    try { return await waitForSharedFlight(promise, signal); } catch (e) {
      if (signal?.aborted || e?.name === 'AbortError') throw e;
      /* try the next enabled credential */
    }
  }
  return MODELS.map(toModelEntry);
}



// ============================ §P 鉴权与 admin ============================

function isAnonymousAllowed(env) {
  return String(env.ALLOW_ANONYMOUS || '').toLowerCase() === 'true';
}

function configuredClientKeys(env) {
  const keys = String(env.API_KEYS || '').split(',').map((s) => s.trim()).filter(Boolean);
  return keys;
}

function clientAuthConfigured(env) {
  return configuredClientKeys(env).length > 0;
}

async function checkClientAuth(request, env) {
  const keys = configuredClientKeys(env);
  if (!keys.length) return isAnonymousAllowed(env);
  // Authorization: Bearer <key> 或 x-api-key <key>（Anthropic SDK 风格）
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

async function buildPoolState(env, pool, accountList) {
  const strategy = pickClientStrategy(env);
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

// 验证并入库一把 Command Code 密钥（手动粘贴与浏览器登录共用）。
// 失败抛出带 status 的 Error。
async function provisionAccount(env, key, label, signal) {
  const cfg = getConfig(env);
  const pool = getPool(env);
  if (!/^[\x21-\x7e]+$/.test(key)) {
    const e = new Error('密钥格式不对：检测到中文或全角字符。密钥应是纯 ASCII 字符串，请重新复制粘贴。');
    e.status = 400;
    throw e;
  }
  // 先注册设备指纹，再验证额度，最后由池以一个原子操作入库。
  try { await ensureInitialized(key, env, signal); } catch (e) {
    if (e?.name === 'AbortError' && signal?.aborted) throw e;
    console.warn(JSON.stringify({ message: 'initialization best-effort failed', error: e instanceof Error ? e.message : String(e) }));
  }
  let report;
  try { report = await fetchReport(cfg.apiBase, key, signal, cfg.controlPlaneTimeoutMs); }
  catch (e) {
    const err = new Error(e.message || '验证失败');
    err.status = e.status === 401 || e.status === 403 ? 400 : (e.status || 502);
    throw err;
  }
  return pool.addWithQuota(key, label || report.account?.userName || maskKey(key), report);
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
    try { id = await provisionAccount(env, key, label, request.signal); }
    catch (e) { return json({ error: e.message || '验证失败' }, e.status || 502); }
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
      const deadline = new AbortController();
      const onClientAbort = () => deadline.abort(request.signal.reason);
      const timeoutMs = getConfig(env).controlPlaneTimeoutMs * 4;
      const timer = setTimeout(() => deadline.abort(controlPlaneTimeoutError(`${base}/refresh/${a.id}`)), timeoutMs);
      request.signal.addEventListener('abort', onClientAbort, { once: true });
      try {
        const apiKey = await pool.getCredential(a.id);
        if (!apiKey) return { ...a, lastError: 'account not found' };
        try { await ensureInitialized(apiKey, env, deadline.signal); } catch (e) {
          if (request.signal.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError') throw e;
          console.warn(JSON.stringify({ message: 'refresh initialization failed', accountId: a.id, error: e instanceof Error ? e.message : String(e) }));
        }
        const report = await fetchReport(base, apiKey, deadline.signal, getConfig(env).controlPlaneTimeoutMs);
        await pool.saveQuota(a.id, report);
        return (await pool.list()).find((x) => x.id === a.id) || { ...a, lastError: '' };
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError' || e?.name === 'TimeoutError' || deadline.signal.aborted) throw e;
        await pool.markQuotaError(a.id, e.message || String(e));
        return (await pool.list()).find((x) => x.id === a.id) || { ...a, lastError: e.message || String(e) };
      } finally {
        clearTimeout(timer);
        request.signal.removeEventListener('abort', onClientAbort);
        deadline.abort();
      }
    };
    // 子请求数限制：最多并发刷 12 个
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

// ============================ §Q 路由 ============================

function serviceDescriptor() {
  return json({
    service: 'commandcode-pool',
    endpoints: ['/v1/chat/completions', '/v1/messages', '/v1/responses', '/v1/models', '/health', '/admin', '/api/accounts', '/api/state', '/api/refresh', '/api/usage/daily'],
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const base = getConfig(env).apiBase;
    const needsPool = path.startsWith('/v1/') || path === '/api/accounts' || path.startsWith('/api/accounts/') || path === '/api/refresh' || path === '/api/state' || path === '/api/usage/daily' || path === '/v1/models';

    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: JSON_HEADERS });

    maybeCheckProtocolDrift();

    // ACCOUNTS 环境变量作为 D1 号池的引导：首次（每 isolate 一次）自动入库

    // 健康检查（无需鉴权）
    if (path === '/health') return json({ status: 'ok' });

    // ---------- /v1/* ----------
    if (path === '/v1/chat/completions' || path === '/v1/messages' || path === '/v1/responses') {
      if (request.method !== 'POST') return json({ error: 'method not allowed' }, 405);
      if (!clientAuthConfigured(env) && !isAnonymousAllowed(env)) {
        return errorResponse(503, 'server_error', 'client authentication is not configured', 'auth_not_configured');
      }
      if (!(await checkClientAuth(request, env))) return errorResponse(401, 'authentication_error', 'invalid api key');
      if (needsPool && env.DB && env.ACCOUNTS) await getPool(env).seedIfEmpty(parseAccountsEnv(env.ACCOUNTS));
      const kind = path === '/v1/chat/completions' ? 'chat' : (path === '/v1/messages' ? 'messages' : 'responses');
      try {
        return await handleGenerate(request, env, ctx, kind);
      } catch (e) {
        if (request.signal.aborted || e?.name === 'AbortError') throw e;
        console.error(JSON.stringify({ message: 'generate request failed', error: e instanceof Error ? e.message : String(e), kind }));
        return errorResponse(500, 'server_error', 'internal server error', 'internal_error');
      }
    }

    if (path === '/v1/models' && request.method === 'GET') {
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
        return json({ error: e.status && e.status >= 400 && e.status < 600 ? (e.message || 'request failed') : 'internal server error' }, e.status && e.status >= 400 && e.status < 600 ? e.status : 500);
      }
    }

    // ---------- 静态页面（wrangler [assets] 托管 public/） ----------
    // /admin 无对应静态文件名 → 由 Worker 改写为 /admin.html 交给 assets。
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

// ============================ §R 测试导出 ============================
// 注意：Workers 入口模块的具名导出只能是函数（workerd 校验），常量请走 wireConstants()。
export {
  buildCcRequest, convertAnthropicToOpenAI, convertResponsesToChat,
  buildAnthropicResponse, buildResponsesObject, buildResponsesUsage,
  createSseTranslator, createAnthropicSseTranslator, createResponsesSseTranslator,
  mapCcError, mapCcEventError, mapFinishReason, incompleteUpstreamDetail,
  normalizeUsage, anthropicInputTokens,
  generateFingerprint, getFingerprint, sessionForKey, ensureInitialized, forwardToCC,
  wireConstants,
  parseAccountsEnv, D1Pool, MemPool, getPool,
  fetchReport, planInfo, maskKey, safeEqual, rowToAccount,
  collectCcStream, parseSSE, getConfig,
};

function wireConstants() {
  return { CC_PROTOCOL_VERSION, FP_SALT, MODELS, TOOL_NAME_ALIASES };
}

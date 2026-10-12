// lib/constants.js
// Command Code 协议常量、已知套餐定义、配置缺省值

export const DEFAULT_BASE = 'https://api.commandcode.ai';
export const STUDIO_BASE_URL = 'https://commandcode.ai';

// 对齐 command-code@1.53.1 CLI wire 协议版本
export const CC_PROTOCOL_VERSION = '1.53.1';

export const DEFAULT_MAX_TOKENS = 64000;
export const MAX_MAX_TOKENS = 200000;
export const DEFAULT_MODEL = 'deepseek/deepseek-v4-flash';
export const DEFAULT_429_COOLDOWN_S = 60;
export const AUTH_403_COOLDOWN_S = 600;
export const ZERO_OUTPUT_COOLDOWN_S = 10;
export const MODEL_CACHE_TTL_MS = 10 * 60 * 1000;
export const MAX_REQUEST_BODY_BYTES = 10 * 1024 * 1024;
export const MAX_UPSTREAM_JSON_BYTES = 2 * 1024 * 1024;
export const MAX_SSE_EVENT_CHARS = 2 * 1024 * 1024;

export const DEFAULT_STREAM_IDLE_MS = 30000;
export const DEFAULT_NONSTREAM_IDLE_MS = 90000;
export const STREAM_IDLE_TIMEOUT = Symbol('STREAM_IDLE_TIMEOUT');

export const DEFAULT_TZ_OFFSET = 8;
export const USAGE_BUCKET_SECONDS = 5 * 60;

export const USAGE_RANGES = {
  '5m':  { key: '5m',  label: '最近 5 分钟', seconds: 5 * 60, intervalSeconds: 60 },
  '1h':  { key: '1h',  label: '最近 1 小时', seconds: 60 * 60, intervalSeconds: 5 * 60 },
  '5h':  { key: '5h',  label: '最近 5 小时', seconds: 5 * 60 * 60, intervalSeconds: 15 * 60 },
  '1d':  { key: '1d',  label: '最近 24 小时', seconds: 24 * 60 * 60, intervalSeconds: 60 * 60 },
  '3d':  { key: '3d',  label: '最近 3 天', seconds: 3 * 24 * 60 * 60, intervalSeconds: 3 * 60 * 60 },
  '7d':  { key: '7d',  label: '最近 7 天', seconds: 7 * 24 * 60 * 60, intervalSeconds: 6 * 60 * 60 },
};

export const MAX_USAGE_RANGE_SECONDS = Math.max(...Object.values(USAGE_RANGES).map(range => range.seconds));
// 保留最大查询范围加一天余量，避免过期边界缺少用量桶。
export const USAGE_RETENTION_SECONDS = MAX_USAGE_RANGE_SECONDS + 86400;

// 已知套餐的月度信用额映射（来自官方 CLI 反编译及社区验证）
export const KNOWN_PLANS = {
  'individual-go':       { name: 'Go',         monthlyCredits: 10,  isGoPlan: true },
  'individual-goat':     { name: 'GOAT',       monthlyCredits: 70,  isGoPlan: false },
  'individual-pro':      { name: 'Pro',        monthlyCredits: 30,  isGoPlan: false },
  'individual-pro-v1':   { name: 'Pro',        monthlyCredits: 80,  isGoPlan: false },
  'individual-provider': { name: 'Provider',   monthlyCredits: 15,  isGoPlan: false },
  'individual-max':      { name: 'Max',        monthlyCredits: 150, isGoPlan: false },
  'individual-ultra':    { name: 'Ultra',      monthlyCredits: 300, isGoPlan: false },
  'teams-pro':           { name: 'Teams Pro',  monthlyCredits: 40,  isGoPlan: false },
};

// 按前缀长度降序排，保证 individual-pro-v1 优先于 individual-pro 匹配
export const PLAN_PREFIXES = Object.keys(KNOWN_PLANS).sort((a, b) => b.length - a.length);

export function planInfo(planId) {
  if (!planId) return undefined;
  const norm = String(planId).toLowerCase().replace(/_/g, '-');
  const prefix = PLAN_PREFIXES.find(p => norm.startsWith(p));
  return prefix ? KNOWN_PLANS[prefix] : undefined;
}

// 兜底模型目录
export const BUILTIN_MODELS = [
  { id: 'deepseek/deepseek-v4-flash', object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'deepseek/deepseek-v4',       object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'anthropic/claude-3-7-sonnet', object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'anthropic/claude-3-5-sonnet', object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'anthropic/claude-3-5-haiku',  object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'openai/gpt-4o',              object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'openai/gpt-4o-mini',         object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'openai/o1',                  object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'openai/o3-mini',             object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'google/gemini-2.0-flash',    object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
  { id: 'google/gemini-2.5-pro',      object: 'model', created: 1735689600, owned_by: 'commandcode', permission: [] },
];

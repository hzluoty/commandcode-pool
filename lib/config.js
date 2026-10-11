// lib/config.js
import { DEFAULT_BASE, DEFAULT_TZ_OFFSET } from './constants.js';

export function getConfig(env = process.env) {
  const defaults = {
    port: parseInt(env.PORT || '3000', 10),
    host: env.HOST || '0.0.0.0',
    apiBase: (env.CC_API_BASE || env.API_BASE || DEFAULT_BASE).replace(/\/+$/, ''),
    projectSlug: env.PROJECT_SLUG || 'cc-pool',
    adminToken: env.ADMIN_TOKEN || '',
    apiKeys: (env.API_KEYS || '').split(',').map(s => s.trim()).filter(Boolean),
    accounts: env.ACCOUNTS || '',
    poolStrategy: (env.POOL_STRATEGY || 'sticky').toLowerCase() === 'round_robin' ? 'round_robin' : 'sticky',
    zdr: env.CMD_ZDR === '1' || env.COMMANDCODE_ZDR === '1',
    fingerprintSalt: env.FINGERPRINT_SALT || env.CC_FINGERPRINT_SALT || '',
    deviceProjectDir: env.DEVICE_PROJECT_DIR || env.CC_DEVICE_PROJECT_DIR || 'C:\\Users\\dev\\projects\\app',
    emptySystemPlaceholder: env.EMPTY_SYSTEM_PLACEHOLDER !== 'false' && env.CC_EMPTY_SYSTEM_PLACEHOLDER !== 'false',
    tzOffset: Number.isFinite(Number(env.TIMEZONE_OFFSET)) ? Number(env.TIMEZONE_OFFSET) : DEFAULT_TZ_OFFSET,
    // Vercel KV / Upstash Redis
    kvUrl: env.KV_REST_API_URL || env.UPSTASH_REDIS_REST_URL || '',
    kvToken: env.KV_REST_API_TOKEN || env.UPSTASH_REDIS_REST_TOKEN || '',
    // Postgres / Database URL
    postgresUrl: env.POSTGRES_URL || env.DATABASE_URL || '',
  };
  return defaults;
}

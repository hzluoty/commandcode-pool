// lib/failure-policy.js
// 统一错误映射与账号健康策略（对齐 proxy.mjs 与 failure-policy 规范）
import { DEFAULT_429_COOLDOWN_S, AUTH_403_COOLDOWN_S, ZERO_OUTPUT_COOLDOWN_S } from './constants.js';

export function parseRetryAfter(headers) {
  if (!headers) return null;
  const val = typeof headers.get === 'function' ? headers.get('retry-after') : headers['retry-after'];
  if (!val) return null;
  const s = parseInt(val, 10);
  if (Number.isFinite(s) && s > 0) return s;
  const d = Date.parse(val);
  if (!Number.isNaN(d)) {
    const diff = Math.ceil((d - Date.now()) / 1000);
    return diff > 0 ? diff : null;
  }
  return null;
}

export function evaluateAccountAction(status, bodyText = '', headers = null) {
  const retryAfter = parseRetryAfter(headers);

  if (status === 401) {
    return { action: 'disable', reason: 'invalid_key', message: 'API Key 失效 (401)' };
  }

  if (status === 429) {
    const cooldownSec = retryAfter || DEFAULT_429_COOLDOWN_S;
    return { action: 'cooldown', seconds: cooldownSec, message: `速率受限 (429), 冷却 ${cooldownSec} 秒` };
  }

  if (status === 402) {
    return { action: 'cooldown', seconds: 3600, message: '额度已用尽 (402), 冷却 1 小时' };
  }

  if (status === 403) {
    return { action: 'cooldown', seconds: AUTH_403_COOLDOWN_S, message: '无权访问或套餐受限 (403)' };
  }

  if (status >= 500 || status === 0) {
    return { action: 'cooldown', seconds: 15, message: `上游服务错误 (${status})` };
  }

  return { action: 'none' };
}

export function mapClientError(kind, status, message, type = 'api_error', retryAfter = null) {
  const headers = {};
  if (retryAfter) headers['Retry-After'] = String(Math.max(1, Math.ceil(retryAfter)));

  let body;
  if (kind === 'messages') {
    body = {
      type: 'error',
      error: { type: type === 'invalid_request_error' ? 'invalid_request_error' : 'api_error', message },
    };
    if (retryAfter) body.retry_after = retryAfter;
  } else if (kind === 'responses') {
    body = {
      error: {
        message,
        type,
        code: status === 429 ? 'rate_limit_exceeded' : (status === 401 ? 'invalid_api_key' : null),
        param: null,
      },
    };
    if (retryAfter) body.retry_after = retryAfter;
  } else {
    // OpenAI Chat 格式
    body = {
      error: {
        message,
        type,
        param: null,
        code: status === 429 ? 'rate_limit_exceeded' : (status === 401 ? 'invalid_api_key' : null),
      },
    };
    if (retryAfter) body.error.retry_after = retryAfter;
  }

  return { status, body, headers };
}

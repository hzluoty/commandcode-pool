// lib/crypto-utils.js
import crypto from 'node:crypto';
import { USAGE_BUCKET_SECONDS } from './constants.js';

export function isRecord(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

export function num(v) {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

export function str(v) {
  return typeof v === 'string' ? v : null;
}

export function safeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

export function generateTraceparent() {
  const version = '00';
  const traceId = crypto.randomBytes(16).toString('hex');
  const parentId = crypto.randomBytes(8).toString('hex');
  const traceFlags = '01';
  return `${version}-${traceId}-${parentId}-${traceFlags}`;
}

// 去掉终端粘贴包装(ESC[200~ / ESC[201~)、控制字符与首尾空白，参照 pi-commandcode-provider 的 sanitizeApiKey
export function sanitizeApiKey(input) {
  return String(input ?? '')
    .replace(/\u001b?\[20[01]~/g, '')
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .trim();
}

export function maskKey(k) {
  if (!k || typeof k !== 'string') return '';
  if (k.length <= 10) return k.slice(0, 3) + '…';
  return k.slice(0, 6) + '…' + k.slice(-4);
}

export function slugifyProjectPath(p) {
  if (!p) return 'cc-proxy';
  const norm = p.replace(/\\/g, '/').replace(/\/+$/, '');
  const base = norm.split('/').filter(Boolean).pop() || 'project';
  const slug = base.toLowerCase().replace(/[^a-z0-9_-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  return slug || 'cc-proxy';
}

export function toEpochMs(v) {
  if (typeof v === 'number' && Number.isFinite(v)) {
    return v < 1e12 ? v * 1000 : v;
  }
  if (typeof v === 'string' && v) {
    const t = Date.parse(v);
    if (!Number.isNaN(t)) return t;
  }
  return null;
}

export function usageBucketStart(tsMs, tzOffsetHours = 8) {
  const sec = Math.floor(tsMs / 1000);
  const tzShift = tzOffsetHours * 3600;
  const localSec = sec + tzShift;
  const bucketLocal = Math.floor(localSec / USAGE_BUCKET_SECONDS) * USAGE_BUCKET_SECONDS;
  return bucketLocal - tzShift;
}

export const JSON_HEADERS = {
  'Content-Type': 'application/json; charset=utf-8',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Admin-Token, X-Api-Key, x-cmd-zdr',
};

export function json(obj, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...JSON_HEADERS, ...extraHeaders },
  });
}

export async function readBody(request) {
  try {
    const text = await request.text();
    if (!text) return null;
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
}

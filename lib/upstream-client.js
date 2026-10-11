// lib/upstream-client.js
// 上游调用传输层（统一 9 键信封封装、确定性指纹、会话管理及 Go 套餐透明兼容）
import { CC_PROTOCOL_VERSION } from './constants.js';
import { slugifyProjectPath, generateTraceparent } from './crypto-utils.js';
import { ensureInitialized, sessionForKey } from './fingerprint.js';

export async function forwardToCC(body, apiKey, config, signal = null, promptCacheKey = null, clientHeaders = null) {
  const apiBase = config.apiBase;
  const url = `${apiBase}/alpha/generate`;

  // 确保该 key 确定性派生的硬件指纹与初始生命周期已注册
  await ensureInitialized(apiKey, config);

  const sessionId = sessionForKey(apiKey, config);

  // CLI 的 toWireThreadId 校验与 9 键有序重排
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(sessionId))) {
    const ordered = {};
    for (const k of ['config', 'memory', 'taste', 'skills', 'permissionMode']) ordered[k] = body[k];
    ordered.threadId = sessionId;
    for (const k of ['mode', 'promptCache', 'params']) if (k in body) ordered[k] = body[k];
    body = ordered;
  }

  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'cli',
    'x-command-code-version': CC_PROTOCOL_VERSION,
    'x-cli-environment': 'production',
    'x-project-slug': slugifyProjectPath(config.deviceProjectDir),
    'x-taste-learning': 'false',
    'x-session-id': sessionId,
    'Authorization': `Bearer ${apiKey}`,
    'traceparent': generateTraceparent(),
  };

  const clientZdr = clientHeaders && (typeof clientHeaders.get === 'function' ? clientHeaders.get('x-cmd-zdr') : clientHeaders['x-cmd-zdr']);
  if (config.zdr || clientZdr === '1') {
    headers['x-cmd-zdr'] = '1';
  }

  const fetchOptions = {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  };
  if (signal) fetchOptions.signal = signal;

  return fetch(url, fetchOptions);
}

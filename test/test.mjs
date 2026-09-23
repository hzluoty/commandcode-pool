// commandcode-pool local test suite: unit + Provider-native mock E2E
// Run: node test/test.mjs

import worker, {
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
} from '../worker.js';

import { prepareGenerationRequest } from '../lib/api-adapters.js';
import { createProviderFailurePolicy, ClientInputError } from '../lib/failure-policy.js';
import { sendGeneration, providerUrl, PROVIDER_GENERATION_PATHS } from '../lib/upstream-client.js';
import { runSingleAccountGeneration, createSseObserverTransform } from '../lib/generation-runner.js';
import { selectSingleAccount, persistAccountHealth, unavailableAccountResponse } from '../lib/pool-store.js';
import { planInfo, normalizeWindow, buildDetail } from '../lib/quota-client.js';

let passed = 0;
let failed = 0;
const failures = [];

function assert(cond, name, extra) {
  if (cond) {
    passed++;
  } else {
    failed++;
    failures.push(name + (extra !== undefined ? ` :: ${JSON.stringify(extra)}` : ''));
    console.error(`  FAIL: ${name}`, extra !== undefined ? extra : '');
  }
}

function assertEq(actual, expected, name) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  assert(ok, name, ok ? undefined : { actual, expected });
}

const enc = new TextEncoder();

function sseResponse(events, status = 200, headers = {}) {
  const chunks = events.map((e) => (typeof e === 'string' ? e : `data: ${JSON.stringify(e)}\n\n`));
  return new Response(
    new ReadableStream({
      start(c) {
        for (const ch of chunks) c.enqueue(enc.encode(ch));
        c.close();
      },
    }),
    { status, headers: { 'content-type': 'text/event-stream', ...headers } },
  );
}

function jsonResponse(obj, status = 200, headers = {}) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', ...headers },
  });
}

// ============================ Mock Provider 上游 ============================

function installProviderMock({ behavior = {} } = {}) {
  const calls = [];
  const realFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts = {}) => {
    const urlStr = String(url instanceof URL ? url : url);
    const headers = opts.headers || {};
    const auth = headers.Authorization || headers.authorization || '';
    const apiKey = String(auth).replace(/^Bearer\s+/i, '');
    calls.push({
      url: urlStr,
      apiKey,
      method: opts.method || 'GET',
      body: opts.body,
      headers,
      signal: opts.signal,
    });

    const b = behavior[apiKey] || 'ok';

    // Model discovery endpoint
    if (urlStr.endsWith('/provider/v1/models')) {
      if (b === 'badkey') {
        return jsonResponse({ error: { message: 'invalid api key', type: 'authentication_error' } }, 401);
      }
      if (b === 'limited') {
        return jsonResponse({ error: { message: 'rate limited', type: 'rate_limit_error' } }, 429, { 'Retry-After': '300' });
      }
      return jsonResponse({
        data: [
          { id: 'deepseek/deepseek-v4-flash', object: 'model' },
          { id: 'claude-sonnet-4-6', object: 'model' },
        ],
      });
    }

    // Quota endpoints
    if (urlStr.includes('/alpha/')) {
      const ab = behavior[`alpha:${apiKey}`] || b;
      if (ab === 'badkey') {
        return jsonResponse({ error: { message: 'unauthorized', type: 'authentication_error' } }, 401);
      }
      if (ab === 'limited') {
        return jsonResponse({ error: { message: 'rate limited', type: 'rate_limit_error' } }, 429, { 'Retry-After': '120' });
      }
      if (ab === 'server_error') {
        return jsonResponse({ error: { message: 'upstream error', type: 'server_error' } }, 500);
      }
      if (urlStr.endsWith('/alpha/whoami')) {
        return jsonResponse({ user: { id: 'u1', name: 'User 1', userName: 'tester' } });
      }
      if (urlStr.endsWith('/alpha/billing/credits')) {
        return jsonResponse({
          credits: { monthlyCredits: 50, purchasedCredits: 5, freeCredits: 1 },
          windowLimits: { fiveHour: { used: 1, cap: 5 }, weekly: { used: 5, cap: 30 } },
        });
      }
      if (urlStr.includes('/alpha/billing/subscriptions')) {
        return jsonResponse({ data: { planId: 'individual-goat', status: 'active', currentPeriodEnd: 1800000000000 } });
      }
      if (urlStr.endsWith('/alpha/usage/summary')) {
        return jsonResponse({ data: { totalCount: 10, totalCost: 1.2 } });
      }
    }

    // Generation endpoints
    if (
      urlStr.endsWith('/provider/v1/chat/completions') ||
      urlStr.endsWith('/provider/v1/messages') ||
      urlStr.endsWith('/provider/v1/responses')
    ) {
      if (b === 'limited') {
        return jsonResponse({ error: { message: 'rate limited', type: 'rate_limit_error' } }, 429, { 'Retry-After': '3600' });
      }
      if (b === 'badkey') {
        return jsonResponse({ error: { message: 'invalid api key', type: 'authentication_error' } }, 401);
      }
      if (b === 'forbidden') {
        return jsonResponse({ error: { message: 'forbidden', type: 'permission_error' } }, 403);
      }
      if (b === 'server_error') {
        return jsonResponse({ error: { message: 'internal error', type: 'server_error' } }, 500);
      }
      if (b === 'exhausted') {
        return jsonResponse({ error: { message: 'quota exhausted', type: 'rate_limit_error', code: 'USAGE_EXCEEDED' } }, 402);
      }
      if (b === 'hang') {
        return new Response(
          new ReadableStream({
            start(controller) {
              if (opts.signal) {
                opts.signal.addEventListener('abort', () => controller.error(new Error('aborted')));
              }
            },
          }),
          { status: 200, headers: { 'content-type': 'text/event-stream' } },
        );
      }

      const body = opts.body ? JSON.parse(opts.body) : {};
      const stream = body.stream === true;

      if (urlStr.endsWith('/provider/v1/chat/completions')) {
        if (stream) {
          if (b === 'partial') {
            return sseResponse([
              'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"part"}}]}\n\n',
            ]);
          }
          if (b === 'streamerr') {
            return sseResponse([
              'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"before"}}]}\n\n',
              'data: {"error":{"message":"mid-stream chat error","type":"server_error"}}\n\n',
            ]);
          }
          return sseResponse([
            'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}},{"finish_reason":null}]}\n\n',
            'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":4}}}\n\n',
            'data: [DONE]\n\n',
          ]);
        }
        return jsonResponse({
          id: 'c1',
          object: 'chat.completion',
          choices: [{ index: 0, message: { role: 'assistant', content: 'Hello world' }, finish_reason: 'stop' }],
          usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15, prompt_tokens_details: { cached_tokens: 4 } },
        });
      }

      if (urlStr.endsWith('/provider/v1/messages')) {
        if (stream) {
          if (b === 'partial') {
            return sseResponse([
              'event: message_start\ndata: {"type":"message_start","message":{"id":"m1","usage":{"input_tokens":10,"output_tokens":0}}}\n\n',
            ]);
          }
          if (b === 'streamerr') {
            return sseResponse([
              'event: error\ndata: {"type":"error","error":{"type":"server_error","message":"stream error"}}\n\n',
            ]);
          }
          return sseResponse([
            'event: message_start\ndata: {"type":"message_start","message":{"id":"m1","usage":{"input_tokens":10,"output_tokens":0}}}\n\n',
            'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}\n\n',
            'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}\n\n',
            'event: message_stop\ndata: {"type":"message_stop"}\n\n',
          ]);
        }
        return jsonResponse({
          id: 'm1',
          type: 'message',
          content: [{ type: 'text', text: 'Hello world' }],
          usage: { input_tokens: 10, output_tokens: 5 },
        });
      }

      if (urlStr.endsWith('/provider/v1/responses')) {
        if (stream) {
          if (b === 'streamerr') {
            return sseResponse([
              'event: response.failed\ndata: {"type":"response.failed","error":{"message":"resp failed"}}\n\n',
            ]);
          }
          if (b === 'partial') {
            return sseResponse([
              'event: response.incomplete\ndata: {"type":"response.incomplete","incomplete_details":{"reason":"max_tokens"}}\n\n',
            ]);
          }
          return sseResponse([
            'event: response.created\ndata: {"type":"response.created","response":{"id":"r1"}}\n\n',
            'event: response.completed\ndata: {"type":"response.completed","response":{"id":"r1","status":"completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}\n\n',
          ]);
        }
        return jsonResponse({
          id: 'r1',
          object: 'response',
          status: 'completed',
          output: [{ type: 'message', content: [{ type: 'text', text: 'Hello world' }] }],
          usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
        });
      }
    }

    return jsonResponse({ error: { message: 'not found', type: 'invalid_request_error' } }, 404);
  };

  return {
    calls,
    restore() {
      globalThis.fetch = realFetch;
    },
  };
}

// ============================ 测试套件 ============================

async function testApiAdapters() {
  console.log('--- testApiAdapters ---');

  // 1. Chat adapter
  const chatReq = prepareGenerationRequest('chat', {
    model: 'deepseek/deepseek-v4-flash',
    messages: [{ role: 'user', content: 'hello' }],
    prompt_cache_key: 'legacy_key',
  });
  assertEq(chatReq.path, '/provider/v1/chat/completions', 'adapter: chat path');
  assertEq(chatReq.body.model, 'deepseek/deepseek-v4-flash', 'adapter: chat model');
  assertEq(chatReq.body.prompt_cache_key, undefined, 'adapter: strips prompt_cache_key');
  assertEq(chatReq.protocol, 'chat', 'adapter: chat protocol');

  // 2. Messages adapter
  const msgReq = prepareGenerationRequest('messages', {
    messages: [{ role: 'user', content: 'hello' }],
  }, { messagesDefaultModel: 'claude-sonnet-4-6', defaultMaxTokens: 4096 });
  assertEq(msgReq.path, '/provider/v1/messages', 'adapter: messages path');
  assertEq(msgReq.body.model, 'claude-sonnet-4-6', 'adapter: default messages model');
  assertEq(msgReq.body.max_tokens, 4096, 'adapter: default max_tokens');

  // 3. Responses adapter
  const respReq = prepareGenerationRequest('responses', {
    input: 'test input',
  });
  assertEq(respReq.path, '/provider/v1/responses', 'adapter: responses path');

  // 4. Validation errors
  let threw = false;
  try {
    prepareGenerationRequest('chat', 'invalid string body');
  } catch (e) {
    threw = e instanceof ClientInputError && e.status === 400;
  }
  assert(threw, 'adapter: non-object body throws 400 ClientInputError');

  threw = false;
  try {
    prepareGenerationRequest('messages', {});
  } catch (e) {
    threw = e instanceof ClientInputError && e.status === 400;
  }
  assert(threw, 'adapter: missing messages throws 400 ClientInputError');

  threw = false;
  try {
    prepareGenerationRequest('responses', {});
  } catch (e) {
    threw = e instanceof ClientInputError && e.status === 400;
  }
  assert(threw, 'adapter: missing input in responses throws 400 ClientInputError');
}

async function testUpstreamClient() {
  console.log('--- testUpstreamClient ---');
  const mock = installProviderMock();

  try {
    const res = await sendGeneration({
      apiBase: 'https://api.commandcode.ai',
      path: '/provider/v1/chat/completions',
      body: { model: 'm', messages: [] },
      headers: { 'x-cmd-zdr': '1', 'x-custom-ignored': 'val' },
      apiKey: 'test-key-1',
    });

    assertEq(res.status, 200, 'upstream: fetch succeeds');
    assertEq(mock.calls.length, 1, 'upstream: single fetch call');
    const call = mock.calls[0];
    assertEq(call.url, 'https://api.commandcode.ai/provider/v1/chat/completions', 'upstream: correct provider url');
    assertEq(call.headers['Authorization'], 'Bearer test-key-1', 'upstream: bearer auth');
    assertEq(call.headers['Content-Type'], 'application/json', 'upstream: json content type');
    assertEq(call.headers['Accept'], 'application/json', 'upstream: non-stream accept');
    assertEq(call.headers['x-cmd-zdr'], '1', 'upstream: passes x-cmd-zdr');
    assertEq(call.headers['x-command-code-version'], undefined, 'upstream: no cli version header');
    assertEq(call.headers['x-cli-environment'], undefined, 'upstream: no cli env header');

    let threw = false;
    try {
      await sendGeneration({
        apiBase: 'https://api.commandcode.ai',
        path: '/alpha/generate',
        body: {},
        apiKey: 'k',
      });
    } catch (e) {
      threw = e instanceof TypeError;
    }
    assert(threw, 'upstream: rejects undocumented /alpha/generate path');
  } finally {
    mock.restore();
  }
}

async function testFailurePolicy() {
  console.log('--- testFailurePolicy ---');
  const policy = createProviderFailurePolicy({ defaultCooldownSec: 60 });

  // 401 -> disable
  const f401 = policy.fromHttp(401, '{"error":{"message":"bad key"}}', new Headers());
  assertEq(f401.kind, 'disable', 'policy: 401 is disable');
  assertEq(f401.persistHealth, true, 'policy: 401 persists health');
  assertEq(f401.mapped.status, 401, 'policy: 401 mapped status');

  // 429 -> cooldown
  const headers429 = new Headers({ 'Retry-After': '120' });
  const f429 = policy.fromHttp(429, '{"error":{"message":"rate limit"}}', headers429);
  assertEq(f429.kind, 'cooldown', 'policy: 429 is cooldown');
  assertEq(f429.persistHealth, true, 'policy: 429 persists health');
  assertEq(f429.cooldownSec, 120, 'policy: 429 reads Retry-After');
  assertEq(f429.mapped.status, 429, 'policy: 429 mapped status');

  // 402 -> cooldown
  const f402 = policy.fromHttp(402, '{"error":{"message":"quota exceeded"}}', headers429);
  assertEq(f402.kind, 'cooldown', 'policy: 402 is cooldown');
  assertEq(f402.persistHealth, true, 'policy: 402 persists health');
  assertEq(f402.mapped.status, 429, 'policy: 402 mapped to 429 for client');

  // 403 -> operational, persistHealth false
  const f403 = policy.fromHttp(403, '{"error":{"message":"forbidden"}}', new Headers());
  assertEq(f403.kind, 'operational', 'policy: 403 is operational');
  assertEq(f403.persistHealth, false, 'policy: 403 does not persist quota error');

  // 500 -> operational, persistHealth false
  const f500 = policy.fromHttp(500, 'boom', new Headers());
  assertEq(f500.kind, 'operational', 'policy: 500 is operational');
  assertEq(f500.persistHealth, false, 'policy: 500 does not persist quota error');
  assertEq(f500.mapped.status, 502, 'policy: 500 mapped to 502 gateway error');

  // 400 -> request, persistHealth false
  const f400 = policy.fromHttp(400, '{"error":{"message":"bad param"}}', new Headers());
  assertEq(f400.kind, 'request', 'policy: 400 is request error');
  assertEq(f400.persistHealth, false, 'policy: 400 does not persist health');

  // Incomplete stream
  const fInc = policy.incomplete('stream closed');
  assertEq(fInc.kind, 'operational', 'policy: incomplete is operational');
  assertEq(fInc.persistHealth, false, 'policy: incomplete does not persist health');
}

async function testSingleAccountRunner() {
  console.log('--- testSingleAccountRunner (R1: 1 request = 1 account = 1 fetch) ---');
  const mock = installProviderMock({ behavior: { 'acc-limited': 'limited', 'acc-ok': 'ok' } });

  try {
    const pool = new MemPool([
      { key: 'acc-limited', label: 'L' },
      { key: 'acc-ok', label: 'OK' },
    ]);
    const policy = createProviderFailurePolicy();

    // First request: acc-limited is picked, returns 429
    const req1 = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi' }] }),
    });

    const res1 = await runSingleAccountGeneration({
      request: req1,
      ctx: { waitUntil() {} },
      kind: 'chat',
      cfg: { apiBase: 'https://api.commandcode.ai' },
      pool,
      strategy: 'sticky',
      prepared: {
        path: '/provider/v1/chat/completions',
        body: { model: 'm', messages: [{ role: 'user', content: 'hi' }] },
        protocol: 'chat',
      },
      requestId: 'req-1',
      sendGeneration,
      readErrorText: async (r) => r.text(),
      policy,
      renderFailure: (k, m) => new Response(JSON.stringify(m.body), { status: m.status }),
    });

    assertEq(res1.status, 429, 'runner: returns 429 directly to client');
    assertEq(mock.calls.length, 1, 'runner: EXACTLY ONE fetch call made');
    assertEq(mock.calls[0].apiKey, 'acc-limited', 'runner: called acc-limited only, NEVER retried acc-ok');

    // Verify acc-limited is on cooldown
    const accList = await pool.list();
    const limitedAcc = accList.find((a) => a.maskedKey.includes('acc-'));
    assert(limitedAcc.pool.rateLimited, 'runner: acc-limited placed on cooldown');

    // Second independent request: now selects acc-ok
    mock.calls.length = 0;
    const req2 = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model: 'm', messages: [{ role: 'user', content: 'hi 2' }] }),
    });

    const res2 = await runSingleAccountGeneration({
      request: req2,
      ctx: { waitUntil() {} },
      kind: 'chat',
      cfg: { apiBase: 'https://api.commandcode.ai' },
      pool,
      strategy: 'sticky',
      prepared: {
        path: '/provider/v1/chat/completions',
        body: { model: 'm', messages: [{ role: 'user', content: 'hi 2' }] },
        protocol: 'chat',
      },
      requestId: 'req-2',
      sendGeneration,
      readErrorText: async (r) => r.text(),
      policy,
      renderFailure: (k, m) => new Response(JSON.stringify(m.body), { status: m.status }),
    });

    assertEq(res2.status, 200, 'runner: next independent request succeeds');
    assertEq(mock.calls.length, 1, 'runner: next request makes exactly one call');
    assertEq(mock.calls[0].apiKey, 'acc-ok', 'runner: called acc-ok');
  } finally {
    mock.restore();
  }
}

async function testStreamObserver() {
  console.log('--- testStreamObserver ---');
  const policy = createProviderFailurePolicy();

  // Test 1: Normal SSE with [DONE]
  async function drain(readable) {
    const reader = readable.getReader();
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }
  }

  // Test 1: Normal SSE with [DONE]
  {
    const { transformStream, outcomePromise } = createSseObserverTransform({ policy });
    const drainPromise = drain(transformStream.readable);
    const writer = transformStream.writable.getWriter();
    await writer.write(enc.encode('data: {"choices":[{"delta":{"content":"Hi"}}]}\n\n'));
    await writer.write(enc.encode('data: {"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}\n\n'));
    await writer.write(enc.encode('data: [DONE]\n\n'));
    await writer.close();
    await drainPromise;

    const outcome = await outcomePromise;
    assertEq(outcome.status, 'success', 'sse: normal stream registers success');
    assertEq(outcome.usage.prompt_tokens, 10, 'sse: extracted prompt tokens');
    assertEq(outcome.usage.completion_tokens, 2, 'sse: extracted completion tokens');
  }

  // Test 2: In-stream failure event
  {
    const { transformStream, outcomePromise } = createSseObserverTransform({ policy });
    const drainPromise = drain(transformStream.readable);
    const writer = transformStream.writable.getWriter();
    await writer.write(enc.encode('data: {"error":{"message":"mid-stream err","type":"server_error"}}\n\n'));
    await writer.close();
    await drainPromise;

    const outcome = await outcomePromise;
    assertEq(outcome.status, 'failure', 'sse: error event registers failure');
  }

  // Test 3: Stream cut off with no terminal
  {
    const { transformStream, outcomePromise } = createSseObserverTransform({ policy });
    const drainPromise = drain(transformStream.readable);
    const writer = transformStream.writable.getWriter();
    await writer.write(enc.encode('data: {"choices":[{"delta":{"content":"Cut off"}}]}\n\n'));
    await writer.close();
    await drainPromise;

    const outcome = await outcomePromise;
    assertEq(outcome.status, 'incomplete', 'sse: cut off stream registers incomplete');
  }
}

async function testWorkerE2E() {
  console.log('--- testWorkerE2E ---');
  const mock = installProviderMock({
    behavior: {
      'key-1': 'ok',
      'key-limited': 'limited',
      'key-bad': 'badkey',
    },
  });

  try {
    const env = {
      API_BASE: 'https://api.commandcode.ai',
      ALLOW_ANONYMOUS: 'true',
      ACCOUNTS: '[{"key":"key-1","label":"primary"}]',
    };

    // Chat completion non-stream
    const chatReq = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'deepseek/deepseek-v4-flash',
        messages: [{ role: 'user', content: 'ping' }],
        stream: false,
      }),
    });
    const chatRes = await worker.fetch(chatReq, env);
    assertEq(chatRes.status, 200, 'worker: chat non-stream 200');
    const chatJson = await chatRes.json();
    assertEq(chatJson.choices[0].message.content, 'Hello world', 'worker: relays upstream json content');

    // Chat completion stream
    const streamReq = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'deepseek/deepseek-v4-flash',
        messages: [{ role: 'user', content: 'ping' }],
        stream: true,
      }),
    });
    const streamRes = await worker.fetch(streamReq, env);
    assertEq(streamRes.status, 200, 'worker: chat stream 200');
    assertEq(streamRes.headers.get('content-type'), 'text/event-stream', 'worker: chat stream content-type');
    const streamText = await streamRes.text();
    assert(streamText.includes('[DONE]'), 'worker: stream relays [DONE] terminal');

    // Messages non-stream
    const msgReq = new Request('https://gateway.test/v1/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        messages: [{ role: 'user', content: 'ping' }],
      }),
    });
    const msgRes = await worker.fetch(msgReq, env);
    assertEq(msgRes.status, 200, 'worker: messages non-stream 200');
    const msgJson = await msgRes.json();
    assertEq(msgJson.content[0].text, 'Hello world', 'worker: relays messages json content');

    // Responses non-stream
    const respReq = new Request('https://gateway.test/v1/responses', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        input: 'ping',
      }),
    });
    const respRes = await worker.fetch(respReq, env);
    assertEq(respRes.status, 200, 'worker: responses non-stream 200');

    // Client abort propagation
    const abortCtrl = new AbortController();
    const abortedReq = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'hi' }] }),
      signal: abortCtrl.signal,
    });
    abortCtrl.abort(new Error('client cancelled'));

    let abortThrew = false;
    try {
      await worker.fetch(abortedReq, env);
    } catch (e) {
      abortThrew = e.name === 'AbortError' || e.message.includes('cancelled');
    }
    assert(abortThrew, 'worker: client abort cleanly propagates AbortError');
  } finally {
    mock.restore();
  }
}

async function testAdminAndPool() {
  console.log('--- testAdminAndPool ---');
  const mock = installProviderMock({
    behavior: {
      'valid-key': 'ok',
      'invalid-key': 'badkey',
    },
  });

  try {
    const env = {
      API_BASE: 'https://api.commandcode.ai',
      ADMIN_TOKEN: 'test-admin-token',
    };

    // 1. Add account with valid key -> calls GET /provider/v1/models -> 201
    const addReq = new Request('https://gateway.test/api/accounts', {
      method: 'POST',
      headers: {
        'x-admin-token': 'test-admin-token',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ key: 'valid-key', label: 'Valid Account' }),
    });
    const addRes = await worker.fetch(addReq, env);
    assertEq(addRes.status, 201, 'admin: add account with valid key returns 201');
    const addData = await addRes.json();
    assert(addData.account && addData.account.id, 'admin: returns created account');

    // Verify mock was called at /provider/v1/models (NOT /alpha/*)
    const modelsCall = mock.calls.find((c) => c.url.includes('/provider/v1/models') && c.apiKey === 'valid-key');
    assert(modelsCall != null, 'admin: validated via GET /provider/v1/models');
    assert(mock.calls.every((c) => !c.url.includes('/alpha/')), 'admin: ZERO /alpha/* calls made');

    // 2. Add account with invalid key (401 from upstream models) -> 400
    const badAddReq = new Request('https://gateway.test/api/accounts', {
      method: 'POST',
      headers: {
        'x-admin-token': 'test-admin-token',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ key: 'invalid-key', label: 'Bad Account' }),
    });
    const badAddRes = await worker.fetch(badAddReq, env);
    assertEq(badAddRes.status, 400, 'admin: add account with invalid key returns 400');

    // 3. List accounts
    const listReq = new Request('https://gateway.test/api/accounts', {
      headers: { 'x-admin-token': 'test-admin-token' },
    });
    const listRes = await worker.fetch(listReq, env);
    assertEq(listRes.status, 200, 'admin: list accounts 200');
    const listData = await listRes.json();
    assertEq(listData.accounts.length, 1, 'admin: 1 account in list');
    assertEq(listData.accounts[0].maskedKey, maskKey('valid-key'), 'admin: key is masked');

    // 4. Pool state
    const stateReq = new Request('https://gateway.test/api/state', {
      headers: { 'x-admin-token': 'test-admin-token' },
    });
    const stateRes = await worker.fetch(stateReq, env);
    assertEq(stateRes.status, 200, 'admin: state 200');
    const stateData = await stateRes.json();
    assertEq(stateData.strategy, 'sticky', 'admin: state reports unified strategy');
    assertEq(stateData.totals.accounts, 1, 'admin: state account count');

    // 5. Usage daily
    const usageReq = new Request('https://gateway.test/api/usage/daily?range=1d', {
      headers: { 'x-admin-token': 'test-admin-token' },
    });
    const usageRes = await worker.fetch(usageReq, env);
    assertEq(usageRes.status, 200, 'admin: usage daily 200');
  } finally {
    mock.restore();
  }
}

async function testD1E2E() {
  console.log('--- testD1E2E (MockD1 via node:sqlite) ---');
  let DatabaseSync;
  try {
    const sqlite = await import('node:sqlite');
    DatabaseSync = sqlite.DatabaseSync;
  } catch {
    console.log('  node:sqlite not available in this environment, skipping D1 tests');
    return;
  }

  class MockD1 {
    constructor() {
      this.db = new DatabaseSync(':memory:');
    }
    prepare(sql) {
      const self = this;
      const stmt = { sql, params: [] };
      stmt.bind = (...p) => {
        stmt.params = p;
        return stmt;
      };
      stmt.first = async () => self.db.prepare(sql).get(...stmt.params) ?? null;
      stmt.run = async () => {
        const info = self.db.prepare(sql).run(...stmt.params);
        return { meta: { last_row_id: Number(info.lastInsertRowid ?? 0), changes: info.changes } };
      };
      stmt.all = async () => ({ results: self.db.prepare(sql).all(...stmt.params) });
      return stmt;
    }
    async batch(stmts) {
      for (const s of stmts) await s.run();
    }
  }

  const mock = installProviderMock({
    behavior: {
      'd1-key-limited': 'limited',
      'd1-key-ok': 'ok',
    },
  });

  try {
    const db = new MockD1();
    const env = {
      API_BASE: 'https://api.commandcode.ai',
      DB: db,
      ALLOW_ANONYMOUS: 'true',
      ACCOUNTS: '[{"key":"d1-key-limited","label":"L"},{"key":"d1-key-ok","label":"OK"}]',
    };

    // First request: hits d1-key-limited, returns 429 directly, exactly 1 fetch call
    const req1 = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'test' }] }),
    });

    const res1 = await worker.fetch(req1, env);
    assertEq(res1.status, 429, 'd1: first request returns 429 directly');
    assertEq(mock.calls.length, 1, 'd1: exactly 1 fetch call');
    assertEq(mock.calls[0].apiKey, 'd1-key-limited', 'd1: only called d1-key-limited');

    // Second independent request: selects d1-key-ok, returns 200, exactly 1 fetch call
    mock.calls.length = 0;
    const req2 = new Request('https://gateway.test/v1/chat/completions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ messages: [{ role: 'user', content: 'test 2' }] }),
    });

    const res2 = await worker.fetch(req2, env);
    assertEq(res2.status, 200, 'd1: second independent request returns 200');
    assertEq(mock.calls.length, 1, 'd1: exactly 1 fetch call on second request');
    assertEq(mock.calls[0].apiKey, 'd1-key-ok', 'd1: selected d1-key-ok');

    // Verify D1 usage_buckets recording
    const pool = getPool(env);
    const daily = await pool.dailyUsage('1d');
    assert(daily.totals.requests > 0, 'd1: recorded request in usage_buckets');
  } finally {
    mock.restore();
  }
}

async function testQuotaClientAndStorage() {
  console.log('--- testQuotaClientAndStorage ---');

  // 1. planInfo mapping
  assertEq(planInfo('individual-go')?.monthlyCredits, 10, 'planInfo: individual-go is 10');
  assertEq(planInfo('individual-goat')?.monthlyCredits, 70, 'planInfo: individual-goat is 70');
  assertEq(planInfo('individual-pro-v1')?.monthlyCredits, 80, 'planInfo: individual-pro-v1 is 80 (longest prefix)');
  assertEq(planInfo('individual-pro')?.monthlyCredits, 30, 'planInfo: individual-pro is 30');
  assertEq(planInfo('unknown-plan'), undefined, 'planInfo: unknown plan is undefined');

  // 2. normalizeWindow
  const norm = normalizeWindow({ used: 2.5, cap: 5.0, resetAt: 1700000000000 });
  assertEq(norm.used, 2.5, 'normalizeWindow: used');
  assertEq(norm.cap, 5.0, 'normalizeWindow: cap');
  assertEq(norm.exceeded, false, 'normalizeWindow: not exceeded');
  assertEq(norm.resetAt, 1700000000000, 'normalizeWindow: resetAt');

  // 3. MemPool saveQuota and rowToAccount
  const pool = new MemPool([{ key: 'k1', label: 'Account 1' }]);
  const report = {
    account: { userName: 'user-goat' },
    plan: { planId: 'individual-goat', name: 'GOAT' },
    credits: {
      monthlyCredits: 65,
      purchasedCredits: 10,
      freeCredits: 2,
      fiveHour: { used: 1, cap: 5, exceeded: false, resetAt: 123456 },
      weekly: { used: 10, cap: 40, exceeded: false, resetAt: 234567 },
    },
    usage: { totalCount: 42, totalCost: 5 },
  };
  await pool.saveQuota(1, report);
  const list = await pool.list();
  const acc = list[0];
  assertEq(acc.user_name, 'user-goat', 'saveQuota: MemPool userName updated');
  assertEq(acc.plan?.planId, 'individual-goat', 'saveQuota: MemPool planId updated');
  assertEq(acc.plan?.monthlyCredits, 70, 'saveQuota: MemPool plan.monthlyCredits derived from planInfo');
  assertEq(acc.credits?.monthlyCredits, 65, 'saveQuota: MemPool monthlyCredits stored');
  assertEq(acc.credits?.fiveHour?.used, 1, 'saveQuota: MemPool fiveHour.used');
  assertEq(acc.credits?.weekly?.cap, 40, 'saveQuota: MemPool weekly.cap');
}

async function testAdminRefreshAndQuotaIntegration() {
  console.log('--- testAdminRefreshAndQuotaIntegration ---');
  const mock = installProviderMock({
    behavior: {
      'acc-ok': 'ok',
      'acc-429': 'limited',
      'acc-401': 'badkey',
      'acc-quota-429': 'ok',
      'alpha:acc-quota-429': 'limited',
      'acc-transient': 'ok',
      'alpha:acc-transient': 'server_error',
    },
  });

  try {
    const env = {
      API_BASE: 'https://api.commandcode.ai',
      ADMIN_TOKEN: 'sec',
      ACCOUNTS: JSON.stringify([
        { key: 'acc-ok', label: 'OK' },
        { key: 'acc-429', label: 'RateLimitedModels' },
        { key: 'acc-401', label: 'InvalidModels' },
        { key: 'acc-quota-429', label: 'QuotaRateLimited' },
        { key: 'acc-transient', label: 'TransientErr' },
      ]),
    };

    // 1. Refresh acc-ok -> fetches quota, populates monthlyCredits, plan, etc.
    const res1 = await worker.fetch(new Request('https://gateway.test/api/refresh', {
      method: 'POST',
      headers: { 'x-admin-token': 'sec', 'content-type': 'application/json' },
      body: JSON.stringify({ id: 1 }),
    }), env);
    assertEq(res1.status, 200, 'refresh: 200 for acc-ok');
    const data1 = await res1.json();
    const acc1 = data1.accounts[0];
    assertEq(acc1.user_name, 'tester', 'refresh: populated user_name');
    assertEq(acc1.credits?.monthlyCredits, 50, 'refresh: populated monthlyCredits');
    assertEq(acc1.plan?.name, 'GOAT', 'refresh: populated plan name');
    assertEq(acc1.plan?.monthlyCredits, 70, 'refresh: derived monthlyCredits 70 from planInfo');
    assert(acc1.detail?.lastChecked > 0, 'refresh: detail lastChecked populated');

    // 2. Refresh acc-429 -> models returns 429 -> placed on cooldown
    const res2 = await worker.fetch(new Request('https://gateway.test/api/refresh', {
      method: 'POST',
      headers: { 'x-admin-token': 'sec', 'content-type': 'application/json' },
      body: JSON.stringify({ id: 2 }),
    }), env);
    assertEq(res2.status, 200, 'refresh: returns 200 with refreshed status');
    const data2 = await res2.json();
    const acc2 = data2.accounts[0];
    assert(acc2.pool.rateLimited, 'refresh: models 429 puts account on cooldown');

    // 3. Refresh acc-401 -> models returns 401 -> account disabled
    const res3 = await worker.fetch(new Request('https://gateway.test/api/refresh', {
      method: 'POST',
      headers: { 'x-admin-token': 'sec', 'content-type': 'application/json' },
      body: JSON.stringify({ id: 3 }),
    }), env);
    assertEq(res3.status, 200, 'refresh: returns 200');
    const data3 = await res3.json();
    const acc3 = data3.accounts[0];
    assertEq(acc3.enabled, false, 'refresh: models 401 disables account');

    // 4. Refresh acc-quota-429 -> models is ok, quota returns 429 (Retry-After: 120) -> placed on cooldown
    const res4 = await worker.fetch(new Request('https://gateway.test/api/refresh', {
      method: 'POST',
      headers: { 'x-admin-token': 'sec', 'content-type': 'application/json' },
      body: JSON.stringify({ id: 4 }),
    }), env);
    assertEq(res4.status, 200, 'refresh: returns 200');
    const data4 = await res4.json();
    const acc4 = data4.accounts[0];
    assert(acc4.pool.rateLimited, 'refresh: quota 429 short-circuits and puts account on cooldown');

    // 5. Refresh acc-transient -> models is ok, quota returns 500 -> stays enabled, sets lastError
    const res5 = await worker.fetch(new Request('https://gateway.test/api/refresh', {
      method: 'POST',
      headers: { 'x-admin-token': 'sec', 'content-type': 'application/json' },
      body: JSON.stringify({ id: 5 }),
    }), env);
    assertEq(res5.status, 200, 'refresh: returns 200');
    const data5 = await res5.json();
    const acc5 = data5.accounts[0];
    assertEq(acc5.enabled, true, 'refresh: transient error keeps account enabled');
    assertEq(acc5.pool.rateLimited, false, 'refresh: transient error does not cooldown account');
    assert(acc5.lastError.includes('500') || acc5.lastError.includes('无法访问'), 'refresh: lastError recorded transient error');
  } finally {
    mock.restore();
  }
}

async function runAll() {
  console.log('=== Running commandcode-pool Test Suite ===\n');
  await testApiAdapters();
  await testUpstreamClient();
  await testFailurePolicy();
  await testSingleAccountRunner();
  await testStreamObserver();
  await testWorkerE2E();
  await testAdminAndPool();
  await testD1E2E();
  await testQuotaClientAndStorage();
  await testAdminRefreshAndQuotaIntegration();

  console.log(`\n=== Test Results: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) {
    console.error('\nFailures:');
    for (const f of failures) console.error('  - ' + f);
    process.exit(1);
  }
}

runAll().catch((err) => {
  console.error('Test runner fatal error:', err);
  process.exit(1);
});

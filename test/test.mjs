// commandcode-pool 本地测试：单元 + mock 端到端（无需真实密钥/部署）
// 运行：node test/test.mjs
import worker, {
  buildCcRequest, convertAnthropicToOpenAI, convertResponsesToChat,
  createSseTranslator, createAnthropicSseTranslator, createResponsesSseTranslator,
  mapCcError, mapCcEventError, mapFinishReason, incompleteUpstreamDetail,
  generateFingerprint, sessionForKey,
  parseAccountsEnv, MemPool, maskKey,
  wireConstants,
} from '../worker.js';

const { CC_PROTOCOL_VERSION } = wireConstants();

let passed = 0, failed = 0;
const failures = [];

function assert(cond, name, extra) {
  if (cond) { passed++; }
  else { failed++; failures.push(name + (extra !== undefined ? ` :: ${JSON.stringify(extra)}` : '')); }
}
function assertEq(actual, expected, name) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  assert(ok, name, ok ? undefined : { actual, expected });
}

const enc = new TextEncoder();

function sseBody(events) {
  const chunks = events.map((e) => typeof e === 'string' ? e : `${JSON.stringify(e)}\n`);
  return new ReadableStream({
    start(c) { for (const ch of chunks) c.enqueue(enc.encode(ch)); c.close(); },
  });
}
function sseResponse(events, status = 200, headers = {}) {
  return new Response(sseBody(events), { status, headers: { 'content-type': 'text/event-stream', ...headers } });
}
function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json' } });
}

const USAGE = {
  inputTokens: 100, outputTokens: 5, totalTokens: 105,
  cachedInputTokens: 80,
  inputTokenDetails: { cacheReadTokens: 80, cacheWriteTokens: 10 },
};

const CHAT_EVENTS = [
  { type: 'start', messageId: 'm1' },
  { type: 'text-start', id: 't1' },
  { type: 'text-delta', text: 'Hello' },
  { type: 'text-delta', text: ' world' },
  { type: 'text-end', id: 't1' },
  { type: 'reasoning-start', id: 'r1' },
  { type: 'reasoning-delta', text: 'hmm' },
  { type: 'reasoning-end', id: 'r1' },
  { type: 'finish', finishReason: 'stop', totalUsage: USAGE },
];

const TOOL_EVENTS = [
  { type: 'tool-input-start', toolCallId: 'tc1' },
  { type: 'tool-input-delta', toolCallId: 'tc1', delta: '{"city":"' },
  { type: 'tool-input-delta', toolCallId: 'tc1', delta: 'SF"}' },
  { type: 'tool-input-end', toolCallId: 'tc1' },
  { type: 'tool-call', toolCallId: 'tc1', toolName: 'get_weather', input: { city: 'SF' } },
  { type: 'finish', finishReason: 'tool-calls', totalUsage: USAGE },
];

const ZERO_EVENTS = [
  { type: 'text-start', id: 't1' },
  { type: 'text-delta', text: '' },
  { type: 'text-end', id: 't1' },
  { type: 'finish', finishReason: 'stop', totalUsage: { ...USAGE, outputTokens: 0, totalTokens: 100 } },
];

const EMPTY_FINISH_EVENTS = [
  { type: 'finish', finishReason: 'stop', totalUsage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
];

const PARTIAL_EVENTS = [
  { type: 'text-start', id: 't1' },
  { type: 'text-delta', text: 'partial' },
];

const ERR_429_EVENTS = [
  { type: 'error', statusCode: 429, message: '<429> no capacity' },
];

const CHAT_REQUEST = {
  model: 'deepseek/deepseek-v4-flash',
  messages: [{ role: 'user', content: 'Hi' }],
};

// ============================ mock 上游 ============================

function installMock({ behavior = {} } = {}) {
  // behavior: 按 api key 的特殊处理；behavior.events 覆盖默认事件流
  const calls = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, opts = {}) => {
    const urlStr = String(url instanceof URL ? url : url);
    const headers = opts.headers || {};
    const key = headers['Authorization'] || '';
    const apiKey = String(key).replace(/^Bearer\s+/, '');
    calls.push({ url: urlStr, apiKey, method: opts.method || 'GET', body: opts.body, headers });

    if (urlStr.endsWith('/alpha/fingerprint/record') || urlStr.endsWith('/alpha/lifecycle-events')) {
      return jsonResponse({ ok: true });
    }
    if (urlStr.endsWith('/alpha/generate')) {
      const b = behavior[apiKey];
      if (b === 'limited') {
        return new Response(JSON.stringify({ error: { message: 'rate limited' } }), { status: 429, headers: { 'Retry-After': '3600' } });
      }
      if (b === 'bad') return new Response(JSON.stringify({ error: { message: 'invalid api key' } }), { status: 401 });
      if (b === 'forbidden') return new Response(JSON.stringify({ error: { message: 'forbidden' } }), { status: 403 });
      if (b === 'server_error') return new Response('boom', { status: 500 });
      if (b === 'bad_request') return new Response(JSON.stringify({ error: { code: 'BAD_REQUEST', message: 'bad model' } }), { status: 400 });
      if (b === 'exhausted') return new Response(JSON.stringify({ success: false, error: { code: 'USAGE_EXCEEDED', message: 'quota exhausted, resets at 2099-01-01T00:00:00Z' } }), { status: 402 });
      if (b === 'hang') {
        return new Response(new ReadableStream({ start() { /* 永不写出、永不关闭 */ } }), { status: 200, headers: { 'content-type': 'text/event-stream' } });
      }
      if (b === 'err429') return sseResponse(ERR_429_EVENTS);
      if (typeof b === 'function') return b({ url: urlStr, apiKey, body: opts.body, headers });
      const events = behavior['events:' + apiKey] || behavior.events || CHAT_EVENTS;
      return sseResponse(events);
    }
    if (urlStr.endsWith('/provider/v1/models')) {
      if (behavior['models:' + apiKey] === 'bad') return new Response('nope', { status: 401 });
      return jsonResponse({ object: 'list', data: [
        { id: 'deepseek/deepseek-v4-flash', name: 'DSv4', context_length: 128000 },
        { id: 'google/gemini-3-pro', name: 'G3', context_length: 1000000 },
      ] });
    }
    if (urlStr.endsWith('/alpha/whoami')) {
      if (behavior[apiKey] === 'bad') return new Response('nope', { status: 401 });
      return jsonResponse({ user: { id: 'u1', name: 'Tester', userName: 'tester-' + apiKey.slice(-2) }, org: { id: 'o1' } });
    }
    if (urlStr.endsWith('/alpha/billing/credits')) {
      return jsonResponse({
        credits: { monthlyCredits: 10, purchasedCredits: 0, freeCredits: 0, planId: 'individual-go', belowThreshold: false },
        windowLimits: { fiveHour: { used: 3, cap: 5, exceeded: false, resetAt: Date.now() + 3600e3 }, weekly: { used: 8, cap: 30, exceeded: false, resetAt: 0 } },
      });
    }
    if (urlStr.endsWith('/alpha/billing/subscriptions')) {
      return jsonResponse({ data: { planId: 'individual-go', status: 'active', currentPeriodEnd: Date.now() + 86400e3 } });
    }
    if (urlStr.endsWith('/alpha/usage/summary')) {
      return jsonResponse({ data: { totalCount: 42, totalCost: 1.5, totalCredits: 4.2 } });
    }
    return new Response('not found', { status: 404 });
  };
  return {
    calls,
    restore() { globalThis.fetch = realFetch; },
    generateCalls: () => calls.filter((c) => c.url.endsWith('/alpha/generate')),
    initCalls: () => calls.filter((c) => c.url.endsWith('/alpha/fingerprint/record') || c.url.endsWith('/alpha/lifecycle-events')),
  };
}

// ============================ 单元：buildCcRequest ============================

function testBuildCcRequest() {
  const cc = buildCcRequest({
    model: 'deepseek/deepseek-v4-flash',
    messages: [
      { role: 'system', content: 'be nice' },
      { role: 'developer', content: 'also nice' },
      { role: 'user', content: [{ type: 'text', text: 'hello' }, { type: 'image_url', image_url: { url: 'data:image/png;base64,QUJD' } }] },
      { role: 'assistant', content: '', reasoning_content: 'thought', tool_calls: [{ id: 'c1', type: 'function', function: { name: 'bash_output', arguments: '{"x":1}' } }] },
      { role: 'tool', tool_call_id: 'c1', content: 'result text' },
    ],
    tools: [{ type: 'function', function: { name: 'bash_output', description: 'd', parameters: { type: 'object', properties: {} } } }],
    max_tokens: 123,
    temperature: 0.5,
    reasoning_effort: 'high',
    tool_choice: 'required',
    parallel_tool_calls: false,
  }, {});

  // 信封 9 键顺序（无 threadId，sessionId 由 forwardToCC 注入）
  assertEq(Object.keys(cc), ['config', 'memory', 'taste', 'skills', 'permissionMode', 'mode', 'params'], 'ccreq: envelope key order');
  assertEq(cc.memory, null, 'ccreq: memory null');
  assertEq(cc.taste, null, 'ccreq: taste null');
  assertEq(cc.skills, null, 'ccreq: skills null');
  assertEq(cc.permissionMode, 'standard', 'ccreq: permissionMode');
  assertEq(cc.mode, 'agent', 'ccreq: mode agent');
  assertEq(cc.config.environment, 'win32', 'ccreq: config.environment伪装平台');
  assertEq(cc.config.workingDir, 'C:\\Users\\dev\\projects\\app', 'ccreq: config.workingDir 伪造');
  assertEq(cc.config.recentCommits, [], 'ccreq: config.recentCommits');

  // system 块数组：非最后一块补 \n
  assertEq(cc.params.system, [{ type: 'text', text: 'be nice\n' }, { type: 'text', text: 'also nice' }], 'ccreq: system blocks with newline join');

  // params 透传
  assertEq(cc.params.model, 'deepseek/deepseek-v4-flash', 'ccreq: model');
  assertEq(cc.params.max_tokens, 123, 'ccreq: max_tokens honored');
  assertEq(cc.params.stream, true, 'ccreq: always stream upstream');
  assertEq(cc.params.temperature, 0.5, 'ccreq: temperature passthrough');
  assertEq(cc.params.reasoning_effort, 'high', 'ccreq: reasoning_effort passthrough');
  assertEq(cc.params.tool_choice, { type: 'any' }, 'ccreq: tool_choice required→any');
  assertEq(cc.params.parallel_tool_calls, false, 'ccreq: parallel_tool_calls passthrough');
  assertEq(cc.params.tools, [
    { name: 'shell_output', description: 'd', input_schema: { type: 'object', properties: {} } },
  ], 'ccreq: tool alias bash_output→shell_output + wire shape');

  const msgs = cc.params.messages;
  assertEq(msgs.length, 3, 'ccreq: system hoisted out of messages');
  assertEq(msgs[0].content[1], { type: 'image', image: 'data:image/png;base64,QUJD', mimeType: 'image/png' }, 'ccreq: image mimeType');
  assertEq(msgs[1].content[0], { type: 'reasoning', text: 'thought' }, 'ccreq: reasoning first');
  assertEq(msgs[1].content[1].type, 'tool-call', 'ccreq: tool-call');
  assertEq(msgs[1].content[1].input, { x: 1 }, 'ccreq: tool-call input parsed');
  assertEq(msgs[2].content[0].toolName, 'bash_output', 'ccreq: tool-result name back-resolved from history (alias 只作用于 tools 定义)');

  // max_tokens 默认/上限/max_completion_tokens 合并
  assertEq(buildCcRequest({ model: 'm', messages: [{ role: 'user', content: 'x' }] }, {}).params.max_tokens, 64000, 'ccreq: default max_tokens');
  assertEq(buildCcRequest({ model: 'm', messages: [{ role: 'user', content: 'x' }], max_tokens: 999999 }, {}).params.max_tokens, 200000, 'ccreq: max_tokens capped');
  assertEq(buildCcRequest({ model: 'm', messages: [{ role: 'user', content: 'x' }], max_completion_tokens: 77 }, {}).params.max_tokens, 77, 'ccreq: max_completion_tokens wins');

  // 空 system 占位 + prompt_cache_key → cache_control
  const noSys = buildCcRequest({ model: 'm', messages: [{ role: 'user', content: 'x' }] }, {});
  assertEq(noSys.params.system, [{ type: 'text', text: ' ' }], 'ccreq: emptySystemPlaceholder');
  const withKey = buildCcRequest({
    model: 'm', prompt_cache_key: 'my-cache-key',
    messages: [{ role: 'system', content: 'sys' }, { role: 'user', content: 'x' }],
  }, {});
  assertEq(withKey.params.system[0].cache_control, { type: 'ephemeral' }, 'ccreq: prompt_cache_key → ephemeral cache_control');

  // 客户端校验
  let threw = false;
  try { buildCcRequest({ model: 'm', messages: [] }, {}); } catch { threw = true; }
  assert(threw, 'ccreq: empty messages rejected');
}

// ============================ 单元：错误映射 ============================

function testErrorMapping() {
  assertEq(mapCcError(402, '{"success":false,"error":{"code":"USAGE_EXCEEDED","message":"quota"}}').status, 429, 'errmap: 402→429');
  assertEq(mapCcError(403, '{"error":{"message":"forbidden"}}').status, 401, 'errmap: 403→401');
  assertEq(mapCcError(500, 'boom').status, 502, 'errmap: 500→502');
  const e429 = mapCcError(429, '{"error":{"message":"slow down","code":"RATE_LIMITED"}}');
  assertEq(e429.body.retry_after, 30, 'errmap: 429 retry_after');
  assertEq(e429.body.error.code, 'RATE_LIMITED', 'errmap: code passthrough');

  const withPrefix = mapCcEventError({ type: 'error', message: '<503> upstream boom' });
  assertEq(withPrefix.reportedStatus, 503, 'errmap: <NNN> prefix');
  assertEq(withPrefix.status, 503, 'errmap: <503> stays 503');
  const withStatusCode = mapCcEventError({ type: 'error', error: { statusCode: 429, message: 'rate' } });
  assertEq(withStatusCode.reportedStatus, 429, 'errmap: statusCode field');
  assertEq(withStatusCode.status, 429, 'errmap: statusCode 429 mapped');
  const plain = mapCcEventError({ type: 'error', message: 'weird failure' });
  assertEq(plain.reportedStatus, null, 'errmap: no status signal');
  assertEq(plain.status, 502, 'errmap: default 502');

  assertEq(mapFinishReason('tool-calls'), 'tool_calls', 'finish: tool-calls');
  assertEq(mapFinishReason('max_output_tokens'), 'length', 'finish: max_output_tokens→length');
  assertEq(mapFinishReason('model_context_window_exceeded'), 'length', 'finish: ctx exceeded→length');
  assertEq(mapFinishReason('network-error'), 'upstream_error', 'finish: network-error');
  assertEq(mapFinishReason('pause_turn'), 'pause_turn', 'finish: pause_turn passthrough');
  assertEq(incompleteUpstreamDetail(false, 'stop'), 'no finish event', 'incomplete: no finish');
  assertEq(incompleteUpstreamDetail(true, 'upstream_error'), 'provider reported an upstream connection failure', 'incomplete: upstream_error');
  assertEq(incompleteUpstreamDetail(true, 'stop'), null, 'incomplete: normal');
}

// ============================ 单元：翻译器 ============================

async function testTranslators() {
  // OpenAI 翻译器
  const t = createSseTranslator('m', 'chatcmpl-x', 1);
  let frames = [];
  for (const ev of CHAT_EVENTS) {
    const f = t.parseLine(JSON.stringify(ev));
    if (f) frames.push(...f);
  }
  assert(frames.length >= 4, 'translator-openai: frames emitted', frames.length);
  assertEq(t.outputTokens, 5, 'translator-openai: output tokens');
  assertEq(t.incompleteDetail(), null, 'translator-openai: complete');
  const finishFrame = frames[frames.length - 1];
  const finishChunk = JSON.parse(finishFrame.slice('data: '.length));
  assertEq(finishChunk.choices[0].finish_reason, 'stop', 'translator-openai: finish reason');
  assertEq(finishChunk.usage, {
    prompt_tokens: 100, completion_tokens: 5, total_tokens: 105,
    prompt_tokens_details: { cached_tokens: 80 },
  }, 'translator-openai: usage with cached_tokens on finish chunk');

  const tz = createSseTranslator('m', 'id', 1);
  tz.parseLine(JSON.stringify({ type: 'finish', finishReason: 'stop', totalUsage: { ...USAGE, outputTokens: 0, totalTokens: 100 } }));
  assertEq(tz.outputTokens, 0, 'translator-openai: zero output detected');

  const te = createSseTranslator('m', 'id', 1);
  te.parseLine(JSON.stringify({ type: 'error', statusCode: 429, message: '<429> busy' }));
  assertEq(te.upstreamError.status, 429, 'translator-openai: in-stream 429 mapped');

  // Anthropic 翻译器
  const ta = createAnthropicSseTranslator('claude-x', 'msg_1');
  frames = [ta.preamble()];
  for (const ev of CHAT_EVENTS) {
    const f = await ta.parseLine(JSON.stringify(ev));
    if (f) frames.push(...f);
  }
  const text = frames.join('');
  assert(text.startsWith('event: message_start'), 'translator-anthropic: message_start first');
  assert(text.includes('event: content_block_start'), 'translator-anthropic: block start');
  assert(text.includes('"thinking_delta"'), 'translator-anthropic: thinking delta');
  const tail = await ta.finalize();
  const tailText = tail.join('');
  assert(tailText.includes('signature_delta'), 'translator-anthropic: signature delta on close');
  const sigMatch = tailText.match(/"signature":\s*"([^"]+)"/);
  assert(sigMatch && sigMatch[1].startsWith('E'), 'translator-anthropic: signature E-prefix', sigMatch && sigMatch[1].slice(0, 4));
  assert(tailText.includes('event: message_stop'), 'translator-anthropic: message_stop');
  assert(tailText.includes('"input_tokens":10'), 'translator-anthropic: input = total - cacheRead - cacheWrite');
  assert(tailText.includes('"cache_read_input_tokens":80'), 'translator-anthropic: cache_read');
  assertEq(ta.terminalKind(), 'ok', 'translator-anthropic: terminal ok');

  const tz2 = createAnthropicSseTranslator('m', 'id');
  await tz2.parseLine(JSON.stringify({ type: 'finish', finishReason: 'stop', totalUsage: { inputTokens: 0, outputTokens: 0 } }));
  assertEq(tz2.terminalKind(), 'zero', 'translator-anthropic: zero terminal');

  // Responses 翻译器
  const tr = createResponsesSseTranslator('m', 'resp_1', 1);
  frames = [];
  for (const ev of CHAT_EVENTS) {
    const f = tr.parseLine(JSON.stringify(ev));
    if (f) frames.push(...f);
  }
  frames.push(...tr.finish());
  const rtext = frames.join('');
  assert(rtext.includes('response.created'), 'translator-responses: created');
  assert(rtext.includes('response.output_text.delta'), 'translator-responses: text delta');
  assert(rtext.includes('response.reasoning_summary_text.delta'), 'translator-responses: reasoning delta');
  assert(rtext.includes('response.completed'), 'translator-responses: completed');
  const seqs = [...rtext.matchAll(/"sequence_number":(\d+)/g)].map((m) => Number(m[1]));
  assertEq(seqs, seqs.map((_, i) => i), 'translator-responses: sequence_number monotonic');
  const doneItem = rtext.match(/"output_text","text":"Hello world"/);
  assert(!!doneItem, 'translator-responses: aggregated text in completed');

  const tr2 = createResponsesSseTranslator('m', 'resp_2', 1);
  for (const ev of TOOL_EVENTS) tr2.parseLine(JSON.stringify(ev));
  const tr2out = tr2.finish().join('');
  assert(tr2out.includes('"type":"function_call"'), 'translator-responses: function_call item');
  assert(tr2out.includes('response.completed'), 'translator-responses: tool-call completes');
}

// ============================ 单元：协议身份（指纹/会话/信封线形） ============================

async function testProtocolIdentity() {
  const fp1 = await generateFingerprint('user_testkey123');
  const fp2 = await generateFingerprint('user_testkey123');
  assertEq(fp1, fp2, 'identity: fingerprint deterministic per key');
  const fp3 = await generateFingerprint('user_testkey123', 'salt-a');
  assert(fp3.components.machineIdHash !== fp1.components.machineIdHash, 'identity: salt changes machine');
  assertEq(fp3.components.platform, 'win32', 'identity: platform win32');
  assertEq(fp3.components.runtime, 'cli', 'identity: runtime cli');
  assert(/^[0-9a-f]{64}$/.test(fp3.thumbmark), 'identity: thumbmark sha256 hex');
  assertEq(fp3.components.macHashes.length >= 2 && fp3.components.macHashes.length <= 5, true, 'identity: mac count 2-5');

  const s1 = await sessionForKey('user_testkey123');
  const s2 = await sessionForKey('user_testkey123');
  assertEq(s1, s2, 'identity: session stable in bucket');
  assert(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(s1), 'identity: session uuid shape');

  // E2E 线形：forwardToCC 的信封键序 + 头集
  const mock = installMock();
  const env = { API_BASE: 'https://upstream.test', ACCOUNTS: '[{"key":"wire-key-1","label":"W"}]', ALLOW_ANONYMOUS: 'true' };
  await worker.fetch(chatReq(CHAT_REQUEST), env);
  const gen = mock.generateCalls()[0];
  const wire = JSON.parse(gen.body);
  assertEq(Object.keys(wire), ['config', 'memory', 'taste', 'skills', 'permissionMode', 'threadId', 'mode', 'params'],
    'identity: envelope key order with threadId');
  assert(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(wire.threadId), 'identity: threadId uuid in envelope');
  assertEq(gen.headers['User-Agent'], 'cli', 'identity: User-Agent cli');
  assertEq(gen.headers['x-command-code-version'], CC_PROTOCOL_VERSION, 'identity: version header 1.53.1');
  assertEq(gen.headers['x-cli-environment'], 'production', 'identity: x-cli-environment');
  assertEq(gen.headers['x-project-slug'], 'c-users-dev-projects-app', 'identity: x-project-slug slugified');
  assertEq(gen.headers['x-taste-learning'], 'false', 'identity: x-taste-learning');
  assert(/^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/.test(gen.headers['traceparent']), 'identity: traceparent shape');
  assert(/^[0-9a-f]{8}-[0-9a-f]{4}/.test(gen.headers['x-session-id']), 'identity: x-session-id uuid');

  // 每个 key 初始化一次（fingerprint + lifecycle 各一）
  const initCalls = mock.initCalls();
  const fpCalls = initCalls.filter((c) => c.url.endsWith('/alpha/fingerprint/record'));
  const lcCalls = initCalls.filter((c) => c.url.endsWith('/alpha/lifecycle-events'));
  assertEq(fpCalls.length, 1, 'identity: one fingerprint record per key');
  assertEq(lcCalls.length, 1, 'identity: one lifecycle event per key');
  const fpBody = JSON.parse(fpCalls[0].body);
  assertEq(fpBody.thumbmark, (await generateFingerprint('wire-key-1')).thumbmark, 'identity: wire fingerprint matches derivation');
  const lcBody = JSON.parse(lcCalls[0].body);
  assertEq(lcBody.eventType, 'cli_session_exists', 'identity: lifecycle eventType');
  assert(/^sess_[0-9a-f]{16}$/.test(lcBody.metadata.sessionId), 'identity: lifecycle sessionId shape');
  assertEq(lcBody.metadata.cliVersion, CC_PROTOCOL_VERSION, 'identity: lifecycle cliVersion');
  assertEq(lcBody.metadata.os, 'win32-x64', 'identity: lifecycle os');
  mock.restore();
}

// ============================ 单元：Anthropic / Responses 转换 ============================

function testProtocolConversion() {
  // Anthropic → OpenAI
  const o = convertAnthropicToOpenAI({
    model: 'claude-sonnet-4-6',
    max_tokens: 500,
    system: [{ type: 'text', text: 'sys block', cache_control: { type: 'ephemeral' } }],
    messages: [
      { role: 'user', content: [{ type: 'text', text: 'describe' }, { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'QUJD' } }] },
      { role: 'assistant', content: [{ type: 'thinking', thinking: 'hmm' }, { type: 'text', text: 'answer' }, { type: 'tool_use', id: 'tu1', name: 'get_weather', input: { city: 'SF' } }] },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'tu1', content: 'sunny' }] },
    ],
    tools: [{ name: 'get_weather', description: 'd', input_schema: { type: 'object' } }],
    tool_choice: { type: 'tool', name: 'get_weather' },
    thinking: { type: 'enabled', budget_tokens: 12000 },
    stop_sequences: ['END'],
    temperature: 0.2,
  });
  assertEq(o.model, 'claude-sonnet-4-6', 'anthropic→oa: model');
  assertEq(o.max_tokens, 500, 'anthropic→oa: max_tokens');
  assertEq(o.messages[0].role, 'system', 'anthropic→oa: system first');
  assertEq(o.messages[0].content[0].cache_control, { type: 'ephemeral' }, 'anthropic→oa: system cache_control kept');
  const assistant = o.messages.find((m) => m.role === 'assistant');
  assertEq(assistant.reasoning_content, 'hmm', 'anthropic→oa: thinking → reasoning_content');
  assertEq(assistant.tool_calls[0].function.name, 'get_weather', 'anthropic→oa: tool_use');
  const toolMsg = o.messages.find((m) => m.role === 'tool');
  assertEq(toolMsg.name, 'get_weather', 'anthropic→oa: tool_result name resolved');
  assertEq(toolMsg.tool_call_id, 'tu1', 'anthropic→oa: tool_use_id');
  const userMsg = o.messages.find((m) => m.role === 'user');
  assertEq(userMsg.content[1].type, 'image_url', 'anthropic→oa: image block → image_url');
  assertEq(o.tool_choice, { type: 'function', function: { name: 'get_weather' } }, 'anthropic→oa: tool_choice tool→function');
  assertEq(o.reasoning_effort, 'high', 'anthropic→oa: budget 12000 → high');
  assertEq(o.stop, ['END'], 'anthropic→oa: stop_sequences → stop');
  assertEq(o.temperature, 0.2, 'anthropic→oa: temperature');
  assertEq(convertAnthropicToOpenAI({ model: 'm', max_tokens: 1, messages: [], thinking: { type: 'enabled', budget_tokens: 3000 } }).reasoning_effort, 'low', 'anthropic→oa: budget 3000 → low');

  // Responses → Chat
  const c = convertResponsesToChat({
    model: 'gpt-5.5',
    instructions: 'be terse',
    input: [
      { role: 'user', content: 'hi' },
      { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'args?' }] },
      { type: 'function_call', call_id: 'fc1', name: 'fn', arguments: '{"a":1}' },
      { type: 'function_call_output', call_id: 'fc1', output: 'done' },
      { role: 'user', content: 'thanks' },
    ],
    tools: [{ type: 'function', name: 'fn', parameters: { type: 'object' } }],
    reasoning: { effort: 'medium' },
    max_output_tokens: 900,
  });
  assertEq(c.messages[0], { role: 'system', content: 'be terse' }, 'responses→chat: instructions');
  assertEq(c.messages[1].content, 'hi', 'responses→chat: stringless message item');
  const asst = c.messages.find((m) => m.role === 'assistant');
  assertEq(asst.tool_calls[0].function.name, 'fn', 'responses→chat: function_call on assistant');
  assertEq(c.messages.filter((m) => m.role === 'tool')[0].tool_call_id, 'fc1', 'responses→chat: function_call_output → tool');
  assertEq(c.reasoning_effort, 'medium', 'responses→chat: reasoning.effort');
  assertEq(c.max_tokens, 900, 'responses→chat: max_output_tokens → max_tokens');
}

// ============================ 单元：池选择（sticky/round_robin） ============================

async function testPoolSelection() {
  const pool = new MemPool(parseAccountsEnv('k1,k2,k3'));
  let a = await pool.pick('sticky');
  assertEq(a.api_key, 'k1', 'pool: sticky first pick lowest id on tie');
  await pool.recordSuccess(a.id, { prompt_tokens: 1, completion_tokens: 1 });
  a = await pool.pick('sticky');
  assertEq(a.api_key, 'k1', 'pool: sticky keeps most-recent account');
  await pool.cooldown(a.id, 60, 'x');
  a = await pool.pick('sticky');
  assertEq(a.api_key, 'k2', 'pool: sticky switches when account cooled');
  await pool.recordSuccess(a.id, {});
  a = await pool.pick('sticky');
  assertEq(a.api_key, 'k2', 'pool: sticky does NOT bounce back after cooldown ends');

  const rr = new MemPool(parseAccountsEnv('k1,k2'));
  await rr.recordSuccess(1, {}); // k1 used
  let b = await rr.pick('round_robin');
  assertEq(b.api_key, 'k2', 'pool: round_robin picks least-recent');
  await rr.recordSuccess(b.id, {});
  b = await rr.pick('round_robin');
  assertEq(b.api_key, 'k1', 'pool: round_robin rotates');

  const p2 = new MemPool(parseAccountsEnv('[{"key":"a","label":"L1"},{"key":"b"}]'));
  assertEq(p2.accounts.length, 2, 'parseAccountsEnv: JSON array');
  assertEq(p2.accounts[0].label, 'L1', 'parseAccountsEnv: label');
  assertEq(parseAccountsEnv('x,y').length, 2, 'parseAccountsEnv: CSV');
  assertEq(parseAccountsEnv(''), [], 'parseAccountsEnv: empty');

  const daily = new MemPool(parseAccountsEnv('daily-key'));
  await daily.recordSuccess(1, { prompt_tokens: 3, completion_tokens: 2 });
  const recent = await daily.dailyUsage(1);
  assertEq(recent.totals.promptTokens, 3, 'pool/daily: recent usage included');
  const recent5m = await daily.dailyUsage('5m');
  assertEq(recent5m.totals.requests, 1, 'pool/range: 5m usage included');
  assertEq(maskKey('abcdefghijkl'), 'abc…ijkl', 'maskKey');
}

// ============================ 端到端（mock 上游） ============================

function workerEnv(extra = {}) {
  return {
    API_BASE: 'https://upstream.test',
    ACCOUNTS: JSON.stringify([{ key: 'acc-limited', label: 'L' }, { key: 'acc-good', label: 'G' }]),
    ALLOW_ANONYMOUS: 'true',
    ...extra,
  };
}
const chatReq = (body) => new Request('https://worker.test/v1/chat/completions', {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});
const messagesReq = (body) => new Request('https://worker.test/v1/messages', {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});
const responsesReq = (body) => new Request('https://worker.test/v1/responses', {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});

async function testE2E() {
  // ---- 非流式 + 429 故障转移 + 跨请求粘住新账号 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'limited' } });
    const env = workerEnv();
    const r1 = await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(r1.status, 200, 'e2e: failover returns 200');
    const j1 = await r1.json();
    assertEq(j1.object, 'chat.completion', 'e2e: chat.completion object');
    assertEq(j1.choices[0].message.content, 'Hello world', 'e2e: aggregated content');
    assertEq(j1.choices[0].finish_reason, 'stop', 'e2e: finish reason');
    assertEq(j1.usage, {
      prompt_tokens: 100, completion_tokens: 5, total_tokens: 105,
      prompt_tokens_details: { cached_tokens: 80 },
    }, 'e2e: usage with cached_tokens');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e: 429 fails over to next account');

    mock.calls.length = 0;
    const r2 = await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(r2.status, 200, 'e2e: second request ok');
    await r2.json();
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-good'], 'e2e: sticky keeps using good account across requests (cache preserved)');
    mock.restore();
  }

  // ---- 流式（usage 附着在 finish chunk） ----
  {
    const mock = installMock();
    const env = workerEnv();
    const req = { ...CHAT_REQUEST, stream: true };
    const resp = await worker.fetch(chatReq(req), env);
    assertEq(resp.status, 200, 'e2e-stream: status');
    assertEq(resp.headers.get('Content-Type'), 'text/event-stream; charset=utf-8', 'e2e-stream: content type');
    const text = await resp.text();
    const lines = text.split('\n').filter((l) => l.startsWith('data: ')).map((l) => l.slice(6));
    assertEq(lines[lines.length - 1], '[DONE]', 'e2e-stream: ends with [DONE]');
    const chunks = lines.slice(0, -1).map((l) => JSON.parse(l));
    assertEq(chunks[0].object, 'chat.completion.chunk', 'e2e-stream: chunk object');
    assertEq(chunks[0].choices[0].delta.role, 'assistant', 'e2e-stream: role on first delta');
    const contents = chunks.filter((c) => c.choices[0] && c.choices[0].delta.content).map((c) => c.choices[0].delta.content).join('');
    assertEq(contents, 'Hello world', 'e2e-stream: streamed content');
    const reasoning = chunks.filter((c) => c.choices[0] && c.choices[0].delta.reasoning_content).map((c) => c.choices[0].delta.reasoning_content).join('');
    assertEq(reasoning, 'hmm', 'e2e-stream: reasoning_content delta');
    const finishChunk = chunks.find((c) => c.choices[0] && c.choices[0].finish_reason);
    assertEq(finishChunk.choices[0].finish_reason, 'stop', 'e2e-stream: finish chunk');
    assertEq(finishChunk.usage.prompt_tokens_details.cached_tokens, 80, 'e2e-stream: usage on finish chunk');
    mock.restore();
  }

  // ---- 流式 + 非流式工具调用 ----
  {
    const mock = installMock({ behavior: { events: TOOL_EVENTS } });
    const resp = await worker.fetch(chatReq({ ...CHAT_REQUEST, stream: true }), workerEnv());
    const chunks = (await resp.text()).split('\n').filter((l) => l.startsWith('data: ') && !l.includes('[DONE]')).map((l) => JSON.parse(l.slice(6)));
    const tc = chunks.find((c) => c.choices[0].delta.tool_calls);
    assert(!!tc, 'e2e-stream-tools: tool call delta emitted');
    assertEq(tc.choices[0].delta.tool_calls[0].function.name, 'get_weather', 'e2e-stream-tools: call name');
    assertEq(tc.choices[0].delta.tool_calls[0].function.arguments, '{"city":"SF"}', 'e2e-stream-tools: complete args');
    const fin = chunks.find((c) => c.choices[0].finish_reason);
    assertEq(fin.choices[0].finish_reason, 'tool_calls', 'e2e-stream-tools: finish reason');
    mock.restore();
  }
  {
    const mock = installMock({ behavior: { events: TOOL_EVENTS } });
    const j = await (await worker.fetch(chatReq(CHAT_REQUEST), workerEnv())).json();
    assertEq(j.choices[0].message.tool_calls, [{ id: 'tc1', type: 'function', function: { name: 'get_weather', arguments: '{"city":"SF"}' } }], 'e2e-nonstream-tools');
    mock.restore();
  }

  // ---- 401 拉黑 / 403 冷却 / 5xx 故障转移 / 400 透传 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'bad' } });
    const env = workerEnv();
    await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-401: failover order');
    mock.calls.length = 0;
    await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-good'], 'e2e-401: dead account disabled permanently');
    mock.restore();
  }
  {
    const mock = installMock({ behavior: { 'acc-limited': 'forbidden' } });
    await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-403: failover order');
    mock.restore();
  }
  {
    const mock = installMock({ behavior: { 'acc-limited': 'server_error' } });
    await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-5xx: failover order');
    mock.restore();
  }
  {
    const mock = installMock({ behavior: { 'acc-limited': 'bad_request', 'acc-good': 'bad_request' } });
    const r = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(r.status, 400, 'e2e-400: status');
    const j = await r.json();
    assertEq(j.error.type, 'invalid_request_error', 'e2e-400: type');
    assertEq(j.error.message, 'bad model', 'e2e-400: message');
    assertEq(j.error.code, 'BAD_REQUEST', 'e2e-400: upstream code passthrough');
    assertEq(mock.generateCalls().length, 1, 'e2e-400: no failover on request-scoped error');
    mock.restore();
  }

  // ---- 402（额度耗尽）按 429 冷却转移 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'exhausted' } });
    const r = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(r.status, 200, 'e2e-402: failover ok');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-402: failover order');
    mock.restore();
  }

  // ---- 零输出 → 429 语义：转移 + 冷却 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'zero', 'events:acc-limited': ZERO_EVENTS } });
    const r = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(r.status, 200, 'e2e-zero: failover ok');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-zero: failover order');
    mock.restore();
  }

  // ---- 全部账号 429 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'limited', 'acc-good': 'limited' } });
    const r = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(r.status, 429, 'e2e-all-429: status');
    assertEq(r.headers.get('Retry-After'), '30', 'e2e-all-429: Retry-After from mapped body (proxy parity)');
    const j = await r.json();
    assertEq(j.error.type, 'rate_limit_error', 'e2e-all-429: type');
    mock.restore();
  }

  // ---- pre-start 流内错误（<429>）转移 ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'err429' } });
    const env = workerEnv();
    const r = await worker.fetch(chatReq({ ...CHAT_REQUEST, stream: true }), env);
    assertEq(r.status, 200, 'e2e-stream-err: pre-start in-stream error fails over');
    const chunks = (await r.text()).split('\n').filter((l) => l.startsWith('data: ')).map((l) => l.slice(6));
    const parsed = chunks.filter((l) => l !== '[DONE]').map((l) => JSON.parse(l));
    const contents = parsed.filter((c) => c.choices && c.choices[0].delta.content).map((c) => c.choices[0].delta.content).join('');
    assertEq(contents, 'Hello world', 'e2e-stream-err: content from second account');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-stream-err: both accounts tried');
    mock.restore();
  }

  // ---- 空闲超时转移（STREAM_IDLE_MS=300, hang 账号） ----
  {
    const mock = installMock({ behavior: { 'acc-limited': 'hang' } });
    const env = workerEnv({ STREAM_IDLE_MS: '300' });
    const r = await worker.fetch(chatReq({ ...CHAT_REQUEST, stream: true }), env);
    assertEq(r.status, 200, 'e2e-timeout: failover after idle timeout');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'e2e-timeout: both accounts tried');
    mock.restore();
  }

  // ---- 客户端鉴权（Bearer + x-api-key） ----
  {
    const mock = installMock();
    const env = workerEnv({ API_KEYS: 'sk-a,sk-b' });
    const r1 = await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(r1.status, 401, 'auth: missing key rejected');
    const r2 = await worker.fetch(new Request('https://w/v1/chat/completions', {
      method: 'POST', headers: { Authorization: 'Bearer sk-b', 'Content-Type': 'application/json' },
      body: JSON.stringify(CHAT_REQUEST),
    }), env);
    assertEq(r2.status, 200, 'auth: valid bearer accepted');
    const r3 = await worker.fetch(new Request('https://w/v1/chat/completions', {
      method: 'POST', headers: { 'x-api-key': 'sk-a', 'Content-Type': 'application/json' },
      body: JSON.stringify(CHAT_REQUEST),
    }), env);
    assertEq(r3.status, 200, 'auth: x-api-key accepted');
    const r4 = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv());
    assertEq(r4.status, 200, 'auth: explicit anonymous mode when API_KEYS unset');
    const noConfig = await worker.fetch(chatReq(CHAT_REQUEST), workerEnv({ ALLOW_ANONYMOUS: 'false' }));
    assertEq(noConfig.status, 503, 'auth: missing API_KEYS rejected by default');
    mock.restore();
  }

  // ---- 客户端错误（本地校验，不打上游） ----
  {
    const mock = installMock();
    const r2 = await worker.fetch(chatReq({ model: 'm', messages: [] }), workerEnv());
    assertEq(r2.status, 400, 'client-err: empty messages');
    assertEq(mock.generateCalls().length, 0, 'client-err: no upstream call');
    mock.restore();
  }

  // ---- /health 与 /v1/models ----
  {
    const mock = installMock();
    const h = await worker.fetch(new Request('https://w/health'), workerEnv());
    assertEq(h.status, 200, 'health: 200');
    const m = await worker.fetch(new Request('https://w/v1/models'), workerEnv());
    const mj = await m.json();
    assertEq(mj.object, 'list', 'models: object');
    assertEq(mj.data.map((x) => x.id), ['deepseek/deepseek-v4-flash', 'google/gemini-3-pro'], 'models: catalog');
    assertEq(mj.data[0].context_window, 128000, 'models: context window');
    assertEq(mock.calls.find((c) => c.url.endsWith('/provider/v1/models')).apiKey, 'acc-limited', 'models: uses a pool key');
    mock.restore();
  }

  // ---- /v1/models 故障转移 ----
  {
    const mock = installMock({ behavior: { 'models:acc-limited': 'bad' } });
    const env = workerEnv({ API_BASE: 'https://models-failover.test' });
    const m = await worker.fetch(new Request('https://w/v1/models'), env);
    assertEq(m.status, 200, 'models-failover: status');
    assertEq(mock.calls.filter((c) => c.url.endsWith('/provider/v1/models')).map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'models-failover: tries next account');
    mock.restore();
  }

  // ---- 上游流中断（无 finish）→ 502 ----
  {
    const mock = installMock({ behavior: { events: PARTIAL_EVENTS } });
    const env = workerEnv();
    const r = await worker.fetch(chatReq(CHAT_REQUEST), env);
    assertEq(r.status, 502, 'incomplete: non-stream 502');
    const j = await r.json();
    assertEq(j.error.type, 'upstream_error', 'incomplete: type');
    assert(/truncated/.test(j.error.message), 'incomplete: message');
    mock.restore();
  }

  // ---- 流式中断不记成功用量 ----
  {
    const mock = installMock({ behavior: { events: PARTIAL_EVENTS } });
    const env = workerEnv();
    const r = await worker.fetch(chatReq({ ...CHAT_REQUEST, stream: true }), env);
    const text = await r.text();
    assert(/upstream_error|truncated/.test(text), 'incomplete-stream: error frame written');
    const state = await (await worker.fetch(new Request('https://w/api/state'), env)).json();
    assertEq(state.totals.requests, 0, 'incomplete: stream failure not counted');
    mock.restore();
  }

  // ---- /admin 由 assets 提供；无 ASSETS 时 404 ----
  {
    const env = workerEnv();
    const noAssets = await worker.fetch(new Request('https://w/admin'), env);
    assertEq(noAssets.status, 404, 'admin-ui: 404 without assets binding');
    const withAssets = await worker.fetch(new Request('https://w/admin'), {
      ...env, ASSETS: { fetch: async (req) => new Response('<html>Command Code Pool admin</html>', { status: 200, headers: { 'content-type': 'text/html' } }) },
    });
    assertEq(withAssets.status, 200, 'admin-ui: served via assets');
    assert((await withAssets.text()).includes('Command Code Pool'), 'admin-ui: html content');
  }

  // ---- 管理 API ----
  {
    const mock = installMock({ behavior: { bad: 'bad' } });
    const env = workerEnv({ ADMIN_TOKEN: 'sec' });
    const noAuth = await worker.fetch(new Request('https://w/api/accounts'), env);
    assertEq(noAuth.status, 401, 'admin: requires token');
    const H = { 'x-admin-token': 'sec', 'Content-Type': 'application/json' };

    const add = await worker.fetch(new Request('https://w/api/accounts', {
      method: 'POST', headers: H, body: JSON.stringify({ key: 'new-key-11', label: 'go-2' }),
    }), env);
    assertEq(add.status, 201, 'admin: add account');
    const addJ = await add.json();
    assertEq(addJ.account.label, 'go-2', 'admin: added label');
    assertEq(addJ.account.user_name, 'tester-11', 'admin: whoami name from validation');
    assertEq(addJ.account.plan.name, 'Go', 'admin: go plan detected');
    assertEq(addJ.account.credits.fiveHour.cap, 5, 'admin: five hour window cached');
    assert(mock.initCalls().some((c) => c.url.endsWith('/alpha/fingerprint/record') && c.apiKey === 'new-key-11'),
      'admin: fingerprint registered on add');

    const badAdd = await worker.fetch(new Request('https://w/api/accounts', {
      method: 'POST', headers: H, body: JSON.stringify({ key: 'bad' }),
    }), env);
    assertEq(badAdd.status, 400, 'admin: invalid key rejected via whoami');

    const list = await (await worker.fetch(new Request('https://w/api/accounts', { headers: H }), env)).json();
    assertEq(list.accounts.length, 3, 'admin: list after add');
    assert(!JSON.stringify(list).includes('acc-good'), 'admin: keys masked in list');

    const st = await (await worker.fetch(new Request('https://w/api/state', { headers: H }), env)).json();
    assertEq(st.strategy, 'sticky', 'admin/state: strategy');
    assertEq(st.totals.enabled, 3, 'admin/state: enabled count');
    assert(st.activeAccount && st.activeAccount.maskedKey, 'admin/state: active account present');

    const patch = await worker.fetch(new Request('https://w/api/accounts/1', {
      method: 'PATCH', headers: H, body: JSON.stringify({ enabled: false }),
    }), env);
    assertEq(patch.status, 200, 'admin: patch ok');
    const st2 = await (await worker.fetch(new Request('https://w/api/state', { headers: H }), env)).json();
    assertEq(st2.totals.enabled, 2, 'admin/state: disabled account excluded');

    const del = await worker.fetch(new Request('https://w/api/accounts/1', { method: 'DELETE', headers: H }), env);
    assertEq(del.status, 200, 'admin: delete ok');

    const login = await worker.fetch(new Request('https://w/api/login', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: 'sec' }),
    }), env);
    assertEq((await login.json()).ok, true, 'admin: login ok');
    mock.restore();
  }

  {
    const noConfig = await worker.fetch(new Request('https://w/api/accounts'), workerEnv({ ALLOW_ANONYMOUS: 'false' }));
    assertEq(noConfig.status, 503, 'admin: missing ADMIN_TOKEN rejected by default');
    const noConfigLogin = await worker.fetch(new Request('https://w/api/login', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: '' }),
    }), workerEnv({ ALLOW_ANONYMOUS: 'false' }));
    assertEq(noConfigLogin.status, 503, 'admin: login reports missing ADMIN_TOKEN');
  }
}

// ============================ /v1/messages 端到端 ============================

async function testMessagesE2E() {
  const MESSAGES_REQUEST = {
    model: 'claude-sonnet-4-6',
    max_tokens: 1000,
    system: 'You are helpful.',
    messages: [{ role: 'user', content: 'hi' }],
  };

  // 非流式：文本 + thinking + usage 换算
  {
    const mock = installMock();
    const r = await worker.fetch(messagesReq(MESSAGES_REQUEST), workerEnv());
    assertEq(r.status, 200, 'msg: status');
    const j = await r.json();
    assertEq(j.type, 'message', 'msg: type');
    assertEq(j.role, 'assistant', 'msg: role');
    assertEq(j.model, 'claude-sonnet-4-6', 'msg: model echo');
    assertEq(j.stop_reason, 'end_turn', 'msg: stop_reason');
    const text = (j.content.find((b) => b.type === 'text') || {}).text;
    assertEq(text, 'Hello world', 'msg: text content');
    const thinking = j.content.find((b) => b.type === 'thinking');
    assertEq(thinking.thinking, 'hmm', 'msg: thinking block');
    assert(thinking.signature.startsWith('E'), 'msg: thinking signature E-prefix');
    // usage：input = 总数 - cacheRead - cacheWrite（100-80-10=10）
    assertEq(j.usage.input_tokens, 10, 'msg: input_tokens excludes cache');
    assertEq(j.usage.cache_read_input_tokens, 80, 'msg: cache_read');
    assertEq(j.usage.cache_creation_input_tokens, 10, 'msg: cache_creation');
    assertEq(j.usage.output_tokens, 5, 'msg: output_tokens');
    mock.restore();
  }

  // 信封转换：system 块 + 请求形状
  {
    const mock = installMock();
    await worker.fetch(messagesReq(MESSAGES_REQUEST), workerEnv());
    const wire = JSON.parse(mock.generateCalls()[0].body);
    assertEq(wire.params.system, [{ type: 'text', text: 'You are helpful.' }], 'msg-wire: system block');
    assertEq(wire.params.max_tokens, 1000, 'msg-wire: max_tokens');
    mock.restore();
  }

  // 工具调用 → tool_use + stop_reason
  {
    const mock = installMock({ behavior: { events: TOOL_EVENTS } });
    const j = await (await worker.fetch(messagesReq(MESSAGES_REQUEST), workerEnv())).json();
    assertEq(j.stop_reason, 'tool_use', 'msg-tools: stop_reason');
    const tu = j.content.find((b) => b.type === 'tool_use');
    assertEq(tu.name, 'get_weather', 'msg-tools: tool_use name');
    assertEq(tu.input, { city: 'SF' }, 'msg-tools: tool_use input');
    mock.restore();
  }

  // 零输出（内容空）→ 转移
  {
    const mock = installMock({ behavior: { 'acc-limited': 'zero', 'events:acc-limited': EMPTY_FINISH_EVENTS } });
    const r = await worker.fetch(messagesReq(MESSAGES_REQUEST), workerEnv());
    assertEq(r.status, 200, 'msg-zero: failover ok');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'msg-zero: failover order');
    mock.restore();
  }

  // 流式：完整事件序列
  {
    const mock = installMock();
    const r = await worker.fetch(messagesReq({ ...MESSAGES_REQUEST, stream: true }), workerEnv());
    assertEq(r.status, 200, 'msg-stream: status');
    assertEq(r.headers.get('Content-Type'), 'text/event-stream; charset=utf-8', 'msg-stream: content type');
    const text = await r.text();
    const events = text.split('\n\n').filter(Boolean).filter((b) => b.startsWith('event: ')).map((block) => {
      const ev = /event: (\S+)/.exec(block)[1];
      const data = JSON.parse(/data: (.+)/.exec(block)[1]);
      return { ev, data };
    });
    const order = events.map((e) => e.ev);
    assertEq(order[0], 'message_start', 'msg-stream: message_start first');
    assertEq(order[order.length - 1], 'message_stop', 'msg-stream: message_stop last');
    assert(order.includes('content_block_start'), 'msg-stream: block start');
    assert(order.includes('thinking_delta'.replace(/^/, 'content_block_delta')) || order.filter((e) => e === 'content_block_delta').length >= 2, 'msg-stream: deltas');
    const md = events.find((e) => e.ev === 'message_delta');
    assertEq(md.data.delta.stop_reason, 'end_turn', 'msg-stream: stop_reason');
    assertEq(md.data.usage.input_tokens, 10, 'msg-stream: usage input');
    const sig = events.find((e) => e.ev === 'content_block_delta' && e.data.delta.type === 'signature_delta');
    assert(sig && sig.data.delta.signature.startsWith('E'), 'msg-stream: signature delta');
    mock.restore();
  }

  // 非流式 429 全冷却 → Anthropic 形错误
  {
    const mock = installMock({ behavior: { 'acc-limited': 'limited', 'acc-good': 'limited' } });
    const r = await worker.fetch(messagesReq(MESSAGES_REQUEST), workerEnv());
    assertEq(r.status, 429, 'msg-all-429: status');
    const j = await r.json();
    assertEq(j.type, 'error', 'msg-all-429: anthropic error shape');
    assertEq(j.error.type, 'rate_limit_error', 'msg-all-429: error type');
    mock.restore();
  }
}

// ============================ /v1/responses 端到端 ============================

async function testResponsesE2E() {
  // 非流式：响应对象形状
  {
    const mock = installMock();
    const r = await worker.fetch(responsesReq({ model: 'gpt-5.5', input: 'hi' }), workerEnv());
    assertEq(r.status, 200, 'resp: status');
    const j = await r.json();
    assertEq(j.object, 'response', 'resp: object');
    assertEq(j.status, 'completed', 'resp: completed');
    assertEq(j.output_text, 'Hello world', 'resp: output_text');
    assertEq(j.usage.input_tokens, 100, 'resp: usage input');
    assertEq(j.usage.output_tokens, 5, 'resp: usage output');
    assertEq(j.usage.total_tokens, 105, 'resp: usage total');
    assertEq(j.usage.input_tokens_details.cached_tokens, 80, 'resp: cached subset');
    const messageItem = j.output.find((o) => o.type === 'message');
    assertEq(messageItem.content[0].text, 'Hello world', 'resp: message item');
    mock.restore();
  }

  // input 数组（function_call 回路）→ 上游 tool 消息
  {
    const mock = installMock();
    await worker.fetch(responsesReq({
      model: 'gpt-5.5',
      input: [
        { role: 'user', content: 'hi' },
        { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'calling' }] },
        { type: 'function_call', call_id: 'fc1', name: 'fn', arguments: '{"a":1}' },
        { type: 'function_call_output', call_id: 'fc1', output: 'ok' },
      ],
    }), workerEnv());
    const wire = JSON.parse(mock.generateCalls()[0].body);
    const roles = wire.params.messages.map((m) => m.role);
    assertEq(roles, ['user', 'assistant', 'tool'], 'resp-wire: roles');
    assertEq(wire.params.messages[1].content.some((p) => p.type === 'tool-call'), true, 'resp-wire: tool-call part');
    mock.restore();
  }

  // previous_response_id → 400
  {
    const mock = installMock();
    const r = await worker.fetch(responsesReq({ model: 'm', input: 'x', previous_response_id: 'resp_123' }), workerEnv());
    assertEq(r.status, 400, 'resp: previous_response_id rejected');
    mock.restore();
  }

  // 流式：completed + 单调 sequence_number
  {
    const mock = installMock();
    const r = await worker.fetch(responsesReq({ model: 'gpt-5.5', input: 'hi', stream: true }), workerEnv());
    assertEq(r.status, 200, 'resp-stream: status');
    const text = await r.text();
    const events = text.split('\n\n').filter(Boolean).filter((b) => b.startsWith('event: ')).map((block) => {
      const ev = /event: (\S+)/.exec(block)[1];
      const data = JSON.parse(/data: (.+)/.exec(block)[1]);
      return { ev, data };
    });
    const order = events.map((e) => e.ev);
    assertEq(order[0], 'response.created', 'resp-stream: created first');
    assertEq(order[order.length - 1], 'response.completed', 'resp-stream: completed last');
    const seqs = events.map((e) => e.data.sequence_number);
    assertEq(seqs, seqs.map((_, i) => i), 'resp-stream: sequence monotonic');
    const completed = events[events.length - 1].data.response;
    assertEq(completed.output_text, 'Hello world', 'resp-stream: output_text');
    assertEq(completed.usage.output_tokens, 5, 'resp-stream: usage');
    mock.restore();
  }

  // 零输出 → 转移
  {
    const mock = installMock({ behavior: { 'acc-limited': 'zero', 'events:acc-limited': EMPTY_FINISH_EVENTS } });
    const r = await worker.fetch(responsesReq({ model: 'gpt-5.5', input: 'hi' }), workerEnv());
    assertEq(r.status, 200, 'resp-zero: failover ok');
    assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'resp-zero: failover order');
    mock.restore();
  }
}

// ============================ D1 路径端到端（node:sqlite 模拟 D1） ============================

async function testD1E2E() {
  const { DatabaseSync } = await import('node:sqlite');
  class MockD1 {
    constructor() { this.db = new DatabaseSync(':memory:'); }
    prepare(sql) {
      const self = this;
      const stmt = { sql, params: [] };
      stmt.bind = (...p) => { stmt.params = p; return stmt; };
      stmt.first = async () => self.db.prepare(sql).get(...stmt.params) ?? null;
      stmt.run = async () => {
        const info = self.db.prepare(sql).run(...stmt.params);
        return { meta: { last_row_id: Number(info.lastInsertRowid ?? 0), changes: info.changes } };
      };
      stmt.all = async () => ({ results: self.db.prepare(sql).all(...stmt.params) });
      return stmt;
    }
    async batch(stmts) { for (const s of stmts) await s.run(); }
  }

  const mock = installMock({ behavior: { 'acc-limited': 'limited' } });
  const db = new MockD1();
  const env = { API_BASE: 'https://upstream.test', DB: db, ALLOW_ANONYMOUS: 'true', ACCOUNTS: '[{"key":"acc-limited","label":"L"},{"key":"acc-good","label":"G"}]' };

  const r1 = await worker.fetch(chatReq(CHAT_REQUEST), env);
  assertEq(r1.status, 200, 'd1: failover returns 200');
  await r1.json();
  assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-limited', 'acc-good'], 'd1: failover order');

  mock.calls.length = 0;
  await (await worker.fetch(chatReq(CHAT_REQUEST), env)).json();
  assertEq(mock.generateCalls().map((c) => c.apiKey), ['acc-good'], 'd1: sticky across requests');

  const acct = db.db.prepare('SELECT requests, prompt_tokens, cache_read_tokens, last_used_at FROM accounts WHERE api_key = ?').get('acc-good');
  assertEq(acct.requests, 2, 'd1: requests counted');
  assertEq(acct.prompt_tokens, 200, 'd1: prompt tokens accumulated');
  assertEq(acct.cache_read_tokens, 160, 'd1: cache read tokens accumulated');
  assert(acct.last_used_at > 0, 'd1: last_used_at set (sticky signal)');

  // 成功请求只写 usage_buckets，不再写 usage_daily（同一 5 分钟桶合并）
  const dailyTables = db.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'usage_daily'").all();
  assertEq(dailyTables.length, 0, 'd1: usage_daily never created on fresh DB');
  const bucketRequests = db.db.prepare('SELECT SUM(requests) AS n FROM usage_buckets').get().n;
  assert(bucketRequests >= 2, 'd1: buckets written', bucketRequests);

  const limited = db.db.prepare('SELECT rate_limited_until FROM accounts WHERE api_key = ?').get('acc-limited');
  assert(limited.rate_limited_until > Date.now(), 'd1: 429 cooldown persisted');

  const H = { 'x-admin-token': '', 'Content-Type': 'application/json' };
  const add = await worker.fetch(new Request('https://w/api/accounts', {
    method: 'POST', headers: H, body: JSON.stringify({ key: 'another-key', label: 'X' }),
  }), env);
  assertEq(add.status, 201, 'd1: admin add ok (saveQuota works)');

  const st = await (await worker.fetch(new Request('https://w/api/state'), env)).json();
  assertEq(st.d1, true, 'd1: state reports d1 backend');
  assertEq(st.totals.accounts, 3, 'd1: state account count');
  mock.restore();

  // 旧版本表缺少新增列时，首次访问自动完成兼容迁移（含 usage_daily → buckets）。
  const legacy = new MockD1();
  legacy.db.exec(`
    CREATE TABLE accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      api_key TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL DEFAULT '',
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE usage_daily (
      day TEXT NOT NULL,
      account_id INTEGER NOT NULL,
      requests INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (day, account_id)
    );
    INSERT INTO accounts (api_key, created_at) VALUES ('legacy-key', 1);
  `);
  const legacyDay = new Date(Date.now() + 8 * 3600e3).toISOString().slice(0, 10);
  legacy.db.prepare('INSERT INTO usage_daily (day, account_id, requests) VALUES (?, ?, ?)').run(legacyDay, 1, 9);
  const legacyEnv = { API_BASE: 'https://upstream.test', DB: legacy, ALLOW_ANONYMOUS: 'true' };
  const legacyState = await worker.fetch(new Request('https://w/api/state'), legacyEnv);
  assertEq(legacyState.status, 200, 'd1-migration: legacy schema accepted');
  const accountColumns = legacy.db.prepare('PRAGMA table_info(accounts)').all().map((row) => row.name);
  assert(accountColumns.includes('prompt_tokens') && accountColumns.includes('detail'), 'd1-migration: account columns added');
  const legacyUsage = await (await worker.fetch(new Request('https://w/api/usage/daily?range=30d'), legacyEnv)).json();
  assertEq(legacyUsage.range, '30d', 'd1-migration: range response');
  assertEq(legacyUsage.totals.requests, 9, 'd1-migration: legacy usage copied to bucket');
}

// ============================ 运行 ============================

testBuildCcRequest();
testErrorMapping();
await testTranslators();
await testProtocolIdentity();
testProtocolConversion();
await testPoolSelection();
await testE2E();
await testMessagesE2E();
await testResponsesE2E();
await testD1E2E();

console.log(`\n${passed} passed, ${failed} failed`);
if (failures.length) {
  console.log('FAILURES:');
  for (const f of failures) console.log('  ✗ ' + f);
  process.exit(1);
}

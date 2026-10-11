// test/run-tests.mjs
// Command Code Pool 全套单元测试与集成测试
import fs from 'node:fs';
import path from 'node:path';
import {
  KNOWN_PLANS,
  planInfo,
  CC_PROTOCOL_VERSION,
} from '../lib/constants.js';

// 清理测试临时数据
try {
  fs.rmSync(path.resolve(process.cwd(), '.data'), { recursive: true, force: true });
  fs.rmSync('/tmp/commandcode-pool.json', { force: true });
} catch (e) {}
import {
  safeEqual,
  maskKey,
  generateTraceparent,
  usageBucketStart,
} from '../lib/crypto-utils.js';
import {
  generateFingerprint,
  sessionForKey,
} from '../lib/fingerprint.js';
import {
  buildCcRequest,
  convertAnthropicToOpenAI,
  convertResponsesToChat,
  createSseTranslator,
  createAnthropicSseTranslator,
  createResponsesSseTranslator,
  mapFinishReason,
  mapCcError,
  mapCcEventError,
  fakeThinkingSignature,
} from '../lib/api-adapters.js';
import {
  evaluateAccountAction,
  parseRetryAfter,
  mapClientError,
} from '../lib/failure-policy.js';
import {
  MemPool,
  mergeQuotaState,
  rowToAccount,
} from '../lib/pool-store.js';
import { handleRequest } from '../api/index.js';

let passed = 0;
let failed = 0;
const errors = [];

function assert(condition, testName, extra) {
  if (condition) {
    passed++;
  } else {
    failed++;
    errors.push({ testName, extra });
    console.error(`❌ FAIL: ${testName}`, extra ? extra : '');
  }
}

function assertEq(actual, expected, testName) {
  const isMatch = JSON.stringify(actual) === JSON.stringify(expected);
  assert(isMatch, testName, { actual, expected });
}

console.log('--- 开始测试 CommandCode-Pool ---');

// 1. Constants & Go Plan 验证
const goPlan = planInfo('individual-go');
assert(goPlan !== undefined, 'Go 套餐可被正确识别');
assertEq(goPlan.name, 'Go', 'Go 套餐名称为 Go');
assertEq(goPlan.monthlyCredits, 10, 'Go 套餐月度额度为 10');
assertEq(goPlan.isGoPlan, true, 'Go 套餐标记 isGoPlan 为 true');

const goatPlan = planInfo('individual-goat');
assertEq(goatPlan.name, 'GOAT', 'GOAT 套餐名称为 GOAT');
assertEq(goatPlan.monthlyCredits, 70, 'GOAT 套餐月度额度为 70');
assertEq(goatPlan.isGoPlan, false, 'GOAT 套餐 isGoPlan 为 false');

// 2. Crypto Utils
assert(safeEqual('abc', 'abc'), 'safeEqual 正确匹配');
assert(!safeEqual('abc', 'xyz'), 'safeEqual 正确拒绝不同字符串');
assert(!safeEqual('abc', 'abcd'), 'safeEqual 正确拒绝不同长度字符串');

assertEq(maskKey('user_1234567890abcdef'), 'user_1…cdef', 'maskKey 正确打码');
assert(generateTraceparent().startsWith('00-'), 'traceparent 格式符合 W3C 规范');

const bucket = usageBucketStart(1700000000000, 8);
assert(typeof bucket === 'number' && bucket > 0, 'usageBucketStart 计算时间戳正常');

// 3. 确定性设备指纹 (Deterministic Device Fingerprint)
const testKey = 'user_test_fake_api_key_123456789';
const fp1 = generateFingerprint(testKey);
const fp2 = generateFingerprint(testKey);
assertEq(fp1.hash, fp2.hash, '同一 API Key 派生出确定性指纹哈希（防漂移）');
assertEq(fp1.components.cpuModel, fp2.components.cpuModel, '同一 API Key 硬件 CPU 型号稳定一致');
assertEq(fp1.components.machineId, fp2.components.machineId, '同一 API Key machineId 稳定一致');

const sess1 = sessionForKey(testKey);
const sess2 = sessionForKey(testKey);
assertEq(sess1, sess2, '同一 API Key 派生出稳定的 sessionId UUID');

// 4. API 请求适配器与 9 键信封构建
const sampleOpenAiReq = {
  model: 'deepseek/deepseek-v4-flash',
  messages: [
    { role: 'system', content: 'You are helpful.' },
    { role: 'user', content: 'Hello' },
  ],
  temperature: 0.7,
  reasoning_effort: 'high',
};

const ccEnvelope = buildCcRequest(sampleOpenAiReq, { emptySystemPlaceholder: true });
assertEq(ccEnvelope.mode, 'agent', '信封 mode 为 agent');
assertEq(ccEnvelope.params.model, 'deepseek/deepseek-v4-flash', '模型透传一致');
assertEq(ccEnvelope.params.reasoning_effort, 'high', '推理强度 high 透传一致');
assertEq(ccEnvelope.params.system[0].text, 'You are helpful.', '系统提示词正确打包');

// 空系统提示词占位符测试（防止上游注入 7.5K 默认提示词）
const emptySysReq = {
  model: 'deepseek/deepseek-v4-flash',
  messages: [{ role: 'user', content: 'Hi' }],
};
const emptySysEnvelope = buildCcRequest(emptySysReq, { emptySystemPlaceholder: true });
assertEq(emptySysEnvelope.params.system[0].text, ' ', '空系统提示词正确注入单个空格占位符');

// Anthropic 协议转换测试
const anthropicReq = {
  model: 'claude-3-5-sonnet',
  system: 'Be concise',
  messages: [
    { role: 'user', content: 'Test' },
  ],
  thinking: { type: 'enabled', budget_tokens: 8000 },
};
const convertedToOpenAi = convertAnthropicToOpenAI(anthropicReq);
assertEq(convertedToOpenAi.messages[0].role, 'system', 'Anthropic system 成功转为 system 消息');
assertEq(convertedToOpenAi.reasoning_effort, 'medium', '8000 budget_tokens 映射为 medium 推理强度');

// Responses 协议转换测试
const responsesReq = {
  model: 'gpt-4o',
  instructions: 'System instructions',
  input: 'Hello Responses',
};
const convertedResponses = convertResponsesToChat(responsesReq);
assertEq(convertedResponses.messages[0].role, 'system', 'Responses instructions 转为 system 消息');
assertEq(convertedResponses.messages[1].content, 'Hello Responses', 'Responses input 转换为 user 消息');

// 5. SSE 翻译器测试
const sseTranslator = createSseTranslator('deepseek/deepseek-v4-flash', 'chatcmpl-test', 1700000000);
const chunk1 = sseTranslator.parseLine('{"type":"text-delta","text":"Hello"}');
assert(chunk1 && chunk1[0].includes('chat.completion.chunk'), 'OpenAI SSE 翻译产出正确 chunk 格式');
assert(chunk1[0].includes('Hello'), 'OpenAI SSE chunk 包含内容');

const finishChunk = sseTranslator.parseLine('{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":10,"outputTokens":5}}');
assert(finishChunk && finishChunk[0].includes('finish_reason'), 'OpenAI SSE 正确产出 finish chunk');

// Anthropic SSE 翻译器测试
const anthropicTranslator = createAnthropicSseTranslator('claude-3-5-sonnet', 'msg-test');
assert(anthropicTranslator.preamble().includes('message_start'), 'Anthropic SSE 产出 message_start 前导帧');

// 6. 错误映射与健康管理 (Failure Policy)
const retryHeaders = new Headers({ 'retry-after': '45' });
assertEq(parseRetryAfter(retryHeaders), 45, 'Retry-After 成功解析为 45 秒');

const action429 = evaluateAccountAction(429, '', retryHeaders);
assertEq(action429.action, 'cooldown', '429 触发账号冷却');
assertEq(action429.seconds, 45, '429 冷却时间对齐 Retry-After 45 秒');

const action401 = evaluateAccountAction(401);
assertEq(action401.action, 'disable', '401 触发账号停用');

const action500 = evaluateAccountAction(500);
assertEq(action500.action, 'cooldown', '500 触发短暂冷却换号');

// 7. 号池管理与 Go 套餐用量记录 (MemPool & Quota State)
import { getConfig } from '../lib/config.js';
import { getPoolStore } from '../lib/pool-store.js';

const config = getConfig();
const pool = getPoolStore(config);
const acctId = await pool.add('user_test_go_key_1', 'go-test-1');
assert(acctId > 0, '成功添加账号到号池');

const mockGoReport = {
  account: { userName: 'gouser1' },
  plan: { planId: 'individual-go', planName: 'Go', isGoPlan: true },
  credits: { monthlyLeft: 6.5, purchased: 0, free: 0, monthlyCap: 10, monthlyUsed: 3.5 },
  fiveHour: { used: 1.2, cap: 5.0, exceeded: false, resetAt: Date.now() + 3600000 },
  weekly: { used: 3.5, cap: 20.0, exceeded: false, resetAt: Date.now() + 86400000 },
  summary: { requests: 42, creditsSpent: 3.5 },
};

await pool.saveQuota(acctId, mockGoReport);
const list = await pool.list();
const savedAcct = list.find(a => a.id === acctId);

assert(savedAcct !== undefined, '成功读取保存的账号');
assertEq(savedAcct.plan.name, 'Go', '保存的套餐名称为 Go');
assertEq(savedAcct.plan.isGoPlan, true, '识别为 Go 套餐');
assertEq(savedAcct.credits.monthlyCap, 10, 'Go 套餐月度上限正确保存为 10');
assertEq(savedAcct.credits.monthlyUsed, 3.5, 'Go 套餐已用额度推导正确 (10 - 6.5 = 3.5)');
assertEq(savedAcct.credits.fiveHour.used, 1.2, '5小时滚动窗口用量记录正确');

// 模拟调用成功记录网关用量
await pool.recordSuccess(acctId, {
  prompt_tokens: 150,
  completion_tokens: 30,
  prompt_tokens_details: { cached_tokens: 100 },
});

const freshList = await pool.list();
const usedAcct = freshList.find(a => a.id === acctId);
assertEq(usedAcct.usage.requests, 1, '网关累计请求计数累加成功');
assertEq(usedAcct.usage.promptTokens, 150, '网关输入 token 记录正确');
assertEq(usedAcct.usage.completionTokens, 30, '网关输出 token 记录正确');
assertEq(usedAcct.usage.cacheReadTokens, 100, '网关缓存命中 token 记录正确');

// 调度策略测试 (Sticky 优先选择最近使用的账号)
const pickedSticky = await pool.pick('sticky');
assertEq(pickedSticky.id, acctId, 'Sticky 策略优先选择最近使用账号');

// 8. Vercel Web Request 路由处理集成测试
console.log('--- 测试 Vercel HTTP 端点 ---');

// 测试 /health
const healthReq = new Request('http://localhost:3000/health');
const healthRes = await handleRequest(healthReq);
assertEq(healthRes.status, 200, '/health 返回 200');
const healthJson = await healthRes.json();
assertEq(healthJson.ok, true, '/health ok 字段为 true');

// 测试 /v1/models
const modelsReq = new Request('http://localhost:3000/v1/models');
const modelsRes = await handleRequest(modelsReq);
assertEq(modelsRes.status, 200, '/v1/models 返回 200');
const modelsJson = await modelsRes.json();
assert(Array.isArray(modelsJson.data) && modelsJson.data.length > 0, '/v1/models 返回非空模型列表');

// 测试 /api/login 免密状态
const loginReq = new Request('http://localhost:3000/api/login', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ password: '' }),
});
const loginRes = await handleRequest(loginReq);
assertEq(loginRes.status, 200, '/api/login 验证成功');

// 测试 /api/auth/browser-url 浏览器登录辅助
const browserUrlReq = new Request('http://localhost:3000/api/auth/browser-url');
const browserUrlRes = await handleRequest(browserUrlReq);
assertEq(browserUrlRes.status, 200, '/api/auth/browser-url 返回 200');
const browserUrlJson = await browserUrlRes.json();
assert(browserUrlJson.authUrl.includes('commandcode.ai/studio/auth/cli'), '生成合法的 Studio CLI 授权 URL');

// 9. Mock 上游推理调用测试（OpenAI / Anthropic / Responses & 故障切换）
console.log('--- 测试 Mock 上游推理调用与故障转移 ---');

const origFetch = globalThis.fetch;
let mockResponses = [];

globalThis.fetch = async (url, options) => {
  const urlStr = String(url);
  if (urlStr.includes('/alpha/fingerprint/record') || urlStr.includes('/alpha/lifecycle-events')) {
    return new Response(JSON.stringify({ ok: true }), { status: 200 });
  }
  if (urlStr.includes('/alpha/generate')) {
    if (mockResponses.length > 0) {
      const nextResp = mockResponses.shift();
      if (typeof nextResp === 'function') return nextResp(url, options);
      return nextResp;
    }
    // 默认成功返回 NDJSON 流
    const events = [
      '{"type":"start"}\n',
      '{"type":"text-start","id":"t1"}\n',
      '{"type":"text-delta","text":"Hello, CommandCode Go!"}\n',
      '{"type":"text-end","id":"t1"}\n',
      '{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":25,"outputTokens":10,"cachedInputTokens":15}}\n',
    ];
    return new Response(events.join(''), {
      status: 200,
      headers: { 'Content-Type': 'text/event-stream' },
    });
  }
  return origFetch(url, options);
};

// 测试 9.1: 非流式 OpenAI 调用 (包括 Go 套餐账号)
const chatReq = new Request('http://localhost:3000/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    model: 'deepseek/deepseek-v4-flash',
    messages: [{ role: 'user', content: 'Say hello' }],
    stream: false,
  }),
});
const chatRes = await handleRequest(chatReq);
assertEq(chatRes.status, 200, '非流式 OpenAI Chat 返回 200');
const chatJson = await chatRes.json();
assertEq(chatJson.choices[0].message.content, 'Hello, CommandCode Go!', '非流式响应内容正确提取');
assertEq(chatJson.usage.prompt_tokens, 25, '返回准确的 prompt_tokens');
assertEq(chatJson.usage.completion_tokens, 10, '返回准确的 completion_tokens');

// 测试 9.2: 流式 OpenAI 调用
const streamReq = new Request('http://localhost:3000/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    model: 'deepseek/deepseek-v4-flash',
    messages: [{ role: 'user', content: 'Say hello' }],
    stream: true,
  }),
});
const streamRes = await handleRequest(streamReq);
assertEq(streamRes.status, 200, '流式 OpenAI Chat 返回 200');
assert(streamRes.headers.get('content-type').includes('text/event-stream'), '流式返回 text/event-stream 头');
const streamText = await streamRes.text();
assert(streamText.includes('data:'), '流式包含 data: SSE 帧');
assert(streamText.includes('Hello, CommandCode Go!'), '流式包含文本 delta');
assert(streamText.includes('[DONE]'), '流式包含 [DONE] 结束标记');

// 测试 9.3: Anthropic Messages 调用
const anthropicCallReq = new Request('http://localhost:3000/v1/messages', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    model: 'claude-3-5-sonnet',
    messages: [{ role: 'user', content: 'Hi' }],
    stream: false,
  }),
});
const anthropicCallRes = await handleRequest(anthropicCallReq);
assertEq(anthropicCallRes.status, 200, 'Anthropic Messages 返回 200');
const anthropicCallJson = await anthropicCallRes.json();
assertEq(anthropicCallJson.type, 'message', '返回合法 Anthropic message 格式');
assertEq(anthropicCallJson.content[0].text, 'Hello, CommandCode Go!', 'Anthropic 文本内容转换正确');

// 测试 9.4: 故障转移与 429 冷却 (当前账号报 429 自动冷却并换号)
const acc2Id = await pool.add('user_failover_2', 'acct-2');

// 注入模拟上游：第一次 429，第二次 200
mockResponses = [
  new Response(JSON.stringify({ error: { message: 'rate limited' } }), {
    status: 429,
    headers: { 'Retry-After': '30' },
  }),
  new Response('{"type":"start"}\n{"type":"text-delta","text":"Failover succeeded!"}\n{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":5,"outputTokens":2}}\n', {
    status: 200,
    headers: { 'Content-Type': 'text/event-stream' },
  }),
];

const failoverReq = new Request('http://localhost:3000/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    model: 'deepseek/deepseek-v4-flash',
    messages: [{ role: 'user', content: 'Test failover' }],
    stream: false,
  }),
});
const failoverRes = await handleRequest(failoverReq);
assertEq(failoverRes.status, 200, '429 触发故障转移后下游客户端仍返回 200 成功');
const failoverJson = await failoverRes.json();
assertEq(failoverJson.choices[0].message.content, 'Failover succeeded!', '故障转移自动切换到正常账号响应');

globalThis.fetch = origFetch;

console.log(`\n测试结果：通过 ${passed} 项，失败 ${failed} 项`);
if (failed > 0) {
  console.error('存在测试未通过：', errors);
  process.exit(1);
} else {
  console.log('🎉 所有单元测试与集成测试全部通过！');
  try {
    fs.rmSync(path.resolve(process.cwd(), '.data'), { recursive: true, force: true });
    fs.rmSync('/tmp/commandcode-pool.json', { force: true });
  } catch (e) {}
}

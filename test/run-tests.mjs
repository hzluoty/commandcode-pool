// test/run-tests.mjs
// Command Code Pool 全套单元测试与集成测试
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import http from 'node:http';
import {
  KNOWN_PLANS,
  planInfo,
  CC_PROTOCOL_VERSION,
  USAGE_RANGES,
  MAX_USAGE_RANGE_SECONDS,
  USAGE_RETENTION_SECONDS,
  MAX_REQUEST_BODY_BYTES,
  MAX_TOOL_IMAGE_BYTES,
} from '../lib/constants.js';

// 清理测试临时数据
try {
  fs.rmSync(path.resolve(process.cwd(), '.data'), { recursive: true, force: true });
  fs.rmSync('/tmp/commandcode-pool.json', { force: true });
} catch (e) {}
import {
  safeEqual,
  maskKey,
  sanitizeApiKey,
  generateTraceparent,
  usageBucketStart,
  readBody,
} from '../lib/crypto-utils.js';
import {
  generateFingerprint,
  sessionForKey,
} from '../lib/fingerprint.js';
import { fetchReport } from '../lib/quota-service.js';
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
  collectCcStream,
} from '../lib/api-adapters.js';
import {
  evaluateAccountAction,
  parseRetryAfter,
  mapClientError,
} from '../lib/failure-policy.js';
import {
  MemPool,
  RedisPool,
  UpstashKvPool,
  normalizeUsageRange,
  mergeQuotaState,
  rowToAccount,
} from '../lib/pool-store.js';
import handler, { handleRequest } from '../api/index.js';

let passed = 0;
let failed = 0;
const errors = [];

function redactForLog(value) {
  if (value == null) return value;
  if (typeof value === 'string') return sanitizeApiKey(value);
  if (Array.isArray(value)) return value.map(redactForLog);
  if (typeof value === 'object') {
    const sensitiveKeys = new Set(['api_key', 'apikey', 'apiKey', 'authorization', 'token', 'secret', 'password']);
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (sensitiveKeys.has(k)) {
        out[k] = typeof v === 'string' ? sanitizeApiKey(v) : '[REDACTED]';
      } else {
        out[k] = redactForLog(v);
      }
    }
    return out;
  }
  return value;
}

function assert(condition, testName, extra) {
  if (condition) {
    passed++;
  } else {
    failed++;
    errors.push({ testName, extra });
    console.error(`❌ FAIL: ${testName}`, extra ? redactForLog(extra) : '');
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

assert(!USAGE_RANGES['30d'], '移除 30 天用量范围');
assertEq(MAX_USAGE_RANGE_SECONDS, 7 * 86400, '最大用量范围为 7 天');
assertEq(USAGE_RETENTION_SECONDS, MAX_USAGE_RANGE_SECONDS + 86400, '保留时间随最大查询范围计算');
assertEq(normalizeUsageRange('30d').key, '1d', '旧 30d 查询降级为 1d');
assertEq(normalizeUsageRange('90').key, '1d', '超长数字范围降级为 1d');
assertEq(normalizeUsageRange('7').key, '7d', '保留数字天数兼容');
assertEq(normalizeUsageRange('2d').seconds, 2 * 86400, '保留范围内自定义天数');
assertEq(normalizeUsageRange('5garbage').key, '1d', '拒绝不完整的天数格式');
assertEq(normalizeUsageRange('constructor').key, '1d', '范围名称不得命中对象原型属性');
assertEq(normalizeUsageRange({ key: '30d', seconds: 30 * 86400 }).key, '1d', '对象不能绕过最大范围限制');
assertEq(normalizeUsageRange({ key: '7d', seconds: Infinity }).seconds, MAX_USAGE_RANGE_SECONDS, '对象范围使用服务端定义');
assert(!fs.readFileSync(new URL('../public/admin.html', import.meta.url), 'utf8').includes('data-range="30d"'), '管理页移除 30 天按钮');

// 2. Crypto Utils
assert(safeEqual('abc', 'abc'), 'safeEqual 正确匹配');
assert(!safeEqual('abc', 'xyz'), 'safeEqual 正确拒绝不同字符串');
assert(!safeEqual('abc', 'abcd'), 'safeEqual 正确拒绝不同长度字符串');

assertEq(sanitizeApiKey('\u001b[200~ user_abc\n\u001b[201~ '), 'user_abc', 'sanitizeApiKey 去除粘贴包装与空白');
assertEq(maskKey('user_1234567890abcdef'), 'user_1…cdef', 'maskKey 正确打码');
assert(generateTraceparent().startsWith('00-'), 'traceparent 格式符合 W3C 规范');

const bucket = usageBucketStart(1700000000000, 8);
assert(typeof bucket === 'number' && bucket > 0, 'usageBucketStart 计算时间戳正常');
const originalQuotaFetch = globalThis.fetch;
globalThis.fetch = async (_url, options) => ({
  ok: true,
  text: () => new Promise(resolve => options.signal.addEventListener('abort', () => resolve('{"ok":true}'), { once: true })),
});
let quotaResult;
try { quotaResult = await fetchReport('https://quota.invalid', 'user_timeout_key'); } finally { globalThis.fetch = originalQuotaFetch; }
assert(quotaResult && !Object.hasOwn(quotaResult, 'error'), '配额响应体在共享 deadline 内读取并解析 JSON');
globalThis.fetch = originalQuotaFetch;
const tooLargeBody = new Request('http://localhost', { method: 'POST', headers: { 'content-length': String(MAX_REQUEST_BODY_BYTES + 1) }, body: '{}' });
let bodyError;
try { await readBody(tooLargeBody); } catch (error) { bodyError = error; }
assertEq(bodyError?.status, 413, 'readBody 按 Content-Length 拒绝超限请求');
const oversizedStream = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(MAX_REQUEST_BODY_BYTES + 1)); } });
bodyError = null;
try { await readBody(new Request('http://localhost', { method: 'POST', body: oversizedStream, duplex: 'half' })); } catch (error) { bodyError = error; }
assertEq(bodyError?.status, 413, 'readBody 按实际字节数拒绝超限请求');

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
const wireUuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
assert(wireUuidPattern.test(sess1), '会话派生值包含合法 UUID version/variant 位');
assert(Array.from({ length: 32 }, (_, i) => sessionForKey(`user_uuid_${i}`)).every(id => wireUuidPattern.test(id)), '所有派生会话均为合法 UUID v4');
assertEq(sessionForKey(testKey, { fingerprintSalt: 'salt' }), sessionForKey(testKey, { fingerprintSalt: 'salt' }), '带盐的会话身份保持确定性');
assert(sessionForKey(testKey, { fingerprintSalt: 'salt' }) !== sess1, '不同盐派生不同会话');

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
assertEq(ccEnvelope.config.environment, 'win32', 'environment 为与设备指纹平台一致的字符串');
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
const responseImage = 'data:image/png;base64,' + Buffer.from('ordinary').toString('base64');
const responseImageRequest = convertResponsesToChat({ input: [{
  type: 'message', role: 'user', content: [
    { type: 'input_text', text: 'describe' },
    { type: 'input_image', image_url: responseImage },
  ],
}] });
assertEq(responseImageRequest.messages[0].role, 'user', '普通 Responses message 保留 user 角色而不产生空 assistant');
const responseImageWire = buildCcRequest(responseImageRequest).params.messages[0];
assertEq(responseImageWire.content, [
  { type: 'text', text: 'describe' },
  { type: 'image', image: responseImage, mimeType: 'image/png' },
], '普通 Responses input_image 精确映射为上游 text 与 image parts');
let systemImageRejected = false;
try { buildCcRequest(convertResponsesToChat({ input: [{ type: 'message', role: 'system', content: [{ type: 'input_image', image_url: responseImage }] }] })); } catch { systemImageRejected = true; }
assert(systemImageRejected, 'system/developer image inputs are explicitly rejected');
const toolImageUrl = 'data:image/png;base64,' + Buffer.from('img').toString('base64');
const convertedToolImages = convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'c1', name: 'shot', arguments: '{}' },
  { type: 'function_call', call_id: 'c2', name: 'shot', arguments: '{}' },
  { type: 'function_call_output', call_id: 'c1', output: `first ${toolImageUrl}` },
  { type: 'function_call_output', call_id: 'c2', output: [{ type: 'input_image', image_url: { url: toolImageUrl } }] },
] });
assertEq(convertedToolImages.messages.map(message => message.role), ['assistant', 'tool', 'tool'], '并行工具回复保持连续，图片按工具文本占位符处理');
assertEq(convertedToolImages.messages.slice(1, 3).map(message => message.tool_call_id), ['c1', 'c2'], '并行工具回复仍关联各自调用 ID');
assertEq(convertedToolImages.messages[1].content, 'first [image]', 'tool output 文本保留图片占位符');
const toolImageWire = buildCcRequest(convertedToolImages).params.messages;
assertEq(toolImageWire.map(message => message.role), ['assistant', 'tool', 'tool'], 'Responses 工具调用与结果转换为上游标准 role 顺序');
assertEq(toolImageWire[0].content.map(part => part.type), ['tool-call', 'tool-call'], '上游 assistant 消息包含并行 tool-call');
assertEq(toolImageWire[0].content.map(part => part.toolName), ['shot', 'shot'], '上游并行 tool-call 名称与调用保持关联');
assertEq(toolImageWire[1].content[0], { type: 'tool-result', toolCallId: 'c1', toolName: 'shot', output: { type: 'text', value: 'first [image]' } }, '上游 role=tool 消息保留第一条结果文本与 call ID');
assertEq(toolImageWire[2].content[0], { type: 'tool-result', toolCallId: 'c2', toolName: 'shot', output: { type: 'text', value: '[image]' } }, '第二条工具图片以占位文本保留在其 call_id 对应结果中');
const cappedToolImages = convertResponsesToChat({ input: [{ type: 'function_call_output', output: `data:image/png;base64,${'A'.repeat(MAX_TOOL_IMAGE_BYTES + 1)}` }] });
assert(!JSON.stringify(cappedToolImages.messages).includes('data:image/png'), '超限工具图像不会进入上游请求');
assert(cappedToolImages.messages.at(-1).content.includes('image omitted'), '超限工具图像以文本占位说明而不是丢弃 tool result');
const assistantAfterToolImage = convertResponsesToChat({ input: [
  { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'inspect' }] },
  { type: 'function_call', call_id: 'img-call', name: 'shot', arguments: '{}' },
  { type: 'function_call_output', call_id: 'img-call', output: toolImageUrl },
  { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'done' }] },
] });
assertEq(assistantAfterToolImage.messages.map(message => message.role), ['user', 'assistant', 'tool', 'assistant'], '工具图片占位符留在对应 tool result，assistant 续写保序');
assertEq(assistantAfterToolImage.messages[2].tool_call_id, 'img-call', '工具图片前的工具回复保持调用关联');
const userAfterToolImage = convertResponsesToChat({ input: [
  { type: 'function_call_output', output: toolImageUrl },
  { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'next' }] },
] });
assertEq(userAfterToolImage.messages.map(message => message.role), ['tool', 'user'], '工具图片占位符不产生脱离 call_id 的 user 消息');
assertEq(userAfterToolImage.messages[0].content, '[image]', '工具图片占位符保留在对应 tool result');
assertEq(buildCcRequest(userAfterToolImage).params.messages[0].content[0].toolName, '', '没有匹配 function_call 的 tool output 不伪造工具名称');
const imagesFollowedByUser = convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'a', name: 'shot', arguments: '{}' },
  { type: 'function_call', call_id: 'b', name: 'shot', arguments: '{}' },
  { type: 'function_call_output', call_id: 'a', output: toolImageUrl },
  { type: 'function_call_output', call_id: 'b', output: 'second result' },
  { type: 'message', role: 'user', content: 'next turn' },
] });
assertEq(imagesFollowedByUser.messages.map(message => message.role), ['assistant', 'tool', 'tool', 'user'], '工具图片占位符不改变并行工具轮次结构');
assertEq(imagesFollowedByUser.messages.slice(1, 3).map(message => message.tool_call_id), ['a', 'b'], '带图片的并行工具结果保持连续');
const reusedCallId = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'same', name: 'first', arguments: '{}' },
  { type: 'function_call_output', call_id: 'same', output: 'one' },
  { type: 'function_call', call_id: 'same', name: 'second', arguments: '{}' },
  { type: 'function_call_output', call_id: 'same', output: 'two' },
] })).params.messages;
assertEq(reusedCallId.map(message => message.role), ['assistant', 'tool', 'assistant', 'tool'], '重用 call_id 的多轮工具结果保持批次顺序');
assertEq(reusedCallId.map(message => message.content[0].toolName), ['first', 'first', 'second', 'second'], '重用 call_id 时每个 tool result 使用本轮名称');
const reasoningThenTool = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'reasoning-call', name: 'inspect', arguments: '{}' },
  { type: 'reasoning', summary: [{ text: 'thinking' }] },
  { type: 'function_call_output', call_id: 'reasoning-call', output: 'found' },
] })).params.messages;
assertEq(reasoningThenTool.map(message => message.role), ['assistant', 'tool'], 'reasoning 与 tool call 合并在同一 assistant 消息');
assertEq(reasoningThenTool[1].content[0].toolName, 'inspect', 'reasoning 插入后 tool result 保留调用名称');
const toolAfterAssistantItem = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'deferred-call', name: 'deferred_tool', arguments: '{}' },
  { type: 'message', role: 'assistant', content: 'tool transcript' },
  { type: 'function_call_output', call_id: 'deferred-call', output: 'done' },
] })).params.messages;
assertEq(toolAfterAssistantItem.at(-1).content[0].toolName, 'deferred_tool', 'assistant 文本分段后仍按 call_id 关联尚未返回的工具');
const toolOutputAfterReasoning = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'reasoning-call', name: 'reasoned_tool', arguments: '{}' },
  { type: 'reasoning', summary: [{ text: 'tool is running' }] },
  { type: 'function_call_output', call_id: 'reasoning-call', output: 'ready' },
] })).params.messages;
assertEq(toolOutputAfterReasoning.at(-1).content[0].toolName, 'reasoned_tool', 'reasoning 分段后未完成的工具仍保留调用名称');
const toolOutputAfterAssistantMessage = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'message-call', name: 'message_tool', arguments: '{}' },
  { type: 'message', role: 'assistant', content: 'tool transcript' },
  { type: 'function_call_output', call_id: 'message-call', output: 'done' },
] })).params.messages;
assertEq(toolOutputAfterAssistantMessage.at(-1).content[0].toolName, 'message_tool', 'assistant transcript 分段后未完成的工具仍保留调用名称');
const reasoningBetweenCalls = buildCcRequest(convertResponsesToChat({ input: [
  { type: 'function_call', call_id: 'first', name: 'first_tool', arguments: '{}' },
  { type: 'reasoning', summary: [{ text: 'between call batches' }] },
  { type: 'function_call', call_id: 'second', name: 'second_tool', arguments: '{}' },
  { type: 'function_call_output', call_id: 'first', output: 'one' },
  { type: 'function_call_output', call_id: 'second', output: 'two' },
] })).params.messages;
assertEq(reasoningBetweenCalls.map(message => message.role), ['assistant', 'assistant', 'tool', 'tool'], 'reasoning 分批时先发送所有 tool-call 再发送对应结果');
assertEq(reasoningBetweenCalls.filter(message => message.role === 'assistant').map(message => message.content.map(part => part.type)), [['reasoning', 'tool-call'], ['tool-call']], 'reasoning 保留于首批调用，不与下一批 tool-call 合并');
assertEq(reasoningBetweenCalls.filter(message => message.role === 'tool').map(message => message.content[0].toolName), ['first_tool', 'second_tool'], 'reasoning 后两批工具结果仍关联各自名称');
for (const userInput of ['new turn', { type: 'message', role: 'user', content: 'new turn' }]) {
  const staleCallId = buildCcRequest(convertResponsesToChat({ input: [
    { type: 'function_call', call_id: 'reused', name: 'old_tool', arguments: '{}' },
    userInput,
    { type: 'function_call_output', call_id: 'reused', output: 'unmatched' },
  ] })).params.messages;
  assertEq(staleCallId.at(-1).content[0].toolName, '', '跨 user 轮次的未完成 call_id 不复用旧工具名');
}
const invalidImage = convertResponsesToChat({ input: [{ type: 'message', role: 'user', content: [{ type: 'input_image', image_url: { url: 'javascript:alert(1)' } }] }] });
assertEq(invalidImage.messages[0].content, '', '非 HTTP(S) 和非 data 的图片 URI 被拒绝');

// 5. SSE 翻译器测试
const sseTranslator = createSseTranslator('deepseek/deepseek-v4-flash', 'chatcmpl-test', 1700000000);
const chunk1 = sseTranslator.parseLine('{"type":"text-delta","text":"Hello"}');
assert(chunk1 && chunk1[0].includes('chat.completion.chunk'), 'OpenAI SSE 翻译产出正确 chunk 格式');
assert(chunk1[0].includes('Hello'), 'OpenAI SSE chunk 包含内容');

assertEq(sseTranslator.parseLine('{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":10,"outputTokens":5}}'), null, 'OpenAI 完成帧暂存，不在 parseLine 提前发送');
assert(sseTranslator.finalize().includes('finish_reason'), 'OpenAI finalize 产出完成帧');

// Anthropic SSE 翻译器测试
const anthropicTranslator = createAnthropicSseTranslator('claude-3-5-sonnet', 'msg-test');
assert(anthropicTranslator.preamble().includes('message_start'), 'Anthropic SSE 产出 message_start 前导帧');

// 完成帧用量不能依赖末尾换行或旧 finish-step 快照。
const usageEvent = { type: 'finish', finishReason: 'stop', usage: { inputTokens: 31, outputTokens: 12, cachedInputTokens: 7 } };
// 旧步骤快照不得泄漏进最终用量（含缓存细分），三种协议一致。
const stepSnapshot = {
  type: 'finish-step',
  usage: { inputTokens: 100, outputTokens: 20, cachedInputTokens: 70, inputTokenDetails: { noCacheTokens: 30, cacheWriteTokens: 10 } },
};
for (const translator of [createSseTranslator('model', 'id', 1), createAnthropicSseTranslator('model', 'id'), createResponsesSseTranslator('model', 'id', 1)]) {
  await translator.parseLine(JSON.stringify(stepSnapshot));
  await translator.parseLine(JSON.stringify(usageEvent));
  assertEq([translator.inputTokens, translator.outputTokens, translator.cachedInputTokens], [31, 12, 7], 'finish.usage 整份替换旧 step 用量');
}
const stepLeaked = await collectCcStream(new Response(JSON.stringify(stepSnapshot) + '\n' + JSON.stringify(usageEvent)));
assertEq(stepLeaked.usage, usageEvent.usage, '非流式最终用量整份替换旧 step 快照');
const stepOnlyUsage = await collectCcStream(new Response(JSON.stringify(stepSnapshot)));
assertEq(stepOnlyUsage.usage, stepSnapshot.usage, '最终无 usage 时才回退步骤快照');
const noNewlineCollected = await collectCcStream(new Response('{"type":"text-delta","text":"Hello"}\n' + JSON.stringify(usageEvent)));
assertEq(noNewlineCollected.usage, usageEvent.usage, '非流式解析末尾无换行的用量帧');
assertEq(noNewlineCollected.sawFinish, true, '无末尾换行仍识别完成事件');
const stepOnly = { type: 'finish-step', finishReason: 'stop', usage: usageEvent.usage };
for (const translator of [createSseTranslator('model', 'id', 1), createAnthropicSseTranslator('model', 'id'), createResponsesSseTranslator('model', 'id', 1)]) {
  await translator.parseLine(JSON.stringify(stepOnly));
  assertEq(translator.incompleteDetail(), 'no finish event', '步骤完成不代表整体请求完成');
}
assertEq((await collectCcStream(new Response(JSON.stringify(stepOnly)))).sawFinish, false, '非流式仅步骤完成的截断不得统计成功');

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
assertEq((await new MemPool([], 0).dailyUsage('1d')).tzOffset, 0, '用量接口返回真实 UTC 时区偏移');

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

const legacyUsageRes = await handleRequest(new Request('http://localhost:3000/api/usage/daily?range=30d'));
assertEq(legacyUsageRes.status, 200, '旧 30d API 查询正常返回');
assertEq(await legacyUsageRes.json(), await pool.dailyUsage('1d'), '旧 30d API 查询降级为 1d');

// 测试 /v1/models
const modelsReq = new Request('http://localhost:3000/v1/models');
const modelsRes = await handleRequest(modelsReq);
assertEq(modelsRes.status, 200, '/v1/models 返回 200');
const modelsJson = await modelsRes.json();
assert(Array.isArray(modelsJson.data) && modelsJson.data.length > 0, '/v1/models 返回非空模型列表');

const systemImageResponse = await handleRequest(new Request('http://localhost:3000/v1/responses', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ input: [{ type: 'message', role: 'system', content: [{ type: 'input_image', image_url: responseImage }] }] }),
}));
assertEq(systemImageResponse.status, 400, 'system/developer 图片通过 HTTP 映射为客户端 400');
assertEq((await systemImageResponse.json()).error.type, 'invalid_request_error', 'system/developer 图片返回 invalid_request_error');

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
assert(browserUrlJson.callbackUrl.startsWith('http://127.0.0.1:'), '回调地址为 localhost');
assert(new URL(browserUrlJson.authUrl).searchParams.get('state') === browserUrlJson.state, 'state 一致');
assert(!new URL(browserUrlJson.authUrl).searchParams.has('mode'), '不使用 mode=redirect(密钥走 POST，页面拿不到)');

// 已移除的无鉴权回调路由不得再写入号池
const oldCbRes = await handleRequest(new Request('http://localhost:3000/api/auth/callback?apiKey=x'));
assertEq(oldCbRes.status, 404, '/api/auth/callback 已移除，不再无鉴权写入');

// 9. Mock 上游推理调用测试（OpenAI / Anthropic / Responses & 故障切换）
console.log('--- 测试 RedisPool (内存假 Redis 客户端) ---');
{
  const store = new Map(); // key -> Map(field -> value) | number
  const calls = [];
  const batches = [];
  const fake = {
    async sendCommand(argv) {
      const [cmd, key, ...rest] = argv;
      calls.push(argv);
      if (cmd === 'INCR') { const n = (store.get(key) || 0) + 1; store.set(key, n); return n; }
      if (cmd === 'EXPIRE') return 1;
      if (cmd === 'DEL') { store.delete(argv[1]); return 1; }
      if (cmd === 'EXISTS') return store.has(argv[1]) ? 1 : 0;
      if (cmd === 'SET') { store.set(argv[1], argv[2]); return 'OK'; }
      if (cmd === 'SMEMBERS') {
        const set = store.get(argv[1]);
        if (!set) return [];
        if (set instanceof Map) throw new Error('WRONGTYPE');
        return [...set];
      }
      if (cmd === 'SADD') {
        const set = store.get(argv[1]) || new Set();
        if (set instanceof Map) throw new Error('WRONGTYPE');
        const added = !set.has(argv[2]);
        set.add(argv[2]);
        store.set(argv[1], set);
        return Number(added);
      }
      if (cmd === 'SREM') {
        const set = store.get(argv[1]);
        if (!set) return 0;
        if (set instanceof Map) throw new Error('WRONGTYPE');
        const removed = set.delete(argv[2]);
        if (!set.size) store.delete(argv[1]);
        return Number(removed);
      }
      if (cmd === 'EVAL') {
        const hash = store.get(argv[3]);
        const addScript = Number(argv[2]) === 3 && argv[1].includes('local key = ARGV[1]');
        const removeScript = argv[1].includes("local account = redis.call('HGET'");
        if (addScript) {
          const accountsKey = argv[3], indexKey = argv[4], markerKey = argv[5];
          if (Number(argv[2]) !== 3) throw new Error('ADD EVAL key count mismatch');
          const key = argv[6], id = argv[7], raw = argv[8];
          let index = store.get(indexKey);
          if (index instanceof Map) throw new Error('WRONGTYPE');
          if (!store.has(markerKey)) {
            index ||= new Set();
            for (const value of (store.get(accountsKey) || new Map()).values()) {
              const account = JSON.parse(value);
              if (account.api_key) index.add(account.api_key);
            }
            store.set(indexKey, index);
            store.set(markerKey, '1');
          }
          index ||= new Set();
          if (index.has(key)) return 0;
          const accounts = store.get(accountsKey) || new Map();
          accounts.set(id, raw);
          index.add(key);
          store.set(accountsKey, accounts);
          store.set(indexKey, index);
          return 1;
        }
        if (removeScript) {
          const accountsKey = argv[3], indexKey = argv[4];
          if (Number(argv[2]) !== 2) throw new Error('REMOVE EVAL key count mismatch');
          const id = argv[5], accounts = store.get(accountsKey) || new Map();
          const value = accounts.get(id);
          if (!value) return 0;
          accounts.delete(id);
          store.set(accountsKey, accounts);
          store.get(indexKey)?.delete(JSON.parse(value).api_key);
          return 1;
        }
        const previous = hash?.get(argv[4]);
        if (!previous) return 0;
        hash.set(argv[4], JSON.stringify({ ...JSON.parse(previous), ...JSON.parse(argv[5]) }));
        return 1;
      }
      const h = store.get(key) || new Map();
      if (cmd === 'HSET') { h.set(rest[0], rest[1]); store.set(key, h); return 1; }
      if (cmd === 'HGET') return h.get(rest[0]) ?? null;
      if (cmd === 'HDEL') return h.delete(rest[0]) ? 1 : 0;
      if (cmd === 'HGETALL') return [...h].flat();
      throw new Error('unexpected command ' + cmd);
    },
    multi() {
      const commands = [];
      return {
        sendCommand(argv) { commands.push(argv); return this; },
        execAsPipeline() {
          batches.push(commands);
          return Promise.all(commands.map(argv => fake.sendCommand(argv)));
        },
      };
    },
  };
  const rp = new RedisPool('redis://unused', 8, fake);
  await rp.cmd('DEL', 'cc_pool:api_keys');
  await rp.cmd('DEL', 'cc_pool:api_keys_migrated');
  const legacyKey = 'user_legacy_key_0000001';
  store.set('cc_pool:accounts', new Map([['99', JSON.stringify({ id: 99, api_key: legacyKey, label: '', enabled: true })]]));
  let migrationDuplicate;
  try { await rp.add(legacyKey, 'legacy duplicate'); } catch (error) { migrationDuplicate = error; }
  assertEq(migrationDuplicate?.status, 409, '首次 Lua add 从旧账号哈希迁移 API key 索引并拒绝重复');
  assert((await rp.list()).some(account => account.id === 99), '索引迁移保留旧账号');
  const rid = await rp.add('user_redis_test_key_0001', 'redis-1');
  let duplicateError;
  try { await rp.add('user_redis_test_key_0001', 'duplicate'); } catch (error) { duplicateError = error; }
  assertEq(duplicateError?.status, 409, 'RedisPool Lua add rejects duplicate keys atomically');
  const successfulAdds = await Promise.allSettled([
    rp.add('user_redis_concurrent_key_0002', 'first'),
    rp.add('user_redis_concurrent_key_0002', 'second'),
  ]);
  assertEq(successfulAdds.filter(result => result.status === 'fulfilled').length, 1, '并发 Redis add 对相同 API Key 仅创建一条记录');
  assertEq(successfulAdds.filter(result => result.status === 'rejected' && result.reason.status === 409).length, 1, '并发重复 Redis add 返回 409');
  assertEq((await rp.list()).length, 3, 'RedisPool add/list');
  assertEq((await rp.getRaw(rid)).api_key, 'user_redis_test_key_0001', 'RedisPool getRaw');
  const testBucket = usageBucketStart(Date.now(), 8);
  store.set(`cc_pool:bucket:${testBucket}`, new Map([[String(rid), JSON.stringify({ requests: 2, prompt_tokens: 10 })]]));
  store.set(`cc_pool:bucket:${testBucket - 86400}`, new Map([[String(rid), JSON.stringify({ requests: 3, completion_tokens: 8 })]]));
  const redisUsage = await rp.dailyUsage('7d');
  assertEq(redisUsage.totals, { requests: 5, promptTokens: 10, completionTokens: 8, cacheReadTokens: 0 }, 'Redis pipeline 保留桶与账号的聚合结果');
  assertEq(batches.map(batch => batch.length), [256, 256, 256, 256, 256, 256, 256, 224], '7d 使用 8 个有界 pipeline，而非 2016 次串行往返');
  assertEq(await rp.cmdMany([]), [], 'Redis 空 pipeline 不发命令');
  await rp.cmdMany([['HGET', 'cc_pool:accounts', rid]]);
  assert(calls.every(argv => argv.every(arg => typeof arg === 'string')), 'Redis 单条与批量命令参数全部为字符串');
  await rp.patch(rid, { enabled: false });
  assertEq((await rp.list())[0].enabled, false, 'RedisPool patch');
  await rp.patch(rid, { label: 'renamed', enabled: true });
  await rp.cooldown(rid, 30, 'slow down');
  await rp.disable(rid, 'disabled');
  await rp.markQuotaError(rid, 'refresh failed');
  const metadata = await rp.getRaw(rid);
  assertEq(metadata.label, 'renamed', '元数据更新保留其他字段');
  assertEq(metadata.last_error, 'refresh failed', '元数据通过部分合并更新');
  assertEq(metadata.disabled_reason, 'disabled', '停用原因保留');
  assert(metadata.rate_limited_until > Date.now(), '冷却时间写入');
  assertEq(await rp.patch(999, { label: 'missing' }), false, '部分合并不得创建已删除账号');
  await rp.remove(rid);
  await rp.remove(successfulAdds.find(result => result.status === 'fulfilled').value);
  await rp.remove(99);
  assertEq((await rp.list()).length, 0, 'RedisPool remove');
}

console.log('--- 测试 Upstash REST pipeline ---');
{
  const savedFetch = globalThis.fetch;
  const calls = [];
  let errorBody = null;
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), commands: JSON.parse(options.body), headers: options.headers });
    const commands = JSON.parse(options.body);
    const body = errorBody || commands.map((_, i) => ({ result: i === 0 ? ['1', JSON.stringify({ requests: 4 })] : null }));
    return new Response(JSON.stringify(body), { status: 200 });
  };
  try {
    const kv = new UpstashKvPool('https://test.invalid/', 'test-token');
    const result = await kv.cmdMany([['HGETALL', 'bucket:1'], ['HGETALL', 'bucket:2']]);
    assertEq(result, [['1', JSON.stringify({ requests: 4 })], null], 'REST pipeline 按原顺序返回结果');
    assertEq(calls[0].url, 'https://test.invalid/pipeline', 'REST 使用 /pipeline');
    assertEq(calls[0].headers.Authorization, 'Bearer test-token', 'REST pipeline 保留鉴权');
    assertEq(await kv.cmdMany([]), [], 'REST 空 pipeline 不发请求');
    assertEq(calls.length, 1, 'REST 批量命令只有一次 HTTP 请求');
    errorBody = [{ result: [] }, { error: 'WRONGTYPE' }];
    let error;
    try { await kv.cmdMany([['HGETALL', 'bucket:1'], ['HGETALL', 'bucket:2']]); } catch (e) { error = e; }
    assert(error?.message.includes('WRONGTYPE'), 'REST pipeline 命令错误不得静默丢失');
    errorBody = { error: 'ERR script failed' };
    error = null;
    try { await kv.cmd('EVAL', 'test script', 0); } catch (e) { error = e; }
    assert(error?.message.includes('script failed'), 'REST 单条命令错误不得静默丢失');
  } finally {
    globalThis.fetch = savedFetch;
  }
}

console.log('--- 测试 Mock 上游推理调用与故障转移 ---');

const origFetch = globalThis.fetch;
let mockResponses = [];
const wireRequests = [];

globalThis.fetch = async (url, options) => {
  const urlStr = String(url);
  if (urlStr.includes('/alpha/')) {
    wireRequests.push({ url: urlStr, headers: options.headers, body: JSON.parse(options.body) });
  }
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

// 管理在线测试与客户端鉴权必须保持独立，不能向浏览器泄露 API_KEYS。
{
  const savedEnv = { API_KEYS: process.env.API_KEYS, ADMIN_TOKEN: process.env.ADMIN_TOKEN };
  const adminToken = 'test-admin-password';
  const clientKey = 'sk-test-client';
  process.env.API_KEYS = `${clientKey},sk-test-client-2`;
  process.env.ADMIN_TOKEN = adminToken;
  const testBody = { model: 'deepseek/deepseek-v4-flash', messages: [{ role: 'user', content: 'Hi' }], stream: false };
  const call = (endpoint, headers = {}, body = testBody) => handleRequest(new Request(`http://localhost:3000${endpoint}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body),
  }));
  try {
    for (const endpoint of ['/v1/chat/completions', '/v1/messages', '/v1/responses']) {
      for (const headers of [{}, { 'X-Admin-Token': adminToken }, { Authorization: `Bearer ${adminToken}` }]) {
        const before = wireRequests.length;
        assertEq((await call(endpoint, headers)).status, 401, `${endpoint} 不允许匿名或管理员凭据替代 API_KEYS`);
        assertEq(wireRequests.length, before, '未授权客户端不调用上游');
      }
    }
    for (const headers of [{}, { 'X-Admin-Token': 'wrong' }, { Authorization: `Bearer ${clientKey}` }]) {
      const before = wireRequests.length;
      assertEq((await call('/api/test', headers)).status, 401, '管理测试必须通过 X-Admin-Token 鉴权');
      assertEq(wireRequests.length, before, '未授权管理测试不调用上游');
    }
    for (const [endpoint, headers, body] of [
      ['/v1/chat/completions', { Authorization: `Bearer ${clientKey}` }, testBody],
      ['/v1/messages', { 'x-api-key': 'sk-test-client-2' }, { ...testBody, max_tokens: 1024 }],
      ['/v1/responses', { Authorization: `Bearer ${clientKey}` }, { model: testBody.model, input: 'Hi' }],
    ]) {
      const response = await call(endpoint, headers, body);
      assertEq(response.status, 200, `${endpoint} 保留 API_KEYS 客户端调用`);
      assert((await response.text()).includes('Hello, CommandCode Go!'), '受保护协议入口正常返回模型内容');
    }
    for (const stream of [false, true]) {
      const requestsBefore = (await pool.getRaw(acctId)).requests;
      const response = await call('/api/test', { 'X-Admin-Token': adminToken }, { ...testBody, stream });
      assertEq(response.status, 200, `管理在线测试支持 ${stream ? '流式' : '非流式'}`);
      const text = await response.text();
      assert(text.includes('Hello, CommandCode Go!'), '管理测试复用推理响应');
      assert(!stream || text.includes('[DONE]'), '管理测试流式正确结束');
      assert(!text.includes(clientKey), '管理测试不泄露客户端密钥');
      assertEq((await pool.getRaw(acctId)).requests, requestsBefore + 1, '管理测试通过共享号池记录用量');
    }

    // 执行实际管理页脚本并触发按钮，检查请求路径、凭据和输出，而非仅匹配 HTML 文本。
    const elements = new Map();
    const document = { getElementById(id) {
      if (!elements.has(id)) elements.set(id, {
        value: '', checked: false, style: {}, textContent: '',
        classList: { add() {}, remove() {}, toggle() {} },
        listeners: {}, addEventListener(type, fn) { this.listeners[type] = fn; },
      });
      return elements.get(id);
    } };
    document.getElementById('test-model').value = testBody.model;
    document.getElementById('test-prompt').value = 'Hi';
    const browserRequests = [];
    const timers = [];
    const context = vm.createContext({
      document, TextDecoder,
      localStorage: { getItem: () => adminToken }, sessionStorage: { getItem: () => null },
      setInterval: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
      fetch: (endpoint, options) => {
        browserRequests.push({ endpoint, options });
        if (endpoint === '/api/refresh') return Promise.resolve(new Response('{"ok":true}'));
        return handleRequest(new Request(`http://localhost:3000${endpoint}`, options));
      },
    });
    const adminHtml = fs.readFileSync(new URL('../public/admin.html', import.meta.url), 'utf8');
    vm.runInContext(adminHtml.match(/<\s*script\b[^>]*>([\s\S]*?)<\s*\/\s*script\b[^>]*>/i)[1], context);
    const sendButton = document.getElementById('test-send-btn');
    for (const stream of [false, true]) {
      document.getElementById('test-stream').checked = stream;
      mockResponses = [new Response([
        { type: 'reasoning-delta', text: '思考一' }, { type: 'reasoning-delta', text: '思考二' },
        { type: 'text-delta', text: 'Hello, CommandCode Go!' }, usageEvent,
      ].map(event => JSON.stringify(event)).join('\n'))];
      await sendButton.listeners.click.call(sendButton);
      const sent = browserRequests.filter(r => JSON.parse(r.options.body || '{}').messages).at(-1);
      assertEq(sent.endpoint, '/api/test', '管理页按钮走管理测试入口');
      assertEq(sent.options.headers['X-Admin-Token'], adminToken, '管理页按钮发送现有管理员凭据');
      assert(!JSON.stringify(sent).includes(clientKey), '管理页不获取或发送 API_KEYS');
      const output = document.getElementById('test-result').textContent;
      assert(output.includes('Hello, CommandCode Go!'), '实际管理页显示模型响应');
      assert(!output.includes('思考一') && !output.includes('思考二'), '思考不挤占正文输出');
      assertEq(document.getElementById('test-reasoning').textContent, '思考一思考二', '思考增量连续拼接到独立区域');
      assert(output.includes('[请求模型] ' + testBody.model), '显示请求模型而非依赖模型自报身份');
      assert(output.includes('Prompt: 31') && output.includes('Completion: 12'), '两种在线测试都显示准确 Tokens');
      assert(browserRequests.some(r => r.endpoint.startsWith('/api/usage/daily')), '在线测试结束刷新用量');
      assertEq(sendButton.disabled, false, '管理测试结束后按钮恢复');
    }
    const refreshButton = document.getElementById('refresh-all');
    const beforeRefresh = browserRequests.length;
    await refreshButton.listeners.click.call(refreshButton);
    assert(browserRequests.slice(beforeRefresh).some(r => r.endpoint.startsWith('/api/usage/daily')), '全部刷新必须刷新用量统计');

    // 自动刷新固定在 5 秒，且跟随当前标签。
    assertEq(timers.map(t => t.ms), [5000], '仅注册一个 5 秒自动刷新定时器');
    const autoRefresh = timers[0].fn;
    const beforeAuto = browserRequests.length;
    await autoRefresh();
    assert(browserRequests.slice(beforeAuto).every(r => r.endpoint.startsWith('/api/accounts')), '额度标签自动刷新账号');
    document.getElementById('tabbtn-usage').listeners.click.call(document.getElementById('tabbtn-usage'));
    await Promise.resolve();
    const beforeUsageAuto = browserRequests.length;
    await autoRefresh();
    assert(browserRequests.slice(beforeUsageAuto).some(r => r.endpoint.startsWith('/api/usage/daily')), '用量标签自动刷新用量统计');

    mockResponses = [new Response('{"type":"text-delta","text":"Partial"}\n')];
    await sendButton.listeners.click.call(sendButton);
    assert(document.getElementById('test-result').textContent.includes('❌'), '不完整 SSE 不显示完成');
    assert(!document.getElementById('test-result').textContent.includes('✓ 完成'), '缺少完成帧不伪报成功');
    await pool.patch(acctId, { enabled: true });
    // 不完整流会冷却账号，恢复测试账号以隔离后续用例。
    await pool.cooldown(acctId, 0);

    delete process.env.ADMIN_TOKEN;
    const before = wireRequests.length;
    const noAdminResponse = await call('/api/test');
    assertEq(noAdminResponse.status, 403, '未配置 ADMIN_TOKEN 时管理测试不能绕过 API_KEYS');
    assert((await noAdminResponse.text()).includes('ADMIN_TOKEN'), '缺少管理员配置时给出明确提示');
    assertEq(wireRequests.length, before, '未配置管理密码不得调用受保护上游');
    delete process.env.API_KEYS;
    const openTestResponse = await call('/api/test');
    assertEq(openTestResponse.status, 200, '无鉴权配置时保留本地免密管理测试');
    await openTestResponse.text();
  } finally {
    for (const [name, value] of Object.entries(savedEnv)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

// Vercel/Node 真实 HTTP 适配器：请求体读完不是客户端断开。
{
  const server = http.createServer((req, res) => handler(req, res));
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    for (const stream of [false, true]) {
      const before = (await pool.getRaw(acctId)).requests;
      mockResponses = [async (url, options) => {
        await new Promise(resolve => setTimeout(resolve, 10));
        if (options.signal?.aborted) throw new DOMException('Request was aborted', 'AbortError');
        return new Response('{"type":"text-delta","text":"HTTP works"}\n' + JSON.stringify(usageEvent));
      }];
      const response = await origFetch(`http://127.0.0.1:${server.address().port}/v1/chat/completions`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: [{ role: 'user', content: 'Hi' }], stream }),
      });
      assertEq(response.status, 200, '真实 HTTP 请求体读完不误取消上游');
      assert((await response.text()).includes('HTTP works'), 'Node HTTP 适配器返回完整内容');
      assertEq((await pool.getRaw(acctId)).requests, before + 1, '真实 HTTP 调用成功记录用量');
    }
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
}

// 成功终止帧必须等持久化完成，防止 SDK 收到完成标记后断开导致漏计。
for (const [endpoint, body, terminal] of [
  ['/v1/chat/completions', { messages: [{ role: 'user', content: 'Hi' }] }, '[DONE]'],
  ['/v1/messages', { messages: [{ role: 'user', content: 'Hi' }], max_tokens: 1024 }, 'event: message_stop'],
  ['/v1/responses', { input: 'Hi' }, 'event: response.completed'],
]) {
  const before = (await pool.getRaw(acctId)).requests;
  const originalRecord = pool.recordSuccess;
  let release, notify;
  const gate = new Promise(resolve => { release = resolve; });
  const recording = new Promise(resolve => { notify = resolve; });
  pool.recordSuccess = async function (id, usage) {
    notify();
    await gate;
    return originalRecord.call(this, id, usage);
  };
  let received = '';
  try {
    const response = await handleRequest(new Request('http://localhost:3000' + endpoint, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, stream: true }),
    }));
    const reader = response.body.getReader();
    const consuming = (async () => {
      const decoder = new TextDecoder();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        received += decoder.decode(value, { stream: true });
      }
    })();
    await recording;
    await new Promise(resolve => setImmediate(resolve));
    assert(!received.includes(terminal), '持久化完成前不发协议成功结束标记');
    assert(!received.includes('"finish_reason":"stop"'), '持久化完成前不发 OpenAI 完成帧');
    release();
    await consuming;
    assert(received.includes(terminal), '持久化完成后才结束 SSE');
    assertEq((await pool.getRaw(acctId)).requests, before + 1, '流式调用仅计一次成功');
  } finally {
    release();
    pool.recordSuccess = originalRecord;
  }
}

// 三种协议、两种输出方式共用成功规则：实际正文/思考/工具输出与整体 finish 缺一不可。
for (const [endpoint, body, terminal] of [
  ['/v1/chat/completions', { messages: [{ role: 'user', content: 'Hi' }] }, '[DONE]'],
  ['/v1/messages', { messages: [{ role: 'user', content: 'Hi' }], max_tokens: 1024 }, 'event: message_stop'],
  ['/v1/responses', { input: 'Hi' }, 'event: response.completed'],
]) {
  for (const stream of [false, true]) {
    for (const [events, success] of [
      [[{ type: 'finish', finishReason: 'stop', usage: { inputTokens: 0, outputTokens: 0 } }], false],
      [[{ type: 'finish', finishReason: 'stop' }], false],
      [[{ type: 'text-delta', text: 'Partial' }, stepOnly], false],
      [[{ type: 'text-delta', text: 'No usage provided' }, { type: 'finish', finishReason: 'stop' }], true],
      [[{ type: 'reasoning-delta', text: 'Thinking only' }, { type: 'finish', finishReason: 'stop' }], true],
      [[{ type: 'tool-call', toolCallId: 'call_test', toolName: 'read_file', input: { path: 'test.txt' } }, { type: 'finish', finishReason: 'tool_calls' }], true],
      ...['text-delta', 'reasoning-delta', 'tool-call'].map(type => [[
        { type, text: 'Explicit zero usage but real output', toolCallId: 'call_zero', toolName: 'read_file', input: {} },
        { type: 'finish', finishReason: type === 'tool-call' ? 'tool_calls' : 'stop', usage: { inputTokens: 31, outputTokens: 0, cachedInputTokens: 7 } },
      ], true]),
      [[{ type: 'text-delta', text: 'Input usage without output count' }, { type: 'finish', usage: { inputTokens: 31, cachedInputTokens: 7 } }], true],
      [[{ type: 'text-delta', text: 'Multiple' }, { type: 'text-delta', text: 'chunks' }, { type: 'finish', finishReason: 'stop' }], true],
    ]) {
      const beforeAccount = (await pool.list()).find(a => a.id === acctId).usage;
      const beforeTotals = (await pool.dailyUsage('1d')).totals;
      const before = beforeAccount.requests;
      mockResponses = [new Response(events.map(event => JSON.stringify(event)).join('\n'))];
      const response = await handleRequest(new Request('http://localhost:3000' + endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...body, stream }),
      }));
      const text = await response.text();
      assertEq(response.status, stream || success ? 200 : 429, `${endpoint} ${stream ? '流式' : '非流式'} 完成规则一致`);
      const hasError = stream ? /(?:event: error|data: \{"error":)/.test(text) : !!JSON.parse(text).error;
      assertEq(hasError, !success, '空响应/步骤截断报错，有实际输出但缺用量仍合法');
      assert(!stream || text.includes(terminal) === success, '失败不发送协议成功结束帧');
      assertEq((await pool.getRaw(acctId)).requests, before + Number(success), '跨协议成功计数与完成规则一致');
      const reported = success ? events.at(-1).usage || {} : {};
      const expectedIncrement = [Number(success), reported.inputTokens ?? 0, reported.outputTokens ?? 0, reported.cachedInputTokens ?? 0];
      const afterAccount = (await pool.list()).find(a => a.id === acctId).usage;
      const afterTotals = (await pool.dailyUsage('1d')).totals;
      const fields = ['requests', 'promptTokens', 'completionTokens', 'cacheReadTokens'];
      assertEq(fields.map(f => afterAccount[f] - beforeAccount[f]), expectedIncrement, '跨协议账号只保存上游实际用量，不估算或抹掉输入缓存');
      assertEq(fields.map(f => afterTotals[f] - beforeTotals[f]), expectedIncrement, '统计桶用量与账号增量一致');
      if (success && endpoint === '/v1/messages') {
        const anthropicUsage = stream
          ? text.split('\n').filter(line => line.startsWith('data:')).map(line => JSON.parse(line.slice(5))).find(event => event.type === 'message_delta').usage
          : JSON.parse(text).usage;
        assertEq([anthropicUsage.input_tokens, anthropicUsage.output_tokens, anthropicUsage.cache_read_input_tokens],
          [Math.max(0, expectedIncrement[1] - expectedIncrement[3]), expectedIncrement[2], expectedIncrement[3]], 'Anthropic 响应用量与落库真实用量一致');
      }
      await pool.cooldown(acctId, 0);
    }
  }
}

// 上游报错或截断不能记作成功，也不能发送 DONE。
for (const event of [{ type: 'error', error: { message: '<400> upstream rejected', statusCode: 400 } }, { type: 'text-delta', text: 'Partial' }, stepOnly]) {
  const before = (await pool.getRaw(acctId)).requests;
  mockResponses = [new Response(JSON.stringify(event))];
  const response = await handleRequest(new Request('http://localhost:3000/v1/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages: [{ role: 'user', content: 'Hi' }], stream: true }),
  }));
  const text = await response.text();
  assert(text.includes('"error"'), '上游 SSE 错误或截断向客户端返回错误');
  assert(!text.includes('[DONE]'), '上游 SSE 错误或截断不发送成功 DONE');
  assertEq((await pool.getRaw(acctId)).requests, before, '失败流不增加成功计数');
  await pool.cooldown(acctId, 0);
}

// 仅 finish-step 的非流式截断必须换号重试，不能增加成功计数。
{
  const before = (await pool.getRaw(acctId)).requests;
  mockResponses = [new Response(JSON.stringify(stepOnly))];
  const response = await handleRequest(new Request('http://localhost:3000/v1/chat/completions', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages: [{ role: 'user', content: 'Hi' }], stream: false }),
  }));
  assertEq(response.status, 429, '无可切换账号时截断请求不伪报成功');
  assertEq((await pool.getRaw(acctId)).requests, before, '非流式步骤截断不计成功');
  await pool.cooldown(acctId, 0);
}

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

const generateRequests = wireRequests.filter(r => r.url.includes('/alpha/generate'));
assert(generateRequests.length > 0, '已捕获各协议发送的真实上游信封');
assert(generateRequests.every(r => typeof r.body.config.environment === 'string'), '所有协议上游 environment 均为字符串');
assert(generateRequests.every(r => wireUuidPattern.test(r.body.threadId)), '所有协议上游 threadId 均通过严格 UUID 校验');
assert(wireRequests.every(r => r.headers['x-session-id'] === sessionForKey(r.headers.Authorization.slice(7), config)), '所有上游头复用同一确定性会话身份');
assert(generateRequests.every(r => r.body.threadId === r.headers['x-session-id']), 'threadId 与 x-session-id 一致');
assert(wireRequests.filter(r => r.url.includes('/alpha/lifecycle-events')).every(r =>
  r.body.events[0].sessionId === r.headers['x-session-id'] && wireUuidPattern.test(r.body.events[0].sessionId)), '生命周期注册 sessionId 与上游 UUID 一致');

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

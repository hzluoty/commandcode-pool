// lib/api-adapters.js
// OpenAI / Anthropic / Responses 协议双向转换、流式 SSE 翻译、非流收集
import crypto from 'node:crypto';
import {
  DEFAULT_MAX_TOKENS,
  MAX_MAX_TOKENS,
  DEFAULT_MODEL,
  MAX_SSE_EVENT_CHARS,
  STREAM_IDLE_TIMEOUT,
} from './constants.js';
import { num, isRecord, generateTraceparent } from './crypto-utils.js';

export function normalizeUsage(u) {
  if (!u) return;
  const ot = Number(u.outputTokens);
  if (!ot) {
    u.inputTokens = 0;
    u.cachedInputTokens = 0;
  }
}

export function anthropicInputTokens(usage, noCacheOverride) {
  const u = usage || {};
  if (typeof noCacheOverride === 'number' && noCacheOverride >= 0) return noCacheOverride;
  const noCache = u.inputTokenDetails && u.inputTokenDetails.noCacheTokens;
  if (typeof noCache === 'number' && noCache >= 0) return noCache;
  const cacheRead = u.cachedInputTokens || (u.inputTokenDetails && u.inputTokenDetails.cacheReadTokens) || 0;
  const cacheWrite = (u.inputTokenDetails && u.inputTokenDetails.cacheWriteTokens) || 0;
  return Math.max(0, (u.inputTokens || 0) - cacheRead - cacheWrite);
}

export function mapFinishReason(reason) {
  const r = String(reason ?? '').trim().toLowerCase();
  if (!r) return 'stop';
  if (r === 'tool-calls' || r === 'tool_calls' || r === 'tool_use') return 'tool_calls';
  if (r === 'length' || r === 'max_tokens' || r === 'max_output_tokens' || r === 'model_context_window_exceeded') return 'length';
  if (/^(?:network|connection|upstream)[-_\s]?error$/.test(r)) return 'upstream_error';
  return r;
}

export function toOpenAIFinishReason(finishReason) {
  return finishReason === 'pause_turn' ? 'length' : finishReason;
}

export function mapAnthropicStopReason(finishReason) {
  switch (finishReason) {
    case 'tool_calls': return 'tool_use';
    case 'length': return 'max_tokens';
    case 'stop': return 'end_turn';
    case 'pause_turn': return 'pause_turn';
    case 'refusal': return 'refusal';
    default: return 'end_turn';
  }
}

export function incompleteUpstreamDetail(sawFinish, finishReason) {
  if (!sawFinish) return 'no finish event';
  if (finishReason === 'upstream_error') return 'provider reported an upstream connection failure';
  return null;
}

export function incompleteUpstreamError(detail) {
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

export function zeroOutputError() {
  return {
    status: 429,
    retryAfter: 10,
    body: {
      error: { message: 'Empty response from upstream (zero output tokens)', type: 'rate_limit_error' },
      retry_after: 10,
    },
  };
}

export function idleTimeoutError() {
  return {
    status: 429,
    retryAfter: 5,
    body: {
      error: { message: 'Response timeout - request timed out', type: 'rate_limit_error' },
      retry_after: 5,
    },
  };
}

export function transportError(message) {
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

export function mapCcError(ccStatus, ccBody) {
  const mapped = CC_STATUS_MAP[ccStatus] || { status: 502, type: 'upstream_error' };
  let message = `CC API error (${ccStatus})`;
  let code = null;

  if (ccBody) {
    try {
      const parsed = typeof ccBody === 'string' ? JSON.parse(ccBody) : ccBody;
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

export function mapCcEventError(event) {
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

export async function fakeThinkingSignature(thinkingText) {
  const hash = crypto.createHash('sha256').update(thinkingText || 'dsh-proxy-thinking').digest();
  const seed = hash.subarray(0, 64);
  const raw = Buffer.alloc(2 + seed.length);
  raw[0] = 0x12;
  raw[1] = seed.length;
  seed.copy(raw, 2);
  return raw.toString('base64');
}

const TOOL_NAME_ALIASES = {
  bash_output: 'shell_output',
  task_output: 'shell_output',
  tool_search: 'search_tools',
  read_multiple_files: 'read_file',
};

function toWireToolName(name) {
  return TOOL_NAME_ALIASES[name] || name;
}

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

export function buildCcRequest(openaiReq, config = {}) {
  const { model, messages, max_tokens, max_completion_tokens, temperature, tools, reasoning_effort, tool_choice, parallel_tool_calls, prompt_cache_key } = openaiReq;
  const msgs = Array.isArray(messages) ? messages : [];
  if (!msgs.length) throw new Error('messages is required');

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

  const hasCacheMarker = systemBlocks.some((b) => b.cache_control) || ccMessages.some((msg) =>
    Array.isArray(msg.content) && msg.content.some((part) => part?.cache_control));
  if (prompt_cache_key && !hasCacheMarker && systemBlocks.length) {
    systemBlocks[systemBlocks.length - 1].cache_control = { type: 'ephemeral' };
  }

  const maxTok = num(max_completion_tokens) ?? num(max_tokens) ?? DEFAULT_MAX_TOKENS;
  const projectDir = config.deviceProjectDir || 'C:\\Users\\dev\\projects\\app';

  const body = {
    config: {
      workingDir: projectDir,
      date: new Date().toISOString(),
      environment: { platform: 'win32', arch: 'x64', osRelease: '10.0.22631', isContainer: false },
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
    mode: config.cliMode || 'agent',
    params: {
      model: model || DEFAULT_MODEL,
      messages: ccMessages,
      max_tokens: Math.min(maxTok, MAX_MAX_TOKENS),
      stream: true,
    },
  };

  if (systemBlocks.length) {
    body.params.system = systemBlocks;
  } else if (config.emptySystemPlaceholder !== false) {
    body.params.system = [{ type: 'text', text: ' ' }];
  }
  if (temperature !== undefined) body.params.temperature = temperature;
  if (reasoning_effort !== undefined) body.params.reasoning_effort = reasoning_effort;
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

export function createSseTranslator(model, completionId, created) {
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
          break;
        }

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

export function createAnthropicSseTranslator(model, messageId) {
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
      out += frame('content_block_delta', {
        type: 'content_block_delta',
        index: idx,
        delta: { type: 'signature_delta', signature: await fakeThinkingSignature(currentThinkingText) },
      });
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
      return close + frame('content_block_start', {
        type: 'content_block_start',
        index: currentBlockIndex,
        content_block: contentBlock,
      });
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
        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          out.push(await startBlock('thinking', { type: 'thinking', thinking: '' }));
          currentThinkingText += text;
          out.push(frame('content_block_delta', {
            type: 'content_block_delta',
            index: currentBlockIndex,
            delta: { type: 'thinking_delta', thinking: text },
          }));
          break;
        }

        case 'text-delta': {
          const text = event.text || '';
          out.push(await startBlock('text', { type: 'text', text: '' }));
          out.push(frame('content_block_delta', {
            type: 'content_block_delta',
            index: currentBlockIndex,
            delta: { type: 'text_delta', text },
          }));
          outputTokens += 1;
          break;
        }

        case 'tool-call': {
          const close = await closeBlock();
          if (close) out.push(close);
          const id = event.toolCallId || `toolu_${crypto.randomBytes(6).toString('hex')}`;
          const name = event.toolName || '';
          const input = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          const tcIndex = nextBlockIndex++;
          out.push(frame('content_block_start', {
            type: 'content_block_start',
            index: tcIndex,
            content_block: { type: 'tool_use', id, name, input: {} },
          }));
          out.push(frame('content_block_delta', {
            type: 'content_block_delta',
            index: tcIndex,
            delta: { type: 'input_json_delta', partial_json: input },
          }));
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

        default:
          break;
      }

      this.inputTokens = inputTokens;
      this.outputTokens = outputTokens;
      this.cachedInputTokens = cachedInputTokens;

      return out.length ? out : null;
    },

    async finalize() {
      const out = [];
      if (hasError) return out;

      if (blockStarted) {
        const idx = currentBlockIndex;
        const type = currentBlockType;
        if (type === 'thinking') {
          out.push(frame('content_block_delta', {
            type: 'content_block_delta',
            index: idx,
            delta: { type: 'signature_delta', signature: await fakeThinkingSignature(currentThinkingText) },
          }));
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
        out.push(frame('error', {
          type: 'error',
          error: { type: 'rate_limit_error', message: 'Empty response from upstream (zero output tokens)' },
          retry_after: 10,
        }));
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

export function createResponsesSseTranslator(model, responseId, created) {
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

    preamble() {
      return startResponse().join('');
    },

    incompleteDetail() {
      return incompleteUpstreamDetail(sawFinish, finishReason);
    },

    parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) return null;
      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;

      const out = [];

      switch (event.type) {
        case 'reasoning-delta': {
          const delta = event.text || '';
          if (!delta) break;
          if (!current || current.kind !== 'reasoning') {
            out.push(...openItem('reasoning', {
              id: `rs_${crypto.randomBytes(6).toString('hex')}`,
              type: 'reasoning',
              status: 'in_progress',
              summary: [],
            }));
          }
          current.textBuf += delta;
          out.push(sse('response.reasoning_summary_text.delta', {
            item_id: current.item.id, output_index: current.index, summary_index: 0, delta,
          }));
          break;
        }

        case 'text-delta': {
          const delta = event.text || event.delta || '';
          if (!delta) break;
          if (!current || current.kind !== 'message') {
            out.push(...openItem('message', {
              id: `msg_${crypto.randomBytes(6).toString('hex')}`,
              type: 'message',
              status: 'in_progress',
              role: 'assistant',
              content: [],
            }));
          }
          current.textBuf += delta;
          textAcc += delta;
          out.push(sse('response.output_text.delta', {
            item_id: current.item.id, output_index: current.index, content_index: 0, delta, logprobs: [],
          }));
          break;
        }

        case 'tool-call': {
          const callId = event.toolCallId || `call_${crypto.randomBytes(6).toString('hex')}`;
          const name = event.toolName || '';
          const args = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          out.push(...openItem('function_call', {
            id: `fc_${crypto.randomBytes(6).toString('hex')}`,
            type: 'function_call',
            status: 'in_progress',
            call_id: callId,
            name,
            arguments: args,
          }));
          out.push(sse('response.function_call_arguments.delta', {
            item_id: current.item.id, output_index: current.index, delta: args,
          }));
          out.push(...closeItem());
          break;
        }

        case 'finish-step': {
          sawFinish = true;
          if (event.finishReason) finishReason = mapFinishReason(event.finishReason);
          if (event.usage) usage = event.usage;
          break;
        }

        case 'finish': {
          sawFinish = true;
          if (event.finishReason) finishReason = mapFinishReason(event.finishReason);
          usage = event.totalUsage || usage || event.usage;
          out.push(...closeItem());
          const finalResp = baseResponse('completed', doneItems);
          finalResp.output_text = textAcc;
          const u = usage || {};
          normalizeUsage(u);
          this.inputTokens = u.inputTokens ?? 0;
          this.outputTokens = u.outputTokens ?? 0;
          this.cachedInputTokens = u.cachedInputTokens ?? 0;
          finalResp.usage = {
            input_tokens: this.inputTokens,
            output_tokens: this.outputTokens,
            total_tokens: this.inputTokens + this.outputTokens,
            input_tokens_details: { cached_tokens: this.cachedInputTokens },
          };
          out.push(sse('response.completed', { response: finalResp }));
          break;
        }

        case 'error': {
          this.upstreamError = mapCcEventError(event);
          break;
        }

        default:
          break;
      }

      return out.length ? out : null;
    },
  };
}

export async function readWithIdle(reader, idleMs) {
  let timer = null;
  try {
    return await Promise.race([
      reader.read(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('STREAM_IDLE_TIMEOUT')), idleMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function collectCcStream(upstreamResp, idleMs = 90000) {
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
            id: event.toolCallId || `call_${crypto.randomBytes(4).toString('hex')}`,
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
    try { reader.releaseLock(); } catch {}
  }

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

export function collectedUsageOpenAI(usage) {
  const u = usage || {};
  normalizeUsage(u);
  return {
    prompt_tokens: u.inputTokens ?? 0,
    completion_tokens: u.outputTokens ?? 0,
    total_tokens: (u.inputTokens ?? 0) + (u.outputTokens ?? 0),
    prompt_tokens_details: { cached_tokens: u.cachedInputTokens ?? 0 },
  };
}

export async function buildAnthropicResponse(model, fullText, toolCalls, finishReason, usage, thinkingText) {
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
    id: `msg_${crypto.randomBytes(6).toString('hex')}`,
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

export function convertAnthropicToOpenAI(anthropicReq) {
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
            const mediaType = block.source?.media_type || 'image/jpeg';
            const data = block.source?.data || '';
            parts.push({ type: 'image_url', image_url: { url: `data:${mediaType};base64,${data}` } });
          } else if (block.type === 'tool_result') {
            toolResults.push(block);
          }
        }
      }
      if (parts.length > 0 || textContent) {
        openaiMessages.push({ role: 'user', content: (parts.length > 1 || textHasCache) ? parts : textContent });
      }
      for (const tr of toolResults) {
        openaiMessages.push({
          role: 'tool',
          tool_call_id: tr.tool_use_id,
          name: toolNameFromId[tr.tool_use_id] || '',
          content: typeof tr.content === 'string' ? tr.content : JSON.stringify(tr.content || ''),
        });
      }
    }
  }

  const tools = (anthropicReq.tools || []).map((t) => ({
    type: 'function',
    function: { name: t.name, description: t.description, parameters: t.input_schema },
  }));

  let reasoningEffort;
  if (anthropicReq.thinking?.type === 'enabled') {
    const budget = anthropicReq.thinking.budget_tokens || 0;
    if (budget >= 10000) reasoningEffort = 'high';
    else if (budget >= 5000) reasoningEffort = 'medium';
    else reasoningEffort = 'low';
  }

  return {
    model: anthropicReq.model,
    messages: openaiMessages,
    max_tokens: anthropicReq.max_tokens,
    temperature: anthropicReq.temperature,
    tools: tools.length ? tools : undefined,
    reasoning_effort: reasoningEffort,
    stream: anthropicReq.stream,
  };
}

export function convertResponsesToChat(responsesReq) {
  const model = responsesReq.model || DEFAULT_MODEL;
  const input = responsesReq.input;
  const messages = [];

  if (typeof responsesReq.instructions === 'string' && responsesReq.instructions.trim()) {
    messages.push({ role: 'system', content: responsesReq.instructions });
  }

  if (typeof input === 'string') {
    messages.push({ role: 'user', content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (typeof item === 'string') {
        messages.push({ role: 'user', content: item });
      } else if (item && typeof item === 'object') {
        if (item.type === 'function_call_output') {
          messages.push({
            role: 'tool',
            tool_call_id: item.call_id || '',
            content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output || ''),
          });
        } else if (item.role) {
          const role = item.role === 'developer' ? 'system' : item.role;
          messages.push({ role, content: item.content || '' });
        }
      }
    }
  }

  const tools = (responsesReq.tools || []).filter((t) => t.type === 'function').map((t) => ({
    type: 'function',
    function: { name: t.name, description: t.description, parameters: t.parameters },
  }));

  return {
    model,
    messages,
    max_tokens: responsesReq.max_output_tokens,
    temperature: responsesReq.temperature,
    tools: tools.length ? tools : undefined,
    stream: responsesReq.stream,
  };
}

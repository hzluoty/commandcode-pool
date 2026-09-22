// Public Provider API request boundary.
// The gateway routes expose the same native schemas as Command Code's Provider API,
// so requests are forwarded without converting to undocumented CLI/NDJSON formats.

import { ClientInputError } from './failure-policy.js';

const PATHS = Object.freeze({
  chat: '/provider/v1/chat/completions',
  messages: '/provider/v1/messages',
  responses: '/provider/v1/responses',
});

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function fail(message) {
  throw new ClientInputError(message, 400, 'invalid_request_error');
}

function copyProviderBody(body) {
  const result = { ...body };
  // This was a gateway-only legacy account/session-affinity input. It is not in
  // the documented Provider schemas and must never influence credential choice.
  delete result.prompt_cache_key;
  return result;
}

function requestHeaders(request) {
  const headers = {};
  if (request?.headers?.get?.('x-cmd-zdr') === '1') headers['x-cmd-zdr'] = '1';
  return headers;
}

/**
 * Validate only the minimum gateway invariants, then retain the native protocol
 * body and endpoint. Provider API schema validation remains authoritative.
 */
export function prepareGenerationRequest(kind, body, helpers = {}) {
  if (!isRecord(body)) fail('bad request body');
  const path = PATHS[kind];
  if (!path) throw new Error(`unsupported generation protocol: ${kind}`);

  const providerBody = copyProviderBody(body);
  if ((kind === 'chat' || kind === 'messages') && !Array.isArray(providerBody.messages)) {
    fail('messages is required');
  }
  if (kind === 'responses' && providerBody.input === undefined && providerBody.previous_response_id == null) {
    fail('input is required');
  }

  if (providerBody.model == null || providerBody.model === '') {
    providerBody.model = kind === 'messages'
      ? (helpers.messagesDefaultModel || 'claude-sonnet-4-6')
      : (helpers.defaultModel || 'deepseek/deepseek-v4-flash');
  }
  // Anthropic's native schema requires max_tokens. This is an explicit gateway
  // default, not a CLI envelope conversion.
  if (kind === 'messages' && providerBody.max_tokens == null && helpers.defaultMaxTokens) {
    providerBody.max_tokens = helpers.defaultMaxTokens;
  }

  return {
    path,
    body: providerBody,
    protocol: kind,
    responseProtocol: kind,
    model: providerBody.model,
    stream: providerBody.stream === true,
    headers: requestHeaders(helpers.request),
  };
}

// Provider responses own their IDs and terminal semantics. Metadata is for
// request-scoped logs only; it is never emitted as a synthetic provider object.
export function newResponseMeta(prepared, helpers = {}) {
  const nowUnix = helpers.nowUnix || (() => Math.floor(Date.now() / 1000));
  return { model: prepared.model, protocol: prepared.protocol, created: nowUnix() };
}

export { PATHS };
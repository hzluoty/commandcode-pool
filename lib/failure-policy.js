// One failure-policy owner for public Provider API calls.
// A failure is client-visible and carries explicit account-health intent. The
// policy never authorizes a retry or a replacement account.

export class ClientInputError extends Error {
  constructor(message, status = 400, code = 'invalid_request_error') {
    super(message);
    this.name = 'ClientInputError';
    this.status = status;
    this.code = code;
  }
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function parseErrorBody(text) {
  if (!text) return {};
  try {
    const value = JSON.parse(text);
    return isRecord(value) ? value : {};
  } catch {
    return {};
  }
}

function retryAfterSeconds(headers) {
  const raw = headers?.get?.('Retry-After');
  if (!raw) return 0;
  const seconds = Number.parseInt(raw, 10);
  if (Number.isFinite(seconds) && seconds > 0) return seconds;
  const timestamp = Date.parse(raw);
  return Number.isNaN(timestamp) ? 0 : Math.max(0, Math.ceil((timestamp - Date.now()) / 1000));
}

function statusType(status) {
  if (status === 400 || status === 404 || status === 422) return 'invalid_request_error';
  if (status === 401) return 'authentication_error';
  if (status === 403) return 'permission_error';
  if (status === 429 || status === 402) return 'rate_limit_error';
  return 'server_error';
}

function mappedProviderError(status, bodyText, headers) {
  const parsed = parseErrorBody(bodyText);
  const nested = isRecord(parsed.error) ? parsed.error : {};
  const message = String(
    nested.message || parsed.message || bodyText || `Provider API error (${status})`,
  ).slice(0, 1000);
  const type = String(nested.type || parsed.type || statusType(status));
  const code = nested.code ?? parsed.code ?? null;
  const retryAfter = status === 429 || status === 402 ? retryAfterSeconds(headers) : 0;
  // 402 is a provider billing/quota response. Expose the stable rate-limit
  // shape to clients while retaining the raw status on the failure object.
  const clientStatus = status === 402 ? 429 : (status >= 500 && status !== 503 ? 502 : status);
  return {
    status: clientStatus,
    ...(code ? { code } : {}),
    ...(retryAfter ? { retryAfter } : {}),
    body: {
      error: { message, type: status === 402 ? 'rate_limit_error' : type, ...(code ? { code } : {}) },
      ...(retryAfter ? { retry_after: retryAfter } : {}),
    },
  };
}

function makeFailure(kind, mapped, options = {}) {
  const safeMapped = mapped || {
    status: 502,
    body: { error: { message: 'Provider request failed', type: 'server_error' } },
  };
  return {
    kind,
    mapped: safeMapped,
    rawStatus: options.rawStatus ?? safeMapped.status,
    cooldownSec: Math.max(0, Number(options.cooldownSec) || 0),
    persistHealth: options.persistHealth === true,
    message: String(options.message || safeMapped.body?.error?.message || 'provider failure'),
  };
}

function failureFromInputError(error) {
  const status = Number.isInteger(error?.status) ? error.status : 400;
  return makeFailure('request', {
    status,
    body: { error: { message: String(error?.message || 'invalid request'), type: error?.code || 'invalid_request_error' } },
  }, { rawStatus: status, persistHealth: false, message: error?.message });
}

export function createProviderFailurePolicy({ defaultCooldownSec = 60 } = {}) {
  const transport = (message, status = 502) => makeFailure('operational', {
    status,
    body: {
      error: {
        message: `Provider transport error: ${String(message || 'request failed')}`.slice(0, 1000),
        type: 'server_error',
      },
    },
  }, {
    rawStatus: status,
    persistHealth: false,
    message: String(message || 'request failed'),
  });

  const incomplete = (detail = 'no terminal event') => makeFailure('operational', {
    status: 502,
    body: {
      error: {
        message: `Provider stream ended without a completion terminal (${String(detail)})`,
        type: 'server_error',
      },
    },
  }, { rawStatus: 502, persistHealth: false, message: String(detail) });

  const fromHttp = (status, bodyText, headers) => {
    const numericStatus = Number(status) || 502;
    const mapped = mappedProviderError(numericStatus, bodyText, headers);
    const retryAfter = mapped.retryAfter || defaultCooldownSec;
    if (numericStatus === 401) {
      return makeFailure('disable', mapped, {
        rawStatus: numericStatus,
        persistHealth: true,
        message: 'provider rejected account credential (401)',
      });
    }
    if (numericStatus === 402 || numericStatus === 429) {
      return makeFailure('cooldown', mapped, {
        rawStatus: numericStatus,
        cooldownSec: retryAfter,
        persistHealth: true,
        message: `provider quota/rate limit (${numericStatus})`,
      });
    }
    // A 403 may represent plan/API policy rather than invalid credentials. It
    // is returned as-is and deliberately does not mutate quota/account health.
    return makeFailure(numericStatus >= 500 || numericStatus === 403 ? 'operational' : 'request', mapped, {
      rawStatus: numericStatus,
      persistHealth: false,
    });
  };

  const fromStreamEvent = (event) => {
    const source = isRecord(event?.error) ? event.error : (isRecord(event) ? event : {});
    const type = String(source.type || event?.type || 'server_error');
    const message = String(source.message || event?.message || 'provider returned a stream error');
    let status = Number(source.status || source.status_code || event?.status || event?.status_code);
    if (!Number.isInteger(status) || status < 400 || status > 599) {
      if (type === 'authentication_error') status = 401;
      else if (type === 'rate_limit_error') status = 429;
      else if (type === 'permission_error') status = 403;
      else status = 502;
    }
    return fromHttp(status, JSON.stringify({ error: { type, message, code: source.code } }), null);
  };

  const normalize = (candidate) => {
    if (candidate?.mapped?.body?.error?.message && candidate.kind) return candidate;
    if (candidate?.name === 'ClientInputError') return failureFromInputError(candidate);
    if (candidate instanceof ClientInputError) return failureFromInputError(candidate);
    return transport(candidate?.message || 'unknown provider failure');
  };

  return Object.freeze({ fromHttp, fromStreamEvent, transport, incomplete, normalize });
}

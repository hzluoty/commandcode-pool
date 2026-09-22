import { persistAccountHealth, selectSingleAccount, unavailableAccountResponse } from './pool-store.js';

const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

function schedule(ctx, promise) {
  if (ctx?.waitUntil) {
    ctx.waitUntil(promise);
    return;
  }
  void promise;
}

function relayHeaders(source) {
  const headers = new Headers();
  for (const [name, value] of source.entries()) {
    if (!HOP_BY_HOP_HEADERS.has(name.toLowerCase())) headers.set(name, value);
  }
  return headers;
}

function relayResponse(response) {
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: relayHeaders(response.headers),
  });
}

function usageFromPayload(payload) {
  if (!payload || typeof payload !== 'object') return null;
  const usage = payload.usage || payload.response?.usage || payload.message?.usage || payload.total_usage;
  if (!usage || typeof usage !== 'object') return null;
  const prompt = Number(usage.prompt_tokens ?? usage.input_tokens ?? usage.inputTokens ?? 0);
  const completion = Number(usage.completion_tokens ?? usage.output_tokens ?? usage.outputTokens ?? 0);
  const total = Number(usage.total_tokens ?? usage.totalTokens ?? prompt + completion);
  const cached = Number(
    usage.prompt_tokens_details?.cached_tokens ??
    usage.input_tokens_details?.cached_tokens ??
    usage.cache_read_input_tokens ??
    usage.cachedInputTokens ??
    0,
  );
  return {
    prompt_tokens: Number.isFinite(prompt) ? prompt : 0,
    completion_tokens: Number.isFinite(completion) ? completion : 0,
    total_tokens: Number.isFinite(total) ? total : 0,
    prompt_tokens_details: { cached_tokens: Number.isFinite(cached) ? cached : 0 },
  };
}

function streamSuccess(usage) {
  return { status: 'success', usage: usage || {} };
}

function streamFailure(failure) {
  return { status: 'failure', failure };
}

function streamIncomplete(failure) {
  return { status: 'incomplete', failure };
}

function eventType(payload, eventName) {
  return String(payload?.type || payload?.event || payload?.object || eventName || '');
}

function incompleteDetail(payload, type) {
  const detail = payload?.incomplete_details || payload?.response?.incomplete_details;
  if (detail && typeof detail === 'object') {
    return String(detail.reason || detail.message || type || 'provider marked response incomplete');
  }
  return String(payload?.message || type || 'provider marked response incomplete');
}

/**
 * Creates a TransformStream that passes SSE bytes through untouched to the client
 * while observing SSE lines to detect terminal state and usage.
 * No clone(), no unbounded buffering.
 */
function createSseObserverTransform(options) {
  const { policy, log = console } = options;
  const decoder = new TextDecoder();
  let buffer = '';
  let terminal = null;
  let usage = null;
  let currentEvent = '';

  const processLine = (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    if (trimmed.startsWith('event:')) {
      currentEvent = trimmed.slice(6).trim();
      return;
    }
    if (!trimmed.startsWith('data:') && trimmed !== '[DONE]') return;

    const data = trimmed === '[DONE]' ? '[DONE]' : trimmed.slice(5).trim();
    const eventName = currentEvent;
    currentEvent = '';
    if (data === '[DONE]') {
      if (!terminal) terminal = streamSuccess(usage);
      return;
    }
    if (!data) return;

    let payload;
    try {
      payload = JSON.parse(data);
    } catch {
      return;
    }

    usage = usageFromPayload(payload) || usage;
    const type = eventType(payload, eventName);
    if (type === 'response.completed' || type === 'message_stop') {
      if (!terminal) terminal = streamSuccess(usage);
      return;
    }
    if (type === 'response.failed') {
      if (!terminal) terminal = streamFailure(policy.fromStreamEvent(payload));
      return;
    }
    if (type === 'response.incomplete') {
      if (!terminal) terminal = streamIncomplete(policy.incomplete(incompleteDetail(payload, type)));
      return;
    }
    if (type === 'error' || payload.error) {
      if (!terminal) terminal = streamFailure(policy.fromStreamEvent(payload));
      return;
    }
    if (payload.choices?.some?.((choice) => choice.finish_reason != null)) {
      if (!terminal) terminal = streamSuccess(usage);
    }
  };

  let resolveOutcome;
  const outcomePromise = new Promise((resolve) => {
    resolveOutcome = resolve;
  });

  const settle = (result) => {
    if (resolveOutcome) {
      const fn = resolveOutcome;
      resolveOutcome = null;
      fn(result);
    }
  };

  const transformStream = new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(chunk);
      try {
        buffer += decoder.decode(chunk, { stream: true });
        const lines = buffer.split(/\r?\n/);
        buffer = lines.pop() || '';
        for (const line of lines) processLine(line);
      } catch (err) {
        log.warn?.(JSON.stringify({ message: 'sse observer chunk decode error', error: String(err) }));
      }
    },
    flush() {
      try {
        buffer += decoder.decode();
        if (buffer) processLine(buffer);
      } catch (err) {
        log.warn?.(JSON.stringify({ message: 'sse observer flush decode error', error: String(err) }));
      }
      if (terminal) {
        settle(terminal);
      } else {
        const failure = policy.incomplete('provider stream ended without a completion terminal');
        log.warn?.(JSON.stringify({ message: 'native provider stream ended without terminal event' }));
        settle(streamIncomplete(failure));
      }
    },
  });

  return { transformStream, outcomePromise, settle };
}

function abortError(request) {
  if (request?.signal?.reason instanceof Error) return request.signal.reason;
  const error = new Error('client closed request');
  error.name = 'AbortError';
  return error;
}

/**
 * One logical Provider request selects one account and performs one upstream
 * generation call. Native Provider JSON/SSE is relayed unchanged; this module
 * never translates CC/NDJSON frames and never chooses a replacement account.
 */
export async function runSingleAccountGeneration(options) {
  const {
    request,
    ctx,
    kind,
    cfg,
    pool,
    strategy,
    prepared,
    requestId,
    sendGeneration,
    readErrorText,
    policy,
    renderFailure,
    unavailableResponse,
    log = console,
  } = options;

  if (!policy || typeof policy.normalize !== 'function' || typeof policy.fromHttp !== 'function') {
    throw new TypeError('runSingleAccountGeneration requires a failure policy');
  }
  if (!prepared?.path || !prepared?.body) {
    throw new TypeError('runSingleAccountGeneration requires a prepared Provider request');
  }

  const account = await selectSingleAccount(pool, strategy);
  if (!account) {
    if (typeof unavailableResponse === 'function') return unavailableResponse();
    return unavailableAccountResponse(pool, (status, type, message, retryAfter) =>
      renderFailure(kind, {
        status,
        retryAfter,
        body: { error: { message, type } },
      }));
  }

  if (request.signal.aborted) throw abortError(request);
  const abortController = new AbortController();
  const onClientAbort = () => abortController.abort(request.signal.reason);
  let observerOwnsAbortListener = false;
  request.signal.addEventListener('abort', onClientAbort, { once: true });

  const persistFailure = async (candidate, phase = 'generation') => {
    const failure = policy.normalize(candidate);
    await persistAccountHealth(pool, account, failure, log);
    log.warn?.(JSON.stringify({
      message: 'provider generation failed',
      phase,
      requestId,
      accountId: account.id,
      failureKind: failure.kind,
      rawStatus: failure.rawStatus ?? null,
      persistHealth: failure.persistHealth,
    }));
    return renderFailure(kind, failure.mapped);
  };

  const persistObservedFailure = async (candidate, phase) => {
    const failure = policy.normalize(candidate);
    await persistAccountHealth(pool, account, failure, log);
    log.warn?.(JSON.stringify({
      message: 'provider stream did not complete successfully',
      phase,
      requestId,
      accountId: account.id,
      failureKind: failure.kind,
      rawStatus: failure.rawStatus ?? null,
      persistHealth: failure.persistHealth,
    }));
  };

  const persistSuccess = async (usage) => {
    try {
      await pool.recordSuccess(account.id, usage || {});
    } catch (error) {
      log.error?.(JSON.stringify({
        message: 'failed to record request usage',
        requestId,
        accountId: account.id,
        error: error instanceof Error ? error.message : String(error),
      }));
    }
  };

  try {
    const upstream = await sendGeneration({
      apiBase: cfg.apiBase,
      path: prepared.path,
      body: prepared.body,
      headers: prepared.headers,
      apiKey: account.api_key,
      signal: abortController.signal,
    });

    if (!upstream.ok) {
      let bodyText = '';
      try {
        bodyText = await readErrorText(upstream, abortController.signal);
      } catch (error) {
        return persistFailure(policy.transport(error?.message || 'provider error response could not be read'), 'error-body');
      }
      return persistFailure(policy.fromHttp(upstream.status, bodyText, upstream.headers), 'http');
    }

    const isSse = Boolean(
      prepared.stream ||
      upstream.headers.get('content-type')?.toLowerCase().includes('text/event-stream')
    );

    if (isSse && upstream.body) {
      observerOwnsAbortListener = true;
      const { transformStream, outcomePromise, settle } = createSseObserverTransform({ policy, log });

      const pipePromise = upstream.body.pipeTo(transformStream.writable).catch((error) => {
        if (request.signal.aborted || abortController.signal.aborted) {
          settle({ status: 'aborted' });
        } else {
          settle({ status: 'error', error });
        }
      });

      const backgroundTask = (async () => {
        try {
          const outcome = await outcomePromise;
          if (outcome.status === 'success') {
            await persistSuccess(outcome.usage);
          } else if (outcome.status === 'failure') {
            await persistObservedFailure(outcome.failure, 'stream-failure');
          } else if (outcome.status === 'incomplete') {
            await persistObservedFailure(outcome.failure, 'stream-incomplete');
          } else if (outcome.status === 'error') {
            await persistObservedFailure(policy.transport(outcome.error?.message || 'stream pipe error'), 'stream-error');
          }
          await pipePromise.catch(() => {});
        } catch (err) {
          log.warn?.(JSON.stringify({ message: 'background stream observer failed', error: String(err) }));
        } finally {
          request.signal.removeEventListener('abort', onClientAbort);
        }
      })();

      schedule(ctx, backgroundTask);

      return new Response(transformStream.readable, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers: relayHeaders(upstream.headers),
      });
    }

    // Non-stream response (plain JSON)
    let bodyText = '';
    try {
      bodyText = await upstream.text();
    } catch (error) {
      request.signal.removeEventListener('abort', onClientAbort);
      return persistFailure(policy.transport(error?.message || 'failed to read upstream body'), 'response-body');
    }
    request.signal.removeEventListener('abort', onClientAbort);

    let usage = null;
    try {
      const payload = JSON.parse(bodyText);
      usage = usageFromPayload(payload);
    } catch {
      // not json or invalid, ignore usage parsing
    }

    const task = persistSuccess(usage);
    schedule(ctx, task);

    return new Response(bodyText, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: relayHeaders(upstream.headers),
    });
  } catch (error) {
    if (request.signal.aborted) throw abortError(request);
    return persistFailure(policy.normalize(error), 'transport');
  } finally {
    if (!observerOwnsAbortListener) {
      request.signal.removeEventListener('abort', onClientAbort);
    }
  }
}

export {
  HOP_BY_HOP_HEADERS,
  relayHeaders,
  relayResponse,
  usageFromPayload,
  createSseObserverTransform,
};

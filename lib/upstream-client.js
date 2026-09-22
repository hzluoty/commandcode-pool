// Public Provider API transport: one credential, one request, no retry or identity emulation.

const PROVIDER_GENERATION_PATHS = new Set([
  '/provider/v1/chat/completions',
  '/provider/v1/messages',
  '/provider/v1/responses',
]);

function providerUrl(apiBase, path) {
  if (!PROVIDER_GENERATION_PATHS.has(path)) {
    throw new TypeError(`unsupported Provider API generation path: ${path}`);
  }
  return new URL(path, apiBase).toString();
}

function documentedRequestHeaders(headers = {}) {
  const result = {};
  const source = headers instanceof Headers ? Object.fromEntries(headers.entries()) : headers;
  // This is the only optional client header documented by the Provider API.
  if (source && (source['x-cmd-zdr'] === '1' || source['X-Cmd-Zdr'] === '1')) {
    result['x-cmd-zdr'] = '1';
  }
  return result;
}

/**
 * Send exactly one documented Provider API generation request.
 * Credential selection and account health are deliberately owned by the caller.
 */
export async function sendGeneration({ apiBase, path, body, headers, apiKey, signal }) {
  const stream = body?.stream === true;
  return fetch(providerUrl(apiBase, path), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': stream ? 'text/event-stream' : 'application/json',
      'Authorization': `Bearer ${apiKey}`,
      ...documentedRequestHeaders(headers),
    },
    body: JSON.stringify(body),
    signal,
  });
}

export { PROVIDER_GENERATION_PATHS, providerUrl };

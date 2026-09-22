// Pool-store boundary shared by D1Pool and MemPool.
// Generation routing selects one account once and never asks for a replacement.

export async function selectSingleAccount(pool, strategy) {
  return pool.pick(strategy);
}

/**
 * Persist only provider evidence that changes future account eligibility.
 * Operational, request-validation, transport, timeout, and incomplete-output
 * failures must not be recorded as quota exhaustion.
 */
export async function persistAccountHealth(pool, account, failure, log = console) {
  if (!account || !failure || failure.persistHealth !== true) return;

  const reason = String(
    failure.message || failure.mapped?.body?.error?.message || 'provider account failure',
  ).slice(0, 200);

  try {
    if (failure.kind === 'disable') {
      await pool.disable(account.id, reason || 'provider authentication failed');
    } else if (failure.kind === 'cooldown') {
      await pool.cooldown(account.id, Math.max(1, Number(failure.cooldownSec) || 0), reason);
    }
  } catch (error) {
    log.error?.(JSON.stringify({
      message: 'failed to persist account health',
      accountId: account.id,
      failureKind: failure.kind,
      error: error instanceof Error ? error.message : String(error),
    }));
  }
}

/** Only used when selection failed before an upstream generation request existed. */
export async function unavailableAccountResponse(pool, makeError) {
  const earliest = await pool.earliestRateLimitedUntil();
  const now = Date.now();
  const waitSec = earliest > now ? Math.ceil((earliest - now) / 1000) : 0;
  if (waitSec > 0) {
    return makeError(429, 'rate_limit_error', `assigned Command Code account is unavailable; retry after ${waitSec}s`, waitSec);
  }
  return makeError(503, 'server_error', 'no enabled Command Code account is available');
}
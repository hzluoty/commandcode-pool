// Quota client for Command Code control-plane quota reporting.
// Encapsulates /alpha/* endpoints, plan estimation, window normalization, and detail mapping.

export const KNOWN_PLANS = {
  'individual-go':       { name: 'Go',         monthlyCredits: 10 },
  'individual-goat':     { name: 'GOAT',       monthlyCredits: 70 },
  'individual-pro':      { name: 'Pro',        monthlyCredits: 30 },
  'individual-pro-v1':   { name: 'Pro',        monthlyCredits: 80 },
  'individual-provider': { name: 'Provider',   monthlyCredits: 15 },
  'individual-max':      { name: 'Max',        monthlyCredits: 150 },
  'individual-ultra':    { name: 'Ultra',      monthlyCredits: 300 },
  'teams-pro':           { name: 'Teams Pro',  monthlyCredits: 40 },
};

const PLAN_PREFIXES = Object.keys(KNOWN_PLANS).sort((a, b) => b.length - a.length);

export function planInfo(planId) {
  if (!planId) return undefined;
  const norm = String(planId).toLowerCase().replace(/_/g, '-');
  const prefix = PLAN_PREFIXES.find((p) => norm.startsWith(p));
  return prefix ? KNOWN_PLANS[prefix] : undefined;
}

function isRecord(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
function num(v) { return typeof v === 'number' && Number.isFinite(v) ? v : null; }
function str(v) { return typeof v === 'string' ? v : null; }

function toEpochMs(v) {
  if (typeof v === 'number' && Number.isFinite(v)) {
    return v < 1e12 ? v * 1000 : v;
  }
  if (typeof v === 'string' && v) {
    const t = Date.parse(v);
    if (!Number.isNaN(t)) return t;
  }
  return null;
}

export function pickWindow(wl, names) {
  if (!isRecord(wl)) return undefined;
  for (const n of names) {
    if (isRecord(wl[n])) return wl[n];
  }
  return undefined;
}

export function normalizeWindow(raw) {
  if (!isRecord(raw)) return undefined;
  const used = num(raw.used) ?? num(raw.usage) ?? num(raw.usedCredits) ?? num(raw.used_credits);
  const cap = num(raw.cap) ?? num(raw.limit) ?? num(raw.capCredits);
  const exceeded = raw.exceeded === true || raw.exceeded === 'true' ||
    (used != null && cap != null && cap > 0 && used >= cap);
  return {
    used: used ?? 0,
    cap: cap ?? 0,
    exceeded,
    resetAt: toEpochMs(raw.resetAt ?? raw.reset_at ?? raw.resetsAt) ?? 0,
  };
}

export function buildDetail(report) {
  const c = report.credits || {};
  const p = report.plan || {};
  return JSON.stringify({
    usage: report.usage || null,
    limited: c.limited ?? null,
    exceeded: c.exceeded || null,
    belowThreshold: c.belowThreshold ?? null,
    creditThreshold: c.creditThreshold ?? null,
    cancelAtPeriodEnd: p.cancelAtPeriodEnd ?? null,
    currentPeriodStart: p.currentPeriodStart ?? null,
    currentPeriodEnd: p.currentPeriodEnd ?? null,
    planStatus: p.status || null,
    pendingPhase: p.pendingPhase ?? null,
    lastChecked: Date.now(),
    partial: Boolean(report.failures?.length),
    failures: report.failures || [],
  });
}

async function getJson(url, key, signal, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);
  if (signal) signal.addEventListener('abort', () => controller.abort(signal.reason), { once: true });
  try {
    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${key}`,
        'Accept': 'application/json',
      },
      signal: controller.signal,
    });
    const text = await resp.text();
    if (!resp.ok) {
      const err = new Error(`HTTP ${resp.status}`);
      err.status = resp.status;
      const retryAfter = Number(resp.headers.get('retry-after')) || 0;
      if (retryAfter > 0) err.retryAfter = retryAfter;
      err.body = text.slice(0, 300);
      throw err;
    }
    return JSON.parse(text);
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchAccountQuota(base, key, { signal, timeoutMs = 10000 } = {}) {
  const failures = [];
  const report = {};
  const baseUrl = String(base || '').replace(/\/+$/, '');

  let orgId;
  try {
    const who = await getJson(`${baseUrl}/alpha/whoami`, key, signal, timeoutMs);
    const user = isRecord(who.user) ? who.user : (isRecord(who.data) && isRecord(who.data.user) ? who.data.user : undefined);
    if (user) {
      report.account = {
        id: str(user.id) ?? '',
        name: str(user.name) ?? '',
        userName: str(user.userName) ?? str(user.username) ?? '',
      };
    }
    const org = isRecord(who.org) ? who.org : undefined;
    orgId = org ? str(org.id) : undefined;
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError') throw e;
    if (e.status === 401 || e.status === 403 || e.status === 429) {
      throw e;
    }
    failures.push(`whoami: ${e.message}`);
  }

  try {
    const cr = await getJson(`${baseUrl}/alpha/billing/credits`, key, signal, timeoutMs);
    const credits = isRecord(cr.credits) ? cr.credits :
      (isRecord(cr.data) && isRecord(cr.data.credits) ? cr.data.credits : undefined);
    const wl = isRecord(cr.windowLimits) ? cr.windowLimits :
      (isRecord(cr.data) && isRecord(cr.data.windowLimits) ? cr.data.windowLimits : undefined);

    if (credits || wl) {
      report.credits = {
        monthlyCredits: num(credits?.monthlyCredits) ?? num(credits?.monthly_credits) ?? null,
        purchasedCredits: num(credits?.purchasedCredits) ?? num(credits?.purchased_credits) ?? null,
        freeCredits: num(credits?.freeCredits) ?? num(credits?.free_credits) ?? null,
        limited: wl?.limited === true,
        exceeded: str(wl?.exceeded) ?? '',
        belowThreshold: credits?.belowThreshold === true,
        creditThreshold: num(credits?.creditThreshold) ?? null,
        fiveHour: normalizeWindow(pickWindow(wl || {}, ['fiveHour', 'five_hour', 'rolling5h', '5h'])),
        weekly: normalizeWindow(pickWindow(wl || {}, ['weekly', 'week'])),
      };
      if (credits) report._planIdFallback = str(credits.planId) ?? str(credits.plan_id);
    }
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError') throw e;
    if (e.status === 401 || e.status === 403 || e.status === 429) throw e;
    failures.push(`billing/credits: ${e.message}`);
  }

  try {
    const subUrl = orgId
      ? `${baseUrl}/alpha/billing/subscriptions?orgId=${encodeURIComponent(orgId)}`
      : `${baseUrl}/alpha/billing/subscriptions`;
    const sub = await getJson(subUrl, key, signal, timeoutMs);
    const data = isRecord(sub.data) ? sub.data : (isRecord(sub.subscription) ? sub.subscription : undefined);
    const planId = str(data?.planId) ?? str(data?.plan_id) ?? report._planIdFallback;
    if (data || planId) {
      const info = planInfo(planId);
      report.plan = {
        planId: planId ?? '',
        name: info?.name ?? planId ?? '',
        status: str(data?.status) ?? '',
        monthlyCredits: info ? info.monthlyCredits : null,
        currentPeriodEnd: toEpochMs(data?.currentPeriodEnd ?? data?.current_period_end) ?? 0,
        cancelAtPeriodEnd: data?.cancelAtPeriodEnd === true,
        currentPeriodStart: toEpochMs(data?.currentPeriodStart ?? data?.current_period_start) ?? 0,
        pendingPhase: data?.pendingPhase ?? null,
      };
    }
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError') throw e;
    if (e.status === 401 || e.status === 403 || e.status === 429) throw e;
    const planId = report._planIdFallback;
    if (planId) {
      const info = planInfo(planId);
      report.plan = {
        planId,
        name: info?.name ?? planId,
        status: '',
        monthlyCredits: info ? info.monthlyCredits : null,
        currentPeriodEnd: 0,
        cancelAtPeriodEnd: false,
        currentPeriodStart: 0,
        pendingPhase: null,
      };
    }
    failures.push(`billing/subscriptions: ${e.message}`);
  }
  delete report._planIdFallback;

  try {
    const us = await getJson(`${baseUrl}/alpha/usage/summary`, key, signal, timeoutMs);
    const u = isRecord(us.data) ? us.data : us;
    if (isRecord(u)) {
      report.usage = {
        totalCount: num(u.totalCount) ?? 0,
        totalCost: num(u.totalCost) ?? 0,
        averageCost: num(u.averageCost) ?? null,
        successRate: num(u.successRate) ?? 0,
        completedCount: num(u.completedCount) ?? 0,
        failedCount: num(u.failedCount) ?? 0,
        totalTokensIn: num(u.totalTokensIn) ?? 0,
        totalTokensOut: num(u.totalTokensOut) ?? 0,
        totalCredits: num(u.totalCredits) ?? 0,
        periodBasis: str(u.periodBasis) ?? '',
      };
    }
  } catch (e) {
    if (signal?.aborted || e?.name === 'AbortError') throw e;
    if (e.status === 401 || e.status === 403 || e.status === 429) throw e;
    failures.push(`usage/summary: ${e.message}`);
  }

  if (failures.length) report.failures = failures;
  if (!report.account && !report.credits && !report.plan) {
    const err = new Error(`所有 quota 端点均无法访问: ${failures.join('; ')}`);
    err.status = 502;
    throw err;
  }
  return report;
}

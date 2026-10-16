// lib/quota-service.js
// 抓取并解析 Command Code 账号配额与用量（对齐 commandcode-usage 与 pi-commandcode-provider）
// 深度支持 Go 套餐（individual-go）：额度 10 credits/月、5h 窗口、周窗口及用量推导
import { planInfo, KNOWN_PLANS } from './constants.js';
import { isRecord, num, str, toEpochMs } from './crypto-utils.js';

async function getJson(base, path, key) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  try {
    const resp = await fetch(base + path, {
      signal: controller.signal,
      headers: {
        'Authorization': 'Bearer ' + key,
        'Accept': 'application/json',
        'User-Agent': 'commandcode-pool/1.0',
      },
    });
    const text = await resp.text();
    if (!resp.ok) {
      const err = new Error(`HTTP ${resp.status}: ${text.slice(0, 150)}`);
      err.status = resp.status;
      err.body = text.slice(0, 300);
      throw err;
    }
    try {
      return JSON.parse(text);
    } catch {
      const err = new Error('non-JSON response');
      err.status = 502;
      throw err;
    }
  } finally {
    clearTimeout(timer);
  }
}

function normalizeWindow(raw) {
  if (!isRecord(raw)) return null;
  const used = num(raw.used) ?? num(raw.usage) ?? num(raw.usedCredits) ?? num(raw.used_credits) ?? 0;
  const cap = num(raw.cap) ?? num(raw.limit) ?? num(raw.capCredits) ?? 0;
  const exceeded = raw.exceeded === true || raw.exceeded === 'true' || (cap > 0 && used >= cap);
  return {
    used,
    cap,
    exceeded,
    resetAt: toEpochMs(raw.resetAt ?? raw.reset_at ?? raw.resetsAt) ?? 0,
  };
}

export async function fetchReport(apiBase, apiKey) {
  // 1. whoami 鉴权并获取身份（此接口挂则整账号判定失效）
  const who = await getJson(apiBase, '/alpha/whoami', apiKey);
  const u = isRecord(who?.user) ? who.user : (isRecord(who) ? who : {});
  const org = isRecord(who?.org) ? who.org : (isRecord(who?.organization) ? who.organization : {});

  const accountInfo = {
    id: str(u.id) || '',
    name: str(u.name) || '',
    userName: str(u.userName) || str(u.username) || str(u.name) || '',
    email: str(u.email) || '',
    orgId: str(org.id) || '',
  };

  // 2. 账单信用额 & 限制窗口（/alpha/billing/credits）
  let creditsRaw = null;
  try {
    creditsRaw = await getJson(apiBase, '/alpha/billing/credits', apiKey);
  } catch (e) {
    // 允许降级
  }

  // 3. 订阅状态与套餐 ID（/alpha/billing/subscriptions）
  let subRaw = null;
  try {
    const q = accountInfo.orgId ? `?orgId=${encodeURIComponent(accountInfo.orgId)}` : '';
    subRaw = await getJson(apiBase, `/alpha/billing/subscriptions${q}`, apiKey);
  } catch (e) {
    // 允许降级
  }

  // 4. 账期用量汇总（/alpha/usage/summary）
  let summaryRaw = null;
  try {
    summaryRaw = await getJson(apiBase, '/alpha/usage/summary', apiKey);
  } catch (e) {
    // 允许降级
  }

  // 解析套餐（核心处理 Go 套餐及其它套餐）
  const subData = isRecord(subRaw?.data) ? subRaw.data : (Array.isArray(subRaw?.data) ? subRaw.data[0] : (isRecord(subRaw) ? subRaw : {}));
  const rawPlanId = str(subData.planId) || str(subData.plan_id) || str(creditsRaw?.credits?.planId) || '';
  const matchedPlan = planInfo(rawPlanId);

  const planId = rawPlanId || (matchedPlan ? 'individual-go' : 'unknown');
  const planName = matchedPlan ? matchedPlan.name : (planId ? planId.replace(/^individual-/, '').toUpperCase() : 'Free');
  const isGoPlan = rawPlanId.toLowerCase().includes('go') && !rawPlanId.toLowerCase().includes('goat');

  // 解析 Credits
  const c = isRecord(creditsRaw?.credits) ? creditsRaw.credits : {};
  const monthlyLeft = num(c.monthlyCredits) ?? num(c.monthly_credits);
  const purchased = num(c.purchasedCredits) ?? num(c.purchased_credits) ?? 0;
  const free = num(c.freeCredits) ?? num(c.free_credits) ?? 0;

  // 套餐月度上限与已用推导（Go 套餐 cap 恒定为 10，以此准确计算进度条）
  let monthlyCap = matchedPlan ? matchedPlan.monthlyCredits : (isGoPlan ? 10 : null);
  let monthlyUsed = null;
  if (monthlyCap != null && monthlyLeft != null) {
    monthlyUsed = Math.max(0, monthlyCap - monthlyLeft);
  }

  // 解析 5 小时与周滚动限制窗口
  const wl = isRecord(creditsRaw?.windowLimits) ? creditsRaw.windowLimits : (isRecord(creditsRaw?.window_limits) ? creditsRaw.window_limits : {});
  const fiveHour = normalizeWindow(wl.fiveHour ?? wl.five_hour ?? wl['5h']);
  const weekly = normalizeWindow(wl.weekly ?? wl.week ?? wl['7d']);

  // 账期总计
  const summary = isRecord(summaryRaw) ? {
    requests: num(summaryRaw.requests) ?? 0,
    successRate: num(summaryRaw.successRate) ?? num(summaryRaw.success_rate) ?? null,
    tokensIn: num(summaryRaw.tokensIn) ?? num(summaryRaw.tokens_in) ?? 0,
    tokensOut: num(summaryRaw.tokensOut) ?? num(summaryRaw.tokens_out) ?? 0,
    creditsSpent: num(summaryRaw.creditsSpent) ?? num(summaryRaw.credits_spent) ?? 0,
    periodBasis: str(summaryRaw.periodBasis) || str(summaryRaw.period_basis) || '',
  } : null;

  return {
    account: accountInfo,
    plan: {
      planId,
      planName,
      isGoPlan,
      status: str(subData.status) || 'active',
      currentPeriodStart: toEpochMs(subData.currentPeriodStart ?? subData.current_period_start),
      currentPeriodEnd: toEpochMs(subData.currentPeriodEnd ?? subData.current_period_end),
      cancelAtPeriodEnd: subData.cancelAtPeriodEnd === true || subData.cancel_at_period_end === true,
    },
    credits: {
      monthlyLeft,
      purchased,
      free,
      totalRemaining: (monthlyLeft ?? 0) + purchased + free,
      monthlyCap,
      monthlyUsed,
    },
    fiveHour: fiveHour || { used: 0, cap: 0, exceeded: false, resetAt: 0 },
    weekly: weekly || { used: 0, cap: 0, exceeded: false, resetAt: 0 },
    summary,
    fetchedAt: Date.now(),
  };
}

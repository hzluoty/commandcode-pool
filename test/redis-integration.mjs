// Run only against an empty, disposable local Redis database.
import assert from 'node:assert/strict';
import { createClient } from 'redis';
import { RedisPool } from '../lib/pool-store.js';
import { USAGE_RETENTION_SECONDS } from '../lib/constants.js';
import { usageBucketStart } from '../lib/crypto-utils.js';

const url = new URL(process.argv[2] || 'redis://127.0.0.1:16379');
assert(url.protocol === 'redis:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname), 'Use a disposable loopback Redis instance, not production');
const options = { url: url.href, socket: { reconnectStrategy: false, connectTimeout: 2000 } };
const clients = [createClient(options), createClient(options)];
for (const client of clients) client.on('error', () => {});
const realNow = Date.now;
const now = realNow();
Date.now = () => now;

try {
  await Promise.all(clients.map(client => client.connect()));
  assert.equal(await clients[0].dbSize(), 0, 'Test database must be empty');
  const [pool, otherPool] = clients.map(client => new RedisPool(url.href, 8, client));
  await clients[0].hSet('cc_pool:accounts', '99', JSON.stringify({ id: 99, api_key: 'user_local_redis_legacy_key_1', label: 'legacy' }));
  await assert.rejects(pool.add('user_local_redis_legacy_key_1', 'legacy duplicate'), error => error.status === 409, 'First add must migrate existing accounts before duplicate checking');
  assert.equal(Boolean(await clients[0].sIsMember('cc_pool:api_keys', 'user_local_redis_legacy_key_1')), true, 'Legacy key is added to the API-key index');
  const id = await pool.add('user_local_redis_test_key_1', 'existing account');
  const duplicateAdds = await Promise.allSettled([
    pool.add('user_local_redis_duplicate_key_1', 'duplicate-a'),
    otherPool.add('user_local_redis_duplicate_key_1', 'duplicate-b'),
  ]);
  assert.equal(duplicateAdds.filter(result => result.status === 'fulfilled').length, 1, 'Concurrent adds of one API key must create one account');
  assert.equal(duplicateAdds.filter(result => result.status === 'rejected' && result.reason.status === 409).length, 1, 'Concurrent duplicate add must return HTTP 409');
  const raw = await pool.getRaw(id);
  Object.assign(raw, { requests: 5, prompt_tokens: 10, plan_id: 'individual-go' });
  await clients[0].hSet('cc_pool:accounts', String(id), JSON.stringify(raw));

  const currentBucket = usageBucketStart(now, 8);
  const oldBucketKey = `cc_pool:bucket:${currentBucket - 2 * 86400}`;
  await clients[0].hSet(oldBucketKey, String(id), JSON.stringify({ requests: 3, prompt_tokens: 12 }));
  await clients[0].expire(oldBucketKey, 31 * 86400);
  const beforeStats = await clients[0].info('commandstats');
  const expireCalls = info => Number(info.match(/cmdstat_expire:calls=(\d+)/)?.[1] || 0);
  const usage = { prompt_tokens: 2, completion_tokens: 3, prompt_tokens_details: { cached_tokens: 1 } };
  await Promise.all(Array.from({ length: 100 }, (_, i) => [pool, otherPool][i % 2].recordSuccess(id, usage)));
  const afterStats = await clients[0].info('commandstats');
  assert.equal(expireCalls(afterStats) - expireCalls(beforeStats), 1, 'EXPIRE runs once for the new bucket, not per request');
  const updated = await pool.getRaw(id);
  assert.equal(updated.requests, 105, '100 concurrent writes from two clients must not lose account counts');
  assert.equal(updated.prompt_tokens, 210);
  assert.equal(updated.completion_tokens, 300);
  assert.equal(updated.cache_read_tokens, 100);
  assert.equal(updated.plan_id, 'individual-go', 'Existing account metadata is preserved');
  assert.equal(updated.label, 'existing account');
  const bucketKey = `cc_pool:bucket:${currentBucket}`;
  const bucket = JSON.parse(await clients[0].hGet(bucketKey, String(id)));
  assert.deepEqual(bucket, { requests: 100, prompt_tokens: 200, completion_tokens: 300, cache_read_tokens: 100 });
  const ttl = await clients[0].ttl(bucketKey);
  assert(ttl > USAGE_RETENTION_SECONDS - 10 && ttl <= USAGE_RETENTION_SECONDS, 'New buckets expire after max range + one day');

  const id2 = await otherPool.add('user_local_redis_test_key_2', 'second account');
  await clients[0].expire(bucketKey, 10);
  await otherPool.recordSuccess(id2, usage);
  assert((await clients[0].ttl(bucketKey)) <= 10, 'Another account writing to the same bucket must not refresh its TTL');
  assert.equal(await clients[0].hLen(bucketKey), 2);

  const sevenDays = await pool.dailyUsage('7d');
  assert.deepEqual(sevenDays.totals, { requests: 104, promptTokens: 214, completionTokens: 303, cacheReadTokens: 101 }, 'Pipelined aggregation includes existing JSON buckets');
  assert.equal(sevenDays.daily.flatMap(day => day.accounts).length, 3);
  assert.deepEqual(await pool.dailyUsage('30d'), await pool.dailyUsage('1d'), 'Removed 30d range falls back without an error');
  assert((await clients[0].ttl(oldBucketKey)) > 30 * 86400, 'Legacy buckets keep their existing TTL and are not deleted');
  // Force a metadata read to return an old snapshot only after a second client has recorded success.
  const counters = raw => [raw.requests, raw.prompt_tokens, raw.completion_tokens, raw.cache_read_tokens];
  const expectedCounters = counters(await pool.getRaw(id));
  const originalGetRaw = pool.getRaw;
  let interleaved = false;
  pool.getRaw = async accountId => {
    const snapshot = await originalGetRaw.call(pool, accountId);
    pool.getRaw = originalGetRaw;
    await otherPool.recordSuccess(id, usage);
    interleaved = true;
    return snapshot;
  };
  await pool.saveQuota(id, {
    account: { userName: 'fresh-name' },
    plan: { planId: 'individual-go', planName: 'Go', isGoPlan: true },
    credits: { monthlyLeft: 8, monthlyCap: 10, monthlyUsed: 2 },
  });
  assert(interleaved, 'Test must force the stale-read interleaving');
  assert.deepEqual(counters(await pool.getRaw(id)), expectedCounters.map((value, i) => value + [1, 2, 3, 1][i]), 'Quota refresh must not overwrite concurrent success counts');
  assert.equal((await pool.getRaw(id)).monthly_left, 8);
  assert.equal((await pool.getRaw(id)).user_name, 'fresh-name');

  // Delay each metadata write until another client has incremented the latest counters.
  for (const update of [
    () => pool.patch(id, { label: 'renamed', enabled: true }),
    () => pool.cooldown(id, 30, 'rate limited'),
    () => pool.disable(id, 'disabled by test'),
    () => pool.markQuotaError(id, 'quota failed'),
  ]) {
    const before = counters(await pool.getRaw(id));
    const originalCmd = pool.cmd;
    let delayed = false;
    pool.cmd = async (...args) => {
      const isWrite = args[0] === 'EVAL' || args[0] === 'HSET';
      if (!delayed && isWrite) {
        delayed = true;
        await otherPool.recordSuccess(id, usage);
      }
      return originalCmd.call(pool, ...args);
    };
    try { await update(); } finally { pool.cmd = originalCmd; }
    assert(delayed, 'Test must delay a metadata write');
    assert.deepEqual(counters(await pool.getRaw(id)), before.map((value, i) => value + [1, 2, 3, 1][i]), 'Metadata writes must preserve concurrent success counts');
  }
  assert.equal((await pool.getRaw(id)).label, 'renamed');
  assert.equal((await pool.getRaw(id)).disabled_reason, 'disabled by test');
  assert.equal((await pool.getRaw(id)).last_error, 'quota failed');
  assert.deepEqual((await pool.dailyUsage('5m')).totals, { requests: 106, promptTokens: 212, completionTokens: 318, cacheReadTokens: 106 }, 'Account and bucket counts agree after interleaved metadata updates');

  // A refresh that completes after deletion must not recreate the account.
  pool.getRaw = async accountId => {
    const snapshot = await originalGetRaw.call(pool, accountId);
    pool.getRaw = originalGetRaw;
    await otherPool.remove(accountId);
    return snapshot;
  };
  await pool.saveQuota(id2, { account: { userName: 'must not return' } });
  assert.equal(await pool.getRaw(id2), null, 'Quota refresh must not resurrect a deleted account');
  assert.equal(await pool.patch(id2, { enabled: true }), false);

  await clients[0].set('wrong-type', 'not a hash');
  await assert.rejects(pool.cmdMany([['HGETALL', 'wrong-type']]), /WRONGTYPE/, 'Pipeline failures propagate');

  Date.now = () => now + 300_000;
  await pool.recordSuccess(9999, usage);
  assert.equal(await clients[0].exists(`cc_pool:bucket:${currentBucket + 300}`), 0, 'Missing accounts must not create orphan usage buckets');
  console.log('Redis 7 integration passed: concurrent counters, interleaved metadata writes, legacy JSON, one-time TTL, pipelines, and 30d fallback.');
} finally {
  Date.now = realNow;
  for (const client of clients) if (client.isOpen) client.destroy();
}

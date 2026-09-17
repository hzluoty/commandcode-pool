-- commandcode-pool D1 schema（worker 启动时也会自动创建，此文件供参考/手动初始化）
-- 注意：usage_daily 为旧版遗留表（只读）。Worker 不再写入它；检测到旧数据时会在启动时
-- 自动做一次迁移（按天折叠进 usage_buckets 的 5 分钟桶），迁移标记存在 usage_meta。
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  api_key TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  -- 额度缓存（/alpha/* 刷新结果）
  user_name TEXT NOT NULL DEFAULT '',
  plan_id TEXT NOT NULL DEFAULT '',
  plan_name TEXT NOT NULL DEFAULT '',
  monthly_left REAL,
  purchased REAL,
  free REAL,
  five_hour_used REAL,
  five_hour_cap REAL,
  five_hour_exceeded INTEGER NOT NULL DEFAULT 0,
  five_hour_reset INTEGER NOT NULL DEFAULT 0,
  weekly_used REAL,
  weekly_cap REAL,
  weekly_exceeded INTEGER NOT NULL DEFAULT 0,
  weekly_reset INTEGER NOT NULL DEFAULT 0,
  -- 号池状态
  rate_limited_until INTEGER NOT NULL DEFAULT 0,
  disabled_reason TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  last_used_at INTEGER NOT NULL DEFAULT 0,
  -- 本网关累计用量
  requests INTEGER NOT NULL DEFAULT 0,
  prompt_tokens INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  detail TEXT
);
CREATE TABLE IF NOT EXISTS usage_daily (
  day TEXT NOT NULL,
  account_id INTEGER NOT NULL,
  requests INTEGER NOT NULL DEFAULT 0,
  prompt_tokens INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (day, account_id),
  FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_usage_daily_day ON usage_daily(day);
CREATE TABLE IF NOT EXISTS usage_buckets (
  bucket_start INTEGER NOT NULL,
  account_id INTEGER NOT NULL,
  requests INTEGER NOT NULL DEFAULT 0,
  prompt_tokens INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, account_id),
  FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_usage_buckets_start ON usage_buckets(bucket_start);
CREATE TABLE IF NOT EXISTS usage_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_accounts_enabled ON accounts(enabled, rate_limited_until);

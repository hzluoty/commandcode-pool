// lib/fingerprint.js
// 逐字段对齐 commandcode-proxy 与 Command Code 官方 CLI 1.53.1 设备指纹与生命周期事件
import crypto from 'node:crypto';
import { CC_PROTOCOL_VERSION } from './constants.js';
import { slugifyProjectPath } from './crypto-utils.js';

const FP_SALT = 'command-code:device-fingerprint:v1';

const FINGERPRINT_CPUS = [
  { model: '12th Gen Intel(R) Core(TM) i7-12650H', cores: 10, threads: 16 },
  { model: '12th Gen Intel(R) Core(TM) i5-12400F', cores: 6,  threads: 12 },
  { model: '12th Gen Intel(R) Core(TM) i9-12900K', cores: 16, threads: 24 },
  { model: '13th Gen Intel(R) Core(TM) i7-13700K', cores: 16, threads: 24 },
  { model: '13th Gen Intel(R) Core(TM) i5-13600K', cores: 14, threads: 20 },
  { model: '13th Gen Intel(R) Core(TM) i9-13900K', cores: 24, threads: 32 },
  { model: 'Intel(R) Core(TM) Ultra 7 155H',       cores: 16, threads: 22 },
  { model: 'Intel(R) Core(TM) Ultra 9 285H',       cores: 16, threads: 16 },
  { model: 'Intel(R) Core(TM) i9-14900K',          cores: 24, threads: 32 },
  { model: 'Intel(R) Core(TM) i7-14700K',          cores: 20, threads: 28 },
  { model: 'AMD Ryzen 7 7800X3D',                  cores: 8,  threads: 16 },
  { model: 'AMD Ryzen 9 7950X',                    cores: 16, threads: 32 },
  { model: 'AMD Ryzen 5 7600',                     cores: 6,  threads: 12 },
  { model: 'AMD Ryzen 9 7900X',                    cores: 12, threads: 24 },
  { model: 'AMD Ryzen 7 5800X3D',                  cores: 8,  threads: 16 },
];

const FINGERPRINT_MEMS = [8, 16, 24, 32, 48, 64];

const FINGERPRINT_TZS = [
  'America/New_York', 'America/Chicago', 'America/Los_Angeles', 'America/Toronto',
  'Europe/London', 'Europe/Berlin', 'Europe/Paris', 'Europe/Moscow',
  'Asia/Shanghai', 'Asia/Tokyo', 'Asia/Singapore', 'Asia/Seoul', 'Asia/Hong_Kong',
  'Australia/Sydney', 'Pacific/Auckland',
];

const FP_OS_USERS = ['dev', 'user', 'admin', 'coder', 'engineer', 'work'];
const FP_MAIL_DOMAINS = ['gmail.com', 'outlook.com', 'qq.com', '163.com'];

function fpDigest(apiKey, field, salt = '') {
  return crypto.createHash('sha256')
    .update(`${salt}\0${apiKey}\0${field}`)
    .digest();
}

function fpPickIndex(apiKey, field, items, labelOf, salt = '') {
  let bestIdx = 0;
  let bestScore = null;
  for (let i = 0; i < items.length; i++) {
    const score = fpDigest(apiKey, `${field}\0${labelOf(i)}`, salt);
    if (!bestScore || Buffer.compare(score, bestScore) > 0) {
      bestScore = score;
      bestIdx = i;
    }
  }
  return bestIdx;
}

function fingerprintHash(value) {
  const v = String(value ?? '').trim();
  if (!v) return undefined;
  return crypto.createHash('sha256').update(`${FP_SALT}\0${v.toLowerCase()}`).digest('hex');
}

export function generateFingerprint(apiKey, config = {}) {
  const salt = config.fingerprintSalt || '';
  const projectDir = config.deviceProjectDir || 'C:\\Users\\dev\\projects\\app';

  const cpu = FINGERPRINT_CPUS[fpPickIndex(apiKey, 'cpu', FINGERPRINT_CPUS, i => FINGERPRINT_CPUS[i].model, salt)];
  const memGb = FINGERPRINT_MEMS[fpPickIndex(apiKey, 'mem', FINGERPRINT_MEMS, i => String(FINGERPRINT_MEMS[i]), salt)];
  const timezone = FINGERPRINT_TZS[fpPickIndex(apiKey, 'tz', FINGERPRINT_TZS, i => FINGERPRINT_TZS[i], salt)];

  const rawMachineGuid = fpDigest(apiKey, 'machineGuid', salt).toString('hex').slice(0, 32)
    .replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
  const hashedMachineId = fingerprintHash(rawMachineGuid);

  // 随机 2~4 个 MAC
  const macCount = 2 + (fpDigest(apiKey, 'macCount', salt)[0] % 3);
  const rawMacs = [];
  for (let i = 0; i < macCount; i++) {
    const b = fpDigest(apiKey, `mac:${i}`, salt);
    const first = (b[0] & 0xfe) | 0x02; // unicast locally administered
    const hex = [first, b[1], b[2], b[3], b[4], b[5]]
      .map(x => x.toString(16).padStart(2, '0'))
      .join(':');
    rawMacs.push(hex);
  }
  const hashedMac = fingerprintHash(rawMacs.sort().join(','));

  const osUser = FP_OS_USERS[fpPickIndex(apiKey, 'osUser', FP_OS_USERS, i => FP_OS_USERS[i], salt)];
  const hashedOsUser = fingerprintHash(osUser);

  const hostSuffix = fpDigest(apiKey, 'hostSuffix', salt).toString('hex').slice(0, 6);
  const hostname = `DESKTOP-${hostSuffix.toUpperCase()}`;
  const hashedHostname = fingerprintHash(hostname);

  const mailDomain = FP_MAIL_DOMAINS[fpPickIndex(apiKey, 'mailDomain', FP_MAIL_DOMAINS, i => FP_MAIL_DOMAINS[i], salt)];
  const gitEmail = `${osUser}_${hostSuffix.toLowerCase()}@${mailDomain}`;
  const hashedGitEmail = fingerprintHash(gitEmail);

  const components = {
    cpuModel: cpu.model,
    cpuCount: cpu.threads,
    totalMemoryBytes: memGb * 1024 * 1024 * 1024,
    timezone,
    osPlatform: 'win32',
    osRelease: '10.0.22631',
    osArch: 'x64',
    isContainer: false,
    machineId: hashedMachineId,
    mac: hashedMac,
    osUser: hashedOsUser,
    hostname: hashedHostname,
    gitEmail: hashedGitEmail,
  };

  const payload = JSON.stringify(components);
  const hash = crypto.createHash('sha256').update(payload).digest('hex');

  return {
    components,
    hash,
    raw: {
      cpu,
      memGb,
      timezone,
      machineGuid: rawMachineGuid,
      macs: rawMacs,
      osUser,
      hostname,
      gitEmail,
      projectDir,
    },
  };
}

export function sessionForKey(apiKey, config = {}) {
  const salt = config.fingerprintSalt || '';
  const bytes = fpDigest(apiKey, 'sessionId', salt).subarray(0, 16);
  // 保留确定性会话身份，补齐上游要求的 UUID version/variant 位。
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  return bytes.toString('hex').replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5');
}

// 记录已初始化的 Key，避免每个请求重复向 CC 注册设备
const initializedKeys = new Set();

export async function ensureInitialized(apiKey, config = {}) {
  if (initializedKeys.has(apiKey)) return;
  const fp = generateFingerprint(apiKey, config);
  const sessionId = sessionForKey(apiKey, config);
  const apiBase = config.apiBase || DEFAULT_BASE;
  const projectSlug = slugifyProjectPath(config.deviceProjectDir);

  const headers = {
    'Content-Type': 'application/json',
    'User-Agent': 'cli',
    'x-command-code-version': CC_PROTOCOL_VERSION,
    'x-cli-environment': 'production',
    'x-project-slug': projectSlug,
    'x-session-id': sessionId,
    'Authorization': `Bearer ${apiKey}`,
    'traceparent': `00-${crypto.randomBytes(16).toString('hex')}-${crypto.randomBytes(8).toString('hex')}-01`,
  };
  if (config.zdr) headers['x-cmd-zdr'] = '1';

  // 1. 注册设备指纹
  try {
    await fetch(`${apiBase}/alpha/fingerprint/record`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        components: fp.components,
        thumbmark: fp.hash,
        recordedAt: new Date().toISOString(),
      }),
    });
  } catch (e) {
    // 忽略指纹注册瞬态网络错误
  }

  // 2. 发送初始生命周期事件
  try {
    await fetch(`${apiBase}/alpha/lifecycle-events`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        events: [
          {
            type: 'session_start',
            sessionId,
            timestamp: new Date().toISOString(),
            metadata: {
              version: CC_PROTOCOL_VERSION,
              os: 'win32',
              arch: 'x64',
              mode: 'interactive',
            },
          },
        ],
      }),
    });
  } catch (e) {
    // 忽略生命周期事件瞬态网络错误
  }

  initializedKeys.add(apiKey);
}

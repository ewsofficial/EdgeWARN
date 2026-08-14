import os from 'os';
import path from 'path';

const DEFAULT_BASE_DIR = process.platform === 'win32'
  ? 'C:\\EdgeWARN_input'
  : path.join(os.homedir(), 'EdgeWARN_input');
const INTEGER = /^(?:0|[1-9][0-9]*)$/;

function readFlag(argv, names) {
  const values = [];
  for (let index = 0; index < argv.length; index += 1) {
    for (const name of names) {
      if (argv[index] === name && argv[index + 1] !== undefined) values.push(argv[index + 1]);
      if (argv[index].startsWith(`${name}=`)) values.push(argv[index].slice(name.length + 1));
    }
  }
  return values;
}

function oneValue(values, label) {
  const distinct = [...new Set(values.filter((value) => value !== undefined && value !== ''))];
  if (distinct.length > 1) throw new Error(`Conflicting ${label} values`);
  return distinct[0];
}

function parseInteger(value, fallback, label, { minimum = 0 } = {}) {
  if (value === undefined) return fallback;
  if (typeof value !== 'string' || !INTEGER.test(value)) throw new Error(`Invalid ${label}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) throw new Error(`Invalid ${label}`);
  return parsed;
}

function parseOrigins(value) {
  if (!value) return [];
  const origins = value.split(',').map((origin) => origin.trim()).filter(Boolean);
  for (const origin of origins) {
    let url;
    try { url = new URL(origin); } catch { throw new Error(`Invalid allowed origin: ${origin}`); }
    if (url.origin !== origin || !['http:', 'https:'].includes(url.protocol)) throw new Error(`Invalid allowed origin: ${origin}`);
  }
  return Object.freeze([...new Set(origins)]);
}

function parseTrustProxy(value, env) {
  if (value === undefined || value === '' || value === 'false') return false;
  if (value === 'true') {
    if (env.NODE_ENV === 'production') throw new Error('TRUST_PROXY=true is unsafe in production; set TRUST_PROXY_IPS');
    return 1;
  }
  return Object.freeze(value.split(',').map((entry) => entry.trim()).filter(Boolean));
}

export function createConfig({ env = process.env, argv = process.argv.slice(2), packageVersion = '2.7.0' } = {}) {
  const canonicalCli = oneValue(readFlag(argv, ['--base-dir']), '--base-dir');
  const deprecatedCli = oneValue(readFlag(argv, ['--base_dir']), '--base_dir');
  const canonicalEnv = env.EDGEWARN_BASE_DIR;
  const deprecatedEnv = env.BASE_DIR;
  const explicit = [canonicalCli, canonicalEnv, deprecatedCli, deprecatedEnv].filter(Boolean);
  if (new Set(explicit.map((value) => path.resolve(value))).size > 1) throw new Error('Conflicting base directory settings');
  const baseDir = path.resolve(explicit[0] || DEFAULT_BASE_DIR);
  const configDirCli = oneValue(readFlag(argv, ['--config-dir']), '--config-dir');
  const configDirEnv = env.EDGEWARN_CONFIG_DIR;
  const configDir = configDirCli || configDirEnv ? path.resolve(configDirCli || configDirEnv) : undefined;
  const port = parseInteger(env.PORT, 5000, 'PORT', { minimum: 1 });
  const requestTimeoutMs = parseInteger(env.REQUEST_TIMEOUT_MS, 30_000, 'REQUEST_TIMEOUT_MS', { minimum: 1 });
  const rateLimitMaxSec = parseInteger(env.RATE_LIMIT_MAX_SEC, 40, 'RATE_LIMIT_MAX_SEC');
  const rateLimitMaxMin = parseInteger(env.RATE_LIMIT_MAX_MIN, 2000, 'RATE_LIMIT_MAX_MIN');
  return Object.freeze({
    baseDir, dataDir: path.join(baseDir, 'data'), guiDir: path.join(baseDir, 'gui'), wpcDir: path.join(baseDir, 'wpc'),
    configDir,
    port, packageVersion, isProduction: env.NODE_ENV === 'production', requestTimeoutMs,
    allowedOrigins: parseOrigins(env.ALLOWED_ORIGINS), trustProxy: parseTrustProxy(env.TRUST_PROXY_IPS || env.TRUST_PROXY, env),
    rateLimits: Object.freeze({ perSecond: rateLimitMaxSec, perMinute: rateLimitMaxMin })
  });
}

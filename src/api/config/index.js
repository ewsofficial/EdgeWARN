import { readFileSync } from 'fs';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';
import { configRoot, expandPath, getProvenance, loadConfig, repoRoot, srcRoot } from '../../config/loader.js';

// package.json is the sole owner of the version. This was a literal default,
// which agreed with the manifest only until one of the two was bumped.
const PACKAGE_VERSION = JSON.parse(readFileSync(
  path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../package.json'),
  'utf8',
)).version;

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
  if (Array.isArray(value)) return Object.freeze([...new Set(value)]);
  if (!value) return [];
  const origins = value.split(',').map((origin) => origin.trim()).filter(Boolean);
  for (const origin of origins) {
    let url;
    try { url = new URL(origin); } catch { throw new Error(`Invalid allowed origin: ${origin}`); }
    if (url.origin !== origin || !['http:', 'https:'].includes(url.protocol)) throw new Error(`Invalid allowed origin: ${origin}`);
  }
  return Object.freeze([...new Set(origins)]);
}

export function parseTrustProxy(value, env = {}) {
  if (value === false || value === undefined || value === '') return false;
  if (Number.isInteger(value) || Array.isArray(value)) return value;
  if (value === true || typeof value === 'string') {
    const normalized = typeof value === 'string' ? value.trim().toLowerCase() : 'true';
    if (normalized === '' || normalized === 'false') return false;
    if (normalized === 'true') {
      if (env.NODE_ENV === 'production') throw new Error('TRUST_PROXY=true is unsafe in production; set TRUST_PROXY_IPS');
      return 1;
    }
    if (/^\d+$/.test(normalized)) {
      const hops = Number(normalized);
      if (Number.isSafeInteger(hops) && hops >= 0 && hops <= 8) return hops;
      throw new Error('Invalid TRUST_PROXY hop count');
    }
    return Object.freeze(value.split(',').map((entry) => entry.trim()).filter(Boolean));
  }
  throw new Error('Invalid TRUST_PROXY');
}

function defaultEnvironment() {
  // Keep the canonical shared base-directory variable explicit on the Node
  // surface while still copying the environment for dependency injection.
  return { ...process.env, EDGEWARN_BASE_DIR: process.env.EDGEWARN_BASE_DIR };
}

// The token expansion and traversal rejection moved into the shared loader, so
// Python and Node enforce one contract instead of two. What is left here is the
// only part specific to this caller: which token is in scope, and which key to
// name when the value is bad.
function resolveRuntimeDirectory(baseDir, template, label) {
  return expandPath(template, { base_dir: baseDir }, {
    filename: 'api.yaml',
    dottedPath: `base_dir.derived.${label}`,
  });
}

function resolveSourcePath(template, label) {
  return expandPath(template, { src_dir: srcRoot() }, {
    filename: 'api.yaml',
    dottedPath: `server.${label}`,
  });
}

export function createConfig({ env = defaultEnvironment(), argv = process.argv.slice(2), packageVersion = PACKAGE_VERSION } = {}) {
  const configDirCli = oneValue(readFlag(argv, ['--config-dir']), '--config-dir');
  const configDirEnv = env.EDGEWARN_CONFIG_DIR;
  const selectedConfigDir = configDirCli || configDirEnv;
  const api = loadConfig('api', { configDir: selectedConfigDir });
  // The API serves WPC surface analyses out of a directory the ingest side names,
  // so it reads that file's naming keys rather than restating them here.
  const wpc = loadConfig('wpc', { configDir: selectedConfigDir }).wpc;
  const resolvedConfigRoot = configRoot(selectedConfigDir);
  const apiProvenance = getProvenance('api', { configDir: selectedConfigDir });
  const wpcProvenance = getProvenance('wpc', { configDir: selectedConfigDir });
  const canonicalCli = oneValue(readFlag(argv, ['--base-dir']), '--base-dir');
  const deprecatedCli = oneValue(readFlag(argv, ['--base_dir']), '--base_dir');
  const canonicalEnv = env.EDGEWARN_BASE_DIR;
  const deprecatedEnv = env.BASE_DIR;
  const explicit = [canonicalCli, canonicalEnv, deprecatedCli, deprecatedEnv].filter(Boolean);
  if (new Set(explicit.map((value) => path.resolve(value))).size > 1) throw new Error('Conflicting base directory settings');
  const configuredBaseDir = process.platform === 'win32' ? api.base_dir.windows : api.base_dir.posix;
  const baseDir = path.resolve(explicit[0] || configuredBaseDir.replace(/^~(?=$|[\\/])/, os.homedir()));
  const port = parseInteger(env.PORT, api.server.port, 'PORT', { minimum: 1 });
  const requestTimeoutMs = parseInteger(env.REQUEST_TIMEOUT_MS, api.server.request_timeout_ms, 'REQUEST_TIMEOUT_MS', { minimum: 1 });
  const rateLimitMaxSec = parseInteger(env.RATE_LIMIT_MAX_SEC, api.rate_limits.per_second.max, 'RATE_LIMIT_MAX_SEC');
  const rateLimitMaxMin = parseInteger(env.RATE_LIMIT_MAX_MIN, api.rate_limits.per_minute.max, 'RATE_LIMIT_MAX_MIN');
  const activeOverrides = [
    ...(configDirCli ? ['--config-dir'] : configDirEnv ? ['EDGEWARN_CONFIG_DIR'] : []),
    ...(canonicalCli ? ['--base-dir'] : canonicalEnv ? ['EDGEWARN_BASE_DIR'] : deprecatedCli ? ['--base_dir'] : deprecatedEnv ? ['BASE_DIR'] : []),
    ...(env.PORT === undefined ? [] : ['PORT']),
    ...(env.REQUEST_TIMEOUT_MS === undefined ? [] : ['REQUEST_TIMEOUT_MS']),
    ...(env.RATE_LIMIT_MAX_SEC === undefined ? [] : ['RATE_LIMIT_MAX_SEC']),
    ...(env.RATE_LIMIT_MAX_MIN === undefined ? [] : ['RATE_LIMIT_MAX_MIN']),
    ...(env.ALLOWED_ORIGINS === undefined ? [] : ['ALLOWED_ORIGINS']),
    ...(env.TRUST_PROXY_IPS ? ['TRUST_PROXY_IPS'] : env.TRUST_PROXY ? ['TRUST_PROXY'] : []),
  ];
  const diagnostics = Object.freeze({
    source: Object.freeze({
      file: apiProvenance.path,
      schemaVersion: apiProvenance.schema_version,
      root: resolvedConfigRoot,
      // ancillary.js derives the surface-analysis filenames from wpc.yaml, making it
      // a second effective source that /health/live would otherwise not name.
      wpc: Object.freeze({ file: wpcProvenance.path, schemaVersion: wpcProvenance.schema_version }),
    }),
    overrides: Object.freeze(activeOverrides),
    effective: Object.freeze({
      baseDir,
      port,
      requestTimeoutMs,
      rateLimits: Object.freeze({ perSecond: rateLimitMaxSec, perMinute: rateLimitMaxMin }),
      allowedOriginCount: env.ALLOWED_ORIGINS === undefined ? api.security.allowed_origins.length : parseOrigins(env.ALLOWED_ORIGINS).length,
      renderProductCount: api.product_catalog.entries,
      radarProductCount: api.validation.radar_products.length,
    }),
  });
  return Object.freeze({
    baseDir,
    dataDir: resolveRuntimeDirectory(baseDir, api.base_dir.derived.data, 'data'),
    guiDir: resolveRuntimeDirectory(baseDir, api.base_dir.derived.gui, 'gui'),
    wpcDir: resolveRuntimeDirectory(baseDir, api.base_dir.derived.wpc, 'wpc'),
    configDir: resolvedConfigRoot,
    repoDir: repoRoot(selectedConfigDir),
    staticDir: resolveSourcePath(api.server.static_root, 'static_root'),
    openApiPath: resolveSourcePath(api.server.openapi_spec, 'openapi_spec'),
    api,
    wpc,
    diagnostics,
    port, packageVersion, isProduction: env.NODE_ENV === 'production', requestTimeoutMs,
    allowedOrigins: parseOrigins(env.ALLOWED_ORIGINS === undefined ? api.security.allowed_origins : env.ALLOWED_ORIGINS),
    trustProxy: parseTrustProxy(env.TRUST_PROXY_IPS || env.TRUST_PROXY || api.security.trust_proxy, env),
    rateLimits: Object.freeze({
      perSecond: rateLimitMaxSec,
      perMinute: rateLimitMaxMin,
      perSecondWindowMs: api.rate_limits.per_second.window_ms,
      perMinuteWindowMs: api.rate_limits.per_minute.window_ms,
      standardHeaders: api.rate_limits.standard_headers,
      legacyHeaders: api.rate_limits.legacy_headers,
    })
  });
}

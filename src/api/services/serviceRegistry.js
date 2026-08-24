import fs from 'fs';
import path from 'path';

// Mirror of src/util/runtime/services.py (the schema owner). Keep both sides
// in lockstep: canonical names, heartbeat schema version, route-family map,
// and the active/stale/disabled/degraded/unsupported-schema classification.
export const CANONICAL_SERVICE_NAMES = ['edgewarn', 'ewmrs', 'nexrad'];
export const HEARTBEAT_SCHEMA_VERSION = 1;

export const ROUTE_SERVICE_REQUIREMENTS = {
  '/api/v3/cells': 'edgewarn',
  '/api/v3/storm-snapshots': 'edgewarn',
  '/api/v3/alert-snapshots': 'edgewarn',
  '/api/v3/alerts': 'edgewarn',
  '/api/v3/render-products': 'ewmrs',
  '/api/v3/models/rap': 'ewmrs',
  '/api/v3/analyses/wpc': 'ewmrs',
  '/api/v3/styles/colormaps': 'ewmrs',
  '/api/v3/radar-sites': 'nexrad',
  '/renders': 'ewmrs',
  '/wpc': 'ewmrs',
  '/colormaps': 'ewmrs',
  '/rap': 'ewmrs',
  '/nexrad': 'nexrad',
};

const SERVICE_STATES = ['active', 'stale', 'disabled', 'degraded', 'unsupported-schema'];

export function servicesDir(baseDir) {
  return path.join(baseDir, 'state', 'realtime', 'services');
}

export function requiredServiceForRoute(routePath) {
  let best = null;
  for (const [prefix, service] of Object.entries(ROUTE_SERVICE_REQUIREMENTS)) {
    if (routePath === prefix || routePath.startsWith(`${prefix}/`)) {
      if (!best || prefix.length > best.length) best = prefix;
    }
  }
  return best ? ROUTE_SERVICE_REQUIREMENTS[best] : null;
}

function parseIsoTimestamp(value) {
  if (typeof value !== 'string' || value === '') return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function parseHeartbeat(service, payload) {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return null;
  if (payload.schema_version !== HEARTBEAT_SCHEMA_VERSION) return null;
  if (payload.service !== service) return null;
  const pid = payload.pid;
  if (!Number.isInteger(pid) || pid <= 0) return null;
  if (typeof payload.run_id !== 'string' || payload.run_id === '') return null;
  const updatedAt = parseIsoTimestamp(payload.updated_at);
  if (!updatedAt) return null;
  const degradedChildren = payload.degraded_children ?? [];
  if (!Array.isArray(degradedChildren) || degradedChildren.some((c) => typeof c !== 'string')) return null;
  let lastSuccessfulActivity = null;
  if (payload.last_successful_activity !== undefined && payload.last_successful_activity !== null) {
    lastSuccessfulActivity = parseIsoTimestamp(payload.last_successful_activity);
    if (!lastSuccessfulActivity) return null;
  }
  return {
    service,
    pid,
    runId: payload.run_id,
    updatedAt,
    phase: typeof payload.phase === 'string' && payload.phase !== '' ? payload.phase : 'unknown',
    version: typeof payload.version === 'string' ? payload.version : null,
    lastSuccessfulActivity,
    degradedChildren,
  };
}

export function classifyHeartbeatState(raw, { service, staleAfterSeconds, now = new Date() }) {
  if (raw === null) return { state: 'disabled', heartbeat: null };
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return { state: 'unsupported-schema', heartbeat: null };
  }
  const heartbeat = parseHeartbeat(service, payload);
  if (!heartbeat) return { state: 'unsupported-schema', heartbeat: null };
  if ((now.getTime() - heartbeat.updatedAt.getTime()) / 1000 > staleAfterSeconds) {
    return { state: 'stale', heartbeat };
  }
  if (heartbeat.degradedChildren.length > 0) return { state: 'degraded', heartbeat };
  return { state: 'active', heartbeat };
}

let cache = new Map();
const CACHE_TTL_MS = 1000;

function readCachedStates(baseDir, options) {
  const { staleAfterSeconds, now } = options;
  const entry = cache.get(baseDir);
  const nowMs = Date.now();
  if (entry && nowMs - entry.readAt < CACHE_TTL_MS) return entry.states;
  const states = {};
  for (const name of CANONICAL_SERVICE_NAMES) {
    let raw = null;
    try {
      raw = fs.readFileSync(path.join(servicesDir(baseDir), `${name}.json`), 'utf8');
    } catch {
      raw = null;
    }
    states[name] = classifyHeartbeatState(raw, { service: name, staleAfterSeconds, now });
  }
  cache.set(baseDir, { readAt: nowMs, states });
  return states;
}

export function resetServiceStateCache() {
  cache = new Map();
}

export function scanServiceStates(baseDir, { staleAfterSeconds, now = new Date(), cached = true }) {
  if (!cached) {
    const saved = cache.get(baseDir);
    cache.delete(baseDir);
    try {
      return readCachedStates(baseDir, { staleAfterSeconds, now });
    } finally {
      if (saved) cache.set(baseDir, saved);
    }
  }
  return readCachedStates(baseDir, { staleAfterSeconds, now });
}

export function createServiceRegistry({ baseDir, staleAfterSeconds }) {
  return {
    baseDir,
    staleAfterSeconds,
    states(now) {
      return scanServiceStates(baseDir, { staleAfterSeconds, now });
    },
    stateFor(service, now) {
      return this.states(now)[service];
    },
  };
}

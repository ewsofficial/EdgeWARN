import fs from 'fs/promises';
import path from 'path';

import {
  ALLOWED_NEXRAD_PRODUCTS,
  isSafeNexradElevation,
  isSafeNexradTimestamp,
  parseNexradElevationNumber,
} from './validation.js';

export function getNexradRoot(req) {
  return path.resolve(req.app.locals.GUI_DIR, 'NEXRAD');
}

export function resolveUnder(root, ...segments) {
  const resolvedRoot = path.resolve(root);
  const resolvedPath = path.resolve(resolvedRoot, ...segments);
  const relative = path.relative(resolvedRoot, resolvedPath);

  if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    return resolvedPath;
  }

  throw Object.assign(new Error('Resolved path escapes NEXRAD root'), { statusCode: 400 });
}

export async function directoryExists(dirPath) {
  try {
    const stat = await fs.stat(dirPath);
    return stat.isDirectory();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return false;
    }
    throw err;
  }
}

export async function fileExists(filePath) {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return false;
    }
    throw err;
  }
}

export async function listSafeDirectories(root, predicate) {
  const entries = await fs.readdir(root, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory() && predicate(entry.name))
    .map((entry) => entry.name)
    .sort();
}

function buildNexradProductFilename(site, product, elevation, timestamp) {
  return `${site}_${product}_${elevation}_${timestamp}.bin.gz`;
}

function parseNexradProductFilename(site, elevation, fileName) {
  if (!fileName.endsWith('.bin.gz')) {
    return null;
  }

  const prefix = `${site}_`;
  const infix = `_${elevation}_`;
  if (!fileName.startsWith(prefix) || !fileName.endsWith('.bin.gz')) {
    return null;
  }

  const stem = fileName.slice(0, -'.bin.gz'.length);
  const infixIndex = stem.lastIndexOf(infix);
  if (infixIndex <= prefix.length) {
    return null;
  }

  const product = stem.slice(prefix.length, infixIndex);
  const timestamp = stem.slice(infixIndex + infix.length);
  if (!ALLOWED_NEXRAD_PRODUCTS.has(product) || !isSafeNexradTimestamp(timestamp)) {
    return null;
  }

  return { product, timestamp };
}

async function listTimestampEntriesForElevation(site, elevation, elevationDir) {
  const entries = await fs.readdir(elevationDir, { withFileTypes: true });
  const timestamps = new Set();

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    const parsed = parseNexradProductFilename(site, elevation, entry.name);
    if (parsed !== null) {
      timestamps.add(parsed.timestamp);
    }
  }

  return [...timestamps].sort().reverse();
}

export async function listSiteTimestampElevations(siteDir, site) {
  const entries = await fs.readdir(siteDir, { withFileTypes: true });
  const timestamps = new Map();

  for (const entry of entries) {
    if (!entry.isDirectory() || !isSafeNexradElevation(entry.name)) {
      continue;
    }

    const elevationDir = resolveUnder(siteDir, entry.name);
    for (const timestamp of await listTimestampEntriesForElevation(site, entry.name, elevationDir)) {
      const elevations = timestamps.get(timestamp) || new Set();
      elevations.add(parseNexradElevationNumber(entry.name));
      timestamps.set(timestamp, elevations);
    }
  }

  return Object.fromEntries(
    [...timestamps.entries()]
      .sort(([left], [right]) => right.localeCompare(left))
      .map(([timestamp, elevations]) => [timestamp, [...elevations].sort((left, right) => left - right)])
  );
}

export async function siteHasAnyData(siteDir, site) {
  const timestampMap = await listSiteTimestampElevations(siteDir, site);
  return Object.keys(timestampMap).length > 0;
}

export function resolveNexradProductFile(nexradRoot, site, timestamp, elevation, product) {
  return resolveUnder(nexradRoot, site, elevation, buildNexradProductFilename(site, product, elevation, timestamp));
}

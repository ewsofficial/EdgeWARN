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

export async function listTimestampDirectories(siteDir) {
  const timestamps = await listSafeDirectories(siteDir, isSafeNexradTimestamp);
  return timestamps.sort().reverse();
}

async function hasAllowedProductFiles(elevationDir) {
  const entries = await fs.readdir(elevationDir, { withFileTypes: true });
  return entries.some((entry) => entry.isFile() && ALLOWED_NEXRAD_PRODUCTS.has(path.basename(entry.name, '.bin.gz')) && entry.name.endsWith('.bin.gz'));
}

export async function listElevationsWithAllowedProducts(timestampDir) {
  const entries = await fs.readdir(timestampDir, { withFileTypes: true });
  const elevations = [];

  for (const entry of entries) {
    if (!entry.isDirectory() || !isSafeNexradElevation(entry.name)) {
      continue;
    }

    const elevationDir = resolveUnder(timestampDir, entry.name);
    if (await hasAllowedProductFiles(elevationDir)) {
      elevations.push(parseNexradElevationNumber(entry.name));
    }
  }

  return elevations.sort((left, right) => left - right);
}

export async function siteHasAnyData(siteDir) {
  const timestamps = await listTimestampDirectories(siteDir);
  for (const timestamp of timestamps) {
    const timestampDir = resolveUnder(siteDir, timestamp);
    const elevations = await listElevationsWithAllowedProducts(timestampDir);
    if (elevations.length > 0) {
      return true;
    }
  }
  return false;
}

import fs from 'fs';
import path from 'path';
import { LRUCache } from 'lru-cache';

// Initialize LRU Cache
// Max 500 items, default TTL of 1 minute (60 * 1000 ms)
const cache = new LRUCache({
  max: 500,
  ttl: 60 * 1000,
  // Approximate size calculation if we wanted to limit by bytes
  // sizeCalculation: (value) => JSON.stringify(value).length,
  // maxSize: 50 * 1024 * 1024 // 50MB
});

/**
 * Check if filename is safe (no path traversal)
 * @param {string} name - Filename to check
 * @returns {boolean} True if safe
 */
export function isSafeFilename(name) {
  if (!name) return false;
  if (name.includes('..') || name.includes('/') || name.includes('\\')) return false;
  return name.toLowerCase().endsWith('.json') && path.basename(name) === name;
}

/**
 * Safely read JSON file with path traversal protection
 * @param {string} dir - Directory path
 * @param {string} name - Filename
 * @returns {Promise<object>} Parsed JSON content
 * @throws {Error} With code property for specific error types
 */
export async function readJsonFileSafe(dir, name) {
  if (!isSafeFilename(name)) {
    const e = new Error('Invalid filename');
    e.code = 'EINVAL';
    throw e;
  }

  const full = path.join(dir, name);
  
  // Ensure resolved path is inside dir (prevent traversal)
  const resolvedDir = path.resolve(dir);
  const resolvedFull = path.resolve(full);
  
  if (!resolvedFull.startsWith(resolvedDir + path.sep) && resolvedFull !== resolvedDir) {
    const e = new Error('Path outside allowed directory');
    e.code = 'EACCES';
    throw e;
  }

  // Check cache
  if (cache.has(full)) {
    return cache.get(full);
  }

  if (!fs.existsSync(full)) {
    const e = new Error('Not found');
    e.code = 'ENOENT';
    throw e;
  }

  const txt = await fs.promises.readFile(full, 'utf8');
  const json = JSON.parse(txt);

  // Cache the result.
  // If the file is 'stormcells_{timestamp}.json', it's likely immutable, so we could cache it longer.
  // But for simplicity, we stick to default TTL.
  cache.set(full, json);

  return json;
}

/**
 * Read index file (stormcell_index.json or cell_index.json)
 * @param {string} indexPath - Full path to index file
 * @returns {Promise<object>} Parsed index content
 * @throws {Error} If file doesn't exist or can't be read
 */
export async function readIndexFile(indexPath) {
  // Check cache with a shorter TTL for index files as they change more often
  if (cache.has(indexPath)) {
    return cache.get(indexPath);
  }

  if (!fs.existsSync(indexPath)) {
    const e = new Error('Index file not found');
    e.code = 'ENOENT';
    throw e;
  }

  const txt = await fs.promises.readFile(indexPath, 'utf8');
  const json = JSON.parse(txt);

  // Cache index files with a shorter TTL (e.g. 5 seconds)
  cache.set(indexPath, json, { ttl: 5 * 1000 });

  return json;
}

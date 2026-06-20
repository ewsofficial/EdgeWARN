import fs from 'fs/promises';
import { LRUCache } from 'lru-cache';

// Short-TTL cache for directory listings. Bounds repeated unbounded
// fs.readdir scans of snapshot dirs that can grow large, while staying
// consistent with the 5s Cache-Control on the timestamp-listing routes.
const cache = new LRUCache({
  max: 64,
  ttl: 5 * 1000
});

/**
 * Read a directory's entries (filenames) with a short-lived cache.
 * @param {string} dir - Directory path to list
 * @returns {Promise<string[]>} Filenames in the directory
 */
export async function listDirCached(dir) {
  const cached = cache.get(dir);
  if (cached !== undefined) {
    return cached;
  }

  const files = await fs.readdir(dir);
  cache.set(dir, files);
  return files;
}

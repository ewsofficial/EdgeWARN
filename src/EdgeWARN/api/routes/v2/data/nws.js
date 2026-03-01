import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';
import { validateTimestampV2, validateMutualExclusion } from '../../utils/validation.js';

const router = express.Router();

/**
 * Helper to scan NWS directory for available snapshot timestamps
 * @returns {Promise<string[]>} - Array of timestamps in YYYYMMDD-HHMMSS format
 */
async function getNwsTimestamps() {
  try {
    const files = await fs.readdir(apiConfig.NWS_DIR);
    const timestamps = [];

    for (const file of files) {
      // NWS snapshot files: nws_snapshot_YYYYMMDD-HHMMSS.json
      const match = file.match(/^nws_snapshot_(\d{8}-\d{6})\.json$/);
      if (match && match[1]) {
        timestamps.push(match[1]);
      }
    }

    // Sort descending (newest first)
    return timestamps.sort().reverse();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return [];
    }
    throw err;
  }
}

/**
 * GET /api/v2/data/nws
 * Query params:
 *   - timestamp: YYYYMMDD-HHMMSS (optional) - Returns NWS snapshot for this timestamp
 *   - id: string (optional) - Alert ID to fetch specific alert
 *
 * Note: timestamp and id are mutually exclusive
 *
 * Returns:
 *   - Without params: JSON array of available timestamps
 *   - With timestamp: JSON NWS snapshot for that time
 *   - With id: JSON specific alert data
 */
router.get('/', async (req, res) => {
  const { timestamp, id } = req.query;

  // Validate mutual exclusion of timestamp and id
  if (!validateMutualExclusion(req.query, 'timestamp', 'id')) {
    return res.status(400).json({
      error: 'Invalid parameters: timestamp and id cannot be specified at the same time'
    });
  }

  try {
    // If id is provided, return specific alert
    if (id !== undefined && id !== '') {
      res.set('Cache-Control', 'public, max-age=60');

      // Read the alerts registry
      const registry = await readJsonFileSafe(apiConfig.NWS_DIR, 'alerts_registry.json', { useCache: true });

      const alert = registry.alerts?.[id];
      if (!alert) {
        return res.status(404).json({
          error: 'Alert not found',
          id: id
        });
      }

      return res.json({
        type: 'nws',
        id: id,
        ...alert
      });
    }

    // If timestamp is provided, return NWS snapshot for that timestamp
    if (timestamp !== undefined && timestamp !== '') {
      // Validate timestamp parameter
      if (!validateTimestampV2(timestamp)) {
        return res.status(400).json({
          error: 'Invalid timestamp parameter. Format: YYYYMMDD-HHMMSS'
        });
      }

      // Set caching header (1 minute for NWS data)
      res.set('Cache-Control', 'public, max-age=60');

      // Build filename: nws_snapshot_{timestamp}.json
      const filename = `nws_snapshot_${timestamp}.json`;
      const data = await readJsonFileSafe(apiConfig.NWS_DIR, filename, { useCache: false });

      return res.json({
        type: 'nws',
        timestamp: timestamp,
        ...data
      });
    }

    // No parameters provided - return list of available timestamps
    res.set('Cache-Control', 'public, max-age=5');

    const timestamps = await getNwsTimestamps();
    res.json(timestamps);

  } catch (err) {
    if (err.code === 'ENOENT') {
      // File doesn't exist
      if (timestamp) {
        return res.status(404).json({
          error: 'NWS snapshot not found for the specified timestamp',
          timestamp: timestamp
        });
      }
      if (id) {
        return res.status(404).json({
          error: 'Alert not found',
          id: id
        });
      }
      return res.json([]);
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading NWS data:', err);
    res.status(500).json({ error: 'Failed to fetch NWS data' });
  }
});

export default router;

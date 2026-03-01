import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';
import { validateTimestampV2 } from '../../utils/validation.js';

const router = express.Router();

/**
 * Helper to scan METAR directory for available timestamps
 * @returns {Promise<string[]>} - Array of timestamps in YYYYMMDD-HHMMSS format
 */
async function getMetarTimestamps() {
  try {
    const files = await fs.readdir(apiConfig.METAR_DIR);
    const timestamps = [];

    for (const file of files) {
      // METAR files: METAR_YYYYMMDD-HHz.json
      const match = file.match(/^METAR_(\d{8}-\d{2})z\.json$/);
      if (match && match[1]) {
        // Convert YYYYMMDD-HH to YYYYMMDD-HHMMSS (00 minutes, 00 seconds)
        timestamps.push(`${match[1]}0000`);
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
 * GET /api/v2/data/metar
 * Query params:
 *   - timestamp: YYYYMMDD-HHMMSS (optional) - Returns METAR data for this timestamp
 *
 * Returns:
 *   - Without timestamp: JSON array of available timestamps
 *   - With timestamp: JSON METAR data for that specific time
 */
router.get('/', async (req, res) => {
  const { timestamp } = req.query;

  try {
    // If timestamp is provided, return METAR data for that timestamp
    if (timestamp !== undefined && timestamp !== '') {
      // Validate timestamp parameter
      if (!validateTimestampV2(timestamp)) {
        return res.status(400).json({
          error: 'Invalid timestamp parameter. Format: YYYYMMDD-HHMMSS'
        });
      }

      // METAR files use hourly format: METAR_YYYYMMDD-HHz.json
      const hourTimestamp = timestamp.slice(0, 11); // YYYYMMDD-HH
      const filename = `METAR_${hourTimestamp}z.json`;

      // Set caching header (1 minute for METAR data)
      res.set('Cache-Control', 'public, max-age=60');

      const data = await readJsonFileSafe(apiConfig.METAR_DIR, filename, { useCache: false });

      res.json({
        type: 'metar',
        timestamp: timestamp,
        data: data
      });
      return;
    }

    // No timestamp provided - return list of available timestamps
    res.set('Cache-Control', 'public, max-age=5');

    const timestamps = await getMetarTimestamps();
    res.json(timestamps);

  } catch (err) {
    if (err.code === 'ENOENT') {
      // METAR file doesn't exist
      if (timestamp) {
        return res.status(404).json({
          error: 'METAR data not found for the specified timestamp',
          timestamp: timestamp
        });
      }
      return res.json([]);
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading METAR data:', err);
    res.status(500).json({ error: 'Failed to fetch METAR data' });
  }
});

export default router;

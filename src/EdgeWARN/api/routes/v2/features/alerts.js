import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../../config.js';
import { readJsonFileSafe } from '../../../utils/fileReader.js';
import { validateTimestampV2, validateMutualExclusion, validateAlertId } from '../../../utils/validation.js';

const router = express.Router();

/**
 * Helper to scan a given TS directory for available snapshot timestamps
 * @returns {Promise<string[]>} - Array of timestamps in YYYYMMDD-HHMMSS format
 */
async function getTimestamps(tsDir) {
  try {
    const files = await fs.readdir(tsDir);
    const timestamps = [];

    for (const file of files) {
      // Snapshot files: YYYYMMDD-HHMMSS.json
      const match = file.match(/^(\d{8}-\d{6})\.json$/);
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
 * Standard GET handler logic for both endpoints - returns only timestamps list
 */
async function handleAlertsRequest(req, res, idsDir, tsDir, typeStr) {
  try {
    const timestamps = await getTimestamps(tsDir);

    // Slight caching logic distinction keeping original semantics for now if needed,
    // though caching on 'edgewarn' can safely follow 'official' since both are file-backed now.
    res.set('Cache-Control', 'public, max-age=5');

    return res.json(timestamps);
  } catch (err) {
    return res.status(500).json({
      success: false,
      error: { code: 'SERVER_ERROR', message: 'Failed to load alerts data' }
    });
  }
}

/**
 * GET /api/v2/features/alerts/official
 */
router.get('/official', async (req, res) => {
  await handleAlertsRequest(
    req,
    res,
    apiConfig.OFFICIAL_ALERTS_IDS_DIR,
    apiConfig.OFFICIAL_ALERTS_TS_DIR,
    'official'
  );
});

/**
 * GET /api/v2/features/alerts/edgewarn
 */
router.get('/edgewarn', async (req, res) => {
  await handleAlertsRequest(
    req,
    res,
    apiConfig.EDGEWARN_ALERTS_IDS_DIR,
    apiConfig.EDGEWARN_ALERTS_TS_DIR,
    'edgewarn'
  );
});

export default router;

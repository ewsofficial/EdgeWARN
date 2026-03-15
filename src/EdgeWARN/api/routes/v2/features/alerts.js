import express from 'express';
import fs from 'fs/promises';
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

function toSafeAlertFilename(id) {
  return id.replace(/:/g, '_').replace(/\//g, '_') + '.json';
}

/**
 * Standard GET handler logic for both endpoints
 */
async function handleAlertsRequest(req, res, idsDir, tsDir) {
  const { timestamp, id } = req.query;

  // Validate mutual exclusion
  if (!validateMutualExclusion(req.query, 'timestamp', 'id')) {
    return res.status(400).json({
      success: false,
      error: {
        code: 'INVALID_INPUT',
        message: 'Parameters timestamp and id cannot be specified at the same time'
      }
    });
  }

  // Handle 'id' modifier
  if (id !== undefined && id !== '') {
    if (!validateAlertId(id)) {
      return res.status(400).json({
        success: false,
        error: {
          code: 'INVALID_INPUT',
          message: 'Invalid id parameter'
        }
      });
    }

    try {
      const safeId = toSafeAlertFilename(id);
      let alert = null;
      try {
        alert = await readJsonFileSafe(idsDir, safeId, { useCache: true });
      } catch (e) {
        // Will be handled by the !alert check below
      }

      if (!alert || Object.keys(alert).length === 0) {
        return res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: 'Alert with the specified ID was not found.'
          }
        });
      }

      res.set('Cache-Control', 'public, max-age=60');

      return res.json(alert.feature ? alert.feature : alert);
    } catch (err) {
      return res.status(500).json({
        success: false,
        error: { code: 'SERVER_ERROR', message: 'Failed to load alerts' }
      });
    }
  }

  // Handle 'timestamp' modifier
  if (timestamp !== undefined && timestamp !== '') {
    if (!validateTimestampV2(timestamp)) {
      return res.status(400).json({
        success: false,
        error: {
          code: 'INVALID_INPUT',
          message: 'Invalid timestamp parameter. Format: YYYYMMDD-HHMMSS'
        }
      });
    }

    try {
      res.set('Cache-Control', 'public, max-age=60');

      // Build filename: {timestamp}.json
      const filename = `${timestamp}.json`;
      try {
        const snapshotData = await readJsonFileSafe(tsDir, filename, { useCache: false });
        const snapshotAlerts = Array.isArray(snapshotData.alerts) ? snapshotData.alerts : [];

        return res.json(snapshotAlerts);
      } catch (fileErr) {
        if (fileErr.code === 'ENOENT') {
          // Gracefully fallback to empty array if the snapshot file doesn't exist yet/anymore
          return res.json([]);
        }
        console.error('Error fetching snapshot:', fileErr);
        throw fileErr;
      }
    } catch (err) {
      return res.status(500).json({
        success: false,
        error: { code: 'SERVER_ERROR', message: 'Failed to load alerts for timestamp' }
      });
    }
  }

  // No modifiers - return timestamps list only
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
    apiConfig.OFFICIAL_ALERTS_TS_DIR
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
    apiConfig.EDGEWARN_ALERTS_TS_DIR
  );
});

export default router;

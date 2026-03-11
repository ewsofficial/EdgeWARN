import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../../config.js';
import { readJsonFileSafe, readIndexFile } from '../../../utils/fileReader.js';
import { validateTimestampV2, validateMutualExclusion, validateAlertId } from '../../../utils/validation.js';

const router = express.Router();

/**
 * Helper to parse a YYYYMMDD-HHMMSS string into a Date object
 */
function parseScanTimestamp(timestampStr) {
  // Format: YYYYMMDD-HHMMSS
  const match = timestampStr.match(/^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})$/);
  if (!match) return null;
  const [_, year, month, day, hour, minute, second] = match;
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`);
}

/**
 * Helper to check if an alert is active at a given timestamp
 */
function isAlertActiveAt(alert, timestampDate) {
  if (!alert.effective || !alert.expires) return false;
  const effective = new Date(alert.effective);
  const expires = new Date(alert.expires);
  return timestampDate >= effective && timestampDate <= expires;
}

/**
 * Helper to get available timestamps from the system (stormcell index)
 */
async function getAvailableTimestamps() {
  try {
    const indexPath = path.join(apiConfig.STORMCELL_DIR, 'stormcell_index.json');
    const indexData = await readIndexFile(indexPath);
    return indexData.timestamps || [];
  } catch (err) {
    console.error('Failed to get available timestamps:', err);
    return [];
  }
}

/**
 * Helper to scan NWS directory for available snapshot timestamps
 * @returns {Promise<string[]>} - Array of timestamps in YYYYMMDD-HHMMSS format
 */
async function getNwsTimestamps() {
  try {
    const files = await fs.readdir(apiConfig.OFFICIAL_ALERTS_TS_DIR);
    const timestamps = [];

    for (const file of files) {
      // NWS snapshot files: YYYYMMDD-HHMMSS.json
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
 * Helper to load all EdgeWARN alerts from the filesystem
 */
async function loadEdgeWARNAlerts() {
  const alerts = [];
  try {
    const files = await fs.readdir(apiConfig.EDGEWARN_ALERTS_DIR);
    for (const file of files) {
      if (file.startsWith('alert_') && file.endsWith('.json')) {
        const data = await readJsonFileSafe(apiConfig.EDGEWARN_ALERTS_DIR, file, { useCache: true });
        if (data && Object.keys(data).length > 0) {
          alerts.push(data);
        }
      }
    }
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.error('Error reading EdgeWARN alerts:', err);
    }
  }
  return alerts;
}

/**
 * Helper to load all Official (NWS) alerts from the registry
 */
async function loadOfficialAlerts() {
  const alerts = [];
  try {
    const files = await fs.readdir(apiConfig.OFFICIAL_ALERTS_IDS_DIR);
    for (const file of files) {
      if (file.endsWith('.json')) {
        const data = await readJsonFileSafe(apiConfig.OFFICIAL_ALERTS_IDS_DIR, file, { useCache: true });
        if (data && Object.keys(data).length > 0) {
          alerts.push(data);
        }
      }
    }
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.error('Error reading Official alerts ids:', err);
    }
  }
  return alerts;
}

/**
 * Standard GET handler logic for both endpoints
 */
async function handleAlertsRequest(req, res, loadAlertsFn, typeStr) {
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
      let alert = null;
      if (typeStr === 'official') {
        const safeId = id.replace(/:/g, "_").replace(/\//g, "_") + ".json";
        try {
          alert = await readJsonFileSafe(apiConfig.OFFICIAL_ALERTS_IDS_DIR, safeId, { useCache: true });
        } catch (e) {
          // Will be handled by the !alert check below
        }
      } else {
        const allAlerts = await loadAlertsFn();
        alert = allAlerts.find(a => a.id === id);
      }

      if (!alert) {
        return res.status(404).json({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: 'Alert with the specified ID was not found.'
          }
        });
      }

      if (typeStr === 'official') {
        res.set('Cache-Control', 'public, max-age=60');
      }

      return res.json({
        success: true,
        data: alert,
        meta: { timestamp: new Date().toISOString() }
      });
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
      if (typeStr === 'official') {
        res.set('Cache-Control', 'public, max-age=60');

        // Build filename: {timestamp}.json
        const filename = `${timestamp}.json`;
        try {
          const snapshotData = await readJsonFileSafe(apiConfig.OFFICIAL_ALERTS_TS_DIR, filename, { useCache: false });
          const alertIds = Array.isArray(snapshotData.alerts) ? snapshotData.alerts : [];

          return res.json({
            success: true,
            data: alertIds,
            meta: {
              timestamp: new Date().toISOString(),
              count: alertIds.length,
              total: snapshotData.count || alertIds.length
            }
          });
        } catch (fileErr) {
          if (fileErr.code !== 'ENOENT') {
            console.error('Error fetching snapshot:', fileErr);
          }
          // If the snapshot file does not exist, gracefully fallback
        }
      }

      const timestampDate = parseScanTimestamp(timestamp);
      const allAlerts = await loadAlertsFn();

      const activeAlertIds = allAlerts
        .filter(alert => isAlertActiveAt(alert, timestampDate))
        .map(alert => alert.id);

      return res.json({
        success: true,
        data: activeAlertIds,
        meta: {
          timestamp: new Date().toISOString(),
          count: activeAlertIds.length,
          total: activeAlertIds.length
        }
      });
    } catch (err) {
      return res.status(500).json({
        success: false,
        error: { code: 'SERVER_ERROR', message: 'Failed to load alerts for timestamp' }
      });
    }
  }

  // No modifiers - return timestamps list only
  try {
    const timestamps = typeStr === 'official'
      ? await getNwsTimestamps()
      : await getAvailableTimestamps();

    if (typeStr === 'official') {
      res.set('Cache-Control', 'public, max-age=5');
    }

    return res.json({
      success: true,
      data: {
        timestamps: timestamps
      },
      meta: { timestamp: new Date().toISOString() }
    });
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
  await handleAlertsRequest(req, res, loadOfficialAlerts, 'official');
});

/**
 * GET /api/v2/features/alerts/edgewarn
 */
router.get('/edgewarn', async (req, res) => {
  await handleAlertsRequest(req, res, loadEdgeWARNAlerts, 'edgewarn');
});

export default router;

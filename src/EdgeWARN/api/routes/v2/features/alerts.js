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
  try {
    const registry = await readJsonFileSafe(apiConfig.OFFICIAL_ALERTS_DIR, 'alerts_registry.json', { useCache: true });
    return Object.values(registry.alerts || {});
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.error('Error reading Official alerts registry:', err);
    }
    return [];
  }
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
      const allAlerts = await loadAlertsFn();
      // EdgeWARN uses `id`, NWS might use `id` or we fall back to extracting it
      const alert = allAlerts.find(a => a.id === id);

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

        // Build filename: nws_snapshot_{timestamp}.json
        const filename = `nws_snapshot_${timestamp}.json`;
        try {
          const snapshotData = await readJsonFileSafe(apiConfig.NWS_DIR, filename, { useCache: false });
          return res.json({
            success: true,
            data: snapshotData.alerts || [],
            meta: {
              timestamp: new Date().toISOString(),
              count: (snapshotData.alerts || []).length,
              total: snapshotData.count || (snapshotData.alerts || []).length
            }
          });
        } catch (fileErr) {
          if (fileErr.code !== 'ENOENT') {
            console.error('Error fetching snapshot:', fileErr);
          }
          // If the snapshot file does not exist, gracefully fallback to filtering `alerts_registry.json`.
        }
      }

      const timestampDate = parseScanTimestamp(timestamp);
      const allAlerts = await loadAlertsFn();

      const activeAlerts = allAlerts.filter(alert => isAlertActiveAt(alert, timestampDate));

      return res.json({
        success: true,
        data: activeAlerts,
        meta: {
          timestamp: new Date().toISOString(),
          count: activeAlerts.length,
          total: activeAlerts.length
        }
      });
    } catch (err) {
      return res.status(500).json({
        success: false,
        error: { code: 'SERVER_ERROR', message: 'Failed to load alerts for timestamp' }
      });
    }
  }

  // No modifiers - return timestamps list and all current alerts
  try {
    const timestamps = typeStr === 'official'
      ? await getNwsTimestamps()
      : await getAvailableTimestamps();

    const allAlerts = await loadAlertsFn();

    if (typeStr === 'official') {
      res.set('Cache-Control', 'public, max-age=5');
    }

    return res.json({
      success: true,
      data: {
        timestamps: timestamps,
        alerts: allAlerts
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

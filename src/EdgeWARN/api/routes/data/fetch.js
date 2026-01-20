import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';

const router = express.Router();

// Regex for timestamp validation: YYYYMMDD-HHMM00
const TIMESTAMP_REGEX = /^\d{8}-\d{4}00$/;

/**
 * Helper to scan directory for JSON files and extract timestamps
 * @param {string} dirPath - Directory to scan
 * @param {RegExp} filenamePattern - Regex to extract timestamp from filename
 * @returns {Promise<string[]>} - Array of timestamps
 */
async function getAvailableTimestamps(dirPath, filenamePattern) {
    try {
        const files = await fs.readdir(dirPath);
        const timestamps = [];

        for (const file of files) {
            if (file.endsWith('.json')) {
                const match = file.match(filenamePattern);
                if (match && match[1]) {
                    timestamps.push(match[1]);
                }
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
 * GET /data/fetch/metar
 * Returns available METAR file timestamps
 */
router.get('/metar', async (req, res) => {
    try {
        res.set('Cache-Control', 'public, max-age=5');

        // METAR files: METAR_YYYYMMDD-HHz.json
        const timestamps = await getAvailableTimestamps(
            apiConfig.METAR_DIR,
            /^METAR_(\d{8}-\d{2})z\.json$/
        );

        // Convert to YYYYMMDD-HHMM00 format for consistency
        const formattedTimestamps = timestamps.map(ts => `${ts}0000`);

        res.json({
            type: 'metar',
            count: formattedTimestamps.length,
            timestamps: formattedTimestamps
        });
    } catch (err) {
        console.error('Error fetching METAR timestamps:', err);
        res.status(500).json({ error: 'Failed to fetch METAR resources' });
    }
});

/**
 * GET /data/fetch/nws
 * Returns available NWS alert file timestamps
 */
router.get('/nws', async (req, res) => {
    try {
        res.set('Cache-Control', 'public, max-age=5');

        // NWS files: NWS_alerts_YYYYMMDD-HHMMSS.json
        const timestamps = await getAvailableTimestamps(
            apiConfig.NWS_DIR,
            /^NWS_alerts_(\d{8}-\d{6})\.json$/
        );

        // Convert to YYYYMMDD-HHMM00 format (truncate seconds)
        const formattedTimestamps = timestamps.map(ts => ts.slice(0, 13) + '00');

        // Remove duplicates (multiple files in same minute)
        const uniqueTimestamps = [...new Set(formattedTimestamps)];

        res.json({
            type: 'nws',
            count: uniqueTimestamps.length,
            timestamps: uniqueTimestamps
        });
    } catch (err) {
        console.error('Error fetching NWS timestamps:', err);
        res.status(500).json({ error: 'Failed to fetch NWS resources' });
    }
});

export default router;

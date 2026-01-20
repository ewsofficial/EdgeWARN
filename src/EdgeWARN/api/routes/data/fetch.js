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
 * GET /data/fetch?type=[nws/metar]
 * Returns available file timestamps for the specified type
 */
router.get('/', async (req, res) => {
    const { type } = req.query;

    if (!type || !['nws', 'metar'].includes(type)) {
        return res.status(400).json({ error: 'Missing or invalid parameter: type. valid values: [nws, metar]' });
    }

    try {
        res.set('Cache-Control', 'public, max-age=5');
        let timestamps = [];
        let formattedTimestamps = [];

        if (type === 'metar') {
            // METAR files: METAR_YYYYMMDD-HHz.json
            timestamps = await getAvailableTimestamps(
                apiConfig.METAR_DIR,
                /^METAR_(\d{8}-\d{2})z\.json$/
            );
            // Convert to YYYYMMDD-HHMM00 format for consistency
            formattedTimestamps = timestamps.map(ts => `${ts}0000`);

        } else if (type === 'nws') {
            // NWS files: alerts_active_YYYYMMDD-HHMM00.json
            timestamps = await getAvailableTimestamps(
                apiConfig.NWS_DIR,
                /^alerts_active_(\d{8}-\d{6})\.json$/
            );
            // timestamps are already in YYYYMMDD-HHMM00 format
            formattedTimestamps = timestamps;
        }

        res.json({
            type: type,
            count: formattedTimestamps.length,
            timestamps: formattedTimestamps
        });
    } catch (err) {
        console.error(`Error fetching ${type} timestamps:`, err);
        res.status(500).json({ error: `Failed to fetch ${type} resources` });
    }
});

export default router;

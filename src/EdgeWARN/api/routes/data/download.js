import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';

const router = express.Router();

// Regex for timestamp validation: YYYYMMDD-HHMM00
const TIMESTAMP_REGEX = /^\d{8}-\d{4}00$/;

/**
 * GET /data/download/metar?timestamp=YYYYMMDD-HHMM00
 * Downloads METAR data for the specified timestamp
 */
router.get('/metar', async (req, res) => {
    const { timestamp } = req.query;

    if (!timestamp) {
        return res.status(400).json({ error: 'Missing required parameter: timestamp' });
    }

    if (!TIMESTAMP_REGEX.test(timestamp)) {
        return res.status(400).json({
            error: 'Invalid timestamp format. Expected: YYYYMMDD-HHMM00'
        });
    }

    try {
        // METAR files use hourly format: METAR_YYYYMMDD-HHz.json
        // Extract the hour portion from the timestamp
        const hourTimestamp = timestamp.slice(0, 11); // YYYYMMDD-HH
        const filename = `METAR_${hourTimestamp}z.json`;
        const filePath = path.join(apiConfig.METAR_DIR, filename);

        // Check if file exists
        try {
            await fs.access(filePath);
        } catch {
            return res.status(404).json({
                error: 'METAR data not found for the specified timestamp',
                timestamp: timestamp,
                searched: filename
            });
        }

        // Read and return the JSON data
        const data = await readJsonFileSafe(apiConfig.METAR_DIR, filename);

        res.set('Cache-Control', 'public, max-age=60');
        res.json({
            type: 'metar',
            timestamp: timestamp,
            data: data
        });
    } catch (err) {
        console.error('Error downloading METAR data:', err);
        res.status(500).json({ error: 'Failed to download METAR data' });
    }
});

/**
 * GET /data/download/nws?timestamp=YYYYMMDD-HHMM00
 * Downloads NWS alert data for the specified timestamp
 */
router.get('/nws', async (req, res) => {
    const { timestamp } = req.query;

    if (!timestamp) {
        return res.status(400).json({ error: 'Missing required parameter: timestamp' });
    }

    if (!TIMESTAMP_REGEX.test(timestamp)) {
        return res.status(400).json({
            error: 'Invalid timestamp format. Expected: YYYYMMDD-HHMM00'
        });
    }

    try {
        // NWS files: alerts_active_YYYYMMDD-HHMM00.json
        // Exact match for minute timestamp
        const filename = `alerts_active_${timestamp}.json`;
        const filePath = path.join(apiConfig.NWS_DIR, filename);

        // Check if file exists
        try {
            await fs.access(filePath);
        } catch {
            return res.status(404).json({
                error: 'NWS data not found for the specified timestamp',
                timestamp: timestamp,
                searched: filename
            });
        }

        // Read and return the JSON data
        const data = await readJsonFileSafe(apiConfig.NWS_DIR, filename);

        res.set('Cache-Control', 'public, max-age=60');
        res.json({
            type: 'nws',
            timestamp: timestamp,
            data: data
        });
    } catch (err) {
        console.error('Error downloading NWS data:', err);
        res.status(500).json({ error: 'Failed to download NWS data' });
    }
});

export default router;

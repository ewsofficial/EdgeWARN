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
        // NWS files: NWS_alerts_YYYYMMDD-HHMMSS.json
        // We need to find a file that matches the minute (ignore seconds)
        const minutePrefix = timestamp.slice(0, 13); // YYYYMMDD-HHMM

        // List files in NWS directory and find matching one
        let files;
        try {
            files = await fs.readdir(apiConfig.NWS_DIR);
        } catch {
            return res.status(404).json({
                error: 'NWS data directory not found',
                timestamp: timestamp
            });
        }

        // Find file matching the minute
        const matchingFile = files.find(f => {
            const match = f.match(/^NWS_alerts_(\d{8}-\d{4})\d{2}\.json$/);
            return match && match[1] === minutePrefix;
        });

        if (!matchingFile) {
            return res.status(404).json({
                error: 'NWS data not found for the specified timestamp',
                timestamp: timestamp
            });
        }

        const filePath = path.join(apiConfig.NWS_DIR, matchingFile);
        const data = await readJsonFileSafe(apiConfig.NWS_DIR, matchingFile);

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

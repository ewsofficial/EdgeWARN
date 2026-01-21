import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';

const router = express.Router();

// Regex for timestamp validation: YYYYMMDD-HHMM00
const TIMESTAMP_REGEX = /^\d{8}-\d{4}00$/;

/**
 * GET /data/download?type=[nws|metar]&timestamp=YYYYMMDD-HHMM00
 * Downloads data for the specified type and timestamp
 */
router.get('/', async (req, res) => {
    const { type, timestamp } = req.query;

    if (!type || !['nws', 'metar', 'surface'].includes(type)) {
        return res.status(400).json({ error: 'Missing or invalid parameter: type. valid values: [nws, metar, surface]' });
    }

    if (!timestamp) {
        return res.status(400).json({ error: 'Missing required parameter: timestamp' });
    }

    if (!TIMESTAMP_REGEX.test(timestamp)) {
        return res.status(400).json({
            error: 'Invalid timestamp format. Expected: YYYYMMDD-HHMM00'
        });
    }

    try {
        let filename;
        let dirPath;
        let errorMessage = 'Data not found for the specified timestamp';

        if (type === 'metar') {
            // METAR files use hourly format: METAR_YYYYMMDD-HHz.json
            const hourTimestamp = timestamp.slice(0, 11); // YYYYMMDD-HH
            filename = `METAR_${hourTimestamp}z.json`;
            dirPath = apiConfig.METAR_DIR;
            errorMessage = 'METAR data not found for the specified timestamp';
        } else if (type === 'nws') {
            // NWS files: alerts_active_YYYYMMDD-HHMM00.json
            filename = `alerts_active_${timestamp}.json`;
            dirPath = apiConfig.NWS_DIR;
            errorMessage = 'NWS data not found for the specified timestamp';
        } else if (type === 'surface') {
            // Surface features: surface_features_YYYYMMDD-HHMM00.json
            filename = `surface_features_${timestamp}.json`;
            dirPath = apiConfig.SURFACE_DIR;
            errorMessage = 'Surface features not found for the specified timestamp';
        }

        const filePath = path.join(dirPath, filename);

        // Check if file exists
        try {
            await fs.access(filePath);
        } catch {
            return res.status(404).json({
                error: errorMessage,
                timestamp: timestamp,
                searched: filename
            });
        }

        // Read and return the JSON data
        const data = await readJsonFileSafe(dirPath, filename);

        res.set('Cache-Control', 'public, max-age=60');
        res.json({
            type: type,
            timestamp: timestamp,
            data: data
        });

    } catch (err) {
        console.error(`Error downloading ${type} data:`, err);
        res.status(500).json({ error: `Failed to download ${type} data` });
    }
});

export default router;

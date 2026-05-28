/**
 * WPC Surface Analysis API Routes
 * 
 * Provides endpoints for accessing WPC surface analysis GeoJSON data.
 */

import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { resolveUnder } from './nexrad/filesystem.js';
const router = express.Router();

// Get BASE_DIR from app.locals (set in server.js)
function getWpcDir(req) {
    const baseDir = req.app.locals.BASE_DIR;
    return path.join(baseDir, 'wpc', 'surface_analysis');
}

async function readJsonFileUnder(root, ...segments) {
    const filePath = resolveUnder(root, ...segments);
    const [realRoot, realFilePath] = await Promise.all([
        fs.realpath(root),
        fs.realpath(filePath)
    ]);
    const relative = path.relative(realRoot, realFilePath);

    if (relative !== '' && (relative.startsWith('..') || path.isAbsolute(relative))) {
        throw Object.assign(new Error('Resolved path escapes WPC root'), { statusCode: 400 });
    }

    const data = await fs.readFile(realFilePath, 'utf-8');
    return JSON.parse(data);
}

/**
 * GET /wpc/fetch
 * 
 * Query Params:
 *   - type: 'sfc' (required)
 * 
 * Returns a list of available timestamps for the specified type.
 */
router.get('/fetch', async (req, res) => {
    try {
        const type = req.query.type;

        if (type !== 'sfc') {
            return res.status(400).json({
                error: 'Invalid type',
                message: "Only 'sfc' type is currently supported"
            });
        }

        const wpcDir = getWpcDir(req);

        const entries = await fs.readdir(wpcDir, { withFileTypes: true });
        const timestamps = [];

        for (const entry of entries) {
            if (entry.isFile() && entry.name.endsWith('.geojson') && entry.name !== 'latest.geojson') {
                // Extract timestamp from filename: wpc_sfc_YYYYMMDD-HH0000.geojson
                const match = entry.name.match(/wpc_sfc_(\d{8}-\d{6})\.geojson/);
                if (match) {
                    timestamps.push(match[1]);
                }
            }
        }

        // Sort descending (newest first)
        timestamps.sort((a, b) => b.localeCompare(a));

        res.json(timestamps);
    } catch (err) {
        if (err.code === 'ENOENT') {
            res.json([]);
        } else {
            res.status(500).json({
                error: 'Failed to list timestamps',
                details: err.message
            });
        }
    }
});

/**
 * GET /wpc/download
 * 
 * Query Params:
 *   - type: 'sfc' (required)
 *   - timestamp: YYYYMMDD-HH0000 (required)
 * 
 * Downloads the specific file matching the timestamp.
 */
router.get('/download', async (req, res) => {
    try {
        const type = req.query.type;
        const timestamp = req.query.timestamp;

        if (type !== 'sfc') {
            return res.status(400).json({
                error: 'Invalid type',
                message: "Only 'sfc' type is currently supported"
            });
        }

        if (!timestamp) {
            return res.status(400).json({
                error: 'Missing timestamp',
                message: 'Timestamp parameter is required'
            });
        }

        // Validate timestamp format: YYYYMMDD-HH0000
        if (!/^\d{8}-\d{6}$/.test(timestamp)) {
            return res.status(400).json({
                error: 'Invalid timestamp format',
                message: 'Timestamp must be in YYYYMMDD-HH0000 format'
            });
        }

        const wpcDir = getWpcDir(req);
        const geojson = await readJsonFileUnder(wpcDir, `wpc_sfc_${timestamp}.geojson`);

        res.json(geojson);
    } catch (err) {
        if (err.statusCode) {
            return res.status(err.statusCode).json({ error: err.message });
        }
        if (err.code === 'ENOENT') {
            res.status(404).json({
                error: 'File not found',
                message: `No data found for timestamp: ${req.query.timestamp}`
            });
        } else {
            res.status(500).json({
                error: 'Failed to download file',
                details: err.message
            });
        }
    }
});

export default router;

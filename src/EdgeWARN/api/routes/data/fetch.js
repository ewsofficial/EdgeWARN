import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';

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
 * GET /data/fetch?type=[nws|metar|surface]
 * 
 * For NWS: Returns list of active alert IDs from the registry
 * For METAR/surface: Returns available file timestamps
 */
router.get('/', async (req, res) => {
    const { type } = req.query;

    if (!type || !['nws', 'metar', 'surface'].includes(type)) {
        return res.status(400).json({ error: 'Missing or invalid parameter: type. valid values: [nws, metar, surface]' });
    }

    try {
        res.set('Cache-Control', 'public, max-age=5');

        if (type === 'nws') {
            // NWS now uses registry-based storage
            // Return active alert IDs from the registry
            const registryPath = path.join(apiConfig.NWS_DIR, 'alerts_registry.json');
            
            try {
                await fs.access(registryPath);
            } catch {
                // Registry doesn't exist yet
                return res.json({
                    type: 'nws',
                    count: 0,
                    last_updated: null,
                    alert_ids: []
                });
            }

            const registry = await readJsonFileSafe(apiConfig.NWS_DIR, 'alerts_registry.json', { useCache: true });
            
            const alertIds = Object.keys(registry.alerts || {});
            const lastUpdated = registry.last_updated || null;

            res.json({
                type: 'nws',
                count: alertIds.length,
                last_updated: lastUpdated,
                alert_ids: alertIds
            });
            
        } else if (type === 'metar') {
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

        } else if (type === 'surface') {
            // Surface features: surface_features_YYYYMMDD-HHMM00.json
            const timestamps = await getAvailableTimestamps(
                apiConfig.SURFACE_DIR,
                /^surface_features_(\d{8}-\d{6})\.json$/
            );

            res.json({
                type: 'surface',
                count: timestamps.length,
                timestamps: timestamps
            });
        }

    } catch (err) {
        console.error(`Error fetching ${type} resources:`, err);
        res.status(500).json({ error: `Failed to fetch ${type} resources` });
    }
});

export default router;

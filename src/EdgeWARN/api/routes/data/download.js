import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';

const router = express.Router();

// Regex for timestamp validation: YYYYMMDD-HHMM00
const TIMESTAMP_REGEX = /^\d{8}-\d{4}00$/;

/**
 * GET /data/download?type=[nws|metar|surface]&timestamp=YYYYMMDD-HHMM00
 * GET /data/download?type=nws&alert_id=<alert_id>
 * 
 * Downloads data for the specified type and timestamp.
 * For NWS, can also fetch a specific alert by ID.
 */
router.get('/', async (req, res) => {
    const { type, timestamp, alert_id } = req.query;

    if (!type || !['nws', 'metar', 'surface'].includes(type)) {
        return res.status(400).json({ error: 'Missing or invalid parameter: type. valid values: [nws, metar, surface]' });
    }

    try {
        let filename;
        let dirPath;
        let errorMessage = 'Data not found for the specified timestamp';

        if (type === 'metar') {
            // METAR files use hourly format: METAR_YYYYMMDD-HHz.json
            if (!timestamp) {
                return res.status(400).json({ error: 'Missing required parameter: timestamp for METAR data' });
            }
            if (!TIMESTAMP_REGEX.test(timestamp)) {
                return res.status(400).json({
                    error: 'Invalid timestamp format. Expected: YYYYMMDD-HHMM00'
                });
            }
            const hourTimestamp = timestamp.slice(0, 11); // YYYYMMDD-HH
            filename = `METAR_${hourTimestamp}z.json`;
            dirPath = apiConfig.METAR_DIR;
            errorMessage = 'METAR data not found for the specified timestamp';
        } else if (type === 'nws') {
            // NWS now uses registry-based storage
            // If alert_id is provided, return specific alert
            // Otherwise, return the full registry
            filename = 'alerts_registry.json';
            dirPath = apiConfig.NWS_DIR;
            errorMessage = 'NWS alerts registry not found';
        } else if (type === 'surface') {
            // Surface features: surface_features_YYYYMMDD-HHMM00.json
            if (!timestamp) {
                return res.status(400).json({ error: 'Missing required parameter: timestamp for surface data' });
            }
            if (!TIMESTAMP_REGEX.test(timestamp)) {
                return res.status(400).json({
                    error: 'Invalid timestamp format. Expected: YYYYMMDD-HHMM00'
                });
            }
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
                timestamp: timestamp || null,
                searched: filename
            });
        }

        // Read the JSON data
        const data = await readJsonFileSafe(dirPath, filename, { useCache: false });

        // For NWS, handle alert_id filtering and format response
        if (type === 'nws') {
            // Handle specific alert request
            if (alert_id) {
                const alert = data.alerts?.[alert_id];
                if (!alert) {
                    return res.status(404).json({
                        error: 'Alert not found',
                        alert_id: alert_id
                    });
                }
                res.set('Cache-Control', 'public, max-age=60');
                return res.json({
                    type: 'nws',
                    alert_id: alert_id,
                    data: alert
                });
            }
            
            // Return full registry with active alerts as FeatureCollection
            // Convert registry format to GeoJSON FeatureCollection for backward compatibility
            // Filter out any invalid entries (missing feature data)
            const features = Object.values(data.alerts || {})
                .filter(alertData => alertData && alertData.feature)
                .map(alertData => alertData.feature);
            
            res.set('Cache-Control', 'public, max-age=60');
            return res.json({
                type: 'nws',
                last_updated: data.last_updated,
                count: features.length,
                data: {
                    "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld", {
                        "@version": "1.1",
                        "wx": "https://api.weather.gov/ontology#",
                        "@vocab": "https://api.weather.gov/ontology#"
                    }],
                    "type": "FeatureCollection",
                    "features": features
                }
            });
        }

        // Default response for METAR and surface
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

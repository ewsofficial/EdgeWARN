/**
 * Tests for API v2 Features Timestamps route
 * @module tests/api/routes/test_v2_features_timestamps
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import timestampsRouter from '../../../src/EdgeWARN/api/routes/v2/features/timestamps.js';
import apiConfig from '../../../src/EdgeWARN/api/config.js';

describe('API v2 Features Timestamps Route', () => {
    let app;
    let tempStormcellDir;
    let originalStormcellDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempStormcellDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-timestamps-test-'));

        // Store original value
        originalStormcellDir = apiConfig.STORMCELL_DIR;

        // Temporarily override config
        apiConfig.STORMCELL_DIR = tempStormcellDir;

        app = express();
        app.use('/api/v2/features/timestamps', timestampsRouter);
    });

    afterEach(async () => {
        // Restore original value
        apiConfig.STORMCELL_DIR = originalStormcellDir;

        // Cleanup
        try {
            await fs.promises.rm(tempStormcellDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    describe('GET /api/v2/features/timestamps', () => {
        it('should return empty array when no stormcell index exists', async () => {
            const response = await request(app)
                .get('/api/v2/features/timestamps')
                .expect(200);

            expect(response.body).toEqual([]);
        });

        it('should return array of timestamps when index exists', async () => {
            // Create stormcell index
            const indexData = {
                timestamps: ['20231015-143000', '20231015-144000', '20231015-145000'],
                lastUpdated: '2023-10-15T14:50:00Z'
            };
            const indexPath = path.join(tempStormcellDir, 'stormcell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/api/v2/features/timestamps')
                .expect(200);

            expect(response.body).toEqual(indexData.timestamps);
        });

        it('should set appropriate cache headers for list', async () => {
            const indexData = { timestamps: ['20231015-143000'] };
            const indexPath = path.join(tempStormcellDir, 'stormcell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/api/v2/features/timestamps')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });
    });

    describe('GET /api/v2/features/timestamps?timestamp={YYYYMMDD-HHMMSS}', () => {
        it('should return 400 for invalid timestamp format', async () => {
            const response = await request(app)
                .get('/api/v2/features/timestamps?timestamp=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp');
        });

        it('should return 400 for timestamp with wrong length', async () => {
            const response = await request(app)
                .get('/api/v2/features/timestamps?timestamp=20231015-14300')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp');
        });

        it('should return 404 for non-existent timestamp', async () => {
            const response = await request(app)
                .get('/api/v2/features/timestamps?timestamp=20991231-000000')
                .expect(404);

            expect(response.body.error).toContain('not found');
            expect(response.body.timestamp).toBe('20991231-000000');
        });

        it('should return stormcell data for valid timestamp', async () => {
            // Create stormcell file
            const stormcellData = {
                timestamp: '20231015-143000',
                cells: [
                    { id: 1, lat: 35.5, lon: 240.1, intensity: 65.5 },
                    { id: 2, lat: 36.2, lon: 241.0, intensity: 45.0 }
                ]
            };
            const stormcellPath = path.join(tempStormcellDir, 'stormcells_20231015-143000.json');
            await fs.promises.writeFile(stormcellPath, JSON.stringify(stormcellData));

            const response = await request(app)
                .get('/api/v2/features/timestamps?timestamp=20231015-143000')
                .expect(200);

            expect(response.body).toEqual(stormcellData);
        });

        it('should set appropriate cache headers for stormcell data', async () => {
            const stormcellData = { timestamp: '20231015-143000', cells: [] };
            const stormcellPath = path.join(tempStormcellDir, 'stormcells_20231015-143000.json');
            await fs.promises.writeFile(stormcellPath, JSON.stringify(stormcellData));

            const response = await request(app)
                .get('/api/v2/features/timestamps?timestamp=20231015-143000')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=3600');
        });
    });
});

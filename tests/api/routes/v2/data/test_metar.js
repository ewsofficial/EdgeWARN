/**
 * Tests for API v2 Data METAR route
 * @module tests/api/routes/v2/data/test_metar
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import metarRouter from '../../../../../src/EdgeWARN/api/routes/v2/data/metar.js';
import apiConfig from '../../../../../src/EdgeWARN/api/config.js';

describe('API v2 Data METAR Route', () => {
    let app;
    let tempMetarDir;
    let originalMetarDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempMetarDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-metar-test-'));

        // Store original value
        originalMetarDir = apiConfig.METAR_DIR;

        // Temporarily override config
        apiConfig.METAR_DIR = tempMetarDir;

        app = express();
        app.use('/api/v2/data/metar', metarRouter);
    });

    afterEach(async () => {
        // Restore original value
        apiConfig.METAR_DIR = originalMetarDir;

        // Cleanup
        try {
            await fs.promises.rm(tempMetarDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    describe('GET /api/v2/data/metar', () => {
        it('should return empty array when no METAR files exist', async () => {
            const response = await request(app)
                .get('/api/v2/data/metar')
                .expect(200);

            expect(response.body).toEqual([]);
        });

        it('should return array of timestamps for existing METAR files', async () => {
            // Create METAR files
            const metarData = { stations: [], observations: [] };
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-14z.json'),
                JSON.stringify(metarData)
            );
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-15z.json'),
                JSON.stringify(metarData)
            );

            const response = await request(app)
                .get('/api/v2/data/metar')
                .expect(200);

            expect(response.body).toContain('20231015-140000');
            expect(response.body).toContain('20231015-150000');
        });

        it('should sort timestamps descending (newest first)', async () => {
            // Create METAR files
            const metarData = { stations: [] };
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-12z.json'),
                JSON.stringify(metarData)
            );
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-15z.json'),
                JSON.stringify(metarData)
            );
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-14z.json'),
                JSON.stringify(metarData)
            );

            const response = await request(app)
                .get('/api/v2/data/metar')
                .expect(200);

            expect(response.body[0]).toBe('20231015-150000');
            expect(response.body[1]).toBe('20231015-140000');
            expect(response.body[2]).toBe('20231015-120000');
        });

        it('should set appropriate cache headers for list', async () => {
            const response = await request(app)
                .get('/api/v2/data/metar')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });
    });

    describe('GET /api/v2/data/metar?timestamp={YYYYMMDD-HHMMSS}', () => {
        it('should return 400 for invalid timestamp format', async () => {
            const response = await request(app)
                .get('/api/v2/data/metar?timestamp=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp');
        });

        it('should return 404 for non-existent timestamp', async () => {
            const response = await request(app)
                .get('/api/v2/data/metar?timestamp=20991231-000000')
                .expect(404);

            expect(response.body.error).toContain('not found');
            expect(response.body.timestamp).toBe('20991231-000000');
        });

        it('should return METAR data for valid timestamp', async () => {
            // Create METAR file
            const metarData = {
                stations: ['KJFK', 'KLAX'],
                observations: [
                    { station: 'KJFK', temp: 25, wind: '10KT' }
                ]
            };
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-14z.json'),
                JSON.stringify(metarData)
            );

            const response = await request(app)
                .get('/api/v2/data/metar?timestamp=20231015-140000')
                .expect(200);

            expect(response.body.type).toBe('metar');
            expect(response.body.timestamp).toBe('20231015-140000');
            expect(response.body.data).toEqual(metarData);
        });

        it('should extract hour from timestamp correctly', async () => {
            const metarData = { stations: ['KORD'] };
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-09z.json'),
                JSON.stringify(metarData)
            );

            const response = await request(app)
                .get('/api/v2/data/metar?timestamp=20231015-093000')
                .expect(200);

            expect(response.body.timestamp).toBe('20231015-093000');
        });

        it('should set appropriate cache headers for METAR data', async () => {
            const metarData = { stations: [] };
            await fs.promises.writeFile(
                path.join(tempMetarDir, 'METAR_20231015-14z.json'),
                JSON.stringify(metarData)
            );

            const response = await request(app)
                .get('/api/v2/data/metar?timestamp=20231015-143000')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });
    });
});

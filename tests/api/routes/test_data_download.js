/**
 * Tests for Data Download API route
 * @module tests/api/routes/test_data_download
 */

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import dataDownloadRouter from '../../../src/EdgeWARN/api/routes/data/download.js';

describe('Data Download Route', () => {
    let app;
    let tempDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-download-test-'));

        // Create subdirectories
        await fs.promises.mkdir(path.join(tempDir, 'METAR'), { recursive: true });
        await fs.promises.mkdir(path.join(tempDir, 'NWS'), { recursive: true });
        await fs.promises.mkdir(path.join(tempDir, 'surface_features'), { recursive: true });

        // Mock config
        jest.unstable_mockModule('../../../src/EdgeWARN/api/config.js', () => ({
            default: {
                METAR_DIR: path.join(tempDir, 'METAR'),
                NWS_DIR: path.join(tempDir, 'NWS'),
                SURFACE_DIR: path.join(tempDir, 'surface_features')
            }
        }));

        const { default: mockedRouter } = await import('../../../src/EdgeWARN/api/routes/data/download.js');

        app = express();
        app.use('/data/download', mockedRouter);
    });

    afterEach(async () => {
        // Cleanup
        try {
            await fs.promises.rm(tempDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
        jest.resetModules();
    });

    describe('GET /data/download', () => {
        it('should return 400 for missing type parameter', async () => {
            const response = await request(app)
                .get('/data/download')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return 400 for invalid type parameter', async () => {
            const response = await request(app)
                .get('/data/download?type=invalid')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return 400 for missing timestamp parameter', async () => {
            const response = await request(app)
                .get('/data/download?type=metar')
                .expect(400);

            expect(response.body.error).toContain('timestamp');
        });

        it('should return 400 for invalid timestamp format', async () => {
            const response = await request(app)
                .get('/data/download?type=metar&timestamp=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp format');
        });

        it('should return 400 for timestamp with wrong length', async () => {
            const response = await request(app)
                .get('/data/download?type=metar&timestamp=20231015-14300')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp format');
        });

        it('should download METAR file successfully', async () => {
            // Create test METAR file
            const testData = { stations: ['KJFK', 'KORD', 'KLAX'] };
            const metarFile = path.join(tempDir, 'METAR', 'METAR_20231015-14z.json');
            await fs.promises.writeFile(metarFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/data/download?type=metar&timestamp=20231015-140000')
                .expect(200);

            expect(response.body.type).toBe('metar');
            expect(response.body.timestamp).toBe('20231015-140000');
            expect(response.body.data).toEqual(testData);
        });

        it('should download NWS alerts registry successfully', async () => {
            // Create test NWS alerts registry
            const testData = {
                last_updated: '20231015-143000',
                alerts: {
                    'alert1': { feature: { id: 'alert1', event: 'Severe Thunderstorm Warning' } },
                    'alert2': { feature: { id: 'alert2', event: 'Tornado Warning' } }
                }
            };
            const nwsFile = path.join(tempDir, 'NWS', 'alerts_registry.json');
            await fs.promises.writeFile(nwsFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/data/download?type=nws')
                .expect(200);

            expect(response.body.type).toBe('nws');
            expect(response.body.last_updated).toBe('20231015-143000');
            expect(response.body.count).toBe(2);
            expect(response.body.data.features.length).toBe(2);
        });

        it('should download surface features file successfully', async () => {
            // Create test surface features file
            const testData = { features: [{ id: 1, type: 'front' }] };
            const surfaceFile = path.join(tempDir, 'surface_features', 'surface_features_20231015-143000.json');
            await fs.promises.writeFile(surfaceFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/data/download?type=surface&timestamp=20231015-143000')
                .expect(200);

            expect(response.body.type).toBe('surface');
            expect(response.body.timestamp).toBe('20231015-143000');
            expect(response.body.data).toEqual(testData);
        });

        it('should return 404 for missing METAR file', async () => {
            const response = await request(app)
                .get('/data/download?type=metar&timestamp=20231015-140000')
                .expect(404);

            expect(response.body.error).toContain('METAR data not found');
            expect(response.body.timestamp).toBe('20231015-140000');
        });

        it('should return 404 for missing NWS alerts registry', async () => {
            const response = await request(app)
                .get('/data/download?type=nws')
                .expect(404);

            expect(response.body.error).toContain('NWS alerts registry not found');
        });

        it('should set Cache-Control header', async () => {
            const testData = { test: 'data' };
            const metarFile = path.join(tempDir, 'METAR', 'METAR_20231015-14z.json');
            await fs.promises.writeFile(metarFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/data/download?type=metar&timestamp=20231015-140000')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });

        it('should handle JSON parsing errors gracefully', async () => {
            // Create invalid JSON file
            const metarFile = path.join(tempDir, 'METAR', 'METAR_20231015-14z.json');
            await fs.promises.writeFile(metarFile, '{ invalid json }');

            const response = await request(app)
                .get('/data/download?type=metar&timestamp=20231015-140000')
                .expect(500);

            expect(response.body.error).toContain('Failed to download');
        });
    });
});

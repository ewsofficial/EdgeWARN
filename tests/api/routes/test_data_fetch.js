/**
 * Tests for Data Fetch API route
 * @module tests/api/routes/test_data_fetch
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import dataFetchRouter from '../../../src/EdgeWARN/api/routes/data/fetch.js';

describe('Data Fetch Route', () => {
    let app;
    let tempDir;
    let originalEnv;

    beforeEach(async () => {
        // Create temp directory structure
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-data-test-'));

        // Create subdirectories
        await fs.promises.mkdir(path.join(tempDir, 'METAR'), { recursive: true });
        await fs.promises.mkdir(path.join(tempDir, 'NWS'), { recursive: true });
        await fs.promises.mkdir(path.join(tempDir, 'surface_features'), { recursive: true });

        // Mock the config
        jest.unstable_mockModule('../../../src/EdgeWARN/api/config.js', () => ({
            default: {
                METAR_DIR: path.join(tempDir, 'METAR'),
                NWS_DIR: path.join(tempDir, 'NWS'),
                SURFACE_DIR: path.join(tempDir, 'surface_features')
            }
        }));

        const { default: mockedRouter } = await import('../../../src/EdgeWARN/api/routes/data/fetch.js');

        app = express();
        app.use('/data/fetch', mockedRouter);
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

    describe('GET /data/fetch', () => {
        it('should return 400 for missing type parameter', async () => {
            const response = await request(app)
                .get('/data/fetch')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return 400 for invalid type parameter', async () => {
            const response = await request(app)
                .get('/data/fetch?type=invalid')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return empty array for METAR when directory is empty', async () => {
            const response = await request(app)
                .get('/data/fetch?type=metar')
                .expect(200);

            expect(response.body).toEqual({
                type: 'metar',
                count: 0,
                timestamps: []
            });
        });

        it('should return METAR timestamps from files', async () => {
            // Create test METAR files
            await fs.promises.writeFile(
                path.join(tempDir, 'METAR', 'METAR_20231015-14z.json'),
                JSON.stringify({ data: 'test' })
            );
            await fs.promises.writeFile(
                path.join(tempDir, 'METAR', 'METAR_20231015-15z.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=metar')
                .expect(200);

            expect(response.body.type).toBe('metar');
            expect(response.body.count).toBe(2);
            expect(response.body.timestamps).toContain('20231015-140000');
            expect(response.body.timestamps).toContain('20231015-150000');
        });

        it('should return NWS timestamps from files', async () => {
            // Create test NWS files
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-143000.json'),
                JSON.stringify({ data: 'test' })
            );
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-144000.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=nws')
                .expect(200);

            expect(response.body.type).toBe('nws');
            expect(response.body.count).toBe(2);
            expect(response.body.timestamps).toContain('20231015-143000');
            expect(response.body.timestamps).toContain('20231015-144000');
        });

        it('should return surface feature timestamps from files', async () => {
            // Create test surface feature files
            await fs.promises.writeFile(
                path.join(tempDir, 'surface_features', 'surface_features_20231015-143000.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=surface')
                .expect(200);

            expect(response.body.type).toBe('surface');
            expect(response.body.count).toBe(1);
            expect(response.body.timestamps).toContain('20231015-143000');
        });

        it('should sort timestamps in descending order (newest first)', async () => {
            // Create files with out-of-order timestamps
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-100000.json'),
                JSON.stringify({ data: 'test' })
            );
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-150000.json'),
                JSON.stringify({ data: 'test' })
            );
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-120000.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=nws')
                .expect(200);

            expect(response.body.timestamps).toEqual([
                '20231015-150000',
                '20231015-120000',
                '20231015-100000'
            ]);
        });

        it('should set Cache-Control header', async () => {
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-143000.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=nws')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });

        it('should ignore non-JSON files', async () => {
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-143000.txt'),
                'not json'
            );
            await fs.promises.writeFile(
                path.join(tempDir, 'NWS', 'alerts_active_20231015-143000.json'),
                JSON.stringify({ data: 'test' })
            );

            const response = await request(app)
                .get('/data/fetch?type=nws')
                .expect(200);

            expect(response.body.count).toBe(1);
            expect(response.body.timestamps).toContain('20231015-143000');
        });
    });
});

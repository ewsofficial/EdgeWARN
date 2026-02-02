/**
 * Tests for Features Download API route
 * @module tests/api/routes/test_features_download
 */

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import featuresDownloadRouter from '../../../src/EdgeWARN/api/routes/features/download.js';

describe('Features Download Route', () => {
    let app;
    let tempDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-features-download-test-'));

        // Create subdirectories
        await fs.promises.mkdir(path.join(tempDir, 'stormcells'), { recursive: true });
        await fs.promises.mkdir(path.join(tempDir, 'cells'), { recursive: true });

        // Mock config
        jest.unstable_mockModule('../../../src/EdgeWARN/api/config.js', () => ({
            default: {
                STORMCELL_DIR: path.join(tempDir, 'stormcells'),
                CELL_DIR: path.join(tempDir, 'cells')
            }
        }));

        const { default: mockedRouter } = await import('../../../src/EdgeWARN/api/routes/features/download.js');

        app = express();
        app.use('/features/download', mockedRouter);
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

    describe('GET /features/download/resources', () => {
        it('should return 400 for missing type parameter', async () => {
            const response = await request(app)
                .get('/features/download/resources')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return 400 for invalid type parameter', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=invalid')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should download stormcell list for type=list', async () => {
            // Create stormcell file
            const testData = {
                source: 'Edgemont Weather Service',
                product: 'EdgeWARN Storm Cells',
                version: '1.3.2',
                latest_timestamp: '20231015-143000',
                features: [
                    { id: 101, centroid: [35.0, -97.0], properties: {} },
                    { id: 102, centroid: [36.0, -96.0], properties: {} }
                ]
            };
            const stormcellFile = path.join(tempDir, 'stormcells', 'stormcells_20231015-143000.json');
            await fs.promises.writeFile(stormcellFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/features/download/resources?type=list&timestamp=20231015-143000')
                .expect(200);

            expect(response.body).toEqual(testData);
        });

        it('should download cell data for type=cell', async () => {
            // Create cell file
            const testData = {
                id: 101,
                centroid: [35.0, -97.0],
                num_gates: 50,
                max_refl: 55.0,
                timestamp: '20231015-143000',
                properties: {
                    GLM_FLASH_COUNT: 5,
                    GLM_TOTAL_ENERGY: 500.0
                }
            };
            const cellFile = path.join(tempDir, 'cells', '101.json');
            await fs.promises.writeFile(cellFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/features/download/resources?type=cell&id=101')
                .expect(200);

            expect(response.body).toEqual(testData);
        });

        it('should return 400 for missing timestamp with type=list', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=list')
                .expect(400);

            expect(response.body.error).toContain('timestamp');
        });

        it('should return 400 for invalid timestamp format with type=list', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=list&timestamp=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid or missing timestamp');
        });

        it('should return 400 for missing id with type=cell', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell')
                .expect(400);

            expect(response.body.error).toContain('id');
        });

        it('should return 400 for invalid id with type=cell', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell&id=invalid')
                .expect(400);

            expect(response.body.error).toContain('id');
        });

        it('should return 400 for zero id with type=cell', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell&id=0')
                .expect(400);

            expect(response.body.error).toContain('id');
        });

        it('should return 400 for negative id with type=cell', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell&id=-1')
                .expect(400);

            expect(response.body.error).toContain('id');
        });

        it('should return 404 for missing stormcell file', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=list&timestamp=20231015-143000')
                .expect(404);

            expect(response.body.error).toContain('not found');
        });

        it('should return 404 for missing cell file', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell&id=999')
                .expect(404);

            expect(response.body.error).toContain('not found');
        });

        it('should set Cache-Control header for stormcell data', async () => {
            const testData = { id: 101, centroid: [35.0, -97.0] };
            const cellFile = path.join(tempDir, 'cells', '101.json');
            await fs.promises.writeFile(cellFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/features/download/resources?type=cell&id=101')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });

        it('should set Cache-Control header for stormcell list', async () => {
            const testData = { features: [] };
            const stormcellFile = path.join(tempDir, 'stormcells', 'stormcells_20231015-143000.json');
            await fs.promises.writeFile(stormcellFile, JSON.stringify(testData));

            const response = await request(app)
                .get('/features/download/resources?type=list&timestamp=20231015-143000')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=3600');
        });

        it('should handle path traversal attempts', async () => {
            const response = await request(app)
                .get('/features/download/resources?type=cell&id=../../../etc/passwd')
                .expect(400);

            expect(response.body.error).toContain('Invalid or missing id parameter');
        });
    });
});

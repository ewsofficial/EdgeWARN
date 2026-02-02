/**
 * Tests for Features Fetch API route
 * @module tests/api/routes/test_features_fetch
 */

import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import featuresFetchRouter from '../../../src/EdgeWARN/api/routes/features/fetch.js';

describe('Features Fetch Route', () => {
    let app;
    let tempDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-features-test-'));

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

        const { default: mockedRouter } = await import('../../../src/EdgeWARN/api/routes/features/fetch.js');

        app = express();
        app.use('/features/fetch', mockedRouter);
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

    describe('GET /features/fetch/resources', () => {
        it('should return 400 for missing type parameter', async () => {
            const response = await request(app)
                .get('/features/fetch/resources')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return 400 for invalid type parameter', async () => {
            const response = await request(app)
                .get('/features/fetch/resources?type=invalid')
                .expect(400);

            expect(response.body.error).toContain('type');
        });

        it('should return stormcell timestamps for type=list', async () => {
            // Create stormcell index
            const indexData = {
                timestamps: ['20231015-143000', '20231015-144000', '20231015-145000'],
                lastUpdated: '2023-10-15T14:50:00Z'
            };
            const indexPath = path.join(tempDir, 'stormcells', 'stormcell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/features/fetch/resources?type=list')
                .expect(200);

            expect(response.body).toEqual(indexData.timestamps);
        });

        it('should return cell IDs for type=cell', async () => {
            // Create cell index
            const indexData = {
                cellIds: [101, 102, 103, 104, 105],
                lastUpdated: '2023-10-15T14:50:00Z'
            };
            const indexPath = path.join(tempDir, 'cells', 'cell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/features/fetch/resources?type=cell')
                .expect(200);

            expect(response.body).toEqual(indexData.cellIds);
        });

        it('should return empty array for missing index file', async () => {
            const response = await request(app)
                .get('/features/fetch/resources?type=list')
                .expect(200);

            expect(response.body).toEqual([]);
        });

        it('should set Cache-Control header', async () => {
            const indexData = { timestamps: ['20231015-143000'] };
            const indexPath = path.join(tempDir, 'stormcells', 'stormcell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/features/fetch/resources?type=list')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });

        it('should handle invalid JSON in index file', async () => {
            // Create invalid index file
            const indexPath = path.join(tempDir, 'stormcells', 'stormcell_index.json');
            await fs.promises.writeFile(indexPath, '{ invalid json }');

            const response = await request(app)
                .get('/features/fetch/resources?type=list')
                .expect(500);

            expect(response.body.error).toContain('Failed to fetch');
        });
    });
});

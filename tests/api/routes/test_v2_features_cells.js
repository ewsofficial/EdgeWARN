/**
 * Tests for API v2 Features Cells route
 * @module tests/api/routes/test_v2_features_cells
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import cellsRouter from '../../../src/EdgeWARN/api/routes/v2/features/cells.js';
import apiConfig from '../../../src/EdgeWARN/api/config.js';

describe('API v2 Features Cells Route', () => {
    let app;
    let tempCellDir;
    let tempStormcellDir;
    let originalCellDir;
    let originalStormcellDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempCellDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-cells-test-'));
        tempStormcellDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-stormcells-test-'));

        // Store original values
        originalCellDir = apiConfig.CELL_DIR;
        originalStormcellDir = apiConfig.STORMCELL_DIR;

        // Temporarily override config
        apiConfig.CELL_DIR = tempCellDir;
        apiConfig.STORMCELL_DIR = tempStormcellDir;

        app = express();
        app.use('/api/v2/features/cells', cellsRouter);
    });

    afterEach(async () => {
        // Restore original values
        apiConfig.CELL_DIR = originalCellDir;
        apiConfig.STORMCELL_DIR = originalStormcellDir;

        // Cleanup
        try {
            await fs.promises.rm(tempCellDir, { recursive: true, force: true });
            await fs.promises.rm(tempStormcellDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    describe('GET /api/v2/features/cells', () => {
        it('should return empty array when no cell index exists', async () => {
            const response = await request(app)
                .get('/api/v2/features/cells')
                .expect(200);

            expect(response.body).toEqual([]);
        });

        it('should return array of cell IDs when index exists', async () => {
            // Create cell index
            const indexData = {
                cellIds: [1, 2, 3, 5, 8, 13],
                lastUpdated: '2023-10-15T14:50:00Z'
            };
            const indexPath = path.join(tempCellDir, 'cell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/api/v2/features/cells')
                .expect(200);

            expect(response.body).toEqual(indexData.cellIds);
        });

        it('should set appropriate cache headers for list', async () => {
            const indexData = { cellIds: [1, 2, 3] };
            const indexPath = path.join(tempCellDir, 'cell_index.json');
            await fs.promises.writeFile(indexPath, JSON.stringify(indexData));

            const response = await request(app)
                .get('/api/v2/features/cells')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });
    });

    describe('GET /api/v2/features/cells?id={int}', () => {
        it('should return 400 for invalid id parameter', async () => {
            const response = await request(app)
                .get('/api/v2/features/cells?id=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid id');
        });

        it('should return 400 for negative id', async () => {
            const response = await request(app)
                .get('/api/v2/features/cells?id=-5')
                .expect(400);

            expect(response.body.error).toContain('Invalid id');
        });

        it('should return 400 for zero id', async () => {
            const response = await request(app)
                .get('/api/v2/features/cells?id=0')
                .expect(400);

            expect(response.body.error).toContain('Invalid id');
        });

        it('should return 404 for non-existent cell', async () => {
            const response = await request(app)
                .get('/api/v2/features/cells?id=999')
                .expect(404);

            expect(response.body.error).toContain('not found');
            expect(response.body.id).toBe('999');
        });

        it('should return cell data for valid id', async () => {
            // Create cell file
            const cellData = {
                id: 123,
                first_seen: '20231015-143000',
                last_seen: '20231015-145000',
                history: [
                    { timestamp: '20231015-143000', lat: 35.5, lon: 240.1 }
                ]
            };
            const cellPath = path.join(tempCellDir, '123.json');
            await fs.promises.writeFile(cellPath, JSON.stringify(cellData));

            const response = await request(app)
                .get('/api/v2/features/cells?id=123')
                .expect(200);

            expect(response.body).toEqual(cellData);
        });

        it('should set appropriate cache headers for single cell', async () => {
            const cellData = { id: 123, history: [] };
            const cellPath = path.join(tempCellDir, '123.json');
            await fs.promises.writeFile(cellPath, JSON.stringify(cellData));

            const response = await request(app)
                .get('/api/v2/features/cells?id=123')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });
    });
});

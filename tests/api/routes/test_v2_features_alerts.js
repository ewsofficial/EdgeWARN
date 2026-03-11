/**
 * Tests for API v2 Features Alerts route
 * @module tests/api/routes/test_v2_features_alerts
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import alertsRouter from '../../../src/EdgeWARN/api/routes/v2/features/alerts.js';
import apiConfig from '../../../src/EdgeWARN/api/config.js';

describe('API v2 Features Alerts Route', () => {
    let app;
    let tempVars = {};

    beforeEach(async () => {
        tempVars.tempStormcellDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-test-stormcell-'));
        tempVars.tempEdgewarnDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-test-ew-'));
        tempVars.tempEdgewarnIdsDir = path.join(tempVars.tempEdgewarnDir, 'ids');
        tempVars.tempEdgewarnTsDir = path.join(tempVars.tempEdgewarnDir, 'timestamps');
        await fs.promises.mkdir(tempVars.tempEdgewarnIdsDir);
        await fs.promises.mkdir(tempVars.tempEdgewarnTsDir);

        tempVars.tempOfficialDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-test-off-'));
        tempVars.tempOfficialIdsDir = path.join(tempVars.tempOfficialDir, 'ids');
        tempVars.tempOfficialTsDir = path.join(tempVars.tempOfficialDir, 'timestamps');
        await fs.promises.mkdir(tempVars.tempOfficialIdsDir);
        await fs.promises.mkdir(tempVars.tempOfficialTsDir);

        tempVars.originalStormcellDir = apiConfig.STORMCELL_DIR;
        tempVars.originalEdgewarnDir = apiConfig.EDGEWARN_ALERTS_DIR;
        tempVars.originalEdgewarnIdsDir = apiConfig.EDGEWARN_ALERTS_IDS_DIR;
        tempVars.originalEdgewarnTsDir = apiConfig.EDGEWARN_ALERTS_TS_DIR;
        tempVars.originalOfficialDir = apiConfig.OFFICIAL_ALERTS_DIR;
        tempVars.originalOfficialIdsDir = apiConfig.OFFICIAL_ALERTS_IDS_DIR;
        tempVars.originalOfficialTsDir = apiConfig.OFFICIAL_ALERTS_TS_DIR;

        apiConfig.STORMCELL_DIR = tempVars.tempStormcellDir;
        apiConfig.EDGEWARN_ALERTS_DIR = tempVars.tempEdgewarnDir;
        apiConfig.EDGEWARN_ALERTS_IDS_DIR = tempVars.tempEdgewarnIdsDir;
        apiConfig.EDGEWARN_ALERTS_TS_DIR = tempVars.tempEdgewarnTsDir;
        apiConfig.OFFICIAL_ALERTS_DIR = tempVars.tempOfficialDir;
        apiConfig.OFFICIAL_ALERTS_IDS_DIR = tempVars.tempOfficialIdsDir;
        apiConfig.OFFICIAL_ALERTS_TS_DIR = tempVars.tempOfficialTsDir;

        app = express();
        app.use('/api/v2/features/alerts', alertsRouter);
    });

    afterEach(async () => {
        apiConfig.STORMCELL_DIR = tempVars.originalStormcellDir;
        apiConfig.EDGEWARN_ALERTS_DIR = tempVars.originalEdgewarnDir;
        apiConfig.EDGEWARN_ALERTS_IDS_DIR = tempVars.originalEdgewarnIdsDir;
        apiConfig.EDGEWARN_ALERTS_TS_DIR = tempVars.originalEdgewarnTsDir;
        apiConfig.OFFICIAL_ALERTS_DIR = tempVars.originalOfficialDir;
        apiConfig.OFFICIAL_ALERTS_IDS_DIR = tempVars.originalOfficialIdsDir;
        apiConfig.OFFICIAL_ALERTS_TS_DIR = tempVars.originalOfficialTsDir;

        for (const dir of [tempVars.tempStormcellDir, tempVars.tempEdgewarnDir, tempVars.tempOfficialDir]) {
            try { await fs.promises.rm(dir, { recursive: true, force: true }); } catch (e) { }
        }
    });

    describe('GET /api/v2/features/alerts/official', () => {
        beforeEach(async () => {
            // Create snapshot files for timestamps
            const snapshotData = { alerts: [] };
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialTsDir, '20260309-110000.json'),
                JSON.stringify(snapshotData)
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialTsDir, '20260309-120000.json'),
                JSON.stringify(snapshotData)
            );
        });

        it('should return list of timestamps', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(2);
            expect(response.body).toContain('20260309-110000');
            expect(response.body).toContain('20260309-120000');
        });
    });

    describe('GET /api/v2/features/alerts/edgewarn', () => {
        beforeEach(async () => {
            // Write Timestamps
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnTsDir, '20260309-120000.json'),
                JSON.stringify({
                    count: 1,
                    alerts: ["id:severe_weather:StormCast:C123:2026.03.09.11.00.00"]
                })
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnTsDir, '20260309-110000.json'),
                JSON.stringify({
                    count: 2,
                    alerts: ["id:severe_weather:StormCast:C123:2026.03.09.11.00.00", "id:flash_flood:FLOHAR:C124:2026.03.09.10.00.00"]
                })
            );
        });

        it('should return list of timestamps', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(2);
            expect(response.body).toContain('20260309-110000');
            expect(response.body).toContain('20260309-120000');
        });
    });
});

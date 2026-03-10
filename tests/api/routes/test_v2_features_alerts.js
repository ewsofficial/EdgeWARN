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
        tempVars.tempOfficialDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-test-off-'));

        tempVars.originalStormcellDir = apiConfig.STORMCELL_DIR;
        tempVars.originalEdgewarnDir = apiConfig.EDGEWARN_ALERTS_DIR;
        tempVars.originalOfficialDir = apiConfig.OFFICIAL_ALERTS_DIR;

        apiConfig.STORMCELL_DIR = tempVars.tempStormcellDir;
        apiConfig.EDGEWARN_ALERTS_DIR = tempVars.tempEdgewarnDir;
        apiConfig.OFFICIAL_ALERTS_DIR = tempVars.tempOfficialDir;
        tempVars.originalNwsDir = apiConfig.NWS_DIR;
        tempVars.tempNwsDir = tempVars.tempOfficialDir;
        apiConfig.NWS_DIR = tempVars.tempNwsDir;

        app = express();
        app.use('/api/v2/features/alerts', alertsRouter);
    });

    afterEach(async () => {
        apiConfig.STORMCELL_DIR = tempVars.originalStormcellDir;
        apiConfig.EDGEWARN_ALERTS_DIR = tempVars.originalEdgewarnDir;
        apiConfig.OFFICIAL_ALERTS_DIR = tempVars.originalOfficialDir;
        apiConfig.NWS_DIR = tempVars.originalNwsDir;

        for (const dir of [tempVars.tempStormcellDir, tempVars.tempEdgewarnDir, tempVars.tempOfficialDir]) {
            try { await fs.promises.rm(dir, { recursive: true, force: true }); } catch (e) { }
        }
    });

    // Test base paths
    describe('Mutual Exclusion Validation', () => {
        it('should return 400 when both id and timestamp are provided', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official?id=urn:oid:test&timestamp=20260309-120000')
                .expect(400);

            expect(response.body.success).toBe(false);
            expect(response.body.error.code).toBe('INVALID_INPUT');
        });
    });

    describe('GET /api/v2/features/alerts/official', () => {
        const sampleRegistry = {
            alerts: {
                "urn:oid:123": {
                    id: "urn:oid:123",
                    alert_type: "severe_weather",
                    effective: "2026-03-09T11:00:00Z",
                    expires: "2026-03-09T13:00:00Z"
                },
                "urn:oid:456": {
                    id: "urn:oid:456",
                    alert_type: "flash_flood",
                    effective: "2026-03-09T10:00:00Z",
                    expires: "2026-03-09T11:30:00Z"
                }
            }
        };

        beforeEach(async () => {
            // Write NWS registry
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialDir, 'alerts_registry.json'),
                JSON.stringify(sampleRegistry)
            );
            // Write System Timestamps
            await fs.promises.writeFile(
                path.join(tempVars.tempStormcellDir, 'stormcell_index.json'),
                JSON.stringify({ timestamps: ["20260309-110000", "20260309-120000"] })
            );

            // Create NWS snapshot files
            const snapshotData = { alerts: [] };
            await fs.promises.writeFile(
                path.join(tempVars.tempNwsDir, 'nws_snapshot_20260309-110000.json'),
                JSON.stringify(snapshotData)
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempNwsDir, 'nws_snapshot_20260309-120000.json'),
                JSON.stringify(snapshotData)
            );
        });

        it('should return timestamps and all alerts when no modifiers given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data.timestamps).toHaveLength(2);
            expect(response.body.data.alerts.length).toBe(2);
        });

        it('should return single alert when id modifier is given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official?id=urn:oid:456')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data.alert_type).toBe('flash_flood');
        });

        it('should return 404 for unknown id modifier', async () => {
            await request(app)
                .get('/api/v2/features/alerts/official?id=urn:oid:unknown')
                .expect(404);
        });

        it('should return correct cache headers for id modifier', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official?id=urn:oid:123')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });

        it('should return active alerts for specific timestamp (legacy static fallback)', async () => {
            // Timestamp is 2026-03-09 12:00:00 Z and there is no snapshot file.
            // Only alert 123 is active at this time
            const response = await request(app)
                .get('/api/v2/features/alerts/official?timestamp=20260309-130000')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data).toHaveLength(1);
            expect(response.body.data[0].id).toBe("urn:oid:123");
            expect(response.headers['cache-control']).toContain('max-age=60');
        });

        it('should return specific historical snapshot if the snapshot file exists', async () => {
            // Write a special snapshot for 20260309-110000
            const specialSnapshot = {
                count: 3,
                alerts: [
                    { id: "urn:oid:snapshot1" },
                    { id: "urn:oid:snapshot2" },
                    { id: "urn:oid:snapshot3" }
                ]
            };
            await fs.promises.writeFile(
                path.join(tempVars.tempNwsDir, 'nws_snapshot_20260309-110000.json'),
                JSON.stringify(specialSnapshot)
            );

            const response = await request(app)
                .get('/api/v2/features/alerts/official?timestamp=20260309-110000')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data).toHaveLength(3);
            expect(response.body.data[0].id).toBe("urn:oid:snapshot1");
            expect(response.body.meta.count).toBe(3);
            expect(response.body.meta.total).toBe(3);
            expect(response.headers['cache-control']).toContain('max-age=60');
        });
    });

    describe('GET /api/v2/features/alerts/edgewarn', () => {
        beforeEach(async () => {
            // Write EdgeWARN alerts
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnDir, 'alert_StormCast_C123.json'),
                JSON.stringify({
                    id: "id:severe_weather:2026.03.09.11.00.00",
                    alert_type: "severe_weather",
                    effective: "2026-03-09T11:00:00Z",
                    expires: "2026-03-09T13:00:00Z"
                })
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnDir, 'alert_FLOHAR_C124.json'),
                JSON.stringify({
                    id: "id:flash_flood:2026.03.09.10.00.00",
                    alert_type: "flash_flood",
                    effective: "2026-03-09T10:00:00Z",
                    expires: "2026-03-09T11:30:00Z"
                })
            );
            // Write Timestamps
            await fs.promises.writeFile(
                path.join(tempVars.tempStormcellDir, 'stormcell_index.json'),
                JSON.stringify({ timestamps: ["20260309-110000", "20260309-120000"] })
            );
        });

        it('should return timestamps and all alerts when no modifiers given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data.timestamps).toHaveLength(2);
            expect(response.body.data.alerts).toHaveLength(2);
        });

        it('should return single alert when id modifier is given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn?id=id:severe_weather:2026.03.09.11.00.00')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data.alert_type).toBe('severe_weather');
        });

        it('should filter correctly based on timestamp modifier', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn?timestamp=20260309-120000')
                .expect(200);

            expect(response.body.success).toBe(true);
            expect(response.body.data).toHaveLength(1);
            expect(response.body.data[0].alert_type).toBe('severe_weather');
        });
    });
});

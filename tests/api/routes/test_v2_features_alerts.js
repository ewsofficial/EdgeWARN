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
import { readJsonFileSafe } from '../../../src/EdgeWARN/api/utils/fileReader.js';
import { validateTimestampV2, validateMutualExclusion, validateAlertId } from '../../../src/EdgeWARN/api/utils/validation.js';

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
                    feature: {
                        type: 'Feature',
                        properties: {
                            event: 'Severe Thunderstorm Warning',
                            effective: "2026-03-09T11:00:00Z",
                            expires: "2026-03-09T13:00:00Z"
                        },
                        geometry: {
                            type: 'Polygon',
                            coordinates: [[[-97.0, 35.0], [-96.0, 35.0], [-96.0, 36.0], [-97.0, 36.0], [-97.0, 35.0]]]
                        }
                    }
                },
                "urn:oid:456": {
                    id: "urn:oid:456",
                    feature: {
                        type: 'Feature',
                        properties: {
                            event: 'Flash Flood Warning',
                            effective: "2026-03-09T10:00:00Z",
                            expires: "2026-03-09T11:30:00Z"
                        },
                        geometry: {
                            type: 'Polygon',
                            coordinates: [[[-95.0, 34.0], [-94.0, 34.0], [-94.0, 35.0], [-95.0, 35.0], [-95.0, 34.0]]]
                        }
                    }
                }
            }
        };

        beforeEach(async () => {
            // Write NWS ID files
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialIdsDir, 'urn_oid_123.json'),
                JSON.stringify(sampleRegistry.alerts["urn:oid:123"])
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialIdsDir, 'urn_oid_456.json'),
                JSON.stringify(sampleRegistry.alerts["urn:oid:456"])
            );
            // Write System Timestamps
            await fs.promises.writeFile(
                path.join(tempVars.tempStormcellDir, 'stormcell_index.json'),
                JSON.stringify({ timestamps: ["20260309-110000", "20260309-120000"] })
            );

            // Create NWS snapshot files
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

        it('should return timestamps only when no modifiers given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(2);
        });

        it('should return single alert when id modifier is given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/official?id=urn:oid:456')
                .expect(200);

            expect(response.body.properties.event).toBe('Flash Flood Warning');
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

        it('should return specific historical snapshot if the snapshot file exists', async () => {
            // Write a special snapshot for 20260309-110000
            const specialSnapshot = {
                count: 3,
                alerts: [
                    {
                        id: 'urn:oid:snapshot1',
                        name: 'Tornado Warning',
                        urn_oid: 'urn:oid:snapshot1',
                        effective: '2026-03-09T11:05:00Z',
                        expires: '2026-03-09T11:35:00Z',
                        geometry: {
                            type: 'Polygon',
                            coordinates: [[[-98.0, 35.0], [-97.5, 35.0], [-97.5, 35.5], [-98.0, 35.5], [-98.0, 35.0]]]
                        }
                    },
                    {
                        id: 'urn:oid:snapshot2',
                        name: 'Severe Thunderstorm Warning',
                        urn_oid: 'urn:oid:snapshot2',
                        effective: '2026-03-09T11:00:00Z',
                        expires: '2026-03-09T11:45:00Z',
                        geometry: {
                            type: 'Polygon',
                            coordinates: [[[-97.0, 34.5], [-96.5, 34.5], [-96.5, 35.0], [-97.0, 35.0], [-97.0, 34.5]]]
                        }
                    },
                    {
                        id: 'urn:oid:snapshot3',
                        name: 'Flash Flood Warning',
                        urn_oid: 'urn:oid:snapshot3',
                        effective: '2026-03-09T10:55:00Z',
                        expires: '2026-03-09T12:15:00Z',
                        geometry: {
                            type: 'Polygon',
                            coordinates: [[[-96.0, 34.0], [-95.5, 34.0], [-95.5, 34.5], [-96.0, 34.5], [-96.0, 34.0]]]
                        }
                    }
                ]
            };
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialTsDir, '20260309-110000.json'),
                JSON.stringify(specialSnapshot)
            );

            const response = await request(app)
                .get('/api/v2/features/alerts/official?timestamp=20260309-110000')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(3);
            expect(response.body[0]).toEqual({
                id: 'urn:oid:snapshot1',
                name: 'Tornado Warning',
                urn_oid: 'urn:oid:snapshot1',
                effective: '2026-03-09T11:05:00Z',
                expires: '2026-03-09T11:35:00Z',
                geometry: {
                    type: 'Polygon',
                    coordinates: [[[-98.0, 35.0], [-97.5, 35.0], [-97.5, 35.5], [-98.0, 35.5], [-98.0, 35.0]]]
                }
            });
            expect(response.body[1]).toMatchObject({
                id: 'urn:oid:snapshot2',
                name: 'Severe Thunderstorm Warning',
                urn_oid: 'urn:oid:snapshot2',
                effective: '2026-03-09T11:00:00Z',
                expires: '2026-03-09T11:45:00Z'
            });
            expect(response.body[2]).toMatchObject({
                id: 'urn:oid:snapshot3',
                name: 'Flash Flood Warning',
                urn_oid: 'urn:oid:snapshot3',
                effective: '2026-03-09T10:55:00Z',
                expires: '2026-03-09T12:15:00Z'
            });
            expect(response.headers['cache-control']).toContain('max-age=60');
        });

        it('should return ingest-produced official snapshot summaries without reading id files', async () => {
            await fs.promises.writeFile(
                path.join(tempVars.tempOfficialTsDir, '20260309-120000.json'),
                JSON.stringify({
                    count: 2,
                    alerts: [
                        {
                            id: 'urn:oid:123',
                            name: 'Severe Thunderstorm Warning',
                            urn_oid: 'urn:oid:123',
                            effective: '2026-03-09T11:00:00Z',
                            expires: '2026-03-09T13:00:00Z',
                            geometry: null
                        },
                        {
                            id: 'urn:oid:missing',
                            name: 'Missing Backing File Warning',
                            urn_oid: 'urn:oid:missing',
                            effective: '2026-03-09T12:00:00Z',
                            expires: '2026-03-09T13:00:00Z',
                            geometry: null
                        }
                    ]
                })
            );

            await fs.promises.rm(path.join(tempVars.tempOfficialIdsDir, 'urn_oid_123.json'));

            const response = await request(app)
                .get('/api/v2/features/alerts/official?timestamp=20260309-120000')
                .expect(200);

            expect(response.body).toHaveLength(2);
            expect(response.body[0]).toMatchObject({
                id: 'urn:oid:123',
                name: 'Severe Thunderstorm Warning',
                urn_oid: 'urn:oid:123',
                effective: '2026-03-09T11:00:00Z',
                expires: '2026-03-09T13:00:00Z'
            });
            expect(response.body[1]).toMatchObject({
                id: 'urn:oid:missing',
                name: 'Missing Backing File Warning',
                urn_oid: 'urn:oid:missing',
                effective: '2026-03-09T12:00:00Z',
                expires: '2026-03-09T13:00:00Z'
            });
        });
    });

    describe('GET /api/v2/features/alerts/edgewarn', () => {
        beforeEach(async () => {
            // Write EdgeWARN alerts
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnIdsDir, 'id_severe_weather_StormCast_C123_2026.03.09.11.00.00.json'),
                JSON.stringify({
                    id: "id:severe_weather:StormCast:C123:2026.03.09.11.00.00",
                    alert_type: "severe_weather",
                    effective: "2026-03-09T11:00:00Z",
                    expires: "2026-03-09T13:00:00Z"
                })
            );
            await fs.promises.writeFile(
                path.join(tempVars.tempEdgewarnIdsDir, 'id_flash_flood_FLOHAR_C124_2026.03.09.10.00.00.json'),
                JSON.stringify({
                    id: "id:flash_flood:FLOHAR:C124:2026.03.09.10.00.00",
                    alert_type: "flash_flood",
                    effective: "2026-03-09T10:00:00Z",
                    expires: "2026-03-09T11:30:00Z"
                })
            );
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

        it('should return timestamps only when no modifiers given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(2);
        });

        it('should return single alert when id modifier is given', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn?id=id:severe_weather:StormCast:C123:2026.03.09.11.00.00')
                .expect(200);

            expect(response.body.alert_type).toBe('severe_weather');
        });

        it('should filter correctly based on timestamp modifier and return IDs only', async () => {
            const response = await request(app)
                .get('/api/v2/features/alerts/edgewarn?timestamp=20260309-120000')
                .expect(200);

            expect(Array.isArray(response.body)).toBe(true);
            expect(response.body).toHaveLength(1);
            expect(response.body[0]).toBe('id:severe_weather:StormCast:C123:2026.03.09.11.00.00');
        });
    });
});

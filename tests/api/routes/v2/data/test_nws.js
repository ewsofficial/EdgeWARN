/**
 * Tests for API v2 Data NWS route
 * @module tests/api/routes/v2/data/test_nws
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import nwsRouter from '../../../../../src/EdgeWARN/api/routes/v2/data/nws.js';
import apiConfig from '../../../../../src/EdgeWARN/api/config.js';

describe('API v2 Data NWS Route', () => {
    let app;
    let tempNwsDir;
    let originalNwsDir;

    beforeEach(async () => {
        // Create temp directory structure
        tempNwsDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-nws-test-'));

        // Store original value
        originalNwsDir = apiConfig.NWS_DIR;

        // Temporarily override config
        apiConfig.NWS_DIR = tempNwsDir;

        app = express();
        app.use('/api/v2/data/nws', nwsRouter);
    });

    afterEach(async () => {
        // Restore original value
        apiConfig.NWS_DIR = originalNwsDir;

        // Cleanup
        try {
            await fs.promises.rm(tempNwsDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    describe('GET /api/v2/data/nws', () => {
        it('should return empty array when no NWS files exist', async () => {
            const response = await request(app)
                .get('/api/v2/data/nws')
                .expect(200);

            expect(response.body).toEqual([]);
        });

        it('should return array of timestamps for existing snapshot files', async () => {
            // Create snapshot files
            const snapshotData = { alerts: [] };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-143000.json'),
                JSON.stringify(snapshotData)
            );
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-144500.json'),
                JSON.stringify(snapshotData)
            );

            const response = await request(app)
                .get('/api/v2/data/nws')
                .expect(200);

            expect(response.body).toContain('20231015-143000');
            expect(response.body).toContain('20231015-144500');
        });

        it('should sort timestamps descending (newest first)', async () => {
            // Create snapshot files
            const snapshotData = { alerts: [] };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-120000.json'),
                JSON.stringify(snapshotData)
            );
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-150000.json'),
                JSON.stringify(snapshotData)
            );
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-140000.json'),
                JSON.stringify(snapshotData)
            );

            const response = await request(app)
                .get('/api/v2/data/nws')
                .expect(200);

            expect(response.body[0]).toBe('20231015-150000');
            expect(response.body[1]).toBe('20231015-140000');
            expect(response.body[2]).toBe('20231015-120000');
        });

        it('should set appropriate cache headers for list', async () => {
            const response = await request(app)
                .get('/api/v2/data/nws')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=5');
        });
    });

    describe('GET /api/v2/data/nws?timestamp={YYYYMMDD-HHMMSS}', () => {
        it('should return 400 for invalid timestamp format', async () => {
            const response = await request(app)
                .get('/api/v2/data/nws?timestamp=invalid')
                .expect(400);

            expect(response.body.error).toContain('Invalid timestamp');
        });

        it('should return 404 for non-existent timestamp', async () => {
            const response = await request(app)
                .get('/api/v2/data/nws?timestamp=20991231-000000')
                .expect(404);

            expect(response.body.error).toContain('not found');
            expect(response.body.timestamp).toBe('20991231-000000');
        });

        it('should return NWS snapshot for valid timestamp', async () => {
            // Create snapshot file
            const snapshotData = {
                count: 2,
                alerts: [
                    { id: 'alert-1', event: 'Tornado Warning' },
                    { id: 'alert-2', event: 'Severe Thunderstorm Warning' }
                ]
            };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-143000.json'),
                JSON.stringify(snapshotData)
            );

            const response = await request(app)
                .get('/api/v2/data/nws?timestamp=20231015-143000')
                .expect(200);

            expect(response.body.type).toBe('nws');
            expect(response.body.timestamp).toBe('20231015-143000');
            expect(response.body.count).toBe(2);
            expect(response.body.alerts).toHaveLength(2);
        });

        it('should set appropriate cache headers for snapshot data', async () => {
            const snapshotData = { count: 0, alerts: [] };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'nws_snapshot_20231015-143000.json'),
                JSON.stringify(snapshotData)
            );

            const response = await request(app)
                .get('/api/v2/data/nws?timestamp=20231015-143000')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });
    });

    describe('GET /api/v2/data/nws?id={alert_id}', () => {
        it('should return 404 for non-existent alert ID', async () => {
            // Create registry without the alert
            const registry = {
                last_updated: '2023-10-15T14:30:00Z',
                alerts: {}
            };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'alerts_registry.json'),
                JSON.stringify(registry)
            );

            const response = await request(app)
                .get('/api/v2/data/nws?id=urn:oid:nonexistent')
                .expect(404);

            expect(response.body.error).toContain('not found');
            expect(response.body.id).toBe('urn:oid:nonexistent');
        });

        it('should return specific alert by ID', async () => {
            // Create registry with the alert
            const registry = {
                last_updated: '2023-10-15T14:30:00Z',
                alerts: {
                    'urn:oid:2.49.0.1.840.0.2406210827.1': {
                        first_seen: '2023-10-15T14:00:00Z',
                        last_seen: '2023-10-15T14:30:00Z',
                        expires: '2023-10-15T15:00:00Z',
                        feature: {
                            id: 'https://api.weather.gov/alerts/urn:oid:...',
                            event: 'Severe Thunderstorm Warning'
                        }
                    }
                }
            };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'alerts_registry.json'),
                JSON.stringify(registry)
            );

            const response = await request(app)
                .get('/api/v2/data/nws?id=urn:oid:2.49.0.1.840.0.2406210827.1')
                .expect(200);

            expect(response.body.type).toBe('nws');
            expect(response.body.id).toBe('urn:oid:2.49.0.1.840.0.2406210827.1');
            expect(response.body.first_seen).toBe('2023-10-15T14:00:00Z');
            expect(response.body.feature.event).toBe('Severe Thunderstorm Warning');
        });

        it('should set appropriate cache headers for alert data', async () => {
            const registry = {
                last_updated: '2023-10-15T14:30:00Z',
                alerts: {
                    'urn:oid:test': { first_seen: '2023-10-15T14:00:00Z' }
                }
            };
            await fs.promises.writeFile(
                path.join(tempNwsDir, 'alerts_registry.json'),
                JSON.stringify(registry)
            );

            const response = await request(app)
                .get('/api/v2/data/nws?id=urn:oid:test')
                .expect(200);

            expect(response.headers['cache-control']).toContain('max-age=60');
        });
    });

    describe('Parameter validation', () => {
        it('should return 400 when both timestamp and id are provided', async () => {
            const response = await request(app)
                .get('/api/v2/data/nws?timestamp=20231015-143000&id=urn:oid:test')
                .expect(400);

            expect(response.body.error).toContain('cannot be specified at the same time');
        });
    });
});

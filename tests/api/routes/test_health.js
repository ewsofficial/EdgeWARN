/**
 * Tests for Health API route
 * @module tests/api/routes/test_health
 */

import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import healthRouter from '../../../src/EdgeWARN/api/routes/health.js';

describe('Health Route', () => {
    let app;

    beforeAll(() => {
        app = express();
        app.use('/health', healthRouter);
    });

    describe('GET /health', () => {
        it('should return status OK and include valid timestamp', async () => {
            const response = await request(app)
                .get('/health')
                .expect('Content-Type', /json/)
                .expect(200);

            expect(response.body.status).toBe('OK');
            expect(response.body).toHaveProperty('timestamp');
            expect(typeof response.body.timestamp).toBe('string');
            expect(new Date(response.body.timestamp).toISOString()).toBe(response.body.timestamp);
        });

        it('should not expose system information (CPU/memory)', async () => {
            const response = await request(app)
                .get('/health')
                .expect(200);

            // Security: Ensure system info is not exposed
            expect(response.body).not.toHaveProperty('cpuUsage');
            expect(response.body).not.toHaveProperty('systemMemoryUsageMB');
        });

        it('should handle multiple requests consistently', async () => {
            // First request
            const response1 = await request(app).get('/health').expect(200);

            // Small delay
            await new Promise(resolve => setTimeout(resolve, 10));

            // Second request
            const response2 = await request(app).get('/health').expect(200);

            expect(response1.body.status).toBe('OK');
            expect(response2.body.status).toBe('OK');
            expect(response1.body).toHaveProperty('timestamp');
            expect(response2.body).toHaveProperty('timestamp');
        });

    });
});

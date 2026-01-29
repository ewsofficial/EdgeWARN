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
        it('should return status OK', async () => {
            const response = await request(app)
                .get('/health')
                .expect('Content-Type', /json/)
                .expect(200);

            expect(response.body.status).toBe('OK');
        });

        it('should include CPU usage percentage', async () => {
            const response = await request(app)
                .get('/health')
                .expect(200);

            expect(response.body).toHaveProperty('cpuUsage');
            expect(typeof response.body.cpuUsage).toBe('number');
            expect(response.body.cpuUsage).toBeGreaterThanOrEqual(0);
        });

        it('should include system memory usage in MB', async () => {
            const response = await request(app)
                .get('/health')
                .expect(200);

            expect(response.body).toHaveProperty('systemMemoryUsageMB');
            expect(typeof response.body.systemMemoryUsageMB).toBe('number');
            expect(response.body.systemMemoryUsageMB).toBeGreaterThan(0);
        });

        it('should handle multiple requests (CPU diff calculation)', async () => {
            // First request
            const response1 = await request(app).get('/health').expect(200);

            // Small delay
            await new Promise(resolve => setTimeout(resolve, 10));

            // Second request
            const response2 = await request(app).get('/health').expect(200);

            expect(response1.body.status).toBe('OK');
            expect(response2.body.status).toBe('OK');

            // CPU usage should be a number in both responses
            expect(typeof response1.body.cpuUsage).toBe('number');
            expect(typeof response2.body.cpuUsage).toBe('number');
        });

        it('should return consistent response structure', async () => {
            const response = await request(app)
                .get('/health')
                .expect(200);

            expect(response.body).toMatchObject({
                status: expect.any(String),
                cpuUsage: expect.any(Number),
                systemMemoryUsageMB: expect.any(Number)
            });
        });
    });
});

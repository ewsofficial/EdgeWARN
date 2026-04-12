import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import mesocyclonesRouter from '../../../src/EdgeWARN/api/routes/v2/features/mesocyclones.js';
import apiConfig from '../../../src/EdgeWARN/api/config.js';

describe('API v2 Features Mesocyclones Route', () => {
  let app;
  let tempMesocycloneDir;
  let originalMesocycloneDir;

  beforeEach(async () => {
    tempMesocycloneDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-v2-mesocyclones-test-'));
    originalMesocycloneDir = apiConfig.MESOCYCLONE_DIR;
    apiConfig.MESOCYCLONE_DIR = tempMesocycloneDir;

    app = express();
    app.use('/api/v2/features/mesocyclones', mesocyclonesRouter);
  });

  afterEach(async () => {
    apiConfig.MESOCYCLONE_DIR = originalMesocycloneDir;

    try {
      await fs.promises.rm(tempMesocycloneDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup errors.
    }
  });

  describe('GET /api/v2/features/mesocyclones', () => {
    it('returns empty array when no mesocyclone files exist', async () => {
      const response = await request(app)
        .get('/api/v2/features/mesocyclones')
        .expect(200);

      expect(response.body).toEqual([]);
    });

    it('returns timestamps from matching filenames only in newest-first order', async () => {
      await fs.promises.writeFile(
        path.join(tempMesocycloneDir, 'mesocyclones_20231015-143000.json'),
        JSON.stringify({ timestamp: '20231015-143000', detections: [] })
      );
      await fs.promises.writeFile(
        path.join(tempMesocycloneDir, 'mesocyclones_20231015-145500.json'),
        JSON.stringify({ timestamp: '20231015-145500', detections: [] })
      );
      await fs.promises.writeFile(
        path.join(tempMesocycloneDir, 'ignore-me.json'),
        JSON.stringify({})
      );

      const response = await request(app)
        .get('/api/v2/features/mesocyclones')
        .expect(200);

      expect(response.body).toEqual(['20231015-145500', '20231015-143000']);
    });

    it('sets short cache headers for timestamp listing', async () => {
      const response = await request(app)
        .get('/api/v2/features/mesocyclones')
        .expect(200);

      expect(response.headers['cache-control']).toContain('max-age=5');
    });
  });

  describe('GET /api/v2/features/mesocyclones?timestamp={YYYYMMDD-HHMMSS}', () => {
    it('returns 400 for invalid timestamp format', async () => {
      const response = await request(app)
        .get('/api/v2/features/mesocyclones?timestamp=invalid')
        .expect(400);

      expect(response.body.error).toContain('Invalid timestamp');
    });

    it('returns 404 for missing mesocyclone snapshot', async () => {
      const response = await request(app)
        .get('/api/v2/features/mesocyclones?timestamp=20991231-000000')
        .expect(404);

      expect(response.body.error).toContain('not found');
      expect(response.body.timestamp).toBe('20991231-000000');
    });

    it('returns mesocyclone payload for valid timestamp', async () => {
      const payload = {
        type: 'MesocycloneDetectionCollection',
        timestamp: '20231015-143000',
        metadata: { count: 1 },
        detections: [{ id: 7, lat: 35.5, lon: -97.5 }]
      };
      await fs.promises.writeFile(
        path.join(tempMesocycloneDir, 'mesocyclones_20231015-143000.json'),
        JSON.stringify(payload)
      );

      const response = await request(app)
        .get('/api/v2/features/mesocyclones?timestamp=20231015-143000')
        .expect(200);

      expect(response.body).toEqual(payload);
    });

    it('sets cache headers for snapshot payloads', async () => {
      await fs.promises.writeFile(
        path.join(tempMesocycloneDir, 'mesocyclones_20231015-143000.json'),
        JSON.stringify({ timestamp: '20231015-143000', detections: [] })
      );

      const response = await request(app)
        .get('/api/v2/features/mesocyclones?timestamp=20231015-143000')
        .expect(200);

      expect(response.headers['cache-control']).toContain('max-age=60');
    });
  });
});

import { describe, expect, it } from '@jest/globals';
import express from 'express';
import request from 'supertest';
import v2Router from '../../../src/EdgeWARN/api/routes/v2/index.js';

describe('API v2 index route', () => {
  function buildApp() {
    const app = express();
    app.use('/api/v2', v2Router);
    return app;
  }

  it('returns API metadata outside production', async () => {
    const originalNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = 'test';

    const response = await request(buildApp())
      .get('/api/v2')
      .expect(200);

    expect(response.body.message).toBe('EdgeWARN API v2');
    expect(response.body.version).toBe('2.0.1');
    expect(response.body.endpoints.features.cells).toBe('/api/v2/features/cells[?id={int}]');

    process.env.NODE_ENV = originalNodeEnv;
  });

  it('returns masked version in production', async () => {
    const originalNodeEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';

    const response = await request(buildApp())
      .get('/api/v2')
      .expect(200);

    expect(response.body.version).toBe('2.x');

    process.env.NODE_ENV = originalNodeEnv;
  });

  it('mounts feature and data subroutes', async () => {
    const app = buildApp();

    await request(app)
      .get('/api/v2/features/cells')
      .expect(200);

    await request(app)
      .get('/api/v2/features/timestamps')
      .expect(200);

    await request(app)
      .get('/api/v2/features/alerts/official')
      .expect(200);

    await request(app)
      .get('/api/v2/data/metar')
      .expect(200);
  });
});

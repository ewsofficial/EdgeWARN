import { afterEach, describe, expect, it, jest } from '@jest/globals';
import request from 'supertest';
import { createApp } from '../../src/EWMRS/api/server.js';

describe('EWMRS API server', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('uses EWMRS CLI rate limit flags and disables a zero-valued bin', async () => {
    const { app } = createApp({
      argv: ['--ewmrs-rate-limit-1s=0', '--ewmrs-rate-limit-1m=2'],
      baseDir: '/tmp/ewmrs-test'
    });

    await request(app).get('/healthz').expect(200);
    await request(app).get('/healthz').expect(200);
    await request(app).get('/healthz').expect(429);
  });

  it('exposes /nexrad in root metadata and mounts the route', async () => {
    const { app } = createApp({ baseDir: '/tmp/ewmrs-test' });

    const rootResponse = await request(app).get('/').expect(200);
    expect(rootResponse.body.endpoints).toContain('/nexrad');
    expect(rootResponse.body.base_dir).toBeUndefined();
    expect(rootResponse.body.gui_dir).toBeUndefined();

    await request(app).get('/nexrad').expect(200);
  });
});

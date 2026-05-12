import { afterEach, describe, expect, it, jest } from '@jest/globals';
import request from 'supertest';
import { createApp, startServer } from '../../src/EWMRS/api/server.js';

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

  it('starts the server with the requested port', () => {
    const listen = jest.fn((port, callback) => {
      callback();
      return { close: jest.fn() };
    });
    const app = { listen };
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const result = startServer({ app, port: 3010, baseDir: '/tmp/ewmrs-test' });

    expect(listen).toHaveBeenCalledWith(3010, expect.any(Function));
    expect(result.port).toBe(3010);
    expect(result.app).toBe(app);
  });

  it('exposes /nexrad in root metadata and mounts the route', async () => {
    const { app } = createApp({ baseDir: '/tmp/ewmrs-test' });

    const rootResponse = await request(app).get('/').expect(200);
    expect(rootResponse.body.endpoints).toContain('/nexrad');

    await request(app).get('/nexrad').expect(200);
  });
});

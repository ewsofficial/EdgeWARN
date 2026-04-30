import { afterEach, describe, expect, it, jest } from '@jest/globals';
import request from 'supertest';
import { createApp, startClusteredServer, startWorkerServer } from '../../src/EdgeWARN/api/server.js';

describe('API server', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('returns detailed root metadata outside production', async () => {
    const app = createApp({ NODE_ENV: 'test' });

    const response = await request(app)
      .get('/')
      .expect(200);

    expect(response.body).toEqual({
      message: 'EdgeWARN Backend API',
      version: '2.5.1'
    });
  });

  it('returns masked root version in production', async () => {
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    const app = createApp({ NODE_ENV: 'production' });

    const response = await request(app)
      .get('/')
      .expect(200);

    expect(response.body.version).toBe('2.x');
    expect(console.warn).toHaveBeenCalledWith(
      '[Security] ALLOWED_ORIGINS not set. CORS requests will be blocked in production.'
    );
  });

  it('applies explicit CORS origins when configured', async () => {
    const app = createApp({
      NODE_ENV: 'production',
      ALLOWED_ORIGINS: 'https://alpha.example, https://beta.example'
    });

    const response = await request(app)
      .get('/')
      .set('Origin', 'https://beta.example')
      .expect(200);

    expect(response.headers['access-control-allow-origin']).toBe('https://beta.example');
  });

  it('rejects removed v1 endpoints', async () => {
    const app = createApp({ NODE_ENV: 'test' });

    const featuresResponse = await request(app)
      .get('/features')
      .expect(410);

    const dataResponse = await request(app)
      .get('/data')
      .expect(410);

    expect(featuresResponse.body.error).toContain('API v1 has been removed');
    expect(dataResponse.body.documentation).toBe('/api/v2');
  });

  it('serves robots.txt', async () => {
    const app = createApp({ NODE_ENV: 'test' });

    const response = await request(app)
      .get('/robots.txt')
      .expect(200);

    expect(response.text).toContain('User-agent: *');
    expect(response.text).toContain('Disallow: /');
  });

  it('applies rate limiting to normal requests but skips internal health checks', async () => {
    const app = createApp({
      NODE_ENV: 'test',
      RATE_LIMIT_WINDOW_MS_SEC: '1000',
      RATE_LIMIT_MAX_SEC: '1',
      RATE_LIMIT_WINDOW_MS_MIN: '60000',
      RATE_LIMIT_MAX_MIN: '100'
    });

    await request(app).get('/').expect(200);
    await request(app).get('/').expect(429);

    await request(app)
      .get('/health')
      .set('x-internal-check', 'true')
      .expect(200);

    await request(app)
      .get('/health')
      .set('x-internal-check', 'true')
      .expect(200);
  });

  it('returns development error details from error middleware', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const app = createApp(
      { NODE_ENV: 'development' },
      {
        beforeErrorHandler(targetApp) {
          targetApp.get('/boom', (req, res, next) => next(new Error('kaboom')));
        }
      }
    );

    const response = await request(app)
      .get('/boom')
      .expect(500);

    expect(response.body).toEqual({ error: 'kaboom' });
    expect(console.error).toHaveBeenCalled();
  });

  it('hides production error details from error middleware', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const app = createApp(
      { NODE_ENV: 'production', ALLOWED_ORIGINS: 'https://api.example' },
      {
        beforeErrorHandler(targetApp) {
          targetApp.get('/boom', (req, res, next) => next(new Error('kaboom')));
        }
      }
    );

    const response = await request(app)
      .get('/boom')
      .expect(500);

    expect(response.body).toEqual({ error: 'Internal server error' });
    expect(console.error).toHaveBeenCalledWith('Error: kaboom');
  });

  it('starts a worker server with the requested host and port', () => {
    const listen = jest.fn((port, host, callback) => {
      callback();
      return { close: jest.fn() };
    });
    const app = { listen };
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const result = startWorkerServer({ app, port: 4321, host: '127.0.0.1' });

    expect(listen).toHaveBeenCalledWith(4321, '127.0.0.1', expect.any(Function));
    expect(result.port).toBe(4321);
    expect(result.app).toBe(app);
  });

  it('forks up to four workers in primary cluster mode and restarts on exit', () => {
    const fork = jest.fn();
    const on = jest.fn((event, handler) => {
      if (event === 'exit') {
        handler({ process: { pid: 222 } });
      }
    });
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const result = startClusteredServer({
      clusterModule: { isPrimary: true, fork, on },
      osModule: { cpus: () => new Array(8).fill({}) },
      env: { NODE_ENV: 'test' },
      port: 5001
    });

    expect(result).toMatchObject({ mode: 'primary', numCPUs: 4, port: 5001 });
    expect(fork).toHaveBeenCalledTimes(5);
    expect(on).toHaveBeenCalledWith('exit', expect.any(Function));
  });

  it('starts the worker branch when cluster mode is not primary', () => {
    const listen = jest.fn((port, host, callback) => {
      callback();
      return { close: jest.fn() };
    });
    jest.spyOn(console, 'log').mockImplementation(() => {});

    const result = startClusteredServer({
      clusterModule: { isPrimary: false },
      osModule: { cpus: () => [{}, {}] },
      app: { listen },
      env: { NODE_ENV: 'test' },
      port: 5002
    });

    expect(result.mode).toBe('worker');
    expect(listen).toHaveBeenCalledWith(5002, '0.0.0.0', expect.any(Function));
  });
});

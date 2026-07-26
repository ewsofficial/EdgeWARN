import { afterEach, describe, expect, it, jest } from '@jest/globals';
import request from 'supertest';
import { createApp } from '../../src/EdgeWARN/api/server.js';

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
      version: '2.7.0'
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

  it('uses EdgeWARN CLI rate limit flags and disables a zero-valued bin', async () => {
    const app = createApp(
      { NODE_ENV: 'test' },
      {
        argv: ['--edgewarn-rate-limit-1s=0', '--edgewarn-rate-limit-1m=2']
      }
    );

    await request(app).get('/').expect(200);
    await request(app).get('/').expect(200);
    await request(app).get('/').expect(429);
  });

  it('returns 413 for JSON bodies over 16kb', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const app = createApp(
      {
        NODE_ENV: 'test',
        RATE_LIMIT_WINDOW_MS_SEC: '1000',
        RATE_LIMIT_MAX_SEC: '100',
        RATE_LIMIT_WINDOW_MS_MIN: '60000',
        RATE_LIMIT_MAX_MIN: '1000'
      },
      {
        beforeErrorHandler(targetApp) {
          targetApp.post('/json-check', (req, res) => res.json({ ok: true, body: req.body }));
        }
      }
    );

    const oversizedBody = JSON.stringify({ payload: 'x'.repeat(17 * 1024) });
    const response = await request(app)
      .post('/json-check')
      .set('Content-Type', 'application/json')
      .send(oversizedBody)
      .expect(413);

    expect(response.body).toEqual({ error: 'Payload too large' });
  });

  it('rate limits abusive JSON requests before body parsing', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const app = createApp(
      {
        NODE_ENV: 'test',
        RATE_LIMIT_WINDOW_MS_SEC: '1000',
        RATE_LIMIT_MAX_SEC: '1',
        RATE_LIMIT_WINDOW_MS_MIN: '60000',
        RATE_LIMIT_MAX_MIN: '100'
      },
      {
        beforeErrorHandler(targetApp) {
          targetApp.post('/json-check', (req, res) => res.json({ ok: true, body: req.body }));
        }
      }
    );

    await request(app)
      .post('/json-check')
      .send({ ok: true })
      .expect(200);

    await request(app)
      .post('/json-check')
      .set('Content-Type', 'application/json')
      .send('{"unterminated":')
      .expect(429);
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

});

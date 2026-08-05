import { afterEach, describe, expect, it } from '@jest/globals';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import request from 'supertest';
import { createApp } from '../../src/api/app.js';

describe('unified API app', () => {
  let baseDir;
  afterEach(async () => { if (baseDir) await fs.rm(baseDir, { recursive: true, force: true }); });

  it('serves discovery, health, v3 envelopes, and redacted problems', async () => {
    baseDir = await fs.mkdtemp(path.join(os.tmpdir(), 'unified-api-'));
    await fs.mkdir(path.join(baseDir, 'data', 'cells'), { recursive: true });
    await fs.mkdir(path.join(baseDir, 'gui'), { recursive: true });
    await fs.mkdir(path.join(baseDir, 'wpc'), { recursive: true });
    await fs.writeFile(path.join(baseDir, 'data', 'cells', 'cell_index.json'), '{"cellIds":["4"]}');
    await fs.writeFile(path.join(baseDir, 'data', 'cells', '4.json'), '{"id":4}');
    const { app } = await createApp({ env: { EDGEWARN_BASE_DIR: baseDir, RATE_LIMIT_MAX_SEC: '0', RATE_LIMIT_MAX_MIN: '0' }, argv: [] });
    const root = await request(app).get('/').expect(200);
    expect(root.body.links.api).toBe('/api/v3');
    await request(app).get('/health/ready').expect(200);
    const cells = await request(app).get('/api/v3/cells').expect(200);
    expect(cells.body.data).toEqual(['4']);
    expect(cells.body.meta.requestId).toBeTruthy();
    const legacyCells = await request(app).get('/api/v2/features/cells').expect(200);
    expect(legacyCells.body).toEqual(['4']);
    expect(legacyCells.headers.deprecation).toBe('true');
    const missing = await request(app).get('/nope').expect(404);
    expect(missing.headers['content-type']).toContain('application/problem+json');
  });
});

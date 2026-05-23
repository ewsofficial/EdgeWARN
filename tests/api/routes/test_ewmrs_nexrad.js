import { afterEach, beforeEach, describe, expect, it } from '@jest/globals';
import express from 'express';
import fs from 'fs';
import os from 'os';
import path from 'path';
import request from 'supertest';

import nexradRouter from '../../../src/EWMRS/api/routes/nexrad/index.js';

function parseBinary(res, callback) {
  const chunks = [];
  res.on('data', (chunk) => chunks.push(chunk));
  res.on('end', () => callback(null, Buffer.concat(chunks)));
}

async function writeFile(filePath, content) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  await fs.promises.writeFile(filePath, content);
}

describe('EWMRS NEXRAD routes', () => {
  let app;
  let tempDir;
  let guiDir;
  let sentinelBytes;

  beforeEach(async () => {
    tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-nexrad-'));
    guiDir = path.join(tempDir, 'gui');
    sentinelBytes = Buffer.from('outside-root-secret');

    app = express();
    app.locals.GUI_DIR = guiDir;
    app.use('/nexrad', nexradRouter);

    await writeFile(path.join(guiDir, 'secret', 'DBZH.bin.gz'), sentinelBytes);
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5', 'KTLH_DBZH_0.5_20260512-004336.bin.gz'), Buffer.from('dbzh-bytes'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.9', 'KTLH_VRADH_0.9_20260512-004336.bin.gz'), Buffer.from('vradh-bytes'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '1.3', 'KTLH_PHIDP_1.3_20260512-004753.bin.gz'), Buffer.from('phidp-bytes'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLX', '0.5', 'KTLX_ZDR_0.5_20260512-004336.bin.gz'), Buffer.from('zdr-bytes'));
    await fs.promises.mkdir(path.join(guiDir, 'NEXRAD', 'BAD..'), { recursive: true });
    await fs.promises.mkdir(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5..'), { recursive: true });
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5..', 'KTLH_DBZH_0.5.._20260512-004336.bin.gz'), Buffer.from('bad-elev'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5', 'KTLH_NOT_ALLOWED_0.5_20260512-004336.bin.gz'), Buffer.from('bad-product'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5', 'KTLH_DBZH_0.5_bad-ts.bin.gz'), Buffer.from('bad-ts'));
    await writeFile(path.join(guiDir, 'NEXRAD', 'KTLH', '0.5', 'KTLX_DBZH_0.5_20260512-004336.bin.gz'), Buffer.from('wrong-site'));
  });

  afterEach(async () => {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  });

  it('returns empty array when the NEXRAD root does not exist', async () => {
    const emptyApp = express();
    const emptyDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-nexrad-empty-'));
    emptyApp.locals.GUI_DIR = path.join(emptyDir, 'gui');
    emptyApp.use('/nexrad', nexradRouter);

    const response = await request(emptyApp).get('/nexrad').expect(200);

    expect(response.body).toEqual([]);
    expect(response.headers['cache-control']).toContain('max-age=5');

    await fs.promises.rm(emptyDir, { recursive: true, force: true });
  });

  it('lists only safe active NEXRAD sites', async () => {
    const response = await request(app).get('/nexrad').expect(200);

    expect(response.body).toEqual(['KTLH', 'KTLX']);
    expect(response.headers['cache-control']).toContain('max-age=5');
  });

  it('returns timestamps mapped to sorted numeric elevations', async () => {
    const response = await request(app).get('/nexrad/KTLH').expect(200);

    expect(response.body).toEqual({
      '20260512-004753': [1.3],
      '20260512-004336': [0.5, 0.9]
    });
    expect(response.headers['cache-control']).toContain('max-age=5');
  });

  it('returns 404 for a missing site', async () => {
    const response = await request(app).get('/nexrad/KBOX').expect(404);
    expect(response.body).toEqual({ error: 'NEXRAD site not found', site: 'KBOX' });
  });

  it('returns 400 for invalid site, timestamp, elevation, and product parameters', async () => {
    await request(app).get('/nexrad/KTLH..').expect(400);
    await request(app).get('/nexrad/KTLH/not-a-ts/0.5?product=DBZH').expect(400);
    await request(app).get('/nexrad/KTLH/20260512-004336/+0.5?product=DBZH').expect(400);
    await request(app).get('/nexrad/KTLH/20260512-004336/0.5?product=dbzh').expect(400);
  });

  it('returns 404 for a missing valid file', async () => {
    const response = await request(app)
      .get('/nexrad/KTLH/20260512-004336/0.5?product=RHOHV')
      .expect(404);

    expect(response.body.error).toContain('not found');
  });

  it('downloads the requested NEXRAD product file', async () => {
    const response = await request(app)
      .get('/nexrad/KTLH/20260512-004336/0.5?product=DBZH')
      .buffer(true)
      .parse(parseBinary)
      .expect(200);

    expect(response.body.equals(Buffer.from('dbzh-bytes'))).toBe(true);
    expect(response.headers['content-type']).toContain('application/gzip');
    expect(response.headers['content-disposition']).toContain('KTLH_20260512-004336_0.5_DBZH.bin.gz');
    expect(response.headers['cache-control']).toContain('max-age=60');
  });

  it('rejects traversal attempts and never serves files outside the NEXRAD root', async () => {
    const attempts = [
      '/nexrad/%2e%2e',
      '/nexrad/..%2Fsecret',
      '/nexrad/KTLH%2F..%2Fsecret',
      '/nexrad/KTLH%5C..%5Csecret',
      '/nexrad/KTLH/..%2F..%2Fsecret/0.5?product=DBZH',
      '/nexrad/KTLH/20260512-004336%2F..%2Fsecret/0.5?product=DBZH',
      '/nexrad/KTLH/%2e%2e/0.5?product=DBZH',
      '/nexrad/KTLH/20260512-004336/..%2F..%2Fsecret?product=DBZH',
      '/nexrad/KTLH/20260512-004336/0.5%2F..%2Fsecret?product=DBZH',
      '/nexrad/KTLH/20260512-004336/0.5%5C..%5Csecret?product=DBZH',
      '/nexrad/KTLH/20260512-004336/0.5?product=../secret/DBZH',
      '/nexrad/KTLH/20260512-004336/0.5?product=..%2Fsecret%2FDBZH',
      '/nexrad/KTLH/20260512-004336/0.5?product=DBZH.bin.gz',
      '/nexrad/KTLH/20260512-004336/0.5?product=DBZH%00',
    ];

    for (const attempt of attempts) {
      const response = await request(app)
        .get(attempt)
        .buffer(true)
        .parse(parseBinary);

      expect([400, 404]).toContain(response.status);
      expect(Buffer.isBuffer(response.body)).toBe(true);
      expect(response.body.equals(sentinelBytes)).toBe(false);
    }

    const legitResponse = await request(app)
      .get('/nexrad/KTLH/20260512-004336/0.5?product=DBZH')
      .buffer(true)
      .parse(parseBinary)
      .expect(200);

    expect(legitResponse.body.equals(Buffer.from('dbzh-bytes'))).toBe(true);
  });
});

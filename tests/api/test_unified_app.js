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
    await request(app).get('/robots.txt').expect(200).expect('Content-Type', /text\/plain/);
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

  it('serves every v3 resource family from one configured runtime tree', async () => {
    baseDir = await fs.mkdtemp(path.join(os.tmpdir(), 'unified-api-all-'));
    const write = async (relative, contents) => { const target = path.join(baseDir, relative); await fs.mkdir(path.dirname(target), { recursive: true }); await fs.writeFile(target, contents); };
    await write('data/stormcells/stormcell_index.json', '{"timestamps":["20260317-200000"]}');
    await write('data/stormcells/stormcells_20260317-200000.json', '[]');
    await write('data/Alerts/official/timestamps/20260317-200000.json', '{"alerts":[]}');
    await write('data/METAR/METAR_20260317-20z.json', '[]');
    await write('gui/CompRefQC/index.json', '{"timestamps":["20260317-200000"],"tile_grid":{"rows":1,"cols":1,"tile_size":350}}');
    await write('gui/CompRefQC/20260317-200000/index.json', '{"tiles":[[0,0]]}');
    await write('gui/CompRefQC/20260317-200000/tile_0_0.png', 'png');
    await write('gui/NEXRAD/KTLH/0.5/KTLH_DBZH_0.5_20260317-200000.bin.gz', 'gzip');
    await write('gui/RAP/CAPE/index.json', '["20260317-200000"]');
    await write('gui/RAP/CAPE/20260317-200000/metadata.json', '{"units":"J/kg","grid":{"ni":1,"nj":1}}');
    await write('gui/RAP/CAPE/20260317-200000/data.u16', 'u16');
    await write('wpc/surface_analysis/wpc_sfc_20260317-200000.geojson', '{"type":"FeatureCollection","features":[]}');
    const { app } = await createApp({ env: { EDGEWARN_BASE_DIR: baseDir, RATE_LIMIT_MAX_SEC: '0', RATE_LIMIT_MAX_MIN: '0' }, argv: [] });
    const paths = [
      '/api/v3/storm-snapshots', '/api/v3/storm-snapshots/20260317-200000',
      '/api/v3/alert-snapshots?source=official', '/api/v3/observations/metar',
      '/api/v3/render-products', '/api/v3/render-products/comp-ref-qc/snapshots',
      '/api/v3/render-products/comp-ref-qc/snapshots/20260317-200000/tiles',
      '/api/v3/radar-sites', '/api/v3/radar-sites/KTLH/availability',
      '/api/v3/models/rap/layers', '/api/v3/models/rap/layers/CAPE/snapshots',
      '/api/v3/models/rap/layers/CAPE/snapshots/20260317-200000/metadata',
      '/api/v3/models/rap/layer-mappings', '/api/v3/analyses/wpc/surface',
      '/api/v3/analyses/wpc/surface/20260317-200000', '/api/v3/styles/colormaps'
    ];
    for (const endpoint of paths) await request(app).get(endpoint).expect(200);
    await request(app).get('/api/v3/render-products/comp-ref-qc/snapshots/20260317-200000/tiles/0/0').expect(200).expect('Content-Type', /image\/png/);
    await request(app).get('/api/v3/radar-sites/KTLH/scans/20260317-200000/elevations/0.5/products/DBZH').expect(200).expect('Content-Type', /application\/gzip/);
    const rap = await request(app).get('/api/v3/models/rap/layers/CAPE/snapshots/20260317-200000/data').expect(200);
    expect(rap.headers['x-units']).toBe('J/kg');
  });
});

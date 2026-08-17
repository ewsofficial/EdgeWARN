import { afterEach, describe, expect, it, jest } from '@jest/globals';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { ArtifactRepository } from '../../src/api/repositories/artifactRepository.js';
import { createAnalysisService } from '../../src/api/services/analysis.js';
import { createRenderService } from '../../src/api/services/renders.js';
import { createAncillaryServices } from '../../src/api/services/ancillary.js';

const RENDER_DEFAULTS = {
  grid: { rows: 10, cols: 20, tile_size: 350 },
  grid_maxima: { rows: 100, cols: 100, tile_size: 4096 },
};

describe('unified API services', () => {
  let root;
  afterEach(async () => { if (root) await fs.rm(root, { recursive: true, force: true }); });

  it('keeps legacy analysis artifacts behind one service boundary', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'api-services-'));
    await fs.mkdir(path.join(root, 'data', 'cells'), { recursive: true });
    await fs.writeFile(path.join(root, 'data', 'cells', 'cell_index.json'), '{"cellIds":["7"]}');
    await fs.writeFile(path.join(root, 'data', 'cells', '7.json'), '{"id":7}');
    const service = createAnalysisService(new ArtifactRepository({ data: path.join(root, 'data') }));
    await expect(service.listCells()).resolves.toEqual(['7']);
    await expect(service.getCell('7')).resolves.toEqual({ id: 7 });
    await expect(service.getCell('../7')).rejects.toMatchObject({ code: 'INVALID_PATH' });
  });

  it('uses canonical render IDs while preserving storage prefixes', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'api-services-'));
    const product = path.join(root, 'gui', 'CompRefQC');
    await fs.mkdir(product, { recursive: true });
    await fs.writeFile(path.join(product, 'index.json'), '{"timestamps":["20260317-200000"]}');
    await fs.writeFile(path.join(product, 'MRMS_MergedReflectivityQC_20260317-200000.png'), 'png');
    const service = createRenderService(new ArtifactRepository({ gui: path.join(root, 'gui') }), RENDER_DEFAULTS, 1024);
    await expect(service.listSnapshots('comp-ref-qc')).resolves.toEqual(['20260317-200000']);
    const opened = await service.image('comp-ref-qc', '20260317-200000');
    await expect(opened.handle.readFile()).resolves.toEqual(Buffer.from('png'));
    await opened.handle.close();
  });

  it('uses a product tile grid when a timestamp index omits grid metadata', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'api-services-'));
    const product = path.join(root, 'gui', 'CompRefQC');
    await fs.mkdir(path.join(product, '20260317-200000'), { recursive: true });
    await fs.writeFile(path.join(product, 'index.json'), '{"tile_grid":{"rows":2,"cols":3,"tile_size":256}}');
    await fs.writeFile(path.join(product, '20260317-200000', 'index.json'), '{"tiles":[[2,1]]}');
    const service = createRenderService(new ArtifactRepository({ gui: path.join(root, 'gui') }), RENDER_DEFAULTS, 1024);
    await expect(service.tiles('comp-ref-qc', '20260317-200000')).resolves.toEqual({ grid: { rows: 2, cols: 3, tileSize: 256 }, tiles: [[2, 1]] });
  });

  it('opens only indexed RGBA chunks with their exact expected length', async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'api-services-'));
    const product = path.join(root, 'gui', 'CompRefQC'); const timestamp = '20260317-200000';
    await fs.mkdir(path.join(product, timestamp, 'chunks'), { recursive: true });
    const format = { version: 2, encoding: 'float16', file_suffix: '.f16.gz', compression: 'gzip', channels: 1, value_kind: 'scalar', no_data: 'nan', bytes_per_component: 2, pixel_row_order: 'top_to_bottom', grid_origin: 'bottom_left' };
    await fs.writeFile(path.join(product, 'index.json'), JSON.stringify({ schema_version: 2, timestamps: [timestamp], representation: 'binary_chunks', chunk_format: { ...format, media_type: 'application/octet-stream' }, tile_grid: { rows: 1, cols: 1, tile_size: 2 } }));
    await fs.writeFile(path.join(product, timestamp, 'index.json'), JSON.stringify({ schema_version: 2, timestamp, representation: 'binary_chunks', chunk_format: format, tile_grid: { rows: 1, cols: 1, tile_size: 2 }, chunks: [[0, 0]] }));
    await fs.writeFile(path.join(product, timestamp, 'chunks', 'chunk_0_0.f16.gz'), Buffer.from('H4sIAAAAAAAC/2NggAAAad8iZQgAAAA=', 'base64'));
    const service = createRenderService(new ArtifactRepository({ gui: path.join(root, 'gui') }), RENDER_DEFAULTS, 1024);
    await expect(service.chunks('comp-ref-qc', timestamp)).resolves.toMatchObject({ chunks: [[0, 0]], grid: { tileSize: 2 } });
    const opened = await service.chunk('comp-ref-qc', timestamp, 0, 0);
    expect(opened.size).toBe(23); await opened.handle.close();
    await expect(service.chunk('comp-ref-qc', timestamp, 1, 0)).rejects.toMatchObject({ code: 'NOT_FOUND' });
  });

  it('closes a RAP data handle when its metadata is malformed', async () => {
    const close = jest.fn().mockResolvedValue();
    const repository = {
      open: jest.fn().mockResolvedValue({ handle: { close }, size: 1 }),
      readJson: jest.fn().mockRejectedValue(Object.assign(new Error('malformed'), { code: 'IN_PROGRESS' }))
    };
    const service = createAncillaryServices(repository);
    await expect(service.rapData('CAPE', '20260317-200000')).rejects.toMatchObject({ code: 'IN_PROGRESS' });
    expect(close).toHaveBeenCalledTimes(1);
  });
});

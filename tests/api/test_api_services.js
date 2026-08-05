import { afterEach, describe, expect, it } from '@jest/globals';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { ArtifactRepository } from '../../src/api/repositories/artifactRepository.js';
import { createAnalysisService } from '../../src/api/services/analysis.js';
import { createRenderService } from '../../src/api/services/renders.js';

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
    const service = createRenderService(new ArtifactRepository({ gui: path.join(root, 'gui') }));
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
    const service = createRenderService(new ArtifactRepository({ gui: path.join(root, 'gui') }));
    await expect(service.tiles('comp-ref-qc', '20260317-200000')).resolves.toEqual({ grid: { rows: 2, cols: 3, tileSize: 256 }, tiles: [[2, 1]] });
  });
});

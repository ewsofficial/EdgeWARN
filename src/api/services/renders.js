import { ArtifactError } from '../repositories/artifactRepository.js';
import { productById, productCatalog } from '../config/productCatalog.js';
import { timestamp } from './validation.js';

const DEFAULT_GRID = { rows: 10, cols: 20, tileSize: 350 };
const grid = (value) => value && Number.isInteger(value.rows) && Number.isInteger(value.cols) && Number.isInteger(value.tile_size)
  && value.rows > 0 && value.rows <= 100 && value.cols > 0 && value.cols <= 100 && value.tile_size > 0 && value.tile_size <= 4096
  ? { rows: value.rows, cols: value.cols, tileSize: value.tile_size } : null;
const chunkFormat = (value) => value && value.version === 1 && value.encoding === 'rgba8' && value.file_suffix === '.rgba'
  && value.compression === 'none' && value.channels === 4 && value.bytes_per_pixel === 4
  && value.alpha === 'straight' && value.pixel_row_order === 'top_to_bottom' && value.grid_origin === 'bottom_left'
  ? value : null;

function chunkIndex(value) {
  if (!value || value.schema_version !== 2 || value.representation !== 'binary_chunks') throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render chunk index');
  const format = chunkFormat(value.chunk_format); const chunkGrid = grid(value.tile_grid);
  if (!format || !chunkGrid || !Array.isArray(value.chunks) || value.chunks.length > chunkGrid.rows * chunkGrid.cols) throw new ArtifactError('INVALID_ARTIFACT', 'Malformed render chunk index');
  const seen = new Set();
  const chunks = value.chunks.map((chunk) => {
    if (!Array.isArray(chunk) || chunk.length !== 2 || !chunk.every(Number.isInteger) || chunk[0] < 0 || chunk[0] >= chunkGrid.cols || chunk[1] < 0 || chunk[1] >= chunkGrid.rows) throw new ArtifactError('INVALID_ARTIFACT', 'Malformed render chunk coordinates');
    const key = `${chunk[0]},${chunk[1]}`;
    if (seen.has(key)) throw new ArtifactError('INVALID_ARTIFACT', 'Duplicate render chunk coordinates');
    seen.add(key); return chunk;
  });
  return { format, grid: chunkGrid, chunks };
}

export function createRenderService(repository) {
  const product = (id) => {
    const found = productById.get(id);
    if (!found) throw new ArtifactError('NOT_FOUND', 'Render product not found');
    return found;
  };
  const productIndex = async (item) => {
    try { return await repository.readJson('gui', [item.storageDirectory, 'index.json']); } catch (error) { if (error.code === 'NOT_FOUND') return []; throw error; }
  };
  return {
    async listProducts() {
      const result = [];
      for (const item of productCatalog) {
        try { await repository.list('gui', [item.storageDirectory], { limit: 1 }); result.push(item); } catch (error) { if (error.code !== 'NOT_FOUND') throw error; }
      }
      return result;
    },
    async getProduct(id) {
      const item = product(id); const index = await productIndex(item);
      if (Array.isArray(index) || index.schema_version !== 2 || index.representation !== 'binary_chunks') return { ...item, grid: grid(Array.isArray(index) ? null : index.tile_grid) || DEFAULT_GRID };
      const format = chunkFormat({ ...index.chunk_format, bytes_per_pixel: 4 });
      if (!format) throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render chunk format');
      return { ...item, representation: index.representation, chunkFormat: index.chunk_format, grid: grid(index.tile_grid) || DEFAULT_GRID };
    },
    async listSnapshots(id) { const index = await productIndex(product(id)); return Array.isArray(index) ? index : (Array.isArray(index.timestamps) ? index.timestamps : []); },
    async image(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id);
      return repository.open('gui', [item.storageDirectory, `${item.legacyFilePrefix}_${value}.png`], { kind: 'image' });
    },
    async tiles(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id); const productData = await productIndex(item); const productGrid = grid(Array.isArray(productData) ? null : productData.tile_grid) || DEFAULT_GRID;
      const index = await repository.readJson('gui', [item.storageDirectory, value, 'index.json']);
      const tileGrid = grid(Array.isArray(index) ? null : index.tile_grid) || productGrid;
      const tiles = (Array.isArray(index) ? index : index.tiles || []).filter((tile) => Array.isArray(tile) && tile.length === 2 && tile.every(Number.isInteger) && tile[0] >= 0 && tile[0] < tileGrid.cols && tile[1] >= 0 && tile[1] < tileGrid.rows);
      return { grid: tileGrid, tiles };
    },
    async tile(id, value, x, y) {
      const tileData = await this.tiles(id, value);
      if (!Number.isInteger(x) || !Number.isInteger(y) || x < 0 || y < 0 || x >= tileData.grid.cols || y >= tileData.grid.rows) throw new ArtifactError('INVALID_PATH', 'Invalid tile coordinates');
      return repository.open('gui', [product(id).storageDirectory, value, `tile_${x}_${y}.png`], { kind: 'image' });
    },
    async chunks(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id); const data = chunkIndex(await repository.readJson('gui', [item.storageDirectory, value, 'index.json']));
      const productData = await productIndex(item);
      if (!productData || productData.schema_version !== 2 || productData.representation !== 'binary_chunks') throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render product index');
      const productFormat = chunkFormat({ ...productData.chunk_format, bytes_per_pixel: 4 });
      if (!productFormat || productData.chunk_format.version !== data.format.version || productData.chunk_format.file_suffix !== data.format.file_suffix) throw new ArtifactError('INVALID_ARTIFACT', 'Conflicting render chunk metadata');
      return data;
    },
    async chunk(id, value, x, y) {
      const data = await this.chunks(id, value);
      if (!Number.isInteger(x) || !Number.isInteger(y) || !data.chunks.some(([chunkX, chunkY]) => chunkX === x && chunkY === y)) throw new ArtifactError('NOT_FOUND', 'Render chunk not found');
      const opened = await repository.open('gui', [product(id).storageDirectory, value, 'chunks', `chunk_${x}_${y}.rgba`], { kind: 'binary' });
      const expectedLength = data.grid.tileSize * data.grid.tileSize * data.format.bytes_per_pixel;
      if (opened.size !== expectedLength) { await opened.handle.close(); throw new ArtifactError('INVALID_ARTIFACT', 'Render chunk has an invalid length'); }
      return { ...opened, chunk: data };
    }
  };
}

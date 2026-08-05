import { ArtifactError } from '../repositories/artifactRepository.js';
import { productById, productCatalog } from '../config/productCatalog.js';
import { timestamp } from './validation.js';

const DEFAULT_GRID = { rows: 10, cols: 20, tileSize: 350 };
const grid = (value) => value && Number.isInteger(value.rows) && Number.isInteger(value.cols) && Number.isInteger(value.tile_size)
  ? { rows: value.rows, cols: value.cols, tileSize: value.tile_size } : DEFAULT_GRID;

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
    async getProduct(id) { const item = product(id); const index = await productIndex(item); return { ...item, grid: grid(Array.isArray(index) ? null : index.tile_grid) }; },
    async listSnapshots(id) { const index = await productIndex(product(id)); return Array.isArray(index) ? index : (Array.isArray(index.timestamps) ? index.timestamps : []); },
    async image(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id);
      return repository.open('gui', [item.storageDirectory, `${item.legacyFilePrefix}_${value}.png`], { kind: 'image' });
    },
    async tiles(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id); const productData = await productIndex(item); const productGrid = grid(Array.isArray(productData) ? null : productData.tile_grid);
      const index = await repository.readJson('gui', [item.storageDirectory, value, 'index.json']);
      const tileGrid = grid(Array.isArray(index) ? null : index.tile_grid) || productGrid;
      const tiles = (Array.isArray(index) ? index : index.tiles || []).filter((tile) => Array.isArray(tile) && tile.length === 2 && tile.every(Number.isInteger) && tile[0] >= 0 && tile[0] < tileGrid.cols && tile[1] >= 0 && tile[1] < tileGrid.rows);
      return { grid: tileGrid, tiles };
    },
    async tile(id, value, x, y) {
      const tileData = await this.tiles(id, value);
      if (!Number.isInteger(x) || !Number.isInteger(y) || x < 0 || y < 0 || x >= tileData.grid.cols || y >= tileData.grid.rows) throw new ArtifactError('INVALID_PATH', 'Invalid tile coordinates');
      return repository.open('gui', [product(id).storageDirectory, value, `tile_${x}_${y}.png`], { kind: 'image' });
    }
  };
}

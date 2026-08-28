import { ArtifactError } from '../repositories/artifactRepository.js';
import { productById, productCatalog } from '../config/productCatalog.js';
import { timestamp } from './validation.js';

const chunkFormat = (value) => value && value.version === 2 && value.encoding === 'float16' && value.file_suffix === '.f16.gz'
  && value.compression === 'gzip' && [1, 3].includes(value.channels) && ['scalar', 'rgb'].includes(value.value_kind)
  && value.bytes_per_component === 2 && value.no_data === 'nan' && value.pixel_row_order === 'top_to_bottom' && value.grid_origin === 'bottom_left'
  ? value : null;

function renderIndex(value, maxima) {
  if (!value || value.schema_version !== 2 || value.representation !== 'binary_file') throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render file index');
  const format = chunkFormat(value.chunk_format); const shape = value.shape;
  const maxHeight = maxima.rows * maxima.tile_size; const maxWidth = maxima.cols * maxima.tile_size;
  if (!format || value.file !== 'values.f16.gz' || !Array.isArray(shape) || shape.length !== 2 || !shape.every(Number.isInteger) || shape[0] < 1 || shape[1] < 1 || shape[0] > maxHeight || shape[1] > maxWidth) throw new ArtifactError('INVALID_ARTIFACT', 'Malformed render file index');
  return { format, file: value.file, shape: { height: shape[0], width: shape[1] } };
}

export function createRenderService(repository, renderDefaults, chunkLengthSlackBytes) {
  const maxima = renderDefaults.grid_maxima;
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
      if (Array.isArray(index) || index.schema_version !== 2 || index.representation !== 'binary_file') throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render product index');
      const format = chunkFormat(index.chunk_format);
      if (!format) throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render file format');
      return { ...item, representation: index.representation, fileName: 'values.f16.gz', chunkFormat: index.chunk_format };
    },
    async listSnapshots(id) { const index = await productIndex(product(id)); return Array.isArray(index) ? index : (Array.isArray(index.timestamps) ? index.timestamps : []); },
    async data(id, value) {
      if (!timestamp(value)) throw new ArtifactError('INVALID_PATH', 'Invalid timestamp');
      const item = product(id); const data = renderIndex(await repository.readJson('gui', [item.storageDirectory, value, 'index.json']), maxima);
      const productData = await productIndex(item);
      if (!productData || productData.schema_version !== 2 || productData.representation !== 'binary_file') throw new ArtifactError('INVALID_ARTIFACT', 'Unsupported render product index');
      const productFormat = chunkFormat(productData.chunk_format);
      if (!productFormat || productData.chunk_format.version !== data.format.version || productData.chunk_format.file_suffix !== data.format.file_suffix) throw new ArtifactError('INVALID_ARTIFACT', 'Conflicting render file metadata');
      const opened = await repository.open('gui', [item.storageDirectory, value, data.file], { kind: 'binary' });
      const expectedLength = data.shape.height * data.shape.width * data.format.channels * data.format.bytes_per_component;
      if (!opened.size || opened.size > expectedLength + chunkLengthSlackBytes) { await opened.handle.close(); throw new ArtifactError('INVALID_ARTIFACT', 'Render file has an invalid length'); }
      return { ...opened, render: data };
    }
  };
}

import express from 'express';
import { page, timestamp } from '../../services/validation.js';
import { productCatalog } from '../../config/productCatalog.js';

const listOptions = (req) => ({ cursor: typeof req.query.cursor === 'string' ? req.query.cursor : undefined, limit: req.query.limit ? Number(req.query.limit) : undefined });
const COLLECTION_PATHS = new Set(['/cells', '/storm-snapshots', '/alert-snapshots', '/observations/metar', '/render-products', '/radar-sites', '/models/rap/layers', '/analyses/wpc/surface']);

function validateQuery(apiConfig) {
  const limitPattern = new RegExp(apiConfig.query.limit_pattern);
  return (req, res, next) => {
  const isCollection = COLLECTION_PATHS.has(req.path) || /\/render-products\/[^/]+\/snapshots$/.test(req.path) || /\/models\/rap\/layers\/[^/]+\/snapshots$/.test(req.path);
  const allowed = new Set(isCollection ? apiConfig.query.allowed_params : []);
  if (req.path === '/alert-snapshots' || /^\/alert-snapshots\/[^/]+$/.test(req.path) || /^\/alerts\/[^/]+$/.test(req.path)) allowed.add('source');
  for (const [key, value] of Object.entries(req.query)) {
    if (!allowed.has(key) || Array.isArray(value) || typeof value !== 'string' || value.length > apiConfig.query.max_value_length) return res.status(400).type('application/problem+json').json({ type: 'about:blank', title: 'Bad Request', status: 400, detail: `Invalid query parameter: ${key}`, instance: req.originalUrl, requestId: req.requestId });
    if (key === 'limit' && !limitPattern.test(value)) return res.status(400).type('application/problem+json').json({ type: 'about:blank', title: 'Bad Request', status: 400, detail: 'Invalid query parameter: limit', instance: req.originalUrl, requestId: req.requestId });
  }
  next();
  };
}

function methodNotAllowed(openApi) {
  const paths = Object.keys(JSON.parse(openApi).paths).map((route) => {
    const localRoute = route.replace(/^\/api\/v3/, '') || '/';
    return new RegExp(`^${localRoute.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\{[^}]+\\\}/g, '[^/]+')}$`);
  });
  return (req, res, next) => {
    if (paths.some((pattern) => pattern.test(req.path))) return res.set('Allow', 'GET, HEAD').status(405).type('application/problem+json').json({ type: 'about:blank', title: 'Method Not Allowed', status: 405, detail: 'This resource only supports GET and HEAD.', instance: req.originalUrl, requestId: req.requestId });
    return next();
  };
}

export function createV3Router({ analysis, renders, ancillary, openApi, apiConfig }) {
  const collection = (req, res, items) => { const result = page(items, listOptions(req)); res.set('Cache-Control', `public, max-age=${apiConfig.cache_control_max_age.collection}`).json({ data: result.data, meta: { nextCursor: result.nextCursor } }); };
  const resource = (req, res, data) => res.set('Cache-Control', `public, max-age=${apiConfig.cache_control_max_age.resource}`).json({ data, meta: {} });
  const geojson = (req, res, data) => res.set('Cache-Control', `public, max-age=${apiConfig.cache_control_max_age.resource}`).type('application/geo+json').json(data);
  const send = (req, res, opened, type, headers = {}) => { res.set(opened.headers || {}).set(headers).set({ 'Cache-Control': `public, max-age=${apiConfig.cache_control_max_age.asset}, immutable`, ETag: opened.etag }).type(type); if (req.fresh) { opened.handle.close(); return res.status(304).end(); } res.set('Content-Length', String(opened.size)); if (req.method === 'HEAD') { opened.handle.close(); return res.end(); } opened.handle.createReadStream().on('error', () => res.destroy()).pipe(res); };
  const router = express.Router();
  router.use(validateQuery(apiConfig));
  router.get('/', (req, res) => resource(req, res, { version: apiConfig.server.v3_api_version, links: { openapi: '/api/v3/openapi.json', cells: '/api/v3/cells', renderProducts: '/api/v3/render-products' } }));
  router.get('/openapi.json', (req, res) => res.type('application/json').send(openApi));
  router.get('/cells', async (req, res, next) => { try { collection(req, res, await analysis.listCells()); } catch (error) { next(error); } });
  router.get('/cells/:cellId', async (req, res, next) => { try { resource(req, res, await analysis.getCell(req.params.cellId)); } catch (error) { next(error); } });
  router.get('/storm-snapshots', async (req, res, next) => { try { collection(req, res, await analysis.listStormSnapshots()); } catch (error) { next(error); } });
  router.get('/storm-snapshots/:timestamp', async (req, res, next) => { try { resource(req, res, { timestamp: req.params.timestamp, validTime: timestamp(req.params.timestamp), cells: await analysis.getStormSnapshot(req.params.timestamp) }); } catch (error) { next(error); } });
  router.get('/alert-snapshots', async (req, res, next) => { try { collection(req, res, await analysis.listAlertSnapshots(req.query.source)); } catch (error) { next(error); } });
  router.get('/alert-snapshots/:timestamp', async (req, res, next) => { try { resource(req, res, { timestamp: req.params.timestamp, validTime: timestamp(req.params.timestamp), alerts: await analysis.getAlertSnapshot(req.query.source, req.params.timestamp) }); } catch (error) { next(error); } });
  router.get('/alerts/:alertId', async (req, res, next) => { try { resource(req, res, await analysis.getAlert(req.query.source, req.params.alertId)); } catch (error) { next(error); } });
  router.get('/observations/metar', async (req, res, next) => { try { collection(req, res, await analysis.listMetarHours()); } catch (error) { next(error); } });
  router.get('/observations/metar/:timestamp', async (req, res, next) => { try { resource(req, res, await analysis.getMetar(req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/render-products', async (req, res, next) => { try { const available = new Set((await renders.listProducts()).map((item) => item.id)); collection(req, res, await Promise.all(productCatalog.filter((item) => available.has(item.id)).map((item) => renders.getProduct(item.id)))); } catch (error) { next(error); } });
  router.get('/render-products/:productId', async (req, res, next) => { try { resource(req, res, await renders.getProduct(req.params.productId)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots', async (req, res, next) => { try { collection(req, res, await renders.listSnapshots(req.params.productId)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/image', async (req, res, next) => { try { send(req, res, await renders.image(req.params.productId, req.params.timestamp), 'image/png'); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/tiles', async (req, res, next) => { try { resource(req, res, await renders.tiles(req.params.productId, req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/tiles/:x/:y', async (req, res, next) => { try { send(req, res, await renders.tile(req.params.productId, req.params.timestamp, Number(req.params.x), Number(req.params.y)), 'image/png'); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/chunks', async (req, res, next) => { try { resource(req, res, await renders.chunks(req.params.productId, req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/chunks/:x/:y', async (req, res, next) => { try {
    const opened = await renders.chunk(req.params.productId, req.params.timestamp, Number(req.params.x), Number(req.params.y)); const { grid: chunkGrid } = opened.chunk;
    send(req, res, opened, 'application/octet-stream', {
      'X-EWMRS-Format-Version': '2', 'X-Data-Type': 'float16', 'X-Value-Kind': opened.chunk.format.value_kind,
      'X-Channel-Count': String(opened.chunk.format.channels), 'X-No-Data': 'nan', 'Content-Encoding': 'gzip',
      'X-Chunk-Width': String(chunkGrid.tileSize), 'X-Chunk-Height': String(chunkGrid.tileSize),
      'X-Grid-Origin': 'bottom-left', 'X-Pixel-Row-Order': 'top-to-bottom'
    });
  } catch (error) { next(error); } });
  router.get('/radar-sites', async (req, res, next) => { try { collection(req, res, await ancillary.listRadarSites()); } catch (error) { next(error); } });
  router.get('/radar-sites/:siteId/availability', async (req, res, next) => { try { resource(req, res, await ancillary.radarAvailability(req.params.siteId.toUpperCase())); } catch (error) { next(error); } });
  router.get('/radar-sites/:siteId/scans/:timestamp/elevations/:elevation/products/:productId', async (req, res, next) => { try { send(req, res, await ancillary.radarField(req.params.siteId, req.params.timestamp, req.params.elevation, req.params.productId), 'application/gzip'); } catch (error) { next(error); } });
  router.get('/models/rap/layers', async (req, res, next) => { try { collection(req, res, await ancillary.listRapLayers()); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots', async (req, res, next) => { try { collection(req, res, await ancillary.rapSnapshots(req.params.layerId)); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots/:timestamp/metadata', async (req, res, next) => { try { resource(req, res, await ancillary.rapMetadata(req.params.layerId, req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots/:timestamp/data', async (req, res, next) => { try { send(req, res, await ancillary.rapData(req.params.layerId, req.params.timestamp), 'application/octet-stream'); } catch (error) { next(error); } });
  router.get('/models/rap/layer-mappings', async (req, res, next) => { try { resource(req, res, await ancillary.rapMappings()); } catch (error) { next(error); } });
  router.get('/analyses/wpc/surface', async (req, res, next) => { try { collection(req, res, await ancillary.listWpcSurface()); } catch (error) { next(error); } });
  router.get('/analyses/wpc/surface/:timestamp', async (req, res, next) => { try { geojson(req, res, await ancillary.wpcSurface(req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/styles/colormaps', async (req, res, next) => { try { resource(req, res, await ancillary.colormaps()); } catch (error) { next(error); } });
  router.use(methodNotAllowed(openApi));
  return router;
}

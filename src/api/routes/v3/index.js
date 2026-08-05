import express from 'express';
import { page, timestamp } from '../../services/validation.js';
import { productCatalog } from '../../config/productCatalog.js';

const listOptions = (req) => ({ cursor: typeof req.query.cursor === 'string' ? req.query.cursor : undefined, limit: req.query.limit ? Number(req.query.limit) : undefined });
const collection = (req, res, items) => { const result = page(items, listOptions(req)); res.json({ data: result.data, meta: { nextCursor: result.nextCursor, requestId: req.requestId } }); };
const resource = (req, res, data) => res.json({ data, meta: { requestId: req.requestId } });
const send = (res, opened, type) => { res.set(opened.headers || {}).type(type); res.set('Content-Length', String(opened.size)); opened.handle.createReadStream().on('error', () => res.destroy()).pipe(res); };

export function createV3Router({ analysis, renders, ancillary, openApi }) {
  const router = express.Router();
  router.get('/', (req, res) => resource(req, res, { version: '3.0.0', links: { openapi: '/api/v3/openapi.json', cells: '/api/v3/cells', renderProducts: '/api/v3/render-products' } }));
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
  router.get('/render-products', async (req, res, next) => { try { const available = new Set((await renders.listProducts()).map((item) => item.id)); collection(req, res, productCatalog.filter((item) => available.has(item.id))); } catch (error) { next(error); } });
  router.get('/render-products/:productId', async (req, res, next) => { try { resource(req, res, await renders.getProduct(req.params.productId)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots', async (req, res, next) => { try { collection(req, res, await renders.listSnapshots(req.params.productId)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/image', async (req, res, next) => { try { send(res, await renders.image(req.params.productId, req.params.timestamp), 'image/png'); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/tiles', async (req, res, next) => { try { resource(req, res, await renders.tiles(req.params.productId, req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/render-products/:productId/snapshots/:timestamp/tiles/:x/:y', async (req, res, next) => { try { send(res, await renders.tile(req.params.productId, req.params.timestamp, Number(req.params.x), Number(req.params.y)), 'image/png'); } catch (error) { next(error); } });
  router.get('/radar-sites', async (req, res, next) => { try { collection(req, res, await ancillary.listRadarSites()); } catch (error) { next(error); } });
  router.get('/radar-sites/:siteId/availability', async (req, res, next) => { try { resource(req, res, await ancillary.radarAvailability(req.params.siteId.toUpperCase())); } catch (error) { next(error); } });
  router.get('/radar-sites/:siteId/scans/:timestamp/elevations/:elevation/products/:productId', async (req, res, next) => { try { send(res, await ancillary.radarField(req.params.siteId, req.params.timestamp, req.params.elevation, req.params.productId), 'application/gzip'); } catch (error) { next(error); } });
  router.get('/models/rap/layers', async (req, res, next) => { try { collection(req, res, await ancillary.listRapLayers()); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots', async (req, res, next) => { try { collection(req, res, await ancillary.rapSnapshots(req.params.layerId)); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots/:timestamp/metadata', async (req, res, next) => { try { resource(req, res, await ancillary.rapMetadata(req.params.layerId, req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/models/rap/layers/:layerId/snapshots/:timestamp/data', async (req, res, next) => { try { send(res, await ancillary.rapData(req.params.layerId, req.params.timestamp), 'application/octet-stream'); } catch (error) { next(error); } });
  router.get('/models/rap/layer-mappings', async (req, res, next) => { try { resource(req, res, await ancillary.rapMappings()); } catch (error) { next(error); } });
  router.get('/analyses/wpc/surface', async (req, res, next) => { try { collection(req, res, await ancillary.listWpcSurface()); } catch (error) { next(error); } });
  router.get('/analyses/wpc/surface/:timestamp', async (req, res, next) => { try { resource(req, res, await ancillary.wpcSurface(req.params.timestamp)); } catch (error) { next(error); } });
  router.get('/styles/colormaps', async (req, res, next) => { try { resource(req, res, await ancillary.colormaps()); } catch (error) { next(error); } });
  return router;
}

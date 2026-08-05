import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { createConfig } from './config/index.js';
import { createCors } from './middleware/cors.js';
import { errorHandler, notFound } from './middleware/errors.js';
import { createRateLimiters } from './middleware/rateLimit.js';
import { requestId } from './middleware/requestId.js';
import { createAccessLog } from './middleware/logging.js';
import { requestTimeout, securityMiddleware } from './middleware/security.js';
import { ArtifactRepository } from './repositories/artifactRepository.js';
import { createAnalysisService } from './services/analysis.js';
import { createRenderService } from './services/renders.js';
import { createAncillaryServices } from './services/ancillary.js';
import { createV3Router } from './routes/v3/index.js';
import { createCompatibilityRouter } from './routes/compatibility/index.js';

export async function createApp(options = {}) {
  const apiDirectory = path.dirname(fileURLToPath(import.meta.url));
  const sourceDirectory = path.resolve(apiDirectory, '..');
  const openApi = await fs.readFile(path.join(apiDirectory, 'openapi/v3.yaml'), 'utf8');
  const packageManifest = JSON.parse(await fs.readFile(path.join(sourceDirectory, '..', 'package.json'), 'utf8'));
  const config = options.config || createConfig({ ...options, packageVersion: packageManifest.version });
  const routeTemplates = Object.keys(JSON.parse(openApi).paths);
  const repository = new ArtifactRepository({ data: config.dataDir, gui: config.guiDir, wpc: config.wpcDir, static: path.join(sourceDirectory, 'EWMRS') });
  const app = express();
  app.set('trust proxy', config.trustProxy);
  app.use(requestId, createAccessLog(routeTemplates), ...securityMiddleware(), createCors(config.allowedOrigins), ...createRateLimiters(config.rateLimits), requestTimeout(config.requestTimeoutMs));
  app.get('/', (req, res) => res.json({ service: 'EdgeWARN Unified API', version: config.packageVersion, links: { api: '/api/v3', openapi: '/api/v3/openapi.json' } }));
  app.get('/robots.txt', (req, res) => res.type('text/plain').send("# No clankers\nUser-agent: *\nDisallow: /\n"));
  app.get('/health/live', (req, res) => res.json({ status: 'ok', requestId: req.requestId }));
  app.get('/health/ready', async (req, res) => { const checks = await Promise.all([config.dataDir, config.guiDir, config.wpcDir].map(async (dir) => { try { return (await fs.stat(dir)).isDirectory(); } catch { return false; } })); res.status(checks.every(Boolean) ? 200 : 503).json({ status: checks.every(Boolean) ? 'ready' : 'not-ready', requestId: req.requestId }); });
  const analysis = createAnalysisService(repository); const renders = createRenderService(repository); const ancillary = createAncillaryServices(repository);
  app.use('/api/v3', createV3Router({ analysis, renders, ancillary, openApi }));
  app.use(createCompatibilityRouter({ analysis, renders, ancillary, packageVersion: config.packageVersion }));
  app.use(notFound); app.use(errorHandler);
  return { app, config };
}

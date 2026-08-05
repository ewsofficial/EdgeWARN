import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { createConfig } from './config/index.js';
import { createCors } from './middleware/cors.js';
import { errorHandler, notFound } from './middleware/errors.js';
import { createRateLimiters } from './middleware/rateLimit.js';
import { requestId } from './middleware/requestId.js';
import { requestTimeout, securityMiddleware } from './middleware/security.js';
import { ArtifactRepository } from './repositories/artifactRepository.js';
import { createAnalysisService } from './services/analysis.js';
import { createRenderService } from './services/renders.js';
import { createV3Router } from './routes/v3/index.js';

export async function createApp(options = {}) {
  const config = options.config || createConfig(options);
  const openApi = await fs.readFile(path.join(path.dirname(new URL(import.meta.url).pathname), 'openapi/v3.yaml'), 'utf8');
  const repository = new ArtifactRepository({ data: config.dataDir, gui: config.guiDir, wpc: config.wpcDir });
  const app = express();
  app.set('trust proxy', config.trustProxy);
  app.use(requestId, ...securityMiddleware(), createCors(config.allowedOrigins), ...createRateLimiters(config.rateLimits), requestTimeout(config.requestTimeoutMs));
  app.get('/', (req, res) => res.json({ service: 'EdgeWARN Unified API', version: config.packageVersion, links: { api: '/api/v3', openapi: '/api/v3/openapi.json' } }));
  app.get('/health/live', (req, res) => res.json({ status: 'ok', requestId: req.requestId }));
  app.get('/health/ready', async (req, res) => { const checks = await Promise.all([config.dataDir, config.guiDir, config.wpcDir].map(async (dir) => { try { return (await fs.stat(dir)).isDirectory(); } catch { return false; } })); res.status(checks.every(Boolean) ? 200 : 503).json({ status: checks.every(Boolean) ? 'ready' : 'not-ready', requestId: req.requestId }); });
  app.use('/api/v3', createV3Router({ analysis: createAnalysisService(repository), renders: createRenderService(repository), openApi }));
  app.use(notFound); app.use(errorHandler);
  return { app, config };
}

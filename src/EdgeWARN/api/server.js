import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import path from 'path';
import healthRouter from './routes/health.js';
import v2Router from './routes/v2/index.js';
import rateLimit, { ipKeyGenerator } from 'express-rate-limit';
import compression from 'compression';
import cluster from 'cluster';
import os from 'os';
import { pathToFileURL } from 'url';
import helmet from 'helmet';
import config from './config.js';

dotenv.config();

function getPort(env) {
  return env.PORT || (config.DEBUG_SERVER ? config.DEBUG_PORT : config.DEFAULT_PORT);
}

export function createApp(env = process.env, options = {}) {
  const { beforeErrorHandler } = options;
  const app = express();

  // Middleware
  // Security headers (Helmet) - enable HSTS for HTTPS enforcement
  app.use(helmet({
    hsts: {
      maxAge: 31536000, // 1 year in seconds
      includeSubDomains: true
    },
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        "default-src": ["'self'"],
      }
    }
  }));

  // Compression
  app.use(compression());

  // CORS configuration
  // Use ALLOWED_ORIGINS if set, otherwise allow all origins (for development/testing)
  const hasExplicitOrigins = !!env.ALLOWED_ORIGINS;
  const allowedOrigins = hasExplicitOrigins
    ? env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
    : [];

  // Determine origin config: explicit origins > otherwise allow all
  const corsOrigin = hasExplicitOrigins
    ? allowedOrigins
    : (env.NODE_ENV === 'production' ? [] : true);

  if (env.NODE_ENV === 'production' && !hasExplicitOrigins) {
    console.warn('[Security] ALLOWED_ORIGINS not set. CORS requests will be blocked in production.');
  }

  app.use(cors({
    origin: corsOrigin,
    credentials: true,
    methods: ['GET', 'HEAD', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
  }));

  app.use(express.json());

  // Trust proxy configuration
  const trustProxy = env.TRUST_PROXY === 'true' || !!env.TRUST_PROXY_IPS;
  if (env.TRUST_PROXY === 'false') {
    app.set('trust proxy', false);
  } else if (env.TRUST_PROXY_IPS) {
    app.set('trust proxy', env.TRUST_PROXY_IPS.split(','));
  } else {
    app.set('trust proxy', false);
  }

  // Rate Limiting - configurable via environment variables
  const rateLimitWindowMsSec = parseInt(env.RATE_LIMIT_WINDOW_MS_SEC, 10) || 1000; // 1 second default
  const rateLimitMaxSec = parseInt(env.RATE_LIMIT_MAX_SEC, 10) || 40; // 40 requests per second default

  const rateLimitWindowMsMin = parseInt(env.RATE_LIMIT_WINDOW_MS_MIN, 10) || 60 * 1000; // 1 minute default
  const rateLimitMaxMin = parseInt(env.RATE_LIMIT_MAX_MIN, 10) || 2000; // 2000 requests per minute default

  const limiterSec = rateLimit({
    windowMs: rateLimitWindowMsSec,
    max: rateLimitMaxSec,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
    skip: (req) => {
      // Optionally skip rate limiting for health checks from internal monitoring
      return req.path === '/health' && req.headers['x-internal-check'] === 'true';
    },
    keyGenerator: (req) => {
      let clientIp;
      if (trustProxy) {
        clientIp = req.ip;
      } else {
        clientIp = req.connection.remoteAddress || req.socket.remoteAddress ||
          (req.connection.socket ? req.connection.socket.remoteAddress : null);
      }
      return ipKeyGenerator(clientIp);
    }
  });

  const limiterMin = rateLimit({
    windowMs: rateLimitWindowMsMin,
    max: rateLimitMaxMin,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
    skip: (req) => {
      return req.path === '/health' && req.headers['x-internal-check'] === 'true';
    },
    keyGenerator: (req) => {
      let clientIp;
      if (trustProxy) {
        clientIp = req.ip;
      } else {
        clientIp = req.connection.remoteAddress || req.socket.remoteAddress ||
          (req.connection.socket ? req.connection.socket.remoteAddress : null);
      }
      return ipKeyGenerator(clientIp);
    }
  });

  // Apply the rate limiting middleware to all requests (both per-second and per-minute)
  app.use(limiterSec);
  app.use(limiterMin);

  // Routes
  app.get('/', (req, res) => {
    // Only expose detailed version in non-production environments
    const version = env.NODE_ENV === 'production' ? '2.x' : '2.2.0';
    res.json({ message: 'EdgeWARN Backend API', version: version });
  });

  // Mount health route
  app.use('/health', healthRouter);

  // Mount API v2 routes (default API version)
  app.use('/api/v2', v2Router);

  // Redirect old v1 paths to v2
  app.use('/features', (req, res) => {
    res.status(410).json({
      error: 'API v1 has been removed. Please use API v2.',
      documentation: '/api/v2'
    });
  });

  app.use('/data', (req, res) => {
    res.status(410).json({
      error: 'API v1 has been removed. Please use API v2.',
      documentation: '/api/v2'
    });
  });

  // Serve robots.txt
  app.get('/robots.txt', (req, res) => {
    const robotsPath = path.resolve(process.cwd(), 'src/EdgeWARN/api/robots.txt');
    res.sendFile(robotsPath, (err) => {
      if (err) {
        console.error('Error sending robots.txt:', err);
        res.status(404).end();
      }
    });
  });

  if (typeof beforeErrorHandler === 'function') {
    beforeErrorHandler(app);
  }

  // Error handling middleware
  app.use((err, req, res, next) => {
    const isDev = env.NODE_ENV !== 'production';
    // Only log stack traces in development
    console.error(isDev ? err.stack : `Error: ${err.message}`);
    // Only expose error details in development
    res.status(500).json({
        error: isDev ? err.message : 'Internal server error'
    });
  });

  return app;
}

export function startWorkerServer(options = {}) {
  const env = options.env || process.env;
  const port = options.port || getPort(env);
  const host = options.host || '0.0.0.0';
  const app = options.app || createApp(env, options);
  const server = app.listen(port, host, () => {
    console.log(`Worker ${process.pid} started on http://localhost:${port}`);
  });

  return { app, server, port };
}

export function startClusteredServer(options = {}) {
  const env = options.env || process.env;
  const clusterModule = options.clusterModule || cluster;
  const osModule = options.osModule || os;
  const port = options.port || getPort(env);
  const numCPUs = Math.min(osModule.cpus().length, 4);

  if (clusterModule.isPrimary) {
    console.log(`Primary ${process.pid} is running`);

    for (let i = 0; i < numCPUs; i++) {
      clusterModule.fork();
    }

    clusterModule.on('exit', (worker) => {
      console.log(`worker ${worker.process.pid} died`);
      clusterModule.fork();
    });

    return { mode: 'primary', port, numCPUs };
  }

  return {
    mode: 'worker',
    ...startWorkerServer({ ...options, env, port })
  };
}

const entryFileUrl = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;

if (entryFileUrl === import.meta.url) {
  startClusteredServer();
}

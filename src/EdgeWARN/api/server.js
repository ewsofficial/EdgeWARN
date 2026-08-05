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

function parseCliIntegerFlag(args, flagName) {
  for (let i = 0; i < args.length; i++) {
    if (args[i] === flagName && args[i + 1] !== undefined) {
      return parseNonNegativeInteger(args[i + 1], flagName);
    }

    if (args[i].startsWith(`${flagName}=`)) {
      return parseNonNegativeInteger(args[i].slice(flagName.length + 1), flagName);
    }
  }

  return undefined;
}

function parseNonNegativeInteger(value, label) {
  const parsed = Number.parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 0) {
    console.warn(`[RateLimit] Ignoring invalid ${label} value: ${value}`);
    return undefined;
  }

  return parsed;
}

function readConfiguredInteger(value, fallback, label) {
  if (value === undefined) {
    return fallback;
  }

  const parsed = parseNonNegativeInteger(value, label);
  return parsed ?? fallback;
}

function getEdgewarnRateLimitConfig(env, args = process.argv.slice(2)) {
  const cliRateLimitMaxSec = parseCliIntegerFlag(args, '--edgewarn-rate-limit-1s');
  const cliRateLimitMaxMin = parseCliIntegerFlag(args, '--edgewarn-rate-limit-1m');

  return {
    rateLimitWindowMsSec: readConfiguredInteger(env.RATE_LIMIT_WINDOW_MS_SEC, 1000, 'RATE_LIMIT_WINDOW_MS_SEC'),
    rateLimitMaxSec: cliRateLimitMaxSec ?? readConfiguredInteger(env.RATE_LIMIT_MAX_SEC, 40, 'RATE_LIMIT_MAX_SEC'),
    rateLimitWindowMsMin: readConfiguredInteger(env.RATE_LIMIT_WINDOW_MS_MIN, 60 * 1000, 'RATE_LIMIT_WINDOW_MS_MIN'),
    rateLimitMaxMin: cliRateLimitMaxMin ?? readConfiguredInteger(env.RATE_LIMIT_MAX_MIN, 2000, 'RATE_LIMIT_MAX_MIN')
  };
}

function getPort(env) {
  return env.PORT || (config.DEBUG_SERVER ? config.DEBUG_PORT : config.DEFAULT_PORT);
}

export function createApp(env = process.env, options = {}) {
  const { beforeErrorHandler, argv = process.argv.slice(2) } = options;
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

  // Compression — skip already-compressed payloads (PNG/JPEG/etc.) where
  // gzip wastes CPU for ~0% size win.
  app.use(compression({
    filter: (req, res) => {
      const contentType = res.getHeader('Content-Type');
      if (typeof contentType === 'string' && /^image\//i.test(contentType)) {
        return false;
      }
      return compression.filter(req, res);
    }
  }));

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

  // Trust proxy configuration
  const trustProxy = env.TRUST_PROXY === 'true' || !!env.TRUST_PROXY_IPS;
  if (env.TRUST_PROXY === 'false') {
    app.set('trust proxy', false);
  } else if (env.TRUST_PROXY_IPS) {
    app.set('trust proxy', env.TRUST_PROXY_IPS.split(','));
  } else {
    app.set('trust proxy', false);
  }

  // Rate Limiting - configurable via environment variables.
  // Mounted before express.json() so abusive bodies are rejected before
  // we spend CPU parsing them.
  const {
    rateLimitWindowMsSec,
    rateLimitMaxSec,
    rateLimitWindowMsMin,
    rateLimitMaxMin
  } = getEdgewarnRateLimitConfig(env, argv);

  const buildLimiter = (windowMs, max) => rateLimit({
    windowMs,
    max,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
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

  // Apply enabled rate limiting middleware to all requests.
  if (rateLimitMaxSec > 0) {
    app.use(buildLimiter(rateLimitWindowMsSec, rateLimitMaxSec));
  }

  if (rateLimitMaxMin > 0) {
    app.use(buildLimiter(rateLimitWindowMsMin, rateLimitMaxMin));
  }

  // Bounded JSON body parser (no v2 route accepts a JSON body, but defend
  // against unbounded body abuse should one be added).
  app.use(express.json({ limit: '16kb', strict: true, type: 'application/json' }));

  // Routes
  app.get('/', (req, res) => {
    // Only expose detailed version in non-production environments
    const version = env.NODE_ENV === 'production' ? '2.x' : '2.7.0';
    res.json({ message: 'EdgeWARN Backend API', version: version });
  });

  // Mount health route
  app.use('/health', healthRouter);

  // Mount API v2 routes (default API version)
  app.use('/api/v2', v2Router);

  // Redirect old v1 paths to v2
  app.use(['/features', '/data', '/api/v1'], (req, res) => {
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
    const status = Number.isInteger(err?.status)
      ? err.status
      : (Number.isInteger(err?.statusCode) ? err.statusCode : 500);
    const responseStatus = status >= 400 && status < 600 ? status : 500;

    // Only log stack traces in development
    console.error(isDev ? err.stack : `Error: ${err.message}`);

    if (responseStatus === 413) {
      return res.status(413).json({ error: 'Payload too large' });
    }

    if (responseStatus >= 400 && responseStatus < 500) {
      return res.status(responseStatus).json({
        error: isDev ? err.message : 'Bad request'
      });
    }

    res.status(500).json({ error: 'Internal server error' });
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
  // Each worker maintains its own in-memory rate-limit store.  The effective
  // service-wide limit is `numWorkers * perWorkerLimit`.  With the default
  // 4 workers, defaults of 40 req/s and 2000 req/min become approximately
  // 160 req/s and 8000 req/min.  Use a shared external store (e.g. Redis) or
  // adjust per-worker limits if exact global limits are required.
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
  console.warn('[Deprecation] src/EdgeWARN/api/server.js now launches the unified API service. Use npm run api.');
  import('../../api/server.js').then(({ startServer }) => startServer()).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

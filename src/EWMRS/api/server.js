import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import cors from 'cors';
import morgan from 'morgan';
import helmet from 'helmet';
import compression from 'compression';
import rateLimit from 'express-rate-limit';
import os from 'os';
import { pathToFileURL } from 'url';
import rendersRouter from './routes/renders.js';
import wpcRouter from './routes/wpc.js';
import colormapsRouter from './routes/colormaps.js';
import rapRouter from './routes/rap.js';
import nexradRouter from './routes/nexrad/index.js';

const DEFAULT_PORT = 3003;
const DEBUG_PORT = 3004;

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

// Parse --base_dir from command line arguments
function getBaseDirFromArgs(args = process.argv.slice(2)) {
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--base_dir' && args[i + 1]) {
      return args[i + 1];
    }
    if (args[i].startsWith('--base_dir=')) {
      return args[i].split('=')[1];
    }
  }
  return null;
}

function hasDebugServerFlag() {
  const args = process.argv.slice(2);
  return args.includes('--debug-server') || args.includes('--debug_server');
}

function getEwmrsRateLimitConfig(env, args = process.argv.slice(2)) {
  const cliRateLimitMaxSec = parseCliIntegerFlag(args, '--ewmrs-rate-limit-1s');
  const cliRateLimitMaxMin = parseCliIntegerFlag(args, '--ewmrs-rate-limit-1m');

  return {
    rateLimitWindowMsSec: 1000,
    rateLimitMaxSec: cliRateLimitMaxSec ?? readConfiguredInteger(env.EWMRS_RATE_LIMIT_MAX_SEC, 30, 'EWMRS_RATE_LIMIT_MAX_SEC'),
    rateLimitWindowMsMin: 60 * 1000,
    rateLimitMaxMin: cliRateLimitMaxMin ?? readConfiguredInteger(env.EWMRS_RATE_LIMIT_MAX_MIN, 1800, 'EWMRS_RATE_LIMIT_MAX_MIN')
  };
}

// Determine BASE_DIR with parity to Python `util/file.py` behaviour:
// Priority order:
// 1. --base_dir command-line argument
// 2. BASE_DIR environment variable
// 3. Platform-specific defaults
function resolveBaseDir(env = process.env, args = process.argv.slice(2)) {
  const argBase = getBaseDirFromArgs(args);
  const envBase = env.BASE_DIR;

  if (argBase) {
    return argBase;
  }

  if (envBase) {
    return envBase;
  }

  if (process.platform === 'win32') {
    return 'C:\\EdgeWARN_input';
  }

  return path.join(os.homedir(), 'EdgeWARN_input');
}

// Known GUI subdirectories (keeps parity with util/file.py)
const GUI_SUBDIRS = [
  'RALA',
  'NLDN',
  'EchoTop18',
  'EchoTop30',
  'QPE_01H',
  'PrecipRate',
  'VIL',
  'ProbSevere',
  'FLASH',
  'VILDensity',
  'VII',
  'RotationTrack30min',
  'CompRefQC',
  'RhoHV',
  'PrecipFlag',
  'AzShearLow',
  'AzShearMid',
  'GOES_ABI_C01',
  'GOES_ABI_C02',
  'GOES_ABI_C03',
  'GOES_ABI_C04',
  'GOES_ABI_C05',
  'GOES_ABI_C06',
  'GOES_ABI_C07',
  'GOES_ABI_C08',
  'GOES_ABI_C09',
  'GOES_ABI_C10',
  'GOES_ABI_C11',
  'GOES_ABI_C12',
  'GOES_ABI_C13',
  'GOES_ABI_C14',
  'GOES_ABI_C15',
  'GOES_ABI_C16',
  'maps'
];

async function listFilesInDir(dirPath, limit = 50) {
  try {
    const entries = await fs.readdir(dirPath, { withFileTypes: true });
    const files = [];
    for (const ent of entries) {
      if (ent.isFile() && path.extname(ent.name).toLowerCase() !== '.idx') {
        const full = path.join(dirPath, ent.name);
        const stat = await fs.stat(full);
        files.push({ name: ent.name, mtime: stat.mtimeMs, size: stat.size });
      }
    }
    files.sort((a, b) => b.mtime - a.mtime); // newest first
    return files.slice(0, limit);
  } catch (err) {
    // If directory doesn't exist or can't be read, return null so caller can note it
    return null;
  }
}

export function createApp(options = {}) {
  const env = options.env || process.env;
  const args = options.argv || process.argv.slice(2);
  const baseDir = options.baseDir || resolveBaseDir(env, args);
  const guiDir = path.join(baseDir, 'gui');
  const {
    rateLimitWindowMsSec,
    rateLimitMaxSec,
    rateLimitWindowMsMin,
    rateLimitMaxMin
  } = getEwmrsRateLimitConfig(env, args);

  const app = express();
  app.use(cors());
  app.use(morgan('tiny'));
  app.use(helmet());
  app.use(compression({
    filter: (req, res) => {
      const contentType = res.getHeader('Content-Type');
      if (typeof contentType === 'string' && /^image\//i.test(contentType)) {
        return false;
      }
      return compression.filter(req, res);
    }
  }));

  const buildLimiter = (windowMs, max) => rateLimit({
    windowMs,
    max,
    standardHeaders: true,
    legacyHeaders: false,
  });

  if (rateLimitMaxSec > 0) {
    app.use(buildLimiter(rateLimitWindowMsSec, rateLimitMaxSec));
  }

  if (rateLimitMaxMin > 0) {
    app.use(buildLimiter(rateLimitWindowMsMin, rateLimitMaxMin));
  }

  // Use new RESTful routes - pass BASE_DIR via app.locals
  app.locals.BASE_DIR = baseDir;
  app.locals.GUI_DIR = guiDir;

  app.use('/renders', rendersRouter);
  app.use('/nexrad', nexradRouter);
  app.use('/rap', rapRouter);
  app.use('/wpc', wpcRouter);

  // Root endpoint to avoid default express 404 "Cannot GET /"
  app.get('/', (req, res) => {
    res.json({
      service: 'EWMRS API',
      endpoints: ['/renders/get-items', '/renders/fetch', '/renders/download', '/nexrad', '/rap/layers', '/rap/fetch', '/rap/metadata', '/rap/data', '/healthz', '/colormaps']
    });
  });

  // Simple healthcheck
  app.get('/healthz', (req, res) => res.json({ ok: true }));

  // Return colormaps.json
  app.use('/colormaps', colormapsRouter);

  return { app, baseDir, guiDir };
}

export function startServer(options = {}) {
  const env = options.env || process.env;
  const port = options.port || env.PORT || (hasDebugServerFlag() ? DEBUG_PORT : DEFAULT_PORT);
  const { app, baseDir } = options.app ? { app: options.app, baseDir: options.baseDir || resolveBaseDir(env) } : createApp(options);
  const server = app.listen(port, () => {
    console.log(`EWMRS API server listening on port ${port}`);
    console.log(`Using BASE_DIR=${baseDir}`);
  });

  return { app, server, port, baseDir };
}

const entryFileUrl = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;

if (entryFileUrl === import.meta.url) {
  console.warn('[Deprecation] src/EWMRS/api/server.js now launches the unified API service. Use npm run api.');
  import('../../api/server.js').then(({ startServer: startUnifiedServer }) => startUnifiedServer()).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import cors from 'cors';
import morgan from 'morgan';
import helmet from 'helmet';
import compression from 'compression';
import rateLimit from 'express-rate-limit';
import os from 'os';
import rendersRouter from './routes/renders.js';
import wpcRouter from './routes/wpc.js';
import colormapsRouter from './routes/colormaps.js';

const app = express();
app.use(cors());
app.use(morgan('tiny'));
app.use(helmet());
app.use(compression());

// Rate Limiting
const limiter = rateLimit({
  // windowMs is in milliseconds. Use 1000 ms for a 1-second window.
  windowMs: 1000, // 1 sec
  max: 30, // Limit each IP to 30 requests per `window` (here, per 1 sec)
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
});
app.use(limiter);

// Parse --base_dir from command line arguments
function getBaseDirFromArgs() {
  const args = process.argv.slice(2);
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

// Determine BASE_DIR with parity to Python `util/file.py` behaviour:
// Priority order:
// 1. --base_dir command-line argument
// 2. BASE_DIR environment variable
// 3. Platform-specific defaults
const argBase = getBaseDirFromArgs();
const envBase = process.env.BASE_DIR;
let BASE_DIR;

if (argBase) {
  BASE_DIR = argBase;
} else if (envBase) {
  BASE_DIR = envBase;
} else if (process.platform === 'win32') {
  BASE_DIR = 'C:\\EdgeWARN_input';
} else {
  BASE_DIR = path.join(os.homedir(), 'EdgeWARN_input');
}

const GUI_DIR = path.join(BASE_DIR, 'gui');

// Known GUI subdirectories (keeps parity with util/file.py)
const GUI_SUBDIRS = [
  'RALA',
  'NLDN',
  'EchoTop18',
  'EchoTop30',
  'QPE_01H',
  'PrecipRate',
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

// Use new RESTful routes - pass BASE_DIR via app.locals
app.locals.BASE_DIR = BASE_DIR;
app.locals.GUI_DIR = GUI_DIR;

app.use('/renders', rendersRouter);
app.use('/wpc', wpcRouter);

// Root endpoint to avoid default express 404 "Cannot GET /"
app.get('/', (req, res) => {
  res.json({
    service: 'EWMRS API',
    base_dir: BASE_DIR,
    gui_dir: GUI_DIR,
    endpoints: ['/renders/get-items', '/renders/fetch', '/renders/download', '/healthz', '/colormaps']
  });
});

// Simple healthcheck
app.get('/healthz', (req, res) => res.json({ ok: true }));

// Return colormaps.json
app.use('/colormaps', colormapsRouter);

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
  console.log(`EWMRS API server listening on port ${PORT}`);
  console.log(`Using BASE_DIR=${BASE_DIR}`);
});

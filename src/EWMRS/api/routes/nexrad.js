import express from 'express';
import fs from 'fs/promises';
import path from 'path';

const router = express.Router();

const TIMESTAMP_PATTERN = /^\d{8}-\d{6}$/;
const SWEEP_PATTERN = /^\d{2}$/;
const SITE_PATTERN = /^[A-Za-z0-9_-]+$/;
const NEXRAD_VARIABLES = ['CCORH', 'DBZH', 'PHIDP', 'RHOHV', 'VRADH', 'WRADH', 'ZDR'];
const NEXRAD_VARIABLE_SET = new Set(NEXRAD_VARIABLES);
const FILE_MAP = {
  azimuths: 'azimuths.f32',
  ranges: 'ranges.f32',
  data: 'data.f16.gz',
};

function getNexradRoot(req) {
  return path.resolve(req.app.locals.GUI_DIR, 'NEXRAD');
}

function normalizeVariable(value) {
  if (typeof value !== 'string') {
    return null;
  }
  const variable = value.toUpperCase();
  return NEXRAD_VARIABLE_SET.has(variable) ? variable : null;
}

router.get('/variables', async (_req, res) => res.json(NEXRAD_VARIABLES));

function normalizeSite(value) {
  if (typeof value !== 'string' || !SITE_PATTERN.test(value)) {
    return null;
  }
  if (value.includes('..') || value.includes('/') || value.includes('\\')) {
    return null;
  }
  return value.toUpperCase();
}

function normalizeTimestamp(value) {
  if (typeof value !== 'string' || !TIMESTAMP_PATTERN.test(value)) {
    return null;
  }
  if (value.includes('/') || value.includes('\\')) {
    return null;
  }
  return value;
}

function normalizeSweep(value) {
  if (value === undefined) {
    return null;
  }
  if (typeof value !== 'string' || !SWEEP_PATTERN.test(value)) {
    return undefined;
  }
  return value;
}

function normalizeFileSelector(value) {
  if (typeof value !== 'string') {
    return null;
  }
  return Object.prototype.hasOwnProperty.call(FILE_MAP, value) ? value : null;
}

function resolveUnder(root, ...segments) {
  const resolved = path.resolve(root, ...segments);
  const relative = path.relative(root, resolved);
  if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    return resolved;
  }
  throw Object.assign(new Error('Resolved path escapes NEXRAD root'), { statusCode: 400 });
}

async function directoryExists(dirPath) {
  try {
    const stat = await fs.stat(dirPath);
    return stat.isDirectory();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return false;
    }
    throw err;
  }
}

async function fileExists(filePath) {
  try {
    const stat = await fs.stat(filePath);
    return stat.isFile();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return false;
    }
    throw err;
  }
}

function variableSweepPattern(variable) {
  return new RegExp(`^NEXRAD_${variable}_SWEEP_(\\d{2})$`);
}

async function getSweepDirs(nexradRoot, variable, sweep) {
  if (sweep) {
    const dirname = `NEXRAD_${variable}_SWEEP_${sweep}`;
    const fullPath = resolveUnder(nexradRoot, dirname);
    if (!await directoryExists(fullPath)) {
      return [];
    }
    return [{ dirname, fullPath, sweep }];
  }

  const entries = await fs.readdir(nexradRoot, { withFileTypes: true });
  const pattern = variableSweepPattern(variable);
  const matches = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const match = entry.name.match(pattern);
      if (!match) {
        return null;
      }
      return {
        dirname: entry.name,
        fullPath: resolveUnder(nexradRoot, entry.name),
        sweep: match[1],
      };
    })
    .filter((entry) => entry !== null)
    .sort((left, right) => Number.parseInt(right.sweep, 10) - Number.parseInt(left.sweep, 10));

  return matches;
}

router.get('/sites', async (req, res) => {
  const variable = normalizeVariable(req.query.variable);
  if (!variable) {
    return res.status(400).json({ error: 'Invalid variable parameter' });
  }

  const sweep = normalizeSweep(req.query.sweep);
  if (sweep === undefined) {
    return res.status(400).json({ error: 'Invalid sweep parameter' });
  }

  const nexradRoot = getNexradRoot(req);
  try {
    const sweepDirs = await getSweepDirs(nexradRoot, variable, sweep);
    if (!sweepDirs.length) {
      return res.json([]);
    }

    const sites = new Set();
    for (const sweepDir of sweepDirs) {
      const entries = await fs.readdir(sweepDir.fullPath, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) {
          continue;
        }
        const site = normalizeSite(entry.name);
        if (site) {
          sites.add(site);
        }
      }
    }

    return res.json(Array.from(sites).sort());
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.json([]);
    }
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error listing NEXRAD sites for ${variable}:`, err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/timestamps', async (req, res) => {
  const variable = normalizeVariable(req.query.variable);
  if (!variable) {
    return res.status(400).json({ error: 'Invalid variable parameter' });
  }

  const site = normalizeSite(req.query.site);
  if (!site) {
    return res.status(400).json({ error: 'Invalid site parameter' });
  }

  const sweep = normalizeSweep(req.query.sweep);
  if (sweep === undefined) {
    return res.status(400).json({ error: 'Invalid sweep parameter' });
  }

  const nexradRoot = getNexradRoot(req);
  try {
    const sweepDirs = await getSweepDirs(nexradRoot, variable, sweep);
    if (!sweepDirs.length) {
      return res.json([]);
    }

    const timestamps = new Set();
    for (const sweepDir of sweepDirs) {
      const siteDir = resolveUnder(nexradRoot, sweepDir.dirname, site);
      if (!await directoryExists(siteDir)) {
        continue;
      }

      const entries = await fs.readdir(siteDir, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory() || !TIMESTAMP_PATTERN.test(entry.name)) {
          continue;
        }
        timestamps.add(entry.name);
      }
    }

    return res.json(Array.from(timestamps).sort((left, right) => right.localeCompare(left)));
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.json([]);
    }
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error listing NEXRAD timestamps for ${variable}/${site}:`, err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/download', async (req, res) => {
  const variable = normalizeVariable(req.query.variable);
  if (!variable) {
    return res.status(400).json({ error: 'Invalid variable parameter' });
  }

  const site = normalizeSite(req.query.site);
  if (!site) {
    return res.status(400).json({ error: 'Invalid site parameter' });
  }

  const timestamp = normalizeTimestamp(req.query.timestamp);
  if (!timestamp) {
    return res.status(400).json({ error: 'Invalid timestamp parameter' });
  }

  const fileSelector = normalizeFileSelector(req.query.file);
  if (!fileSelector) {
    return res.status(400).json({ error: 'Invalid file parameter' });
  }

  const sweep = normalizeSweep(req.query.sweep);
  if (sweep === undefined) {
    return res.status(400).json({ error: 'Invalid sweep parameter' });
  }

  const nexradRoot = getNexradRoot(req);
  try {
    const sweepDirs = await getSweepDirs(nexradRoot, variable, sweep);
    if (!sweepDirs.length) {
      return res.status(404).json({ error: 'NEXRAD layer not found' });
    }

    const expectedFile = FILE_MAP[fileSelector];
    let targetPath = null;
    let resolvedSweep = null;
    for (const sweepDir of sweepDirs) {
      const candidate = resolveUnder(nexradRoot, sweepDir.dirname, site, timestamp, expectedFile);
      if (await fileExists(candidate)) {
        targetPath = candidate;
        resolvedSweep = sweepDir.sweep;
        break;
      }
    }

    if (!targetPath) {
      return res.status(404).json({ error: 'NEXRAD file not found' });
    }

    res.set({
      'Content-Type': 'application/octet-stream',
      'Content-Disposition': `inline; filename="NEXRAD_${variable}_${site}_${timestamp}_SWEEP_${resolvedSweep}_${expectedFile}"`,
      ...(fileSelector === 'data' ? { 'Content-Encoding': 'gzip' } : {}),
    });
    return res.sendFile(targetPath);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(404).json({ error: 'NEXRAD layer not found' });
    }
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error serving NEXRAD data for ${variable}/${site}/${timestamp}:`, err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

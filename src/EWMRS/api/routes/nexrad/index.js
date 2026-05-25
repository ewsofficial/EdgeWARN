import express from 'express';

import {
  directoryExists,
  fileExists,
  getNexradRoot,
  listSiteElevationTimestamps,
  listSafeDirectories,
  resolveNexradProductFile,
  resolveUnder,
  siteHasAnyData,
} from './filesystem.js';
import {
  isAllowedNexradProduct,
  isSafeNexradElevation,
  isSafeNexradSite,
  isSafeNexradTimestamp,
  normalizeNexradSite,
} from './validation.js';

const router = express.Router();

function sendValidationError(res, message) {
  return res.status(400).json({ error: message });
}

router.get('/', async (req, res) => {
  const nexradRoot = getNexradRoot(req);

  try {
    const candidateSites = await listSafeDirectories(nexradRoot, isSafeNexradSite);
    const sites = [];

    for (const site of candidateSites) {
      const siteDir = resolveUnder(nexradRoot, site);
      if (await siteHasAnyData(siteDir, site)) {
        sites.push(site);
      }
    }

    res.set('Cache-Control', 'public, max-age=5');
    res.json(sites);
  } catch (err) {
    if (err.code === 'ENOENT') {
      res.set('Cache-Control', 'public, max-age=5');
      return res.json([]);
    }
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error('Error listing NEXRAD sites:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/:site/', async (req, res) => {
  const site = normalizeNexradSite(req.params.site);
  if (!isSafeNexradSite(site)) {
    return sendValidationError(res, 'Invalid NEXRAD site parameter');
  }

  const nexradRoot = getNexradRoot(req);

  try {
    const siteDir = resolveUnder(nexradRoot, site);
    if (!await directoryExists(siteDir)) {
      return res.status(404).json({ error: 'NEXRAD site not found', site });
    }

    const payload = await listSiteElevationTimestamps(siteDir, site);

    res.set('Cache-Control', 'public, max-age=5');
    res.json(payload);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(404).json({ error: 'NEXRAD site not found', site });
    }
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error listing NEXRAD timestamps for ${site}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/:site/:timestamp/:elevation', async (req, res) => {
  const site = normalizeNexradSite(req.params.site);
  const { timestamp, elevation } = req.params;
  const { product } = req.query;

  if (!isSafeNexradSite(site)) {
    return sendValidationError(res, 'Invalid NEXRAD site parameter');
  }
  if (!isSafeNexradTimestamp(timestamp)) {
    return sendValidationError(res, 'Invalid NEXRAD timestamp parameter. Format: YYYYMMDD-HHMMSS');
  }
  if (!isSafeNexradElevation(elevation)) {
    return sendValidationError(res, 'Invalid NEXRAD elevation parameter');
  }
  if (!isAllowedNexradProduct(product)) {
    return sendValidationError(res, 'Invalid NEXRAD product parameter');
  }

  const nexradRoot = getNexradRoot(req);

  try {
    const filePath = resolveNexradProductFile(nexradRoot, site, timestamp, elevation, product);
    if (!await fileExists(filePath)) {
      return res.status(404).json({ error: 'NEXRAD product file not found', site, timestamp, elevation, product });
    }

    res.set({
      'Cache-Control': 'public, max-age=60',
      'Content-Type': 'application/gzip',
      'Content-Disposition': `attachment; filename="${site}_${timestamp}_${elevation}_${product}.bin.gz"`
    });
    res.sendFile(filePath);
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error serving NEXRAD file for ${site}/${timestamp}/${elevation}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

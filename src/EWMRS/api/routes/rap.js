import express from 'express';
import path from 'path';
import fs from 'fs/promises';

const router = express.Router();

const LAYER_PATTERN = /^[A-Za-z0-9_.-]+$/;
const TIMESTAMP_PATTERN = /^\d{8}-\d{6}$/;

function getRapRoot(req) {
  return path.resolve(req.app.locals.GUI_DIR, 'RAP');
}

function isSafeLayer(layer) {
  return typeof layer === 'string' && LAYER_PATTERN.test(layer) &&
    !layer.includes('..') && !layer.includes('/') && !layer.includes('\\');
}

function isSafeTimestamp(timestamp) {
  return typeof timestamp === 'string' && TIMESTAMP_PATTERN.test(timestamp) &&
    !timestamp.includes('/') && !timestamp.includes('\\');
}

function resolveUnder(root, ...segments) {
  const resolved = path.resolve(root, ...segments);
  const relative = path.relative(root, resolved);

  if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    return resolved;
  }

  throw Object.assign(new Error('Resolved path escapes RAP root'), { statusCode: 400 });
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

async function loadJson(filePath) {
  const data = await fs.readFile(filePath, 'utf8');
  return JSON.parse(data);
}

async function loadIndex(layerDir) {
  const indexPath = path.join(layerDir, 'index.json');

  try {
    const indexData = await loadJson(indexPath);
    return Array.isArray(indexData) ? indexData : (indexData.timestamps || []);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return [];
    }
    throw err;
  }
}

async function loadOptionalMetadata(timestampDir) {
  try {
    return await loadJson(path.join(timestampDir, 'metadata.json'));
  } catch (err) {
    if (err.code === 'ENOENT') {
      return null;
    }
    throw err;
  }
}

function setDecodeHeaders(res, metadata) {
  res.set({
    'Content-Type': 'application/octet-stream',
    'X-Data-Type': 'uint16',
    'X-Byte-Order': 'little_endian',
    'X-Missing-Value': '65535',
  });

  if (!metadata || typeof metadata !== 'object') {
    return;
  }

  const grid = metadata.grid || {};
  if (grid.ni !== undefined) {
    res.set('X-Grid-Ni', String(grid.ni));
  } else if (Array.isArray(metadata.shape) && metadata.shape.length >= 2) {
    res.set('X-Grid-Ni', String(metadata.shape[1]));
  }

  if (grid.nj !== undefined) {
    res.set('X-Grid-Nj', String(grid.nj));
  } else if (Array.isArray(metadata.shape) && metadata.shape.length >= 1) {
    res.set('X-Grid-Nj', String(metadata.shape[0]));
  }

  if (metadata.scale && typeof metadata.scale === 'object') {
    if (metadata.scale.min !== undefined) {
      res.set('X-Scale-Min', String(metadata.scale.min));
    }
    if (metadata.scale.max !== undefined) {
      res.set('X-Scale-Max', String(metadata.scale.max));
    }
  }

  if (metadata.units !== undefined && metadata.units !== null) {
    res.set('X-Units', String(metadata.units));
  }
}

function validateLayerParam(req, res) {
  const { layer } = req.query;
  if (!layer) {
    res.status(400).json({ error: 'Missing layer parameter' });
    return null;
  }
  if (!isSafeLayer(layer)) {
    res.status(400).json({ error: 'Invalid layer parameter' });
    return null;
  }
  return layer;
}

function validateTimestampParam(req, res) {
  const { timestamp } = req.query;
  if (!timestamp) {
    res.status(400).json({ error: 'Missing timestamp parameter' });
    return null;
  }
  if (!isSafeTimestamp(timestamp)) {
    res.status(400).json({ error: 'Invalid timestamp parameter' });
    return null;
  }
  return timestamp;
}

router.get('/layers', async (req, res) => {
  const rapRoot = getRapRoot(req);

  try {
    const entries = await fs.readdir(rapRoot, { withFileTypes: true });
    const layers = [];

    for (const entry of entries) {
      if (!entry.isDirectory() || !isSafeLayer(entry.name)) {
        continue;
      }

      const indexPath = resolveUnder(rapRoot, entry.name, 'index.json');
      if (await fileExists(indexPath)) {
        layers.push(entry.name);
      }
    }

    res.json(layers.sort());
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.json([]);
    }
    console.error('Error listing RAP layers:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/mappings', async (req, res) => {
  const mappingsPath = path.join(path.dirname(new URL(import.meta.url).pathname), '..', '..', 'mappings.json');

  try {
    const data = await fs.readFile(mappingsPath, 'utf-8');
    res.json(JSON.parse(data));
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(404).json({ error: 'mappings.json not found' });
    }
    console.error('Error reading mappings.json:', err);
    res.status(500).json({ error: 'Failed to read colormap mappings' });
  }
});

router.get('/fetch', async (req, res) => {
  const layer = validateLayerParam(req, res);
  if (!layer) {
    return;
  }

  const rapRoot = getRapRoot(req);

  try {
    const layerDir = resolveUnder(rapRoot, layer);
    if (!await directoryExists(layerDir)) {
      return res.status(404).json({ error: 'Layer not found' });
    }

    res.json(await loadIndex(layerDir));
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error reading RAP index for ${layer}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/metadata', async (req, res) => {
  const layer = validateLayerParam(req, res);
  if (!layer) {
    return;
  }

  const timestamp = validateTimestampParam(req, res);
  if (!timestamp) {
    return;
  }

  const rapRoot = getRapRoot(req);

  try {
    const layerDir = resolveUnder(rapRoot, layer);
    if (!await directoryExists(layerDir)) {
      return res.status(404).json({ error: 'Layer not found' });
    }

    const timestampDir = resolveUnder(rapRoot, layer, timestamp);
    if (!await directoryExists(timestampDir)) {
      return res.status(404).json({ error: 'Timestamp not found' });
    }

    const metadataPath = resolveUnder(rapRoot, layer, timestamp, 'metadata.json');
    try {
      res.json(await loadJson(metadataPath));
    } catch (err) {
      if (err.code === 'ENOENT') {
        return res.status(404).json({ error: 'Metadata not found' });
      }
      throw err;
    }
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error reading RAP metadata for ${layer}/${timestamp}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

router.get('/data', async (req, res) => {
  const layer = validateLayerParam(req, res);
  if (!layer) {
    return;
  }

  const timestamp = validateTimestampParam(req, res);
  if (!timestamp) {
    return;
  }

  const rapRoot = getRapRoot(req);

  try {
    const layerDir = resolveUnder(rapRoot, layer);
    if (!await directoryExists(layerDir)) {
      return res.status(404).json({ error: 'Layer not found' });
    }

    const timestampDir = resolveUnder(rapRoot, layer, timestamp);
    if (!await directoryExists(timestampDir)) {
      return res.status(404).json({ error: 'Timestamp not found' });
    }

    const dataPath = resolveUnder(rapRoot, layer, timestamp, 'data.u16');
    if (!await fileExists(dataPath)) {
      return res.status(404).json({ error: 'Data file not found' });
    }

    const metadata = await loadOptionalMetadata(timestampDir);
    setDecodeHeaders(res, metadata);
    res.set('Content-Disposition', `inline; filename="${layer}_${timestamp}.u16"`);
    res.sendFile(dataPath);
  } catch (err) {
    if (err.statusCode) {
      return res.status(err.statusCode).json({ error: err.message });
    }
    console.error(`Error serving RAP data for ${layer}/${timestamp}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

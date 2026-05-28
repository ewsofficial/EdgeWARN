import express from 'express';
const router = express.Router();
import path from 'path';
import fs from 'fs/promises';

const DEFAULT_TILE_GRID = { rows: 10, cols: 20, tile_size: 350 };

// Mapping: User/Folder Product Name -> File Prefix
// Derived from EWMRS/render/config.py
const PRODUCT_MAPPING = {
  'CompRefQC': 'MRMS_MergedReflectivityQC',
  'EchoTop18': 'MRMS_EchoTop18',
  'EchoTop30': 'MRMS_EchoTop30',
  'RALA': 'MRMS_ReflectivityAtLowestAltitude',
  'Ref0C': 'MRMS_ReflectivityAt0C',
  'RefM5C': 'MRMS_ReflectivityAtM5C',
  'RefM15C': 'MRMS_ReflectivityAtM15C',
  'PrecipRate': 'MRMS_PrecipRate',
  'VIL': 'MRMS_VIL',
  'VILDensity': 'MRMS_VILDensity',
  'MESH': 'MRMS_MESH',
  'QPE_01H': 'MRMS_QPE',
  'VII': 'MRMS_VII',
  'AzShearLow': 'MRMS_MergedAzShear_0-2kmAGL',
  'AzShearMid': 'MRMS_MergedAzShear_3-6kmAGL',
  'GOES_ABI_C01': 'GOES_ABI_C01_Reflectance',
  'GOES_ABI_C02': 'GOES_ABI_C02_Reflectance',
  'GOES_ABI_C03': 'GOES_ABI_C03_Reflectance',
  'GOES_ABI_C04': 'GOES_ABI_C04_Reflectance',
  'GOES_ABI_C05': 'GOES_ABI_C05_Reflectance',
  'GOES_ABI_C06': 'GOES_ABI_C06_Reflectance',
  'GOES_ABI_C07': 'GOES_ABI_C07_BrightnessTemp',
  'GOES_ABI_C08': 'GOES_ABI_C08_BrightnessTemp',
  'GOES_ABI_C09': 'GOES_ABI_C09_BrightnessTemp',
  'GOES_ABI_C10': 'GOES_ABI_C10_BrightnessTemp',
  'GOES_ABI_C11': 'GOES_ABI_C11_BrightnessTemp',
  'GOES_ABI_C12': 'GOES_ABI_C12_BrightnessTemp',
  'GOES_ABI_C13': 'GOES_ABI_C13_BrightnessTemp',
  'GOES_ABI_C14': 'GOES_ABI_C14_BrightnessTemp',
  'GOES_ABI_C15': 'GOES_ABI_C15_BrightnessTemp',
  'GOES_ABI_C16': 'GOES_ABI_C16_BrightnessTemp',
  'GOES_RGB_TrueColor': 'GOES_RGB_TrueColor',
  'GOES_RGB_Airmass': 'GOES_RGB_Airmass',
  'GOES_RGB_NighttimeMicrophysics': 'GOES_RGB_NighttimeMicrophysics',
  'GOES_RGB_DayCloudPhase': 'GOES_RGB_DayCloudPhase',
  'GOES_RGB_SimpleWaterVapor': 'GOES_RGB_SimpleWaterVapor',
  'GOES_RGB_Sandwich': 'GOES_RGB_Sandwich',
};

// Helper to get GUI_DIR from app.locals (set by server.js)
function getGuiDir(req) {
  return req.app.locals.GUI_DIR;
}

async function loadProductIndex(productDir) {
  const indexFile = path.join(productDir, 'index.json');

  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const indexData = JSON.parse(data);

    return {
      exists: true,
      timestamps: Array.isArray(indexData) ? indexData : (indexData.timestamps || []),
      tileGrid: Array.isArray(indexData) ? DEFAULT_TILE_GRID : (normalizeTileGrid(indexData.tile_grid) || DEFAULT_TILE_GRID),
    };
  } catch (err) {
    if (err.code === 'ENOENT') {
      return {
        exists: false,
        timestamps: [],
        tileGrid: DEFAULT_TILE_GRID,
      };
    }
    throw err;
  }
}

async function loadTimestampIndex(timestampDir) {
  const indexFile = path.join(timestampDir, 'index.json');

  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const indexData = JSON.parse(data);

    return {
      exists: true,
      tiles: Array.isArray(indexData) ? indexData : (indexData.tiles || []),
      tileGrid: Array.isArray(indexData) ? null : (indexData.tile_grid || null),
    };
  } catch (err) {
    if (err.code === 'ENOENT') {
      return {
        exists: false,
        tiles: [],
        tileGrid: null,
      };
    }
    throw err;
  }
}

function normalizeTileGrid(tileGrid) {
  if (!tileGrid || typeof tileGrid !== 'object') {
    return null;
  }

  const { rows, cols, tile_size: tileSize } = tileGrid;
  if (!Number.isInteger(rows) || !Number.isInteger(cols) || !Number.isInteger(tileSize)) {
    return null;
  }

  return { rows, cols, tile_size: tileSize };
}

function normalizeIndexedTiles(tiles, gridInfo) {
  if (!Array.isArray(tiles)) {
    return [];
  }

  return tiles
    .map((tile) => {
      if (!Array.isArray(tile) || tile.length !== 2) {
        return null;
      }

      const [tileX, tileY] = tile;
      if (!Number.isInteger(tileX) || !Number.isInteger(tileY)) {
        return null;
      }

      if (tileX < 0 || tileX >= gridInfo.cols || tileY < 0 || tileY >= gridInfo.rows) {
        return null;
      }

      return [tileX, tileY];
    })
    .filter((tile) => tile !== null)
    .sort((left, right) => {
      if (left[1] !== right[1]) {
        return left[1] - right[1];
      }
      return left[0] - right[0];
    });
}

// GET /get-items
// Returns a JSON list of all products listed
router.get('/get-items', async (req, res) => {
  try {
    const GUI_DIR = getGuiDir(req);
    const products = [];
    const keys = Object.keys(PRODUCT_MAPPING);

    for (const key of keys) {
      try {
        const p = path.join(GUI_DIR, key);
        const stat = await fs.stat(p);
        if (stat.isDirectory()) {
          products.push(key);
        }
      } catch (e) {
        // Ignore if not found
      }
    }

    res.json(products);
  } catch (err) {
    console.error('Error in get-items:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /fetch?product=[product]
// Returns a list of all available timestamps of a specific product in YYYYMMDD-HHMMSS format
router.get('/fetch', async (req, res) => {
  const product = req.query.product;
  const GUI_DIR = getGuiDir(req);

  if (!product) {
    return res.status(400).json({ error: 'Missing product parameter' });
  }

  // Security: Prevent directory traversal
  if (product.includes('..') || product.includes('/') || product.includes('\\')) {
    return res.status(400).json({ error: 'Invalid product name' });
  }

  // Allowlist: only known products may be fetched
  if (!Object.prototype.hasOwnProperty.call(PRODUCT_MAPPING, product)) {
    return res.status(404).json({ error: 'Unknown product or no mapping found' });
  }

  const productDir = path.join(GUI_DIR, product);
  const indexFile = path.join(productDir, 'index.json');

  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const indexData = JSON.parse(data);

    // Handle both old format (array) and new format (object)
    let timestamps;
    if (Array.isArray(indexData)) {
      // Old format: just an array of timestamps
      timestamps = indexData;
    } else {
      // New format: object with timestamps array
      timestamps = indexData.timestamps || [];
    }

    // According to req "rounded down to the minute".
    // Our python script saves YYYYMMDD-HHMM00. 
    // This is effectively rounded down to the minute (seconds=00).
    // We serve this list directly.
    res.json(timestamps);
  } catch (err) {
    if (err.code === 'ENOENT') {
      // Index file doesn't exist yet, return empty list or 404? 
      // Empty list is friendlier for "no resources yet".
      return res.json([]);
    }
    console.error(`Error reading index.json for ${product}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /download?product=[product]&timestamp=[timestamp]
// Downloads a specific timestamp of a specific product
router.get('/download', async (req, res) => {
  const { product, timestamp } = req.query;
  const GUI_DIR = getGuiDir(req);

  if (!product || !timestamp) {
    return res.status(400).json({ error: 'Missing product or timestamp parameter' });
  }

  // Security checks
  if (product.includes('..') || timestamp.includes('..') || product.includes('/') || product.includes('\\') || timestamp.includes('/') || timestamp.includes('\\')) {
    return res.status(400).json({ error: 'Invalid parameters' });
  }

  const filePrefix = PRODUCT_MAPPING[product];
  if (!filePrefix) {
    // If not in mapping, maybe try using product as prefix? 
    // Or return error. Safe default is error or strict mapping.
    // Given the list in config.py covers the main ones, we'll error if unknown
    // to avoid guessing wrong file names.
    return res.status(404).json({ error: 'Unknown product or no mapping found' });
  }

  // Construct filename: "{product_prefix}_{timestamp}.png"
  // e.g. MRMS_MergedReflectivityQC_20251226-123000.png
  const filename = `${filePrefix}_${timestamp}.png`;
  const filePath = path.join(GUI_DIR, product, filename);

  try {
    await fs.access(filePath);
    res.sendFile(filePath);
  } catch (err) {
    res.status(404).json({ error: 'File not found' });
  }
});

// GET /tile?product=[product]&timestamp=[timestamp]&x=[x]&y=[y]
// Returns a specific tile for a product at a given timestamp
// File path: {GUI_DIR}/{product}/{timestamp}/tile_{x}_{y}.png
router.get('/tile', async (req, res) => {
  const { product, timestamp, x, y } = req.query;
  const GUI_DIR = getGuiDir(req);
  const hasX = x !== undefined;
  const hasY = y !== undefined;

  // 1. Validate required parameters (same pattern as /download)
  if (!product || !timestamp) {
    return res.status(400).json({ error: 'Missing required parameters: product, timestamp' });
  }

  if (hasX !== hasY) {
    return res.status(400).json({ error: 'Missing required parameters: x and y must both be provided together' });
  }

  // 2. Security: Prevent directory traversal (same pattern as /download)
  if (product.includes('..') || timestamp.includes('..') ||
    product.includes('/') || product.includes('\\') ||
    timestamp.includes('/') || timestamp.includes('\\')) {
    return res.status(400).json({ error: 'Invalid parameters' });
  }

  // 3. Validate product using existing mapping (same pattern as /download)
  const filePrefix = PRODUCT_MAPPING[product];
  if (!filePrefix) {
    return res.status(404).json({ error: 'Unknown product or no mapping found' });
  }

  const productDir = path.join(GUI_DIR, product);
  try {
    const indexData = await loadProductIndex(productDir);
    const timestampDir = path.join(productDir, timestamp);
    const timestampIndex = await loadTimestampIndex(timestampDir);
    const gridInfo = normalizeTileGrid(timestampIndex.tileGrid) || indexData.tileGrid;

    if (!hasX && !hasY) {
      if (indexData.exists && !indexData.timestamps.includes(timestamp)) {
        return res.status(404).json({ error: 'Timestamp not found' });
      }

      try {
        await fs.access(timestampDir);
      } catch (err) {
        if (err.code === 'ENOENT') {
          return res.status(404).json({ error: 'Timestamp directory not found' });
        }
        throw err;
      }

      if (!timestampIndex.exists) {
        return res.status(404).json({ error: 'Timestamp tile index not found' });
      }

      const tiles = normalizeIndexedTiles(timestampIndex.tiles, gridInfo);

      return res.json({
        product,
        timestamp,
        tile_grid: gridInfo,
        tiles,
      });
    }

    // 4. Validate x, y are integers
    const xInt = parseInt(x, 10);
    const yInt = parseInt(y, 10);
    if (isNaN(xInt) || isNaN(yInt)) {
      return res.status(400).json({ error: 'x and y must be integers' });
    }

    // 5. Bounds check
    if (xInt < 0 || xInt >= gridInfo.cols || yInt < 0 || yInt >= gridInfo.rows) {
      return res.status(400).json({
        error: `Tile coordinates out of bounds. Valid range: x=[0,${gridInfo.cols - 1}], y=[0,${gridInfo.rows - 1}]`
      });
    }

    // 6. Construct tile path (follows new folder structure)
    const tileFilename = `tile_${xInt}_${yInt}.png`;
    const tilePath = path.join(GUI_DIR, product, timestamp, tileFilename);

    // 7. Send file (same pattern as /download)
    try {
      await fs.access(tilePath);
      res.sendFile(tilePath);
    } catch (err) {
      res.status(404).json({ error: 'Tile not found' });
    }
  } catch (err) {
    console.error(`Error handling tile request for ${product}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /tile-info?product=[product]
// Returns tile grid configuration for a product
router.get('/tile-info', async (req, res) => {
  const { product } = req.query;
  const GUI_DIR = getGuiDir(req);

  // Same validation pattern as /fetch
  if (!product) {
    return res.status(400).json({ error: 'Missing product parameter' });
  }

  if (product.includes('..') || product.includes('/') || product.includes('\\')) {
    return res.status(400).json({ error: 'Invalid product name' });
  }

  // Validate product using existing mapping
  const filePrefix = PRODUCT_MAPPING[product];
  if (!filePrefix) {
    return res.status(404).json({ error: 'Unknown product or no mapping found' });
  }

  try {
    const { timestamps, tileGrid } = await loadProductIndex(path.join(GUI_DIR, product));

    res.json({
      product: product,
      rows: tileGrid.rows,
      cols: tileGrid.cols,
      tile_size: tileGrid.tile_size,
      timestamps: timestamps
    });
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.json({
        product: product,
        rows: DEFAULT_TILE_GRID.rows,
        cols: DEFAULT_TILE_GRID.cols,
        tile_size: DEFAULT_TILE_GRID.tile_size,
        timestamps: []
      });
    }
    console.error(`Error reading index.json for ${product}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

import express from 'express';
const router = express.Router();
import path from 'path';
import fs from 'fs/promises';

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

  // 1. Validate required parameters (same pattern as /download)
  if (!product || !timestamp || x === undefined || y === undefined) {
    return res.status(400).json({ error: 'Missing required parameters: product, timestamp, x, y' });
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

  // 4. Validate x, y are integers
  const xInt = parseInt(x, 10);
  const yInt = parseInt(y, 10);
  if (isNaN(xInt) || isNaN(yInt)) {
    return res.status(400).json({ error: 'x and y must be integers' });
  }

  // 5. Get tile grid info from index.json for bounds checking
  const indexFile = path.join(GUI_DIR, product, 'index.json');
  let gridInfo = { rows: 14, cols: 28, tile_size: 250 }; // defaults
  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const indexData = JSON.parse(data);
    if (indexData.tile_grid) {
      gridInfo = indexData.tile_grid;
    }
  } catch (e) {
    // Use defaults if index.json not found
  }

  // 6. Bounds check
  if (xInt < 0 || xInt >= gridInfo.cols || yInt < 0 || yInt >= gridInfo.rows) {
    return res.status(400).json({
      error: `Tile coordinates out of bounds. Valid range: x=[0,${gridInfo.cols - 1}], y=[0,${gridInfo.rows - 1}]`
    });
  }

  // 7. Construct tile path (follows new folder structure)
  const tileFilename = `tile_${xInt}_${yInt}.png`;
  const tilePath = path.join(GUI_DIR, product, timestamp, tileFilename);

  // 8. Send file (same pattern as /download)
  try {
    await fs.access(tilePath);
    res.sendFile(tilePath);
  } catch (err) {
    res.status(404).json({ error: 'Tile not found' });
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

  const indexFile = path.join(GUI_DIR, product, 'index.json');

  try {
    const data = await fs.readFile(indexFile, 'utf8');
    const indexData = JSON.parse(data);

    // Handle both old format (array) and new format (object with tile_grid)
    let timestamps = [];
    let tileGrid = { rows: 14, cols: 28, tile_size: 250 };

    if (Array.isArray(indexData)) {
      // Old format: just an array of timestamps
      timestamps = indexData;
    } else {
      // New format: object with timestamps and tile_grid
      timestamps = indexData.timestamps || [];
      tileGrid = indexData.tile_grid || tileGrid;
    }

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
        rows: 14,
        cols: 28,
        tile_size: 250,
        timestamps: []
      });
    }
    console.error(`Error reading index.json for ${product}:`, err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;

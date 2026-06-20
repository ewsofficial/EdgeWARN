import express from 'express';
const router = express.Router();
import path from 'path';
import fs from 'fs/promises';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// GET /
// Returns the contents of colormaps.json
router.get('/', async (req, res) => {
  try {
    // colormaps.json is in the project root: .../EWMRS/colormaps.json
    // This file is in .../EWMRS/api/routes/colormaps.js
    // So we need to go up two levels: ../../
    const colormapsPath = path.join(__dirname, '..', '..', 'colormaps.json');
    const data = await fs.readFile(colormapsPath, 'utf-8');
    res.json(JSON.parse(data));
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.error('colormaps.json not found at:', path.join(__dirname, '..', '..', 'colormaps.json'));
      return res.status(404).json({ error: 'Colormaps configuration not found' });
    }
    console.error('Error reading colormaps.json:', err);
    res.status(500).json({ error: 'Failed to read colormaps.json' });
  }
});

export default router;

import express from 'express';
import path from 'path';
import apiConfig from '../../config.js';
import { readIndexFile } from '../../utils/fileReader.js';
import { validateResourceType } from '../../utils/validation.js';

const router = express.Router();

/**
 * GET /features/fetch/resources
 * Query params:
 *   - type: "cell" or "list" (required)
 * 
 * Returns:
 *   - type=list: JSON array of available timestamps
 *   - type=cell: JSON array of available cell IDs
 */
router.get('/resources', async (req, res) => {
  const { type } = req.query;

  // Validate type parameter
  if (!validateResourceType(type)) {
    return res.status(400).json({ 
      error: 'Invalid type parameter. Must be "cell" or "list"' 
    });
  }

  try {
    if (type === 'list') {
      // Read stormcell index
      const indexPath = path.join(apiConfig.STORMCELL_DIR, 'stormcell_index.json');
      const indexData = await readIndexFile(indexPath);
      res.json(indexData.timestamps || []);
      
    } else if (type === 'cell') {
      // Read cell index
      const indexPath = path.join(apiConfig.CELL_DIR, 'cell_index.json');
      const indexData = await readIndexFile(indexPath);
      res.json(indexData.cellIds || []);
    }
  } catch (err) {
    if (err.code === 'ENOENT') {
      // Index file doesn't exist - return empty array
      return res.json([]);
    }
    console.error('Error reading index file:', err);
    res.status(500).json({ error: 'Failed to fetch resources' });
  }
});

export default router;

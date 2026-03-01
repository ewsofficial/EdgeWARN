import express from 'express';
import path from 'path';
import apiConfig from '../../config.js';
import { readJsonFileSafe, readIndexFile } from '../../utils/fileReader.js';
import { validateCellId } from '../../utils/validation.js';

const router = express.Router();

/**
 * GET /api/v2/features/cells
 * Query params:
 *   - id: integer (optional) - Cell ID to fetch specific cell data
 *
 * Returns:
 *   - Without id: JSON array of available cell IDs
 *   - With id: JSON data of the specific cell
 */
router.get('/', async (req, res) => {
  const { id } = req.query;

  try {
    // If id is provided, return specific cell data
    if (id !== undefined && id !== '') {
      // Validate id parameter
      if (!validateCellId(id)) {
        return res.status(400).json({
          error: 'Invalid id parameter. Must be a positive integer'
        });
      }

      // Set caching header (1 minute for cell data)
      res.set('Cache-Control', 'public, max-age=60');

      // Build filename: {id}.json
      const filename = `${id}.json`;
      const content = await readJsonFileSafe(apiConfig.CELL_DIR, filename, { useCache: false });
      res.json(content);
      return;
    }

    // No id provided - return list of available cell IDs
    res.set('Cache-Control', 'public, max-age=5');

    // Read cell index
    const indexPath = path.join(apiConfig.CELL_DIR, 'cell_index.json');
    const indexData = await readIndexFile(indexPath);
    res.json(indexData.cellIds || []);

  } catch (err) {
    if (err.code === 'ENOENT') {
      // Index file or cell file doesn't exist - return empty array or 404
      if (id) {
        return res.status(404).json({
          error: 'Cell not found',
          id: id
        });
      }
      return res.json([]);
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading cell data:', err);
    res.status(500).json({ error: 'Failed to fetch cell data' });
  }
});

export default router;

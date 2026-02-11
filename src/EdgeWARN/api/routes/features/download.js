import express from 'express';
import apiConfig from '../../config.js';
import { readJsonFileSafe } from '../../utils/fileReader.js';
import { validateResourceType, validateTimestamp, validateCellId } from '../../utils/validation.js';

const router = express.Router();

/**
 * GET /features/download/resources
 * Query params:
 *   - type: "cell" or "list" (required)
 *   - timestamp: YYYYMMDD-HHMMSS (required if type="list")
 *   - id: integer (required if type="cell")
 * 
 * Returns:
 *   - JSON content of requested resource
 *   - 404 if resource not found
 */
router.get('/resources', async (req, res) => {
  const { type, timestamp, id } = req.query;

  // Validate type parameter
  if (!validateResourceType(type)) {
    return res.status(400).json({
      error: 'Invalid type parameter. Must be "cell" or "list"'
    });
  }

  try {
    if (type === 'list') {
      // Validate timestamp parameter
      if (!validateTimestamp(timestamp)) {
        return res.status(400).json({
          error: 'Invalid or missing timestamp parameter. Format: YYYYMMDD-HHMMSS'
        });
      }

      // Set caching header (e.g. 1 hour for immutable stormcells data)
      res.set('Cache-Control', 'public, max-age=3600');

      // Build filename: stormcells_{timestamp}.json
      const filename = `stormcells_${timestamp}.json`;
      const content = await readJsonFileSafe(apiConfig.STORMCELL_DIR, filename);
      res.json(content);

    } else if (type === 'cell') {
      // Validate id parameter
      if (!validateCellId(id)) {
        return res.status(400).json({
          error: 'Invalid or missing id parameter. Must be a positive integer'
        });
      }

      // Set caching header (e.g. 1 minute for cell data which might update?)
      // Assuming cell ID data might be transient or update, but still safe for short cache
      res.set('Cache-Control', 'public, max-age=60');

      // Build filename: {id}.json
      const filename = `${id}.json`;
      const content = await readJsonFileSafe(apiConfig.CELL_DIR, filename, { useCache: false });
      res.json(content);
    }
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(404).json({
        error: 'The requested file was not found'
      });
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading resource:', err);
    res.status(500).json({ error: 'Failed to read resource' });
  }
});

export default router;

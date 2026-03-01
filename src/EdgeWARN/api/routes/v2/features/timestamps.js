import express from 'express';
import path from 'path';
import apiConfig from '../../../config.js';
import { readJsonFileSafe, readIndexFile } from '../../../utils/fileReader.js';
import { validateTimestampV2 } from '../../../utils/validation.js';

const router = express.Router();

/**
 * GET /api/v2/features/timestamps
 * Query params:
 *   - timestamp: YYYYMMDD-HHMMSS (optional) - Returns stormcell list for this timestamp
 *
 * Returns:
 *   - Without timestamp: JSON array of available timestamps
 *   - With timestamp: JSON data of stormcells at that specific time
 */
router.get('/', async (req, res) => {
  const { timestamp } = req.query;

  try {
    // If timestamp is provided, return stormcell list for that timestamp
    if (timestamp !== undefined && timestamp !== '') {
      // Validate timestamp parameter
      if (!validateTimestampV2(timestamp)) {
        return res.status(400).json({
          error: 'Invalid timestamp parameter. Format: YYYYMMDD-HHMMSS'
        });
      }

      // Set caching header (1 hour for immutable stormcells data)
      res.set('Cache-Control', 'public, max-age=3600');

      // Build filename: stormcells_{timestamp}.json
      const filename = `stormcells_${timestamp}.json`;
      const content = await readJsonFileSafe(apiConfig.STORMCELL_DIR, filename);
      res.json(content);
      return;
    }

    // No timestamp provided - return list of available timestamps
    res.set('Cache-Control', 'public, max-age=5');

    // Read stormcell index
    const indexPath = path.join(apiConfig.STORMCELL_DIR, 'stormcell_index.json');
    const indexData = await readIndexFile(indexPath);
    res.json(indexData.timestamps || []);

  } catch (err) {
    if (err.code === 'ENOENT') {
      // Index file or stormcell file doesn't exist - return empty array or 404
      if (timestamp) {
        return res.status(404).json({
          error: 'Stormcell data not found for the specified timestamp',
          timestamp: timestamp
        });
      }
      return res.json([]);
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading timestamp data:', err);
    res.status(500).json({ error: 'Failed to fetch timestamp data' });
  }
});

export default router;

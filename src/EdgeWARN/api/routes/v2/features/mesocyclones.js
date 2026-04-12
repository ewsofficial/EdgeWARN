import express from 'express';
import fs from 'fs/promises';
import apiConfig from '../../../config.js';
import { readJsonFileSafe } from '../../../utils/fileReader.js';
import { validateTimestampV2 } from '../../../utils/validation.js';

const router = express.Router();

async function getMesocycloneTimestamps(dir) {
  try {
    const files = await fs.readdir(dir);
    return files
      .map((file) => file.match(/^mesocyclones_(\d{8}-\d{6})\.json$/)?.[1])
      .filter(Boolean)
      .sort()
      .reverse();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return [];
    }
    throw err;
  }
}

router.get('/', async (req, res) => {
  const { timestamp } = req.query;

  try {
    if (timestamp !== undefined && timestamp !== '') {
      if (!validateTimestampV2(timestamp)) {
        return res.status(400).json({
          error: 'Invalid timestamp parameter. Format: YYYYMMDD-HHMMSS'
        });
      }

      res.set('Cache-Control', 'public, max-age=60');

      const filename = `mesocyclones_${timestamp}.json`;
      const content = await readJsonFileSafe(apiConfig.MESOCYCLONE_DIR, filename);
      return res.json(content);
    }

    res.set('Cache-Control', 'public, max-age=5');
    const timestamps = await getMesocycloneTimestamps(apiConfig.MESOCYCLONE_DIR);
    return res.json(timestamps);
  } catch (err) {
    if (err.code === 'ENOENT') {
      if (timestamp) {
        return res.status(404).json({
          error: 'Mesocyclone data not found for the specified timestamp',
          timestamp
        });
      }
      return res.json([]);
    }
    if (err.code === 'EINVAL' || err.code === 'EACCES') {
      return res.status(400).json({
        error: 'Invalid filename or access denied'
      });
    }
    console.error('Error reading mesocyclone data:', err);
    return res.status(500).json({ error: 'Failed to fetch mesocyclone data' });
  }
});

export default router;

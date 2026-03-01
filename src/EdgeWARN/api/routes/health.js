import express from 'express';

const router = express.Router();

/**
 * GET /health
 * Returns minimal server health status
 * Detailed metrics available via separate admin endpoint
 */
router.get('/', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString()
  });
});

export default router;

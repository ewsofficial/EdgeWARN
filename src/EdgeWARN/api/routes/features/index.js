import express from 'express';
import fetchRouter from './fetch.js';
import downloadRouter from './download.js';

const router = express.Router();

// Mount sub-routes
router.use('/fetch', fetchRouter);
router.use('/download', downloadRouter);

// Root features endpoint
router.get('/', (req, res) => {
  res.json({
    message: 'EdgeWARN Features API',
    endpoints: {
      fetch: '/features/fetch/resources?type=[cell|list]',
      download: '/features/download/resources?type=[cell|list]&[timestamp=...|id=...]'
    }
  });
});

export default router;

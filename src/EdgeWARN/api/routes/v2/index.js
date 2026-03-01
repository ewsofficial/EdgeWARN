import express from 'express';
import cellsRouter from './features/cells.js';
import timestampsRouter from './features/timestamps.js';
import nwsRouter from './data/nws.js';
import metarRouter from './data/metar.js';

const router = express.Router();

// Mount v2 feature routes
router.use('/features/cells', cellsRouter);
router.use('/features/timestamps', timestampsRouter);

// Mount v2 data routes
router.use('/data/nws', nwsRouter);
router.use('/data/metar', metarRouter);

// Root v2 endpoint
router.get('/', (req, res) => {
  res.json({
    message: 'EdgeWARN API v2',
    version: '2.0.0',
    endpoints: {
      features: {
        cells: '/api/v2/features/cells[?id={int}]',
        timestamps: '/api/v2/features/timestamps[?timestamp={YYYYMMDD-HHMMSS}]'
      },
      data: {
        nws: '/api/v2/data/nws[?timestamp={YYYYMMDD-HHMMSS}|id={alert_id}]',
        metar: '/api/v2/data/metar[?timestamp={YYYYMMDD-HHMMSS}]'
      }
    }
  });
});

export default router;

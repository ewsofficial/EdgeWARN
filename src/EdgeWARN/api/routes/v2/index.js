import express from 'express';
import cellsRouter from './features/cells.js';
import mesocyclonesRouter from './features/mesocyclones.js';
import timestampsRouter from './features/timestamps.js';
import alertsRouter from './features/alerts.js';
import metarRouter from './data/metar.js';

const router = express.Router();

// Mount v2 feature routes
router.use('/features/cells', cellsRouter);
router.use('/features/mesocyclones', mesocyclonesRouter);
router.use('/features/timestamps', timestampsRouter);
router.use('/features/alerts', alertsRouter);

// Mount v2 data routes
router.use('/data/metar', metarRouter);

// Root v2 endpoint
router.get('/', (req, res) => {
  // Only expose detailed version in non-production environments
  const version = process.env.NODE_ENV === 'production' ? '2.x' : '2.5.1';
  res.json({
    message: 'EdgeWARN API v2',
    version: version,
    endpoints: {
      features: {
        cells: '/api/v2/features/cells[?id={int}]',
        mesocyclones: '/api/v2/features/mesocyclones[?timestamp={YYYYMMDD-HHMMSS}]',
        timestamps: '/api/v2/features/timestamps[?timestamp={YYYYMMDD-HHMMSS}]',
        alerts: {
          official: '/api/v2/features/alerts/official[?id={urn:oid:...}|timestamp={YYYYMMDD-HHMMSS}]',
          edgewarn: '/api/v2/features/alerts/edgewarn[?id={id}|timestamp={YYYYMMDD-HHMMSS}]'
        }
      },
      data: {
        metar: '/api/v2/data/metar[?timestamp={YYYYMMDD-HHMMSS}]'
      }
    }
  });
});

export default router;

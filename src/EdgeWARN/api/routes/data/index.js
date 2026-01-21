import express from 'express';
import fetchRouter from './fetch.js';
import downloadRouter from './download.js';

const router = express.Router();

// Mount sub-routes
router.use('/fetch', fetchRouter);
router.use('/download', downloadRouter);

// Root data endpoint
router.get('/', (req, res) => {
    res.json({
        message: 'EdgeWARN Data API',
        endpoints: {
            fetch: {
                metar: '/data/fetch/metar',
                nws: '/data/fetch/nws'
            },
            download: {
                metar: '/data/download/metar?timestamp=YYYYMMDD-HHMM00',
                nws: '/data/download/nws?timestamp=YYYYMMDD-HHMM00'
            }
        }
    });
});

export default router;

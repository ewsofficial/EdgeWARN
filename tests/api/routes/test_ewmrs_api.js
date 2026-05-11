/**
 * Tests for EWMRS API routes
 * @module tests/api/routes/test_ewmrs_api
 */
import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import request from 'supertest';
import express from 'express';
import fs from 'fs';
import path from 'path';
import os from 'os';
import zlib from 'zlib';

import rendersRouter from '../../../src/EWMRS/api/routes/renders.js';
import colormapsRouter from '../../../src/EWMRS/api/routes/colormaps.js';
import rapRouter from '../../../src/EWMRS/api/routes/rap.js';
import nexradRouter from '../../../src/EWMRS/api/routes/nexrad.js';

function createApp(tempDir) {
    const app = express();
    app.locals.GUI_DIR = path.join(tempDir, 'gui');
    app.use('/renders', rendersRouter);
    app.use('/rap', rapRouter);
    app.use('/nexrad', nexradRouter);
    return app;
}

function parseBinary(res, callback) {
    const chunks = [];
    res.on('data', chunk => chunks.push(chunk));
    res.on('end', () => callback(null, Buffer.concat(chunks)));
}

describe('EWMRS Root Route', () => {
    let app;

    beforeEach(() => {
        app = express();
        app.locals.BASE_DIR = '/tmp/ewmrs-test';
        app.locals.GUI_DIR = '/tmp/ewmrs-test/gui';
        app.get('/', (req, res) => {
            res.json({
                service: 'EWMRS API',
                base_dir: req.app.locals.BASE_DIR,
                gui_dir: req.app.locals.GUI_DIR,
                endpoints: ['/renders/get-items', '/renders/fetch', '/renders/download', '/rap/layers', '/rap/fetch', '/rap/metadata', '/rap/data', '/nexrad/variables', '/nexrad/sites', '/nexrad/timestamps', '/nexrad/download', '/healthz', '/colormaps']
            });
        });
    });

    it('returns service name and endpoints', async () => {
        const res = await request(app).get('/').expect(200);
        expect(res.body.service).toBe('EWMRS API');
        expect(res.body.endpoints).toContain('/renders/get-items');
        expect(res.body.endpoints).toContain('/rap/data');
        expect(res.body.endpoints).toContain('/nexrad/variables');
        expect(res.body.endpoints).toContain('/nexrad/download');
    });
});

describe('EWMRS Health Route', () => {
    let app;

    beforeEach(() => {
        app = express();
        app.get('/healthz', (req, res) => res.json({ ok: true }));
    });

    it('returns ok true', async () => {
        const res = await request(app).get('/healthz').expect(200);
        expect(res.body.ok).toBe(true);
    });
});

describe('GET /renders/get-items', () => {
    let app, tempDir;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-api-test-'));
        const guiDir = path.join(tempDir, 'gui');
        await fs.promises.mkdir(guiDir);
        await fs.promises.mkdir(path.join(guiDir, 'CompRefQC'));
        await fs.promises.mkdir(path.join(guiDir, 'EchoTop18'));
        await fs.promises.mkdir(path.join(guiDir, 'NoProduct'));
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns only directories that exist', async () => {
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('CompRefQC');
        expect(res.body).toContain('EchoTop18');
        expect(res.body).not.toContain('NoProduct');
    });

    it('includes VIL when the product directory exists', async () => {
        await fs.promises.mkdir(path.join(tempDir, 'gui', 'VIL'));
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('VIL');
    });

    it('includes MESH when the product directory exists', async () => {
        await fs.promises.mkdir(path.join(tempDir, 'gui', 'MESH'));
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('MESH');
    });

    it('includes GOES_ABI_C02 when the product directory exists', async () => {
        await fs.promises.mkdir(path.join(tempDir, 'gui', 'GOES_ABI_C02'));
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('GOES_ABI_C02');
    });

    it('includes GOES_ABI_C16 when the product directory exists', async () => {
        await fs.promises.mkdir(path.join(tempDir, 'gui', 'GOES_ABI_C16'));
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('GOES_ABI_C16');
    });

    it('includes GOES_RGB_TrueColor when the product directory exists', async () => {
        await fs.promises.mkdir(path.join(tempDir, 'gui', 'GOES_RGB_TrueColor'));
        const res = await request(app).get('/renders/get-items').expect(200);
        expect(res.body).toContain('GOES_RGB_TrueColor');
    });

    it('returns empty array when no products exist', async () => {
        const emptyDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-empty-'));
        const emptyApp = createApp(emptyDir);
        const res = await request(emptyApp).get('/renders/get-items').expect(200);
        expect(res.body).toEqual([]);
        await fs.promises.rm(emptyDir, { recursive: true, force: true });
    });
});

describe('GET /renders/fetch', () => {
    let app, tempDir;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-fetch-'));
        const guiDir = path.join(tempDir, 'gui');
        await fs.promises.mkdir(guiDir);
        const productDir = path.join(guiDir, 'CompRefQC');
        await fs.promises.mkdir(productDir);
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns 400 when product is missing', async () => {
        const res = await request(app).get('/renders/fetch').expect(400);
        expect(res.body.error).toContain('Missing product');
    });

    it('returns 400 for directory traversal attempt', async () => {
        const res = await request(app).get('/renders/fetch?product=../../../etc').expect(400);
        expect(res.body.error).toContain('Invalid');
    });

    it('returns empty array when index.json missing', async () => {
        const res = await request(app).get('/renders/fetch?product=CompRefQC').expect(200);
        expect(res.body).toEqual([]);
    });

    it('handles old format index.json (array)', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify(['20260317-200000', '20260317-190000'])
        );
        const res = await request(app).get('/renders/fetch?product=CompRefQC').expect(200);
        expect(res.body).toEqual(['20260317-200000', '20260317-190000']);
    });

    it('handles new format index.json (object with timestamps)', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-200000', '20260317-190000'],
                tile_grid: { rows: 10, cols: 20, tile_size: 350 }
            })
        );
        const res = await request(app).get('/renders/fetch?product=CompRefQC').expect(200);
        expect(res.body).toEqual(['20260317-200000', '20260317-190000']);
    });
});

describe('GET /renders/download', () => {
    let app, tempDir;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-dl-'));
        const guiDir = path.join(tempDir, 'gui');
        await fs.promises.mkdir(guiDir);
        const productDir = path.join(guiDir, 'CompRefQC');
        await fs.promises.mkdir(productDir);
        await fs.promises.writeFile(path.join(productDir, 'MRMS_MergedReflectivityQC_20260317-200000.png'), 'fake png');
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns 400 when product is missing', async () => {
        const res = await request(app).get('/renders/download').expect(400);
        expect(res.body.error).toContain('Missing');
    });

    it('returns 400 when timestamp is missing', async () => {
        const res = await request(app).get('/renders/download?product=CompRefQC').expect(400);
        expect(res.body.error).toContain('Missing');
    });

    it('returns 400 for directory traversal in product', async () => {
        const res = await request(app).get('/renders/download?product=../etc&timestamp=20260317-200000').expect(400);
        expect(res.body.error).toContain('Invalid');
    });

    it('returns 400 for directory traversal in timestamp', async () => {
        const res = await request(app).get('/renders/download?product=CompRefQC&timestamp=../20260317').expect(400);
        expect(res.body.error).toContain('Invalid');
    });

    it('returns 404 for unknown product', async () => {
        const res = await request(app).get('/renders/download?product=NoSuchProduct&timestamp=20260317-200000').expect(404);
        expect(res.body.error).toContain('Unknown');
    });

    it('serves VIL files using the MRMS_VIL prefix', async () => {
        const vilDir = path.join(tempDir, 'gui', 'VIL');
        await fs.promises.mkdir(vilDir);
        await fs.promises.writeFile(path.join(vilDir, 'MRMS_VIL_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=VIL&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('serves MESH files using the MRMS_MESH prefix', async () => {
        const meshDir = path.join(tempDir, 'gui', 'MESH');
        await fs.promises.mkdir(meshDir);
        await fs.promises.writeFile(path.join(meshDir, 'MRMS_MESH_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=MESH&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('returns 404 when file does not exist', async () => {
        const res = await request(app).get('/renders/download?product=CompRefQC&timestamp=20260317-000000').expect(404);
        expect(res.body.error).toContain('not found');
    });

    it('serves GOES_ABI_C02 files using the GOES_ABI_C02_Reflectance prefix', async () => {
        const goesDir = path.join(tempDir, 'gui', 'GOES_ABI_C02');
        await fs.promises.mkdir(goesDir);
        await fs.promises.writeFile(path.join(goesDir, 'GOES_ABI_C02_Reflectance_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=GOES_ABI_C02&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('serves GOES_ABI_C13 files using the GOES_ABI_C13_BrightnessTemp prefix', async () => {
        const goesDir = path.join(tempDir, 'gui', 'GOES_ABI_C13');
        await fs.promises.mkdir(goesDir);
        await fs.promises.writeFile(path.join(goesDir, 'GOES_ABI_C13_BrightnessTemp_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=GOES_ABI_C13&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('serves GOES_ABI_C01 files using the GOES_ABI_C01_Reflectance prefix', async () => {
        const goesDir = path.join(tempDir, 'gui', 'GOES_ABI_C01');
        await fs.promises.mkdir(goesDir);
        await fs.promises.writeFile(path.join(goesDir, 'GOES_ABI_C01_Reflectance_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=GOES_ABI_C01&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('serves GOES_ABI_C12 files using the GOES_ABI_C12_BrightnessTemp prefix', async () => {
        const goesDir = path.join(tempDir, 'gui', 'GOES_ABI_C12');
        await fs.promises.mkdir(goesDir);
        await fs.promises.writeFile(path.join(goesDir, 'GOES_ABI_C12_BrightnessTemp_20260317-200000.png'), 'fake png');

        const res = await request(app)
            .get('/renders/download?product=GOES_ABI_C12&timestamp=20260317-200000')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('accepts GOES RGB product mappings for tile requests', async () => {
        const rgbDir = path.join(tempDir, 'gui', 'GOES_RGB_Sandwich');
        const tsDir = path.join(rgbDir, '20260317-200000');
        await fs.promises.mkdir(tsDir, { recursive: true });
        await fs.promises.writeFile(path.join(tsDir, 'tile_0_0.png'), 'fake tile');
        await fs.promises.writeFile(
            path.join(rgbDir, 'index.json'),
            JSON.stringify({ timestamps: ['20260317-200000'], tile_grid: { rows: 1, cols: 1, tile_size: 350 } })
        );

        const res = await request(app)
            .get('/renders/tile?product=GOES_RGB_Sandwich&timestamp=20260317-200000&x=0&y=0')
            .expect(200);

        expect(res.headers['content-type']).toContain('image/png');
    });

    it('serves file when it exists', async () => {
        const res = await request(app)
            .get('/renders/download?product=CompRefQC&timestamp=20260317-200000')
            .expect(200);
        expect(res.headers['content-type']).toContain('image/png');
    });
});

describe('GET /renders/tile', () => {
    let app, tempDir;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-tile-'));
        const guiDir = path.join(tempDir, 'gui');
        await fs.promises.mkdir(guiDir);
        const productDir = path.join(guiDir, 'CompRefQC');
        await fs.promises.mkdir(productDir);
        const tsDir = path.join(productDir, '20260317-200000');
        await fs.promises.mkdir(tsDir);
        await fs.promises.writeFile(path.join(tsDir, 'tile_0_0.png'), 'fake tile');
        await fs.promises.writeFile(path.join(tsDir, 'tile_1_0.png'), 'fake tile');
        await fs.promises.writeFile(path.join(tsDir, 'tile_2_1.png'), 'fake tile');
        await fs.promises.writeFile(
            path.join(tsDir, 'index.json'),
            JSON.stringify({
                tiles: [[0, 0], [1, 0], [2, 1]],
                tile_grid: { rows: 2, cols: 3, tile_size: 350 }
            })
        );
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-200000'],
                tile_grid: { rows: 2, cols: 3, tile_size: 350 }
            })
        );
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns a sparse tile list when x and y are both missing', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000').expect(200);
        expect(res.body).toEqual({
            product: 'CompRefQC',
            timestamp: '20260317-200000',
            tile_grid: { rows: 2, cols: 3, tile_size: 350 },
            tiles: [[0, 0], [1, 0], [2, 1]],
        });
    });

    it('returns 400 when x is missing but y is provided', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&y=0').expect(400);
        expect(res.body.error).toContain('x and y');
    });

    it('returns 400 when y is missing but x is provided', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0').expect(400);
        expect(res.body.error).toContain('x and y');
    });

    it('returns 400 for non-integer x', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=abc&y=0').expect(400);
        expect(res.body.error).toContain('integers');
    });

    it('returns 400 for non-integer y', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0&y=xyz').expect(400);
        expect(res.body.error).toContain('integers');
    });

    it('returns 400 for out-of-bounds x', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=100&y=0').expect(400);
        expect(res.body.error).toContain('out of bounds');
    });

    it('returns 400 for out-of-bounds y', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0&y=50').expect(400);
        expect(res.body.error).toContain('out of bounds');
    });

    it('returns 400 for negative x', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=-1&y=0').expect(400);
        expect(res.body.error).toContain('out of bounds');
    });

    it('returns 404 for tile not found', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=1&y=1').expect(404);
        expect(res.body.error).toContain('not found');
    });

    it('serves tile when it exists', async () => {
        const res = await request(app)
            .get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0&y=0')
            .expect(200);
        expect(res.headers['content-type']).toContain('image/png');
    });

    it('uses defaults when index.json missing', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.unlink(path.join(productDir, 'index.json'));
        const res = await request(app)
            .get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0&y=0')
            .expect(200);
        expect(res.headers['content-type']).toContain('image/png');
    });

    it('listing mode filters invalid and out-of-bounds indexed tiles', async () => {
        const tsDir = path.join(tempDir, 'gui', 'CompRefQC', '20260317-200000');
        await fs.promises.writeFile(
            path.join(tsDir, 'index.json'),
            JSON.stringify({
                tiles: [[0, 0], [1, 0], [2, 1], ['bad', 0], [9, 9], [1]],
                tile_grid: { rows: 2, cols: 3, tile_size: 350 }
            })
        );

        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000').expect(200);
        expect(res.body.tiles).toEqual([[0, 0], [1, 0], [2, 1]]);
    });

    it('listing mode returns an empty tile list for a valid zero-tile timestamp', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        const emptyTsDir = path.join(productDir, '20260317-210000');
        await fs.promises.mkdir(emptyTsDir);
        await fs.promises.writeFile(
            path.join(emptyTsDir, 'index.json'),
            JSON.stringify({
                tiles: [],
                tile_grid: { rows: 2, cols: 3, tile_size: 350 }
            })
        );
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-210000', '20260317-200000'],
                tile_grid: { rows: 2, cols: 3, tile_size: 350 }
            })
        );

        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-210000').expect(200);
        expect(res.body.tiles).toEqual([]);
    });

    it('listing mode returns 404 when the timestamp directory is missing', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-220000').expect(404);
        expect(res.body.error).toContain('Timestamp');
    });

    it('listing mode returns 404 when timestamp is absent from index.json', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        const tsDir = path.join(productDir, '20260317-230000');
        await fs.promises.mkdir(tsDir);

        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-230000').expect(404);
        expect(res.body.error).toContain('Timestamp not found');
    });

    it('listing mode returns 404 when the timestamp index is missing', async () => {
        const tsDir = path.join(tempDir, 'gui', 'CompRefQC', '20260317-200000');
        await fs.promises.unlink(path.join(tsDir, 'index.json'));

        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000').expect(404);
        expect(res.body.error).toContain('tile index');
    });
});

describe('GET /renders/tile-info', () => {
    let app, tempDir;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-tileinfo-'));
        const guiDir = path.join(tempDir, 'gui');
        await fs.promises.mkdir(guiDir);
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns 400 when product is missing', async () => {
        const res = await request(app).get('/renders/tile-info').expect(400);
        expect(res.body.error).toContain('Missing');
    });

    it('returns 400 for directory traversal', async () => {
        const res = await request(app).get('/renders/tile-info?product=../../../etc').expect(400);
        expect(res.body.error).toContain('Invalid');
    });

    it('returns defaults when index.json missing (ENOENT)', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.mkdir(productDir);
        const res = await request(app).get('/renders/tile-info?product=CompRefQC').expect(200);
        expect(res.body.rows).toBe(10);
        expect(res.body.cols).toBe(20);
        expect(res.body.tile_size).toBe(350);
        expect(res.body.timestamps).toEqual([]);
    });

    it('returns tile grid from new format index.json', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.mkdir(productDir);
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-200000'],
                tile_grid: { rows: 7, cols: 14, tile_size: 500 }
            })
        );
        const res = await request(app).get('/renders/tile-info?product=CompRefQC').expect(200);
        expect(res.body.rows).toBe(7);
        expect(res.body.cols).toBe(14);
        expect(res.body.timestamps).toEqual(['20260317-200000']);
    });

    it('returns empty timestamps for old format array index', async () => {
        const productDir = path.join(tempDir, 'gui', 'CompRefQC');
        await fs.promises.mkdir(productDir);
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify(['20260317-200000'])
        );
        const res = await request(app).get('/renders/tile-info?product=CompRefQC').expect(200);
        expect(res.body.timestamps).toEqual(['20260317-200000']);
        expect(res.body.rows).toBe(10);
    });

    it('returns 404 for unknown product', async () => {
        const res = await request(app).get('/renders/tile-info?product=NoSuchProduct').expect(404);
        expect(res.body.error).toContain('Unknown');
    });

    it('returns tile metadata for GOES RGB products', async () => {
        const productDir = path.join(tempDir, 'gui', 'GOES_RGB_TrueColor');
        await fs.promises.mkdir(productDir);
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-200000'],
                tile_grid: { rows: 10, cols: 20, tile_size: 350 }
            })
        );

        const res = await request(app).get('/renders/tile-info?product=GOES_RGB_TrueColor').expect(200);
        expect(res.body.rows).toBe(10);
        expect(res.body.cols).toBe(20);
        expect(res.body.tile_size).toBe(350);
        expect(res.body.timestamps).toEqual(['20260317-200000']);
    });
});

describe('EWMRS RAP Uint16 routes', () => {
    let app, tempDir, rapDir, tempLayerDir, capeLayerDir, timestampDir;

    const timestamp = '20260427-120000';
    const metadata = {
        layer: 'Temperature_2m',
        timestamp,
        shape: [2, 3],
        grid: { ni: 3, nj: 2, point_count: 6 },
        dtype: 'uint16',
        byte_order: 'little_endian',
        scale: { min: 180.0, max: 330.0 },
        missing_value: 65535,
        units: 'K',
        grib: { shortName: '2t', typeOfLevel: 'heightAboveGround', level: 2 }
    };

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-rap-'));
        const guiDir = path.join(tempDir, 'gui');
        rapDir = path.join(guiDir, 'RAP');
        tempLayerDir = path.join(rapDir, 'Temperature_2m');
        capeLayerDir = path.join(rapDir, 'CAPE_0-3km');
        timestampDir = path.join(tempLayerDir, timestamp);

        await fs.promises.mkdir(timestampDir, { recursive: true });
        await fs.promises.mkdir(capeLayerDir, { recursive: true });
        await fs.promises.writeFile(
            path.join(tempLayerDir, 'index.json'),
            JSON.stringify({ timestamps: [timestamp] })
        );
        await fs.promises.writeFile(
            path.join(capeLayerDir, 'index.json'),
            JSON.stringify(['20260427-110000'])
        );
        await fs.promises.writeFile(path.join(timestampDir, 'metadata.json'), JSON.stringify(metadata));
        await fs.promises.writeFile(path.join(timestampDir, 'data.u16'), Buffer.from([1, 0, 2, 0, 255, 255]));

        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('lists RAP layer folders that contain index.json', async () => {
        const noIndexDir = path.join(rapDir, 'NoIndex');
        await fs.promises.mkdir(noIndexDir);

        const res = await request(app).get('/rap/layers').expect(200);

        expect(res.body).toEqual(['CAPE_0-3km', 'Temperature_2m']);
    });

    it('returns timestamps from object index format', async () => {
        const res = await request(app).get('/rap/fetch?layer=Temperature_2m').expect(200);
        expect(res.body).toEqual([timestamp]);
    });

    it('returns timestamps from array index format', async () => {
        const res = await request(app).get('/rap/fetch?layer=CAPE_0-3km').expect(200);
        expect(res.body).toEqual(['20260427-110000']);
    });

    it('returns an empty timestamp list when an existing layer has no index yet', async () => {
        const noIndexDir = path.join(rapDir, 'UWind_925mb');
        await fs.promises.mkdir(noIndexDir);

        const res = await request(app).get('/rap/fetch?layer=UWind_925mb').expect(200);
        expect(res.body).toEqual([]);
    });

    it('returns metadata JSON for a RAP layer timestamp', async () => {
        const res = await request(app)
            .get(`/rap/metadata?layer=Temperature_2m&timestamp=${timestamp}`)
            .expect(200);

        expect(res.body).toEqual(metadata);
    });

    it('serves binary data with Uint16 decode headers', async () => {
        const res = await request(app)
            .get(`/rap/data?layer=Temperature_2m&timestamp=${timestamp}`)
            .buffer(true)
            .parse(parseBinary)
            .expect(200);

        expect(Buffer.isBuffer(res.body)).toBe(true);
        expect(Array.from(res.body)).toEqual([1, 0, 2, 0, 255, 255]);
        expect(res.headers['content-type']).toContain('application/octet-stream');
        expect(res.headers['content-disposition']).toBe(`inline; filename="Temperature_2m_${timestamp}.u16"`);
        expect(res.headers['x-data-type']).toBe('uint16');
        expect(res.headers['x-byte-order']).toBe('little_endian');
        expect(res.headers['x-missing-value']).toBe('65535');
        expect(res.headers['x-grid-ni']).toBe('3');
        expect(res.headers['x-grid-nj']).toBe('2');
        expect(res.headers['x-scale-min']).toBe('180');
        expect(res.headers['x-scale-max']).toBe('330');
        expect(res.headers['x-units']).toBe('K');
    });

    it('returns 400 when layer is missing', async () => {
        const res = await request(app).get('/rap/fetch').expect(400);
        expect(res.body.error).toContain('Missing layer');
    });

    it('returns 400 for invalid layer characters', async () => {
        const res = await request(app).get('/rap/fetch?layer=Bad Layer').expect(400);
        expect(res.body.error).toContain('Invalid layer');
    });

    it('returns 400 for layer traversal attempts', async () => {
        const res = await request(app).get('/rap/fetch?layer=../Temperature_2m').expect(400);
        expect(res.body.error).toContain('Invalid layer');
    });

    it('returns 400 for invalid timestamp format', async () => {
        const res = await request(app)
            .get('/rap/metadata?layer=Temperature_2m&timestamp=20260427-1200')
            .expect(400);

        expect(res.body.error).toContain('Invalid timestamp');
    });

    it('returns 400 for timestamp traversal attempts', async () => {
        const res = await request(app)
            .get('/rap/data?layer=Temperature_2m&timestamp=../20260427-120000')
            .expect(400);

        expect(res.body.error).toContain('Invalid timestamp');
    });

    it('returns 404 for a missing layer folder', async () => {
        const res = await request(app).get('/rap/fetch?layer=MissingLayer').expect(404);
        expect(res.body.error).toContain('Layer not found');
    });

    it('returns 404 for missing metadata', async () => {
        await fs.promises.unlink(path.join(timestampDir, 'metadata.json'));

        const res = await request(app)
            .get(`/rap/metadata?layer=Temperature_2m&timestamp=${timestamp}`)
            .expect(404);

        expect(res.body.error).toContain('Metadata not found');
    });

    it('returns 404 for missing data file', async () => {
        await fs.promises.unlink(path.join(timestampDir, 'data.u16'));

        const res = await request(app)
            .get(`/rap/data?layer=Temperature_2m&timestamp=${timestamp}`)
            .expect(404);

        expect(res.body.error).toContain('Data file not found');
    });
});

describe('EWMRS NEXRAD routes', () => {
    let app, tempDir, nexradRoot;
    const site = 'KAAA';
    const variable = 'DBZH';
    const supportedVariables = ['CCORH', 'DBZH', 'PHIDP', 'RHOHV', 'VRADH', 'WRADH', 'ZDR'];
    const tsA = '20260507-150000';
    const tsB = '20260507-145500';

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'ewmrs-nexrad-'));
        nexradRoot = path.join(tempDir, 'gui', 'NEXRAD');

        const sweep00TsA = path.join(nexradRoot, 'NEXRAD_DBZH_SWEEP_00', site, tsA);
        const sweep00TsB = path.join(nexradRoot, 'NEXRAD_DBZH_SWEEP_00', site, tsB);
        const sweep01TsA = path.join(nexradRoot, 'NEXRAD_DBZH_SWEEP_01', site, tsA);
        const velocitySite = path.join(nexradRoot, 'NEXRAD_VRADH_SWEEP_00', 'KBBB', tsA);

        await fs.promises.mkdir(sweep00TsA, { recursive: true });
        await fs.promises.mkdir(sweep00TsB, { recursive: true });
        await fs.promises.mkdir(sweep01TsA, { recursive: true });
        await fs.promises.mkdir(velocitySite, { recursive: true });

        await fs.promises.writeFile(path.join(sweep00TsA, 'azimuths.f32'), Buffer.from([1, 2, 3, 4]));
        await fs.promises.writeFile(path.join(sweep00TsA, 'ranges.f32'), Buffer.from([5, 6, 7, 8]));
        await fs.promises.writeFile(path.join(sweep00TsA, 'data.f16.gz'), zlib.gzipSync(Buffer.from([9, 10, 11, 12])));
        await fs.promises.writeFile(path.join(sweep01TsA, 'data.f16.gz'), zlib.gzipSync(Buffer.from([99])));
        await fs.promises.writeFile(path.join(sweep00TsB, 'data.f16.gz'), zlib.gzipSync(Buffer.from([77])));

        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('lists unique sites for a variable', async () => {
        const res = await request(app).get('/nexrad/sites?variable=DBZH').expect(200);
        expect(res.body).toEqual(['KAAA']);
    });

    it('lists all supported NEXRAD variables', async () => {
        const res = await request(app).get('/nexrad/variables').expect(200);
        expect(res.body).toEqual(supportedVariables);
    });

    it('accepts the full supported NEXRAD variable set', async () => {
        for (const supportedVariable of supportedVariables) {
            const res = await request(app).get(`/nexrad/sites?variable=${supportedVariable}`).expect(200);
            expect(Array.isArray(res.body)).toBe(true);
        }
    });

    it('lists timestamps for a site and variable in descending order', async () => {
        const res = await request(app)
            .get('/nexrad/timestamps?variable=DBZH&site=KAAA')
            .expect(200);
        expect(res.body).toEqual([tsA, tsB]);
    });

    it('filters timestamps by sweep when provided', async () => {
        const res = await request(app)
            .get('/nexrad/timestamps?variable=DBZH&site=KAAA&sweep=01')
            .expect(200);
        expect(res.body).toEqual([tsA]);
    });

    it('downloads azimuths file bytes', async () => {
        const res = await request(app)
            .get(`/nexrad/download?variable=${variable}&site=${site}&timestamp=${tsA}&file=azimuths&sweep=00`)
            .buffer(true)
            .parse(parseBinary)
            .expect(200);

        expect(Array.from(res.body)).toEqual([1, 2, 3, 4]);
        expect(res.headers['content-type']).toContain('application/octet-stream');
    });

    it('downloads ranges file bytes', async () => {
        const res = await request(app)
            .get(`/nexrad/download?variable=${variable}&site=${site}&timestamp=${tsA}&file=ranges&sweep=00`)
            .buffer(true)
            .parse(parseBinary)
            .expect(200);

        expect(Array.from(res.body)).toEqual([5, 6, 7, 8]);
    });

    it('downloads data file bytes and auto-selects newest sweep when omitted', async () => {
        const res = await request(app)
            .get(`/nexrad/download?variable=${variable}&site=${site}&timestamp=${tsA}&file=data`)
            .buffer(true)
            .parse(parseBinary)
            .expect(200);

        expect(Array.from(res.body)).toEqual([99]);
        expect(res.headers['content-encoding']).toBe('gzip');
        expect(res.headers['content-disposition']).toContain('SWEEP_01');
        expect(res.headers['content-disposition']).toContain('data.f16.gz');
    });

    it('returns 400 for invalid variable parameter', async () => {
        const res = await request(app).get('/nexrad/sites?variable=BAD').expect(400);
        expect(res.body.error).toContain('Invalid variable');
    });

    it('returns 400 for invalid site parameter', async () => {
        const res = await request(app).get('/nexrad/timestamps?variable=DBZH&site=../KAAA').expect(400);
        expect(res.body.error).toContain('Invalid site');
    });

    it('returns 400 for invalid sweep parameter', async () => {
        const res = await request(app).get('/nexrad/timestamps?variable=DBZH&site=KAAA&sweep=1').expect(400);
        expect(res.body.error).toContain('Invalid sweep');
    });

    it('returns 400 for invalid timestamp parameter', async () => {
        const res = await request(app)
            .get('/nexrad/download?variable=DBZH&site=KAAA&timestamp=20260507&file=data')
            .expect(400);
        expect(res.body.error).toContain('Invalid timestamp');
    });

    it('returns 400 for invalid file selector', async () => {
        const res = await request(app)
            .get(`/nexrad/download?variable=${variable}&site=${site}&timestamp=${tsA}&file=unknown`)
            .expect(400);
        expect(res.body.error).toContain('Invalid file');
    });

    it('returns 404 when requested file is missing', async () => {
        const res = await request(app)
            .get('/nexrad/download?variable=DBZH&site=KAAA&timestamp=20260507-140000&file=data&sweep=00')
            .expect(404);
        expect(res.body.error).toContain('not found');
    });
});

describe('GET /colormaps/', () => {
    let app;
    const RAP_COLORMAP_NAMES = [
        'RAP_Temperature',
        'RAP_Dewpoint_2m',
        'RAP_RelativeHumidity',
        'RAP_ThetaE_Surface',
        'RAP_Wind_LL',
        'RAP_Wind_HL',
        'RAP_CAPE',
        'RAP_SRH',
        'RAP_CIN_Surface',
        'RAP_MLCIN',
        'RAP_MUCIN',
        'RAP_SnowDepth_Surface',
        'RAP_SnowWaterEquivalent_Surface',
        'RAP_WetBulbZeroHeight',
        'RAP_FreezingLevelHeight',
        'RAP_LiftedIndex_Surface_500_1000mb',
        'RAP_BestLiftedIndex_180_0mbAGL',
        'RAP_AbsoluteVorticity_500mb'
    ];

    beforeEach(() => {
        app = express();
        app.use('/colormaps', colormapsRouter);
    });

    it('returns colormaps.json content', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        expect(Array.isArray(res.body)).toBe(true);
        expect(res.body.length).toBeGreaterThan(0);
        expect(res.body[0]).toHaveProperty('colormaps');
    });

    it('includes NWS_Reflectivity colormap', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const cmap = res.body[0].colormaps.find(c => c.name === 'NWS_Reflectivity');
        expect(cmap).toBeDefined();
        expect(cmap.interpolate).toBe(true);
        expect(cmap.thresholds.length).toBeGreaterThan(0);
    });

    it('includes GOES_RGB_Raw colormap for visible GOES channels', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const cmap = res.body[0].colormaps.find(c => c.name === 'GOES_RGB_Raw');
        expect(cmap).toBeDefined();
        expect(cmap.interpolate).toBe(true);
        expect(cmap.units).toBe('1');
    });

    it('includes GOES_IR colormap', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const cmap = res.body[0].colormaps.find(c => c.name === 'GOES_IR');
        expect(cmap).toBeDefined();
        expect(cmap.interpolate).toBe(true);
        expect(cmap.units).toBe('K');
    });

    it('includes GOES_ABI_C12_BrightnessTemp colormap', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const cmap = res.body[0].colormaps.find(c => c.name === 'GOES_ABI_C12_BrightnessTemp');
        expect(cmap).toBeDefined();
        expect(cmap.interpolate).toBe(true);
        expect(cmap.units).toBe('K');
    });

    it('includes RAP colormaps for all exported RAP colormap names with valid schema', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const byName = new Map(res.body[0].colormaps.map(c => [c.name, c]));

        for (const colormapName of RAP_COLORMAP_NAMES) {
            const cmap = byName.get(colormapName);
            expect(cmap).toBeDefined();
            expect(typeof cmap.units).toBe('string');
            expect(Array.isArray(cmap.range)).toBe(true);
            expect(cmap.range).toHaveLength(2);
            expect(Array.isArray(cmap.thresholds)).toBe(true);
            expect(cmap.thresholds.length).toBeGreaterThan(0);
        }
    });

    it('uses expected RAP source-family tags for representative layers', async () => {
        const res = await request(app).get('/colormaps/').expect(200);
        const byName = new Map(res.body[0].colormaps.map(c => [c.name, c]));

        expect(byName.get('RAP_CAPE').type).toBe('RAP_GEMPAK_CAPE_UPC');
        expect(byName.get('RAP_CIN_Surface').type).toBe('RAP_GEMPAK_GDPICINH');
        expect(byName.get('RAP_LiftedIndex_Surface_500_1000mb').type).toBe('RAP_GEMPAK_LI_UPC');
        expect(byName.get('RAP_SnowWaterEquivalent_Surface').type).toBe('RAP_METPY_PRECIPITATION');
    });
});

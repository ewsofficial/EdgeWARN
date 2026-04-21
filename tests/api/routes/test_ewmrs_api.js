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

import rendersRouter from '../../../src/EWMRS/api/routes/renders.js';
import colormapsRouter from '../../../src/EWMRS/api/routes/colormaps.js';

function createApp(tempDir) {
    const app = express();
    app.locals.GUI_DIR = path.join(tempDir, 'gui');
    app.use('/renders', rendersRouter);
    return app;
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
                endpoints: ['/renders/get-items', '/renders/fetch', '/renders/download', '/healthz', '/colormaps']
            });
        });
    });

    it('returns service name and endpoints', async () => {
        const res = await request(app).get('/').expect(200);
        expect(res.body.service).toBe('EWMRS API');
        expect(res.body.endpoints).toContain('/renders/get-items');
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
                tile_grid: { rows: 14, cols: 28, tile_size: 250 }
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
        await fs.promises.writeFile(
            path.join(productDir, 'index.json'),
            JSON.stringify({
                timestamps: ['20260317-200000'],
                tile_grid: { rows: 14, cols: 28, tile_size: 250 }
            })
        );
        app = createApp(tempDir);
    });

    afterEach(async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true });
    });

    it('returns 400 when x is missing', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&y=0').expect(400);
        expect(res.body.error).toContain('Missing');
    });

    it('returns 400 when y is missing', async () => {
        const res = await request(app).get('/renders/tile?product=CompRefQC&timestamp=20260317-200000&x=0').expect(400);
        expect(res.body.error).toContain('Missing');
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
        expect(res.body.rows).toBe(14);
        expect(res.body.cols).toBe(28);
        expect(res.body.tile_size).toBe(250);
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
        expect(res.body.rows).toBe(14);
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

describe('GET /colormaps/', () => {
    let app;

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
});

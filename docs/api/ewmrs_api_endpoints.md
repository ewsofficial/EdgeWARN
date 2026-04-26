# EWMRS API Endpoints

This document describes the current HTTP routes implemented in `src/EWMRS/api`.

## API Overview

- Base URL: `/`
- Response format: JSON for metadata/list routes, PNG for render downloads/tiles

## Root Endpoints

### GET /

Returns EWMRS API metadata.

Response:

```json
{
  "service": "EWMRS API",
  "base_dir": "...",
  "gui_dir": "...",
  "endpoints": [
    "/renders/get-items",
    "/renders/fetch",
    "/renders/download",
    "/healthz",
    "/colormaps"
  ]
}
```

### GET /healthz

Response:

```json
{
  "ok": true
}
```

## Render Endpoints

### GET /renders/get-items

Returns available render product folders present in `<BASE_DIR>/gui` that are known by the API mapping.

Response:

- `200`: `string[]`

Currently mapped products include MRMS layers and GOES products.

- `CompRefQC`, `EchoTop18`, `EchoTop30`, `RALA`, `Ref0C`, `RefM5C`, `RefM15C`
- `PrecipRate`, `QPE_01H`, `VIL`, `VILDensity`, `VII`, `MESH`
- `AzShearLow`, `AzShearMid`
- `GOES_ABI_C01`, `GOES_ABI_C02`, `GOES_ABI_C03`, `GOES_ABI_C04`, `GOES_ABI_C05`, `GOES_ABI_C06`
- `GOES_ABI_C07`, `GOES_ABI_C08`, `GOES_ABI_C09`, `GOES_ABI_C10`, `GOES_ABI_C11`, `GOES_ABI_C12`
- `GOES_ABI_C13`, `GOES_ABI_C14`, `GOES_ABI_C15`, `GOES_ABI_C16`
- `GOES_RGB_TrueColor`, `GOES_RGB_Airmass`, `GOES_RGB_NighttimeMicrophysics`
- `GOES_RGB_DayCloudPhase`, `GOES_RGB_SimpleWaterVapor`, `GOES_RGB_Sandwich`

### GET /renders/fetch?product={product}

Returns available timestamps for a product from `index.json`.

Behavior:

- Accepts both index formats:
  - old format: `string[]`
  - new format: `{ "timestamps": string[], "tile_grid": { ... } }`

Responses:

- `200`: `string[]`
- `400`: invalid product parameter
- `500`: read/server failure

### GET /renders/download?product={product}&timestamp={YYYYMMDD-HHMMSS}

Resolves a rendered PNG in the legacy non-tiled naming format when that file exists:

- `<GUI_DIR>/<product>/<file_prefix>_{timestamp}.png`

Current GOES and MRMS renderers write tile-first GUI output, update the product-level `index.json`, and write a timestamp-level `index.json` inside each render folder, so tile-aware clients should prefer `/renders/tile` and `/renders/tile-info`.

Transparent tiles are skipped at write time. A timestamp may therefore have fewer tile PNGs than the declared `tile_grid`, or even zero tile PNGs for a fully transparent render.

Responses:

- `200`: PNG image
- `400`: missing/invalid parameters
- `404`: unknown product or file not found

### GET /renders/tile?product={product}&timestamp={YYYYMMDD-HHMMSS}[&x={int}&y={int}]

Supports two modes:

- image mode when both `x` and `y` are supplied
- listing mode when both `x` and `y` are omitted

Image mode downloads a tile PNG from:

- `<GUI_DIR>/<product>/<timestamp>/tile_{x}_{y}.png`

Tile bounds are validated from the timestamp-level `index.json` `tile_grid` when present, with fallback to the product-level `index.json` `tile_grid`.

Current renderer-written GOES and MRMS products persist:

- `rows=10`
- `cols=20`
- `tile_size=350`

If product-level `index.json` is missing, the route falls back to defaults `rows=10`, `cols=20`, `tile_size=350` for coordinate validation.

Listing mode reads `<GUI_DIR>/<product>/<timestamp>/index.json`, filters invalid or out-of-bounds coordinates, sorts by `y` then `x`, and returns the valid tile coordinates as `[x, y]` pairs. It does not dynamically scan the timestamp directory for tile filenames.

Timestamp-level tile index format:

```json
{
  "tiles": [[0, 0], [1, 3], [2, 6]],
  "tile_grid": {
    "rows": 10,
    "cols": 20,
    "tile_size": 350
  }
}
```

Listing response:

```json
{
  "product": "CompRefQC",
  "timestamp": "20260317-200000",
  "tile_grid": {
    "rows": 10,
    "cols": 20,
    "tile_size": 350
  },
  "tiles": [[0, 0], [1, 3], [2, 6]]
}
```

Responses:

- `200`: PNG image in image mode, JSON tile listing in listing mode
- `400`: missing/invalid/out-of-bounds parameters, or only one of `x`/`y` supplied
- `404`: unknown product, missing tile, missing timestamp directory, missing timestamp tile index in listing mode, or timestamp absent from product-level `index.json`

### GET /renders/tile-info?product={product}

Returns tile-grid metadata and timestamps for a product.

Responses:

- `200`:

```json
{
  "product": "CompRefQC",
  "rows": 10,
  "cols": 20,
  "tile_size": 350,
  "timestamps": ["20260317-200000"]
}
```

- `400`: invalid product parameter
- `404`: unknown product
- `500`: read/server failure

## Colormap Endpoint

### GET /colormaps

Returns `src/EWMRS/colormaps.json`.

Responses:

- `200`: `array` of colormap source blocks
- `500`: read/server failure

## GOES Product Notes

GOES products available through the render routes are the GUI folder names below.

### Single-Channel GOES Products

| Product | Backing render |
| --- | --- |
| `GOES_ABI_C01` | `GOES_ABI_C01_Reflectance` |
| `GOES_ABI_C02` | `GOES_ABI_C02_Reflectance` |
| `GOES_ABI_C03` | `GOES_ABI_C03_Reflectance` |
| `GOES_ABI_C04` | `GOES_ABI_C04_Reflectance` |
| `GOES_ABI_C05` | `GOES_ABI_C05_Reflectance` |
| `GOES_ABI_C06` | `GOES_ABI_C06_Reflectance` |
| `GOES_ABI_C07` | `GOES_ABI_C07_BrightnessTemp` |
| `GOES_ABI_C08` | `GOES_ABI_C08_BrightnessTemp` |
| `GOES_ABI_C09` | `GOES_ABI_C09_BrightnessTemp` |
| `GOES_ABI_C10` | `GOES_ABI_C10_BrightnessTemp` |
| `GOES_ABI_C11` | `GOES_ABI_C11_BrightnessTemp` |
| `GOES_ABI_C12` | `GOES_ABI_C12_BrightnessTemp` |
| `GOES_ABI_C13` | `GOES_ABI_C13_BrightnessTemp` |
| `GOES_ABI_C14` | `GOES_ABI_C14_BrightnessTemp` |
| `GOES_ABI_C15` | `GOES_ABI_C15_BrightnessTemp` |
| `GOES_ABI_C16` | `GOES_ABI_C16_BrightnessTemp` |

### GOES RGB Products

| Product | Recipe channels |
| --- | --- |
| `GOES_RGB_TrueColor` | `C01`, `C02`, `C03`, `C07` |
| `GOES_RGB_Airmass` | `C08`, `C10`, `C12`, `C13` |
| `GOES_RGB_NighttimeMicrophysics` | `C07`, `C13`, `C15` |
| `GOES_RGB_DayCloudPhase` | `C02`, `C05`, `C13` |
| `GOES_RGB_SimpleWaterVapor` | `C08`, `C10`, `C13` |
| `GOES_RGB_Sandwich` | `C02`, `C13` |

Behavior notes:

- GOES products are rendered from locally staged `ABI-L1b-RadC` inputs on a CONUS `EPSG:3857` target grid.
- RGB recipes are skipped individually when a required channel is missing or too far from the selected batch timestamp.
- See `docs/core/goes_pipeline.md` for the full ingest, readiness, and render pipeline.

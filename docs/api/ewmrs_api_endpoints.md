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

Currently mapped products include MRMS layers and GOES ABI layers:

- `CompRefQC`, `EchoTop18`, `EchoTop30`, `RALA`, `Ref0C`, `RefM5C`, `RefM15C`
- `PrecipRate`, `QPE_01H`, `VIL`, `VILDensity`, `VII`, `MESH`
- `AzShearLow`, `AzShearMid`
- `GOES_ABI_C02`, `GOES_ABI_C13`

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

Downloads a rendered PNG in legacy non-tiled naming format:

- `<GUI_DIR>/<product>/<file_prefix>_{timestamp}.png`

Responses:

- `200`: PNG image
- `400`: missing/invalid parameters
- `404`: unknown product or file not found

### GET /renders/tile?product={product}&timestamp={YYYYMMDD-HHMMSS}&x={int}&y={int}

Downloads a tile PNG from:

- `<GUI_DIR>/<product>/<timestamp>/tile_{x}_{y}.png`

Tile bounds are validated from `index.json` `tile_grid` when present, else defaults `rows=14`, `cols=28`, `tile_size=250`.

Responses:

- `200`: PNG image
- `400`: missing/invalid/out-of-bounds parameters
- `404`: unknown product or tile not found

### GET /renders/tile-info?product={product}

Returns tile-grid metadata and timestamps for a product.

Responses:

- `200`:

```json
{
  "product": "CompRefQC",
  "rows": 14,
  "cols": 28,
  "tile_size": 250,
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

# EWMRS API Endpoints

This document describes the current HTTP routes implemented in `src/EWMRS/api`.

## API Overview

- Base URL: `/`
- Response format: JSON for metadata/list routes, PNG for render downloads/tiles, raw binary for RAP Uint16 arrays

Runtime configuration:

- Base directory resolution order: `--base_dir`, then `BASE_DIR`, then platform default
- Debug mode flags: `--debug-server` and `--debug_server`
- Default port: `3003`
- Debug port: `3004` unless `PORT` is set

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
      "/rap/layers",
      "/rap/fetch",
      "/rap/metadata",
      "/rap/data",
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

Rate limiting:

- Default global limits are `30` requests per second and `1800` requests per minute
- CLI overrides: `--ewmrs-rate-limit-1s <count>` and `--ewmrs-rate-limit-1m <count>`
- A value of `0` disables that rate-limit window
- Environment overrides: `EWMRS_RATE_LIMIT_MAX_SEC` and `EWMRS_RATE_LIMIT_MAX_MIN`

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

## RAP Uint16 Endpoints

RAP Uint16 outputs are served from `<BASE_DIR>/gui/RAP`. These routes intentionally remain separate from `/renders/*` because RAP outputs are raw arrays plus metadata, not PNG renders or tiles.

Runtime layout:

```text
<BASE_DIR>/gui/RAP/<LayerFolder>/index.json
<BASE_DIR>/gui/RAP/<LayerFolder>/<YYYYMMDD-HHMMSS>/data.u16
<BASE_DIR>/gui/RAP/<LayerFolder>/<YYYYMMDD-HHMMSS>/metadata.json
```

`data.u16` contains raw little-endian `uint16` values. The reserved missing/no-data value is `65535`. Clients must fetch `metadata.json` to decode the array shape, scale, units, GRIB metadata, and `colormap_key`.

Layer names are on-disk folder names, for example `Temperature_2m`, `CAPE_0-3km`, `UWind_925mb`, `SRH-0_1km`, or `BestLiftedIndex_180-0mbAGL`. They are not derived from Python `RAP_` layer identifiers.

### GET /rap/layers

Returns available RAP layer folders under `<BASE_DIR>/gui/RAP` that contain an `index.json` file.

Responses:

- `200`: `string[]`
- `500`: read/server failure

Example:

```json
["CAPE_0-3km", "Temperature_2m", "UWind_925mb"]
```

### GET /rap/fetch?layer={layer}

Returns available RAP timestamps for a layer from `<BASE_DIR>/gui/RAP/<layer>/index.json`.

Accepted index formats:

- `string[]`
- `{ "timestamps": string[] }`

Responses:

- `200`: `string[]`; returns `[]` when the layer folder exists but `index.json` has not been written yet
- `400`: missing or invalid `layer`
- `404`: layer folder not found
- `500`: JSON parse/read/server failure

### GET /rap/metadata?layer={layer}&timestamp={YYYYMMDD-HHMMSS}

Returns parsed `metadata.json` for the selected layer timestamp.

Example metadata:

```json
{
  "layer": "Temperature_2m",
  "timestamp": "20260427-120000",
  "shape": [337, 451],
  "grid": {
    "ni": 451,
    "nj": 337,
    "point_count": 151987
  },
  "dtype": "uint16",
  "byte_order": "little_endian",
  "scale": {
    "min": 180.0,
    "max": 330.0
  },
  "missing_value": 65535,
  "units": "K",
  "colormap_key": "RAP_Temperature_2m",
  "grib": {
    "shortName": "2t",
    "typeOfLevel": "heightAboveGround",
    "level": 2
  }
}
```

Responses:

- `200`: metadata JSON payload
- `400`: missing/invalid `layer` or `timestamp`
- `404`: layer folder, timestamp folder, or `metadata.json` not found
- `500`: JSON parse/read/server failure

`colormap_key` matches the RAP layer identifier configured in the converter and maps directly to a same-named entry from `GET /colormaps`. RAP colormaps use NOAA/SPC/GEMPAK lineage palettes where practical source tables exist, with documented project fallbacks for variables without a usable standard reference.

### GET /rap/data?layer={layer}&timestamp={YYYYMMDD-HHMMSS}

Streams the raw `<BASE_DIR>/gui/RAP/<layer>/<timestamp>/data.u16` bytes exactly as written by the RAP Uint16 converter.

Response headers include:

- `Content-Type: application/octet-stream`
- `Content-Disposition: inline; filename="{layer}_{timestamp}.u16"`
- `X-Data-Type: uint16`
- `X-Byte-Order: little_endian`
- `X-Missing-Value: 65535`

When `metadata.json` is present, decode headers are also included when available:

- `X-Grid-Ni`
- `X-Grid-Nj`
- `X-Scale-Min`
- `X-Scale-Max`
- `X-Units`

Responses:

- `200`: raw binary `data.u16` payload
- `400`: missing/invalid `layer` or `timestamp`
- `404`: layer folder, timestamp folder, or `data.u16` not found
- `500`: read/server failure

Decode formula:

```text
if value == 65535:
  decoded = missing
else:
  decoded = scale.min + (value / 65534) * (scale.max - scale.min)
```

## NEXRAD Polar Intermediate Endpoints

NEXRAD intermediate outputs are served from:

```text
<BASE_DIR>/gui/NEXRAD/<SITE>/<YYYYMMDD-HHMMSS>/<ELEVATION>/<VARIABLE>/
  azimuths.f32
  ranges.f32
  data.f16.gz
```

Operational mapping currently writes low paired sweeps into canonical elevation folders:

- `sweep 00/01 -> 0.5`
- `sweep 02/03 -> 0.9`

For paired low sweeps, `DBZH` is persisted only from `contiguous_surveillance` and skipped for `contiguous_doppler`.

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

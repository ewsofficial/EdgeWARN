# GOES Pipeline

This document describes the current GOES-East ingest, readiness, rendering, and API-serving flow used by EWMRS.

## Scope

The GOES pipeline in this repository currently supports:

- decoupled realtime ingest of GOES-East ABI CONUS radiance data
- local readiness checks for EWMRS rendering
- single-channel ABI rendering for channels `C01` through `C16`
- six derived RGB composites built from staged ABI channels
- tile-first GUI output for the EWMRS API

EdgeWARN integration still treats GOES differently from EWMRS rendering: EWMRS GOES readiness is based on locally staged ABI render inputs, while EdgeWARN integration separately checks GLM availability.

## Source Data

Realtime GOES ingest is driven from `src/run.py` and `src/common/ingest/mrms/config.py`.

- Bucket: `noaa-goes19`
- ABI product: `ABI-L1b-RadC`
- GLM product: `GLM-L2-LCFA`
- ABI channel coverage: `C01` through `C16`

Each ABI channel is staged into its own runtime directory under the configured base directory. Those staged files are the source of truth for later readiness checks and rendering.

## Generated Products

### Single-Channel Products

These products are written to GUI folders exposed through the EWMRS API as `product` values.

| API product | Internal render name | Derived field |
| --- | --- | --- |
| `GOES_ABI_C01` | `GOES_ABI_C01_Reflectance` | Reflectance |
| `GOES_ABI_C02` | `GOES_ABI_C02_Reflectance` | Reflectance |
| `GOES_ABI_C03` | `GOES_ABI_C03_Reflectance` | Reflectance |
| `GOES_ABI_C04` | `GOES_ABI_C04_Reflectance` | Reflectance |
| `GOES_ABI_C05` | `GOES_ABI_C05_Reflectance` | Reflectance |
| `GOES_ABI_C06` | `GOES_ABI_C06_Reflectance` | Reflectance |
| `GOES_ABI_C07` | `GOES_ABI_C07_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C08` | `GOES_ABI_C08_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C09` | `GOES_ABI_C09_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C10` | `GOES_ABI_C10_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C11` | `GOES_ABI_C11_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C12` | `GOES_ABI_C12_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C13` | `GOES_ABI_C13_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C14` | `GOES_ABI_C14_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C15` | `GOES_ABI_C15_BrightnessTemp` | Brightness temperature |
| `GOES_ABI_C16` | `GOES_ABI_C16_BrightnessTemp` | Brightness temperature |

### RGB Composites

These products are also exposed through the EWMRS API as `product` values.

| API product | Recipe | Required channels |
| --- | --- | --- |
| `GOES_RGB_TrueColor` | True Color RGB | `C01`, `C02`, `C03`, `C07` |
| `GOES_RGB_Airmass` | Airmass RGB | `C08`, `C10`, `C12`, `C13` |
| `GOES_RGB_NighttimeMicrophysics` | Nighttime Microphysics RGB | `C07`, `C13`, `C15` |
| `GOES_RGB_DayCloudPhase` | Day Cloud Phase | `C02`, `C05`, `C13` |
| `GOES_RGB_SimpleWaterVapor` | Simple Water Vapor RGB | `C08`, `C10`, `C13` |
| `GOES_RGB_Sandwich` | Sandwich RGB | `C02`, `C13` |

## Realtime Flow

### 1. Background ingest

`src/run.py` starts a dedicated `goes_loop()` process that:

1. builds the full ABI channel spec list from `get_abi_radc_channel_specs()`
2. downloads GOES files on a 60-second poll cadence
3. prefers async ingest and falls back to sync ingest on failure
4. marks the GOES cycle active while staging is in progress

This loop is decoupled from the shared MRMS ingest cycle so GOES ingest does not block MRMS detection or MRMS-backed rendering.

### 2. Readiness checks

Local GOES readiness is implemented in `src/common/pipeline/goes_readiness.py`.

Current behavior:

- readiness is computed from the configured EWMRS GOES scalar layer set, which currently spans `GOES_ABI_C01` through `GOES_ABI_C16`
- the helper searches the latest staged files per channel and accepts the nearest file whose scan window is within `20` minutes of the target time
- GOES filenames that encode `s..._e...` scan windows are treated as valid across the whole scan interval, not as a single instant
- if any configured ABI channel is missing locally, EWMRS GOES readiness fails for that cycle

This makes the decoupled GOES render phase wait for a complete local ABI set before rendering begins.

### 3. Render task scheduling

When local readiness succeeds, `src/run.py` queues a decoupled GOES render task for `goes_render_loop()`.

Queue behavior is intentionally latest-wins:

- stale queued GOES render tasks are dropped
- only the freshest queued cycle is rendered
- rendering can optionally pause background ingest if `EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER` is enabled

This keeps the render queue from falling behind during busy periods.

## Render Pipeline

`src/EWMRS/pipeline.py` drives GOES rendering through `run_goes_render_pipeline()`.

The current implementation uses a unified cycle when both scalar GOES layers and RGB layers are enabled.

### Projection and grid

GOES products are reprojected from the native ABI fixed grid into a CONUS-focused `EPSG:3857` target grid.

- output raster shape: `3500 x 7000`
- tile grid written by the renderer: `10 x 20`
- tile size: `350` pixels
- tile grid capacity per completed product: `200`

The renderers persist the tile grid into each product `index.json` and each timestamp folder's `index.json`. The EWMRS API falls back to the current `10 x 20` / `350px` grid when the product-level `index.json` is missing.

### Single-channel render path

For `source_type="goes_abi"` layers, the pipeline:

1. selects the staged source file for the channel
2. extracts the GOES timestamp from the filename
3. loads the ABI radiance payload from `CMI` with `Rad` fallback
4. converts radiance to reflectance for `C01` through `C06`, or to brightness temperature for `C07` through `C16`
5. reprojects the normalized array into the GOES Web Mercator target grid
6. applies the configured colormap
7. writes tiled GUI PNGs, updates product-level `index.json`, and writes timestamp-level `index.json`

### RGB render path

For `source_type="goes_abi_rgb"` layers, the pipeline:

1. discovers the latest staged files for the required channels
2. computes a batch timestamp from the newest common candidate set
3. rejects a recipe when any required channel is missing or more than `20` minutes away from that batch timestamp
4. reuses shared per-channel reprojection results through a cycle registry
5. composes recipe-specific RGB arrays
6. writes tiled RGBA output, updates product-level `index.json`, and writes timestamp-level `index.json`

Only the affected recipe is skipped when one RGB dependency is missing or too far from the target timestamp.

### Shared registry and cache reuse

The unified GOES render cycle avoids redundant work in two ways:

- completed tile directories are reused when the timestamp directory exists, the product-level `index.json` contains the timestamp, and the timestamp-level `index.json` lists valid tiles, even if only a sparse subset of non-transparent tiles was written
- reprojected channel arrays are cached in a cycle registry so scalar layers and RGB recipes can share the same prepared channel payloads

This is especially important for channels such as `C02` and `C13`, which are used both as standalone products and as RGB inputs.

## Output Layout

GOES GUI products are written under `<BASE_DIR>/gui`.

Examples:

```text
<BASE_DIR>/gui/GOES_ABI_C13/
├── 20260423-124000/
│   ├── tile_0_0.png
│   ├── ...
│   ├── tile_19_9.png
│   └── index.json
└── index.json

<BASE_DIR>/gui/GOES_RGB_TrueColor/
├── 20260423-124000/
│   ├── tile_0_0.png
│   ├── ...
│   ├── tile_19_9.png
│   └── index.json
└── index.json
```

Current product-level `index.json` format for rendered products is:

```json
{
  "timestamps": ["20260423-124000"],
  "tile_grid": {
    "rows": 10,
    "cols": 20,
    "tile_size": 350
  }
}
```

Current timestamp-level `index.json` format is:

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

## API Exposure

The EWMRS API under `src/EWMRS/api` serves GOES products through:

- `GET /renders/get-items`
- `GET /renders/fetch?product={product}`
- `GET /renders/download?product={product}&timestamp={YYYYMMDD-HHMMSS}`
- `GET /renders/tile?product={product}&timestamp={YYYYMMDD-HHMMSS}[&x={int}&y={int}]`
- `GET /renders/tile-info?product={product}`

Notes:

- `product` values are the GUI folder names shown in the tables above
- tile clients should prefer `/renders/tile` and `/renders/tile-info`
- omitting `x` and `y` from `/renders/tile` reads the timestamp-level `index.json` and returns the listed valid tile coordinates
- `/renders/download` resolves the legacy flat PNG naming contract when such files exist, but the current GOES renderer writes tile-first output

See `docs/api/ewmrs_api_endpoints.md` for route-level behavior.

## Failure Semantics

The GOES pipeline is designed to degrade per layer instead of failing the entire cycle.

- a missing scalar channel causes that layer to return `None`
- a missing RGB dependency skips only the affected recipe
- stale queued render tasks are dropped in favor of the latest cycle
- cleanup runs after GOES rendering and remains constrained to the configured GUI base directory

This keeps MRMS rendering and the rest of the tandem pipeline moving even when some GOES inputs are late or absent.

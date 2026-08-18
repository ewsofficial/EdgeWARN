# EWMRS Compatibility Endpoints

These legacy EWMRS routes are compatibility adapters mounted by the unified
`src/api/` service. New clients should use the `/api/v3` resources documented in
`docs/api/api_endpoints.md`.

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
    "/nexrad",
    "/rap/layers",
    "/rap/fetch",
    "/rap/metadata",
    "/rap/data",
    "/healthz",
    "/colormaps"
  ]
}
```

The `endpoints` array advertised at `/` is a curated subset and does not enumerate every mounted route. The server also mounts `/renders/tile`, `/renders/tile-info`, `/rap/mappings`, and the `/wpc` router; those are documented in their own sections below.

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

### GET /renders/fetch?product={product}

Returns available timestamps for a product from `index.json`.

Behavior:

- Accepts both index formats:
  - old format: `string[]`
  - new format: `{ "timestamps": string[], "tile_grid": { ... } }`

Responses:

- `200`: `string[]`
- `400`: missing product parameter
- `404`: invalid path-like product or unknown product
- `200`: `[]` when the mapped product has no `index.json` yet
- `500`: read/server failure

### GET /renders/download?product={product}&timestamp={YYYYMMDD-HHMMSS}

Resolves a rendered PNG in the legacy non-tiled naming format when that file exists:

- `<GUI_DIR>/<product>/<file_prefix>_{timestamp}.png`

This legacy route remains PNG-only. Current GOES and MRMS renderers publish
binary float16 value chunks through the unified v3 `/api/v3/render-products/.../chunks`
resources; a missing compatibility PNG returns `404` rather than binary bytes
under the PNG contract. PNG compatibility responses include `Deprecation: true`,
a `Sunset: Thu, 31 Dec 2026 23:59:59 GMT` header, and a successor-version link.

Transparent tiles are skipped at write time. A timestamp may therefore have fewer tile PNGs than the declared `tile_grid`, or even zero tile PNGs for a fully transparent render.

Responses:

- `200`: PNG image
- `400`: missing/invalid parameters
- `404`: unknown product or file not found

### GET /renders/tile?product={product}&timestamp={YYYYMMDD-HHMMSS}[&x={int}&y={int}]

Supports two modes:

- image mode when both `x` and `y` are supplied
- listing mode when both `x` and `y` are omitted

Image mode downloads a compatibility tile PNG from:

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
- `200`: default tile-grid metadata with `timestamps: []` when the mapped product has no `index.json` yet
- `500`: read/server failure

## NEXRAD Endpoints

NEXRAD render intermediates are served from `<BASE_DIR>/gui/NEXRAD`.

Runtime layout:

```text
<BASE_DIR>/gui/NEXRAD/<SITE>/<ELEVATION>/<SITE>_<PRODUCT>_<ELEVATION>_<YYYYMMDD-HHMMSS>.bin.gz
```

Allowed products:

- `DBZH`
- `VRADH`
- `WRADH`
- `PHIDP`
- `CCORH`
- `RHOHV`
- `ZDR`

Security and validation rules:

- Site identifiers are normalized to uppercase and must be exactly 4 alphanumeric characters.
- Timestamps must use `YYYYMMDD-HHMMSS` and pass calendar/time validation.
- Elevations must match the regex `^\d{1,3}(?:\.\d{1,2})?$` — common operational labels include `0.5`, `0.9`, `1.2`, `1.3`, `1.8`, `2.4`, and `3.1`.
- Product names must match the exact allowlist above.
- Path traversal attempts and malformed parameters are rejected.
- Unsafe on-disk directory names are ignored during listing.

### GET /nexrad

Returns active radar site folders that contain at least one valid NEXRAD GUI file.

Runtime layout:

```text
<BASE_DIR>/gui/NEXRAD/<SITE>/<ELEVATION>/<SITE>_<PRODUCT>_<ELEVATION>_<YYYYMMDD-HHMMSS>.bin.gz
```

EWMRS populates these files by polling local ingest outputs under `<BASE_DIR>/data/NEXRAD_Level2` every 30 seconds. If a same-timestamp GUI file already exists for an elevation artifact, that artifact is skipped instead of being re-rendered.

Responses:

- `200`: `string[]` with `Cache-Control: public, max-age=5`

Example:

```json
["KTLH", "KTLX"]
```

### GET /nexrad/{site}

Returns valid elevations for one radar site mapped to their available timestamps. Timestamps are parsed from NEXRAD GUI filenames rather than timestamp-named directories.

Responses:

- `200`: object mapping elevation labels to timestamp arrays, `Cache-Control: public, max-age=5`
- `400`: invalid site parameter
- `404`: site not found

Example:

```json
{
  "0.5": ["20260512-004336"],
  "0.9": ["20260512-004336"],
  "1.3": ["20260512-004753"]
}
```

### GET /nexrad/{site}/{timestamp}/{elevation}?product={PRODUCT}

Downloads the requested gzip-compressed NEXRAD binary field file.

Responses:

- `200`: gzip binary payload
- `400`: invalid site, timestamp, elevation, or product parameter
- `404`: site/timestamp/elevation/product file not found

Response headers include:

- `Content-Type: application/gzip`
- `Content-Disposition: attachment; filename="<SITE>_<TIMESTAMP>_<ELEVATION>_<PRODUCT>.bin.gz"`
- `Cache-Control: public, max-age=60`

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

### GET /rap/mappings

Serves `src/EWMRS/mappings.json`, the RAP layer-to-colormap mapping table used by client renderers.

Responses:

- `200`: parsed JSON contents of `mappings.json`
- `404`: `mappings.json` not found
- `500`: read/parse failure

### GET /rap/fetch?layer={layer}

Returns available RAP timestamps for a layer from `<BASE_DIR>/gui/RAP/<layer>/index.json`.

RAP timestamp folders are minute-aligned in the form `YYYYMMDD-HHMM00`.

Accepted index formats:

- `string[]`
- `{ "timestamps": string[] }`

Responses:

- `200`: `string[]`; returns `[]` when the layer folder exists but `index.json` has not been written yet
- `400`: missing or invalid `layer`
- `404`: layer folder not found
- `500`: JSON parse/read/server failure

### GET /rap/metadata?layer={layer}&timestamp={YYYYMMDD-HHMM00}

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

### GET /rap/data?layer={layer}&timestamp={YYYYMMDD-HHMM00}

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
<BASE_DIR>/gui/NEXRAD/<SITE>/<ELEVATION>/<SITE>_<VARIABLE>_<ELEVATION>_<YYYYMMDD-HHMMSS>.bin.gz
```

Each `.bin.gz` payload decompresses to:

```text
[magic bytes EWFFv1S0][azimuth_count uint32 LE][range_count uint32 LE][data float16 LE][azimuths float32 LE][ranges float32 LE]
```

Stored `data` values remain range-major with shape `[range_count, azimuth_count]`.

Operational mapping writes paired sweeps into canonical elevation folders. Per-VCP mappings (from `src/EWMRS/render/nexrad.py`):

| VCP | Sweep indices → elevation label |
| --- | --- |
| `VCP-212` | `0/1 → 0.5`, `2/3 → 0.9`, `4/5 → 1.3`, `6 → 1.8`, `7 → 2.4`, `8 → 3.1` |
| `VCP-215` | `0/1 → 0.5`, `2/3 → 0.9`, `4/5 → 1.2`, `8 → 1.8`, `9 → 2.4`, `10 → 3.1` |
| `VCP-12`  | `0/1 → 0.5`, `2/3 → 0.9`, `4/5 → 1.2`, `6 → 1.8`, `7 → 2.4`, `8 → 3.1` |

For paired low sweeps, `DBZH` is persisted only from `contiguous_surveillance` and skipped for `contiguous_doppler`.

## WPC Surface Analysis Endpoints

WPC outputs are served from:

```text
<BASE_DIR>/wpc/surface_analysis/
```

The current API supports only the surface-analysis type `sfc`.

### GET /wpc/fetch?type=sfc

Lists available timestamped WPC surface-analysis GeoJSON artifacts. The route scans files matching `wpc_sfc_YYYYMMDD-HHMMSS.geojson`, ignores `latest.geojson`, and returns timestamps newest first. The ingest side only ever writes hour-aligned names (`HH0000`), so that is all you will see in practice, but the reader accepts any valid time.

Responses:

- `200`: `string[]`; returns `[]` when the WPC output directory does not exist
- `400`: missing or unsupported `type`
- `500`: directory read failure

Example:

```json
["20260604-120000", "20260604-090000"]
```

### GET /wpc/download?type=sfc&timestamp={YYYYMMDD-HH0000}

Returns the parsed GeoJSON payload from:

```text
<BASE_DIR>/wpc/surface_analysis/wpc_sfc_{timestamp}.geojson
```

WPC surface-analysis artifacts are written on 3-hour analysis times, so the timestamp form is `YYYYMMDD-HH0000`.

Responses:

- `200`: GeoJSON object
- `400`: missing/unsupported `type`, missing timestamp, malformed timestamp, or resolved path escaping the WPC root
- `404`: requested timestamp file not found
- `500`: read/parse failure

## Colormap Endpoint

### GET /colormaps

Returns `src/EWMRS/colormaps.json`.

Responses:

- `200`: `array` of colormap source blocks
- `404`: `colormaps.json` not found
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

Behavior notes:

- GOES products are rendered from locally staged `ABI-L1b-RadC` inputs on a CONUS `EPSG:3857` target grid.
- RGB composites are a client-side derivation and are not rendered server-side.
- See `docs/core/goes_pipeline.md` for the full ingest, readiness, and render pipeline.

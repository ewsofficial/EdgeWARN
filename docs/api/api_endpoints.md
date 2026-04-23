# EdgeWARN API v2 Endpoints

This document describes the current HTTP routes implemented in `src/EdgeWARN/api`.

For backing file schemas, see `docs/api/data_keys.md`.

## API Overview

- Base URL: `/api/v2`
- Response format: JSON
- Version behavior:
  - `2.x` when `NODE_ENV=production`
  - `2.3.0` otherwise

## Root Endpoints

### GET /

Returns API banner metadata.

Response:

```json
{
  "message": "EdgeWARN Backend API",
  "version": "2.3.0"
}
```

### GET /api/v2

Returns API metadata and route map.

Response:

```json
{
  "message": "EdgeWARN API v2",
  "version": "2.3.0",
  "endpoints": {
    "features": {
      "cells": "/api/v2/features/cells[?id={int}]",
      "mesocyclones": "/api/v2/features/mesocyclones[?timestamp={YYYYMMDD-HHMMSS}]",
      "timestamps": "/api/v2/features/timestamps[?timestamp={YYYYMMDD-HHMMSS}]",
      "alerts": {
        "official": "/api/v2/features/alerts/official[?id={urn:oid:...}|timestamp={YYYYMMDD-HHMMSS}]",
        "edgewarn": "/api/v2/features/alerts/edgewarn[?id={id}|timestamp={YYYYMMDD-HHMMSS}]"
      }
    },
    "data": {
      "metar": "/api/v2/data/metar[?timestamp={YYYYMMDD-HHMMSS}]"
    }
  }
}
```

## Feature Endpoints

### GET /api/v2/features/cells

Query:

- `id` (optional): positive integer

Behavior:

- Without `id`: returns `cell_index.json` IDs
- With `id`: returns `cells/{id}.json`

Responses:

- `200` list mode: `number[]`
- `200` id mode: JSON object from file (passthrough)
- `400`: invalid `id` or invalid filename/access
- `404`: cell file not found
- `500`: read/server failure

Cache:

- list: `Cache-Control: public, max-age=5`
- id: `Cache-Control: public, max-age=60`

### GET /api/v2/features/mesocyclones

Query:

- `timestamp` (optional): `YYYYMMDD-HHMMSS`

Behavior:

- Without `timestamp`: returns available snapshot timestamps from `mesocyclones_*.json`
- With `timestamp`: returns `mesocyclones_{timestamp}.json`

Responses:

- `200` list mode: `string[]`
- `200` timestamp mode: mesocyclone payload JSON (passthrough)
- `400`: invalid timestamp or invalid filename/access
- `404`: snapshot not found
- `500`: read/server failure

Cache:

- list: `Cache-Control: public, max-age=5`
- timestamp: `Cache-Control: public, max-age=60`

### GET /api/v2/features/timestamps

Query:

- `timestamp` (optional): `YYYYMMDD-HHMMSS`

Behavior:

- Without `timestamp`: returns `stormcell_index.json` timestamps
- With `timestamp`: returns `stormcells_{timestamp}.json`

Responses:

- `200` list mode: `string[]`
- `200` timestamp mode: stormcell JSON payload (passthrough)
- `400`: invalid timestamp or invalid filename/access
- `404`: snapshot not found
- `500`: read/server failure

Cache:

- list: `Cache-Control: public, max-age=5`
- timestamp: `Cache-Control: public, max-age=3600`

### GET /api/v2/features/alerts/official

### GET /api/v2/features/alerts/edgewarn

Query (mutually exclusive):

- `timestamp` (optional): `YYYYMMDD-HHMMSS`
- `id` (optional): alert identifier string

Behavior:

- Without params: returns available snapshot timestamps
- With `timestamp`: returns snapshot `alerts` array from `{timestamp}.json`
- With `id`: returns a specific alert payload from `ids/{safe_id}.json`

Responses:

- `200` list mode: `string[]`
- `200` timestamp mode (`official`): array of official alert summaries such as:
  - `id`, `name`, `urn_oid`, `effective`, `expires`, `severity`, `geometry`
- `200` timestamp mode (`edgewarn`): array of EdgeWARN alert summaries such as:
  - `id`, `severity`
- `200` id mode:
  - `official`: usually returns nested `feature` object when available
  - `edgewarn`: returns stored alert object
- `400/404/500` error envelope:

```json
{
  "success": false,
  "error": {
    "code": "INVALID_INPUT",
    "message": "..."
  }
}
```

Validation:

- `timestamp` and `id` cannot be sent together
- `timestamp` must match `YYYYMMDD-HHMMSS`
- `id` must pass alert ID validation

Cache:

- list: `Cache-Control: public, max-age=5`
- id/timestamp: `Cache-Control: public, max-age=60`

## Data Endpoints

### GET /api/v2/data/metar

Query:

- `timestamp` (optional): `YYYYMMDD-HHMMSS`

Behavior:

- Without `timestamp`: lists timestamps derived from `METAR_YYYYMMDD-HHz.json`
- With `timestamp`: reads matching hourly file and wraps as:

```json
{
  "type": "metar",
  "timestamp": "YYYYMMDD-HHMMSS",
  "data": []
}
```

Responses:

- `200` list mode: `string[]`
- `200` timestamp mode: wrapper object above
- `400`: invalid timestamp or invalid filename/access
- `404`: hourly METAR file not found
- `500`: read/server failure

Cache:

- list: `Cache-Control: public, max-age=5`
- timestamp: `Cache-Control: public, max-age=60`

## Other Routes

## EWMRS Render Products

The EWMRS service under `src/EWMRS/api` exposes tiled GUI products through:

- `GET /renders/get-items`
- `GET /renders/fetch?product={product}`
- `GET /renders/download?product={product}&timestamp={YYYYMMDD-HHMMSS}`
- `GET /renders/tile?product={product}&timestamp={YYYYMMDD-HHMMSS}&x={int}&y={int}`
- `GET /renders/tile-info?product={product}`

GOES products exposed through those routes include:

- scalar ABI folders `GOES_ABI_C01` through `GOES_ABI_C16`
- RGB folders `GOES_RGB_TrueColor`, `GOES_RGB_Airmass`, `GOES_RGB_NighttimeMicrophysics`, `GOES_RGB_DayCloudPhase`, `GOES_RGB_SimpleWaterVapor`, and `GOES_RGB_Sandwich`

Behavior notes:

- ABI single-channel products and RGB composites are generated from staged `ABI-L1b-RadC` channels on the GOES CONUS `EPSG:3857` tile grid.
- Missing or time-misaligned channels skip only the affected layer or RGB product; other GOES products continue rendering.
- Current GOES renders are tile-first; `/renders/download` only resolves the legacy flat PNG naming contract when such files exist.

See `docs/api/ewmrs_api_endpoints.md` for the full EWMRS route contracts and `docs/core/goes_pipeline.md` for the ingest-to-render flow.

### GET /health

Response:

```json
{
  "status": "OK",
  "timestamp": "2026-01-01T00:00:00.000Z"
}
```

### GET /robots.txt

Serves `src/EdgeWARN/api/robots.txt`.

### Legacy v1-style routes

- `/features/*`
- `/data/*`

Both return `410 Gone` with migration guidance to `/api/v2`.

## Security and Platform Behavior

- Helmet security headers and compression are enabled
- CORS behavior:
  - If `ALLOWED_ORIGINS` is set, that allowlist is used
  - Without `ALLOWED_ORIGINS`, non-production allows all origins
  - Without `ALLOWED_ORIGINS`, production blocks cross-origin requests
- Global rate limiting uses two windows by default:
  - `40` requests per second
  - `2000` requests per minute

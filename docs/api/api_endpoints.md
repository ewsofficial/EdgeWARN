# EdgeWARN API v2 Endpoints

This document describes the currently implemented API routes in `src/EdgeWARN/api`.

## API Overview

- **Base URL**: `/api/v2`
- **Version string**:
  - `2.x` when `NODE_ENV=production`
  - `2.0.0-alpha` otherwise
- **Protocol**: HTTP/HTTPS
- **Response format**: JSON

## Root Endpoints

### GET /

Returns the backend API banner.

Response keys:

- `message` (string): API banner text.
- `version` (string): version label (`2.x` in production, `2.0.0-alpha` otherwise).

```json
{
  "message": "EdgeWARN Backend API",
  "version": "2.0.0-alpha"
}
```

### GET /api/v2

Returns API v2 metadata and endpoint map.

Response keys:

- `message` (string): API group label.
- `version` (string): version label (`2.x` in production, `2.0.0-alpha` otherwise).
- `endpoints` (object): route map.
  - `endpoints.features` (object)
    - `endpoints.features.cells` (string)
    - `endpoints.features.timestamps` (string)
    - `endpoints.features.alerts` (object)
      - `endpoints.features.alerts.official` (string)
      - `endpoints.features.alerts.edgewarn` (string)
  - `endpoints.data` (object)
    - `endpoints.data.metar` (string)

```json
{
  "message": "EdgeWARN API v2",
  "version": "2.0.0-alpha",
  "endpoints": {
    "features": {
      "cells": "/api/v2/features/cells[?id={int}]",
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

## Features Endpoints

### GET /api/v2/features/cells

Query parameters:

- `id` (optional): positive integer

Behavior:

- Without `id`, returns an array of available cell IDs from `cell_index.json`.
- With `id`, returns that cell JSON (`{id}.json`).

Response keys:

- Success (no `id`): array of cell IDs (`number[]`).
- Success (with `id`): passthrough JSON object from `{id}.json` (schema depends on producer data).
- Error `400` (invalid `id`):
  - `error` (string)
- Error `404` (cell not found):
  - `error` (string)
  - `id` (string)
- Error `500`:
  - `error` (string)

Common errors:

- `400` invalid `id`
- `404` cell not found
- `500` file read/server error

Cache headers:

- `max-age=5` (index list)
- `max-age=60` (single cell)

### GET /api/v2/features/timestamps

Query parameters:

- `timestamp` (optional): `YYYYMMDD-HHMMSS`

Behavior:

- Without `timestamp`, returns available timestamps from `stormcell_index.json`.
- With `timestamp`, returns `stormcells_{timestamp}.json`.

Response keys:

- Success (no `timestamp`): array of timestamps (`string[]`, format `YYYYMMDD-HHMMSS`).
- Success (with `timestamp`): passthrough JSON object/array from `stormcells_{timestamp}.json` (schema depends on producer data).
- Error `400` (validation/access):
  - `error` (string)
- Error `404` (snapshot not found):
  - `error` (string)
  - `timestamp` (string)
- Error `500`:
  - `error` (string)

Common errors:

- `400` invalid timestamp
- `404` timestamp file not found
- `500` file read/server error

Cache headers:

- `max-age=5` (timestamp index)
- `max-age=3600` (stormcell snapshot)

### GET /api/v2/features/alerts/official

### GET /api/v2/features/alerts/edgewarn

Query parameters (mutually exclusive):

- `timestamp` (optional): `YYYYMMDD-HHMMSS`
- `id` (optional): alert ID string

Behavior:

- Without params: returns available snapshot timestamps from the alerts timestamp directory.
- With `timestamp`: returns list of alert IDs for that timestamp (`[]` if snapshot missing).
- With `id`: returns full alert feature payload for that ID.

Response keys:

- Success (no params): array of timestamps (`string[]`, format `YYYYMMDD-HHMMSS`).
- Success (`timestamp`): array of alert IDs (`string[]`).
- Success (`id`): alert payload JSON (returns `alert.feature` when present, otherwise full alert object; schema depends on stored alert data).
- Error `400/404/500`:
  - `success` (boolean, always `false`)
  - `error` (object)
    - `error.code` (string)
    - `error.message` (string)

Validation and errors:

- `400` if both `id` and `timestamp` are supplied
- `400` invalid `id` or `timestamp`
- `404` unknown alert `id`
- `500` server error

Notes:

- Alert error responses are wrapped in `{ success: false, error: { code, message } }`.
- Alert IDs are resolved from filename-safe transformations (`:` and `/` replaced with `_`).

Cache headers:

- `max-age=5` (timestamp listing)
- `max-age=60` (`id` and `timestamp` lookups)

## Data Endpoints

### GET /api/v2/data/metar

Query parameters:

- `timestamp` (optional): `YYYYMMDD-HHMMSS`

Behavior:

- Without `timestamp`, returns available METAR timestamps derived from files matching `METAR_YYYYMMDD-HHz.json`.
- With `timestamp`, returns:

```json
{
  "type": "metar",
  "timestamp": "YYYYMMDD-HHMMSS",
  "data": []
}
```

Response keys:

- Success (no `timestamp`): array of timestamps (`string[]`, format `YYYYMMDD-HHMMSS`).
- Success (with `timestamp`):
  - `type` (string): always `"metar"`
  - `timestamp` (string): requested timestamp
  - `data` (object|array): raw METAR payload from hourly file
- Error `400` (validation/access):
  - `error` (string)
- Error `404` (file missing):
  - `error` (string)
  - `timestamp` (string)
- Error `500`:
  - `error` (string)

Common errors:

- `400` invalid timestamp
- `404` METAR file not found
- `500` file read/server error

Cache headers:

- `max-age=5` (timestamp listing)
- `max-age=60` (timestamp file)

## Other Routes

### GET /health

Returns service health:

Response keys:

- `status` (string): always `"OK"`
- `timestamp` (string): ISO-8601 server timestamp

```json
{
  "status": "OK",
  "timestamp": "2023-10-01T12:00:00.123Z"
}
```

### GET /robots.txt

Serves `src/EdgeWARN/api/robots.txt`.

### Legacy v1 routes

- `/features/*`
- `/data/*`

Return `410 Gone` with guidance to use `/api/v2`.

Response keys:

- `error` (string)
- `documentation` (string)

## Security and Platform Behavior

- Rate limiting is enabled globally via `express-rate-limit` (defaults: 60 requests / minute / client key).
- CORS is allowlist-based via `ALLOWED_ORIGINS`.
- Helmet security headers and compression are enabled.
- In production, detailed version strings are intentionally hidden (`2.x`).

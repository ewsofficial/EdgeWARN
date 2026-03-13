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

```json
{
  "message": "EdgeWARN Backend API",
  "version": "2.0.0-alpha"
}
```

### GET /api/v2

Returns API v2 metadata and endpoint map.

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

## Security and Platform Behavior

- Rate limiting is enabled globally via `express-rate-limit` (defaults: 60 requests / minute / client key).
- CORS is allowlist-based via `ALLOWED_ORIGINS`.
- Helmet security headers and compression are enabled.
- In production, detailed version strings are intentionally hidden (`2.x`).

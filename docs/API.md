# EdgeWARN API Documentation

## Overview

This document describes the EdgeWARN Backend API. The current API version is **v2**.

**Base URL:** `http://localhost:5000`

---

## Authentication

Currently, the API does not require authentication.

---

## Endpoints

### 1. Health Check

**GET** `/health`

Returns server health status, uptime, and system resource usage.

#### Response

```json
{
  "status": "OK",
  "uptimeSeconds": 3600,
  "cpu": {
    "cores": 8,
    "usagePercent": 45.23,
    "loadAverage": [2.1, 1.9, 1.7]
  },
  "memory": {
    "rss": 52428800,
    "heapTotal": 10485760,
    "heapUsed": 8388608,
    "external": 1048576,
    "systemTotal": 17179869184,
    "rssPercentOfSystem": 0.31
  }
}
```

---

## API v2 Endpoints

### 2. Features - Cells

**GET** `/api/v2/features/cells`

Returns a list of available cells or a specific cell's data.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | No | Cell ID to fetch specific cell data |

#### Response (without id)

Returns a list of available cell IDs:

```json
[1, 2, 3, 5, 8, 13, 21, 34]
```

#### Response (with id)

Returns the specific cell data:

```json
{
  "id": 123,
  "first_seen": "20260123-120000",
  "last_seen": "20260123-143000",
  "history": [
    {
      "timestamp": "20260123-120000",
      "lat": 35.5,
      "lon": 240.1,
      "intensity": 65.5
    }
  ]
}
```

#### Error Responses

- `400 Bad Request` - Invalid id parameter
- `404 Not Found` - Cell not found
- `500 Internal Server Error` - Server error

---

### 3. Features - Timestamps

**GET** `/api/v2/features/timestamps`

Returns a list of available timestamps or stormcell data for a specific timestamp.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | No | Format: `YYYYMMDD-HHMMSS`. Returns stormcell list for this time |

#### Response (without timestamp)

Returns a list of available timestamps:

```json
[
  "20260123-150000",
  "20260123-143000",
  "20260123-140000"
]
```

#### Response (with timestamp)

Returns the stormcell list for that timestamp:

```json
{
  "timestamp": "20260123-150000",
  "cells": [
    {
      "id": 123,
      "lat": 35.4676,
      "lon": 240.1234,
      "intensity": 65.5,
      "lineage_status": "ACTIVE"
    }
  ]
}
```

#### Error Responses

- `400 Bad Request` - Invalid timestamp format
- `404 Not Found` - Stormcell data not found for timestamp
- `500 Internal Server Error` - Server error

---

### 4. Features - Alerts

**GET** `/api/v2/features/alerts/official`
**GET** `/api/v2/features/alerts/edgewarn`

Returns alert data (either Official NWS alerts or EdgeWARN internal alerts). Supports three modes of operation based on query parameters.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | Conditional | Format: `YYYYMMDD-HHMMSS`. Returns active alerts at this time. Mutually exclusive with `id` |
| `id` | string | Conditional | Alert ID to fetch specific alert. Mutually exclusive with `timestamp` |

**Note:** `timestamp` and `id` cannot be specified at the same time.

#### Response (no parameters)

Returns a list of available timestamps and all current alerts:

```json
{
  "success": true,
  "data": {
    "timestamps": [
      "20260123-150000",
      "20260123-143000"
    ],
    "alerts": [
      {
        "id": "urn:oid:...",
        "first_seen": "2026-02-23T02:00:00Z",
        "last_seen": "2026-02-23T03:40:00Z",
        "expires": "2026-02-23T04:00:00Z",
        "feature": { ... }
      }
    ]
  },
  "meta": {
    "timestamp": "2026-03-10T12:00:00.000Z"
  }
}
```

#### Response (with timestamp)

Returns the active alerts for that timestamp:

```json
{
  "success": true,
  "data": [
    {
      "id": "urn:oid:...",
      "feature": {
        "event": "Severe Thunderstorm Warning"
      }
    }
  ],
  "meta": {
    "timestamp": "2026-03-10T12:00:10.000Z",
    "count": 1,
    "total": 1
  }
}
```

#### Response (with id)

Returns the specific alert:

```json
{
  "success": true,
  "data": {
    "id": "urn:oid:2.49.0.1.840.0.2406210827.1",
    "first_seen": "2026-02-23T02:00:00Z",
    "last_seen": "2026-02-23T03:40:00Z",
    "expires": "2026-02-23T04:00:00Z",
    "feature": {
      "id": "https://api.weather.gov/alerts/urn:oid:...",
      "event": "Severe Thunderstorm Warning"
    }
  },
  "meta": {
    "timestamp": "2026-03-10T12:00:20.000Z"
  }
}
```

#### Error Responses

- `400 Bad Request` - Invalid parameters, mutually exclusive parameters, formatting issues
- `404 Not Found` - Timestamp or alert ID not found
- `500 Internal Server Error` - Server error

---

### 5. Data - METAR

**GET** `/api/v2/data/metar`

Returns METAR data timestamps or specific METAR data.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | No | Format: `YYYYMMDD-HHMMSS`. Returns METAR data for this timestamp |

#### Response (without timestamp)

Returns a list of available timestamps:

```json
[
  "20260123-150000",
  "20260123-140000",
  "20260123-130000"
]
```

#### Response (with timestamp)

Returns the METAR data:

```json
{
  "type": "metar",
  "timestamp": "20260123-150000",
  "data": {
    "stations": ["KJFK", "KLAX"],
    "observations": [
      {
        "station": "KJFK",
        "temp": 25,
        "wind": "10KT"
      }
    ]
  }
}
```

#### Error Responses

- `400 Bad Request` - Invalid timestamp format
- `404 Not Found` - METAR data not found for timestamp
- `500 Internal Server Error` - Server error

---

### 6. API v2 Information

**GET** `/api/v2`

Returns API v2 information and available endpoints.

#### Response

```json
{
  "message": "EdgeWARN API v2",
  "version": "2.x",
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

---

## Data Formats

### Timestamp Format

- **Format**: `YYYYMMDD-HHMMSS` (e.g., `20251230-150000`)
- **Internal JSON**: ISO 8601 (e.g., `2025-12-30T15:00:00Z`)

### Coordinates

- **Latitude**: Decimal degrees [20, 55]
- **Longitude**: Decimal degrees [227, 300] (0-360 format)

---

## HTTP Status Codes

| Code | Meaning |
|------|---------|
| `200 OK` | Request successful |
| `400 Bad Request` | Invalid parameters |
| `404 Not Found` | Resource not found |
| `410 Gone` | API v1 has been removed (for old v1 endpoints) |
| `500 Internal Server Error` | Server error |

---

## Caching

The API uses appropriate cache headers:

- **List endpoints** (`/cells`, `/timestamps`, `/alerts/official`, `/metar` without params): `Cache-Control: public, max-age=5`
- **Specific resource endpoints** (with id/timestamp): `Cache-Control: public, max-age=60`
- **Immutable data** (stormcell files): `Cache-Control: public, max-age=3600`

---

## Storm Cell Lineage Fields

Storm cell JSON objects include lineage fields that track merge and split events:

### Lineage Event Types

| Event Type | Description |
|------------|-------------|
| `ACTIVE` | Normal continuation - cell present in previous scan with same ID |
| `MERGE` | Multiple parent cells combined into this single child cell |
| `SPLIT` | This cell split from a parent cell (secondary child) |

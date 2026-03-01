# EdgeWARN API Documentation

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

### 2. Features API

#### 2.1 Fetch Available Resources

**GET** `/features/fetch/resources`

Retrieves a list of available timestamps or cell IDs from index files.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Resource type: `"cell"` or `"list"` |

#### 2.2 Download Resource

**GET** `/features/download/resources`

Downloads a specific stormcell list or individual cell history JSON.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Resource type: `"cell"` or `"list"` |
| `timestamp` | string | Conditional | Required if `type=list`. Format: YYYYMMDD-HHMMSS |
| `id` | integer | Conditional | Required if `type=cell`. Positive integer cell ID |

---

### 3. Data API

#### 3.1 Fetch Available Data

**GET** `/data/fetch`

Retrieves a list of available timestamps for meteorological data.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Data type: `"nws"`, `"metar"`, or `"surface"` |

**Response Example (METAR/Surface):**
```json
{
  "type": "metar",
  "count": 2,
  "timestamps": [
    "20260123-120000",
    "20260123-110000"
  ]
}
```

**Response Example (NWS - returns active alert IDs):**
```json
{
  "type": "nws",
  "count": 5,
  "last_updated": "2026-02-23T03:40:00Z",
  "alert_ids": [
    "urn:oid:2.49.0.1.840.0.2406210827.1",
    "urn:oid:2.49.0.1.840.0.2406210828.1"
  ]
}
```

#### 3.2 Download Data

**GET** `/data/download`

Downloads meteorological data for a specific type and timestamp.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Data type: `"nws"`, `"metar"`, or `"surface"` |
| `timestamp` | string | Conditional | Format: YYYYMMDD-HHMM00 (required for metar/surface) |
| `alert_id` | string | No | For NWS: fetch specific alert by ID |

**Response Example (METAR/Surface):**
```json
{
  "type": "metar",
  "timestamp": "20260123-120000",
  "data": { ... }
}
```

**Response Example (NWS - all alerts):**
```json
{
  "type": "nws",
  "last_updated": "2026-02-23T03:40:00Z",
  "count": 5,
  "data": {
    "@context": ["https://geojson.org/geojson-ld/geojson-context.jsonld", {...}],
    "type": "FeatureCollection",
    "features": [
      {
        "id": "https://api.weather.gov/alerts/urn:oid:...",
        "type": "Feature",
        "properties": {
          "event": "Severe Thunderstorm Warning",
          "headline": "...",
          "expires": "2026-02-23T22:00:00Z",
          ...
        },
        "Polygon": [[...]]
      }
    ]
  }
}
```

**Response Example (NWS - specific alert):**
```json
{
  "type": "nws",
  "alert_id": "urn:oid:2.49.0.1.840.0.2406210827.1",
  "data": {
    "id": "https://api.weather.gov/alerts/urn:oid:...",
    "first_seen": "2026-02-23T02:00:00Z",
    "last_seen": "2026-02-23T03:40:00Z",
    "expires": "2026-02-23T04:00:00Z",
    "feature": { ... }
  }
}
```

---

### 4. API Information

**GET** `/features/` - Returns Features API info.
**GET** `/data/` - Returns Data API info.

---

## API v2 (New)

API v2 provides a more RESTful interface with cleaner URL structures. The v1 API remains available for backward compatibility.

### v2 Features Endpoints

#### 4.1 List Available Cells

**GET** `/api/v2/features/cells`

Returns a list of available cell IDs.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | No | If specified, returns data for that specific cell |

**Response (without id):**
```json
[1, 2, 3, 5, 8, 13]
```

**Response (with id):**
```json
{
  "id": 123,
  "first_seen": "20260123-120000",
  "last_seen": "20260123-143000",
  "history": [...]
}
```

---

#### 4.2 List Available Timestamps

**GET** `/api/v2/features/timestamps`

Returns a list of available timestamps or stormcell data for a specific timestamp.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | No | Format: `YYYYMMDD-HHMMSS`. Returns stormcell list for this time |

**Response (without timestamp):**
```json
[
  "20260123-150000",
  "20260123-143000",
  "20260123-140000"
]
```

**Response (with timestamp):**
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

---

### v2 Data Endpoints

#### 4.3 NWS Alert Data

**GET** `/api/v2/data/nws`

Returns NWS alert timestamps or specific alert data.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | Conditional | Format: `YYYYMMDD-HHMMSS`. Returns snapshot at this time |
| `id` | string | Conditional | Alert ID to fetch specific alert |

**Note:** `timestamp` and `id` are mutually exclusive.

**Response (no parameters):**
```json
[
  "20260123-150000",
  "20260123-143000",
  "20260123-140000"
]
```

**Response (with timestamp):**
```json
{
  "timestamp": "20260123-150000",
  "count": 5,
  "alerts": [
    {
      "id": "urn:oid:2.49.0.1.840.0.2406210827.1",
      "event": "Severe Thunderstorm Warning",
      "headline": "..."
    }
  ]
}
```

**Response (with id):**
```json
{
  "id": "urn:oid:2.49.0.1.840.0.2406210827.1",
  "first_seen": "2026-02-23T02:00:00Z",
  "last_seen": "2026-02-23T03:40:00Z",
  "expires": "2026-02-23T04:00:00Z",
  "feature": { ... }
}
```

---

#### 4.4 METAR Data

**GET** `/api/v2/data/metar`

Returns METAR timestamps or specific METAR data.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `timestamp` | string | No | Format: `YYYYMMDD-HHMMSS`. Returns METAR data for this time |

**Response (without timestamp):**
```json
[
  "20260123-150000",
  "20260123-140000",
  "20260123-130000"
]
```

**Response (with timestamp):**
```json
{
  "type": "metar",
  "timestamp": "20260123-150000",
  "data": {
    "stations": [...],
    "observations": [...]
  }
}
```

---

## Data Formats

### Timestamp Format

- **Features**: `YYYYMMDD-HHMMSS` (e.g., `20251230-150000`)
- **Data**: `YYYYMMDD-HHMM00` (e.g., `20251230-150000`)
- **Internal JSON**: ISO 8601 (e.g., `2025-12-30T15:00:00Z`)

### Coordinates

- **Latitude**: Decimal degrees [20, 55]
- **Longitude**: Decimal degrees [227, 300] (0-360 format)

---

## Storm Cell Lineage Fields

Storm cell JSON objects include lineage fields that track merge and split events:

### Lineage Event Types

| Event Type | Description |
|------------|-------------|
| `ACTIVE` | Normal continuation - cell present in previous scan with same ID |
| `MERGE` | Multiple parent cells combined into this single child cell |
| `SPLIT` | This cell split from a parent cell (secondary child) |
| `DISSIPATED` | Cell was removed without merging (not included in output) |

### Lineage Fields

| Field | Type | Description |
|-------|------|-------------|
| `event_type` | string | One of: `ACTIVE`, `MERGE`, `SPLIT`, `DISSIPATED` |
| `parent_ids` | List[int] | IDs of parent cells that merged into this cell (empty if not a merge) |
| `split_from` | int or null | ID of parent cell this cell split from (null if not a split) |

### Merge Event Example

When two storm cells merge into one:

```json
{
  "id": 405,
  "event_type": "MERGE",
  "parent_ids": [405, 408],
  "split_from": null,
  "max_refl": 65.0,
  "num_gates": 220,
  "centroid": [35.15, 262.15],
  "bbox": [[35.0, 262.0], [35.0, 262.3], [35.3, 262.3], [35.3, 262.0]]
}
```

The dominant parent (highest `max_refl` or largest `num_gates`) provides the historical tracking data for the merged cell.

### Split Event Example

When one storm cell splits into multiple cells:

```json
{
  "id": 10,
  "event_type": "ACTIVE",
  "parent_ids": [],
  "split_from": 1,
  "max_refl": 55.0,
  "num_gates": 100
}
```

The dominant child (highest `max_refl`) inherits the parent's ID and tracking history, while secondary children are marked with `event_type: "SPLIT"`.

### Hysteresis Buffer

Lineage events require confirmation across multiple scans to prevent false positives from ProbSevere ID instability. By default, events must be detected in 2 consecutive scans before being confirmed.

Buffer state is persisted in `lineage_buffer.json` in the storm cells directory.

---

## Rate Limiting

The API implements rate limiting to ensure stability:
- **Limit**: 100 requests per minute per IP address.
- **Headers**: `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`.

---

## Error Handling

- **400 Bad Request**: Invalid or missing parameters.
- **404 Not Found**: Resource doesn't exist.
- **429 Too Many Requests**: Rate limit exceeded.
- **500 Internal Server Error**: Server-side error.

Error responses include an `error` field.
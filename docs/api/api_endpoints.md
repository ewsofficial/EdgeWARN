# EdgeWARN API v2 Documentation

This document describes the EdgeWARN API v2 endpoints, their parameters, and responses. The API provides access to storm cell data, alerts, and METAR observations.

## API Overview

- **Base URL**: `/api/v2`
- **Version**: 2.0.0
- **Protocol**: HTTP/HTTPS
- **Response Format**: JSON

## Root Endpoint

### GET /api/v2

Returns information about the API version and available endpoints.

**Example Request**:
```http
GET /api/v2 HTTP/1.1
Host: api.edgewarn.com
```

**Response**:
```json
{
  "message": "EdgeWARN API v2",
  "version": "2.0.0",
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

### Cells

#### GET /api/v2/features/cells

Returns storm cell data.

**Query Parameters**:
- `id` (optional): Integer - Cell ID to fetch specific cell data

**Responses**:

1. Without `id` parameter: Returns array of available cell IDs

```http
GET /api/v2/features/cells HTTP/1.1
Host: api.edgewarn.com
```

```json
[12345, 12346, 12347]
```

2. With `id` parameter: Returns specific cell data

```http
GET /api/v2/features/cells?id=12345 HTTP/1.1
Host: api.edgewarn.com
```

```json
{
  "id": 12345,
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [...]
  },
  "properties": {
    "centroid": [35.0, -97.0],
    "max_refl": 55,
    "num_gates": 25,
    "timestamp": "2023-10-01T12:00:00Z",
    "velocity": {
      "u": 5.0,
      "v": 3.0,
      "speed": 5.83,
      "bearing": 59.0
    },
    "confidence": 0.95,
    "tracking_mode": "active",
    "event_type": "ACTIVE",
    "alerts": ["TOR", "HAIL"]
  }
}
```

### Timestamps

#### GET /api/v2/features/timestamps

Returns available timestamps or storm cell data for a specific timestamp.

**Query Parameters**:
- `timestamp` (optional): String - Timestamp in `YYYYMMDD-HHMMSS` format

**Responses**:

1. Without `timestamp` parameter: Returns array of available timestamps

```http
GET /api/v2/features/timestamps HTTP/1.1
Host: api.edgewarn.com
```

```json
["20231001-120000", "20231001-115800", "20231001-115600"]
```

2. With `timestamp` parameter: Returns storm cell data for that timestamp

```http
GET /api/v2/features/timestamps?timestamp=20231001-120000 HTTP/1.1
Host: api.edgewarn.com
```

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "id": 12345,
      "type": "Feature",
      "geometry": {...},
      "properties": {...}
    }
  ],
  "latest_timestamp": "2023-10-01T12:00:00Z"
}
```

### Alerts

#### GET /api/v2/features/alerts/official

Returns official NWS alerts.

**Query Parameters**:
- `id` (optional): String - Alert ID (URN format: `urn:oid:12345`)
- `timestamp` (optional): String - Timestamp in `YYYYMMDD-HHMMSS` format

**Responses**:

1. Without parameters: Returns array of available timestamps

```http
GET /api/v2/features/alerts/official HTTP/1.1
Host: api.edgewarn.com
```

```json
["20231001-120000", "20231001-115800", "20231001-115600"]
```

2. With `timestamp` parameter: Returns alert IDs for that timestamp

```http
GET /api/v2/features/alerts/official?timestamp=20231001-120000 HTTP/1.1
Host: api.edgewarn.com
```

```json
["urn:oid:12345", "urn:oid:12346"]
```

3. With `id` parameter: Returns specific alert data

```http
GET /api/v2/features/alerts/official?id=urn:oid:12345 HTTP/1.1
Host: api.edgewarn.com
```

```json
{
  "id": "urn:oid:12345",
  "type": "Feature",
  "geometry": {
    "type": "Polygon",
    "coordinates": [...]
  },
  "properties": {
    "event": "Tornado Warning",
    "severity": "Severe",
    "sender": "NWS",
    "sent": "2023-10-01T12:00:00Z",
    "effective": "2023-10-01T12:00:00Z",
    "expires": "2023-10-01T12:30:00Z",
    "headline": "Tornado Warning issued for Westchester County"
  }
}
```

#### GET /api/v2/features/alerts/edgewarn

Returns EdgeWARN generated alerts (same interface as official alerts).

**Query Parameters**:
- `id` (optional): String - Alert ID
- `timestamp` (optional): String - Timestamp in `YYYYMMDD-HHMMSS` format

**Responses**:
Same format as official alerts endpoint.

## Data Endpoints

### METAR

#### GET /api/v2/data/metar

Returns METAR weather observations.

**Query Parameters**:
- `timestamp` (optional): String - Timestamp in `YYYYMMDD-HHMMSS` format

**Responses**:

1. Without `timestamp` parameter: Returns array of available timestamps

```http
GET /api/v2/data/metar HTTP/1.1
Host: api.edgewarn.com
```

```json
["20231001-120000", "20231001-110000", "20231001-100000"]
```

2. With `timestamp` parameter: Returns METAR data for that timestamp

```http
GET /api/v2/data/metar?timestamp=20231001-120000 HTTP/1.1
Host: api.edgewarn.com
```

```json
{
  "type": "metar",
  "timestamp": "20231001-120000",
  "data": [
    {
      "station": "KJFK",
      "name": "John F. Kennedy International Airport",
      "latitude": 40.64,
      "longitude": -73.78,
      "observation_time": "2023-10-01T12:00:00Z",
      "temperature": 20.0,
      "dewpoint": 15.0,
      "wind": {
        "direction": 180,
        "speed": 10,
        "gust": null
      },
      "visibility": 10,
      "pressure": 1013,
      "sky_conditions": "Few clouds at 3000ft",
      "flight_category": "VFR"
    }
  ]
}
```

## Health Check

#### GET /health

Returns server health status.

**Example Request**:
```http
GET /health HTTP/1.1
Host: api.edgewarn.com
```

**Response**:
```json
{
  "status": "OK",
  "timestamp": "2023-10-01T12:00:00.123Z"
}
```

## Error Handling

The API returns appropriate HTTP status codes for errors:

- **400 Bad Request**: Invalid parameters or input
- **404 Not Found**: Resource not found
- **500 Internal Server Error**: Server-side error

**Example Error Response**:
```json
{
  "error": "Invalid id parameter. Must be a positive integer"
}
```

## Caching

The API uses caching to improve performance:

- Cell data: 60 seconds
- Alert data: 60 seconds
- Metar data: 60 seconds
- Timestamps: 5 seconds
- Index files: 5 seconds

## Rate Limiting

The API is rate limited to 60 requests per minute per IP address by default. This can be configured via environment variables.

## Cross-Origin Resource Sharing (CORS)

CORS is configured to allow requests from specific origins. In development, this includes `http://localhost:3000` and `http://localhost:8080`. In production, origins must be explicitly configured via the `ALLOWED_ORIGINS` environment variable.

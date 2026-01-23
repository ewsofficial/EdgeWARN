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

**Response Example:**
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

#### 3.2 Download Data

**GET** `/data/download`

Downloads meteorological data for a specific type and timestamp.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Data type: `"nws"`, `"metar"`, or `"surface"` |
| `timestamp` | string | Yes | Format: YYYYMMDD-HHMM00 |

**Response Example:**
```json
{
  "type": "metar",
  "timestamp": "20260123-120000",
  "data": { ... }
}
```

---

### 4. API Information

**GET** `/features/` - Returns Features API info.
**GET** `/data/` - Returns Data API info.

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
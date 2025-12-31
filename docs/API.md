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

### 2. Fetch Available Resources

**GET** `/features/fetch/resources`

Retrieves a list of available timestamps or cell IDs from index files.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Resource type: `"cell"` or `"list"` |

#### Response

**When `type=list`:**

Returns array of available stormcell timestamps in YYYYMMDD-HHMMSS format.

```json
[
  "20251230-150000",
  "20251230-151500",
  "20251230-153000"
]
```

**When `type=cell`:**

Returns array of available cell IDs as integers.

```json
[12345, 67890, 11223, 33445]
```

#### Error Responses

**400 Bad Request** - Invalid type parameter
```json
{
  "error": "Invalid type parameter. Must be \"cell\" or \"list\""
}
```

#### Example Usage

```bash
# Fetch available stormcell timestamps
curl "http://localhost:5000/features/fetch/resources?type=list"

# Fetch available cell IDs
curl "http://localhost:5000/features/fetch/resources?type=cell"
```

---

### 3. Download Resource

**GET** `/features/download/resources`

Downloads a specific stormcell list or individual cell history JSON.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Resource type: `"cell"` or `"list"` |
| `timestamp` | string | Conditional | Required if `type=list`. Format: YYYYMMDD-HHMMSS |
| `id` | integer | Conditional | Required if `type=cell`. Positive integer cell ID |

#### Response

**When `type=list&timestamp=...`:**

Returns complete stormcell list JSON for the specified timestamp.

```json
{
  "source": "EdgeWARN",
  "product": "stormcells",
  "latest_timestamp": "2025-12-30T15:00:00Z",
  "features": [
    {
      "id": 12345,
      "timestamp": "2025-12-30T15:00:00Z",
      "num_gates": 1500,
      "centroid": [40.7128, 285.9936],
      "bbox": [[40.5, 285.8], [40.9, 286.2]],
      "max_refl": 65.5,
      "properties": {
        "ProbSevere": 75,
        "ProbHail": 45,
        "MESH": 1.5,
        "GLM_FLASH_COUNT": 23,
        "GLM_TOTAL_ENERGY": 4567.8,
        ...
      }
    },
    ...
  ]
}
```

**When `type=cell&id=...`:**

Returns cell history as an array of historical states.

```json
[
  {
    "id": 12345,
    "timestamp": "2025-12-30T14:30:00Z",
    "num_gates": 1200,
    "centroid": [40.7, 285.9],
    "bbox": [[40.5, 285.7], [40.9, 286.1]],
    "max_refl": 62.0,
    "properties": { ... }
  },
  {
    "id": 12345,
    "timestamp": "2025-12-30T14:45:00Z",
    "num_gates": 1500,
    "centroid": [40.7128, 285.9936],
    "bbox": [[40.5, 285.8], [40.9, 286.2]],
    "max_refl": 65.5,
    "properties": { ... }
  },
  ...
]
```

#### Error Responses

**400 Bad Request** - Invalid or missing parameters
```json
{
  "error": "Invalid type parameter. Must be \"cell\" or \"list\""
}
```

```json
{
  "error": "Invalid or missing timestamp parameter. Format: YYYYMMDD-HHMMSS"
}
```

```json
{
  "error": "Invalid or missing id parameter. Must be a positive integer"
}
```

**404 Not Found** - Resource doesn't exist
```json
{
  "error": "The requested file was not found"
}
```

#### Example Usage

```bash
# Download stormcell list for specific timestamp
curl "http://localhost:5000/features/download/resources?type=list&timestamp=20251230-150000"

# Download cell history for specific ID
curl "http://localhost:5000/features/download/resources?type=cell&id=12345"
```

---

### 4. API Information

**GET** `/features/`

Returns information about available API endpoints.

#### Response

```json
{
  "message": "EdgeWARN Features API",
  "endpoints": {
    "fetch": "/features/fetch/resources?type=[cell|list]",
    "download": "/features/download/resources?type=[cell|list]&[timestamp=...|id=...]"
  }
}
```

---

## Data Formats

### Timestamp Format

Timestamps are in **YYYYMMDD-HHMMSS** format for filenames and queries, and **ISO 8601** format within JSON content.

- Filename format: `20251230-150000` (December 30, 2025, 15:00:00 UTC)
- JSON format: `2025-12-30T15:00:00Z`

### Cell ID Format

Cell IDs are positive integers assigned by the ProbSevere system.

### Coordinates

- **Latitude**: Decimal degrees, range [20, 55] for CONUS
- **Longitude**: Decimal degrees in 0-360 format, range [227, 300] for CONUS (equivalent to -133 to -60 in standard format)

---

## Rate Limiting

Currently, no rate limiting is implemented.

---

## Data Retention

- **Stormcell lists**: Automatically cleaned up after 24 hours
- **Cell histories**: Automatically cleaned up after 1 hour of inactivity (no timestamp updates)

---

## Index Files

The API uses index files for fast resource lookups:

### stormcell_index.json

Located in `STORMCELL_DIR`, contains:
- `timestamps`: Array of available stormcell timestamps
- `lastUpdated`: ISO timestamp of last index update

### cell_index.json

Located in `CELL_DIR`, contains:
- `cellIds`: Array of available cell IDs
- `lastUpdated`: ISO timestamp of last index update

These indexes are automatically maintained by the processing pipeline and should not be manually edited.

---

## Error Handling

All errors return appropriate HTTP status codes:

- **200 OK**: Successful request
- **400 Bad Request**: Invalid parameters or malformed request
- **404 Not Found**: Requested resource doesn't exist
- **500 Internal Server Error**: Server-side error

Error responses include a JSON object with an `error` field describing the issue.

---

## CORS

CORS is enabled for all origins. The API can be accessed from any domain.

---

## Examples

### JavaScript (Fetch API)

```javascript
// Fetch available timestamps
fetch('http://localhost:5000/features/fetch/resources?type=list')
  .then(res => res.json())
  .then(timestamps => console.log(timestamps));

// Download specific stormcell list
fetch('http://localhost:5000/features/download/resources?type=list&timestamp=20251230-150000')
  .then(res => res.json())
  .then(data => console.log(data));

// Download cell history
fetch('http://localhost:5000/features/download/resources?type=cell&id=12345')
  .then(res => res.json())
  .then(history => console.log(history));
```

### Python (requests)

```python
import requests

# Fetch available timestamps
response = requests.get('http://localhost:5000/features/fetch/resources', params={'type': 'list'})
timestamps = response.json()

# Download specific stormcell list
response = requests.get('http://localhost:5000/features/download/resources', 
                       params={'type': 'list', 'timestamp': '20251230-150000'})
stormcell_data = response.json()

# Download cell history
response = requests.get('http://localhost:5000/features/download/resources',
                       params={'type': 'cell', 'id': 12345})
cell_history = response.json()
```

### cURL

```bash
# Fetch available timestamps
curl "http://localhost:5000/features/fetch/resources?type=list"

# Download stormcell list
curl "http://localhost:5000/features/download/resources?type=list&timestamp=20251230-150000"

# Download cell history
curl "http://localhost:5000/features/download/resources?type=cell&id=12345"

# Check server health
curl "http://localhost:5000/health"
```

---

## Migration from Old API

### Old Endpoints (Deprecated)

| Old Endpoint | New Endpoint |
|-------------|--------------|
| `GET /features/list` | `GET /features/fetch/resources?type=list` |
| `GET /features/list/:name` | `GET /features/download/resources?type=list&timestamp=...` |
| `GET /features/cells` | `GET /features/fetch/resources?type=cell` |
| `GET /features/cells/:name` | `GET /features/download/resources?type=cell&id=...` |

### Migration Example

**Old:**
```javascript
fetch('/features/list/stormcells_20251230-150000.json')
```

**New:**
```javascript
fetch('/features/download/resources?type=list&timestamp=20251230-150000')
```

---

## Notes

- All timestamps are in UTC
- The API automatically maintains indexes during pipeline execution
- Empty arrays are returned for fetch requests when no resources are available
- File names are constructed from query parameters (e.g., `stormcells_{timestamp}.json`, `{id}.json`)

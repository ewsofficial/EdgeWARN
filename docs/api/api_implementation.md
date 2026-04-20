# EdgeWARN API v2 Technical Implementation

This document describes the current implementation in `src/EdgeWARN/api`.

## Server Architecture

The EdgeWARN API is an Express.js service with clustered workers (up to 4), file-backed JSON responses, centralized validation, and safe file reads.

### File Structure

```text
src/EdgeWARN/api/
├── server.js
├── config.js
├── robots.txt
├── routes/
│   ├── health.js
│   └── v2/
│       ├── index.js
│       ├── features/
│       │   ├── cells.js
│       │   ├── mesocyclones.js
│       │   ├── timestamps.js
│       │   └── alerts.js
│       └── data/
│           └── metar.js
└── utils/
    ├── fileReader.js
    └── validation.js
```

## Request Lifecycle

1. `server.js` loads environment variables via `dotenv`
2. Primary process forks up to 4 workers (`cluster`)
3. Worker middleware stack is applied:
   - `helmet`
   - `compression`
   - `cors`
   - `express.json()`
   - per-second and per-minute `express-rate-limit`
4. Routes are mounted:
   - `/`
   - `/health`
   - `/api/v2`
   - legacy guards for `/features/*` and `/data/*` returning `410`
5. Routes read file-backed JSON using guarded readers in `utils/fileReader.js`

## Configuration (`config.js`)

`BASE_DIR` resolution order:

1. CLI arg: `--base-dir` or `--base-dir=...`
2. `EDGEWARN_BASE_DIR`
3. Platform fallback defaults

Data directories are derived from `BASE_DIR/data/...`, including `cells`, `stormcells`, `Mesocyclones`, `METAR`, and alert directories.

At startup, required directories are created if missing.

Debug mode:

- Enabled with `--debug_server`
- Default port `5000`
- Debug port `3001`

## Routing

### `routes/v2/index.js`

Mounts:

- `/features/cells`
- `/features/mesocyclones`
- `/features/timestamps`
- `/features/alerts`
- `/data/metar`

Also serves `GET /api/v2` endpoint metadata.

### `routes/v2/features/cells.js`

- `GET /api/v2/features/cells`
- Optional query: `id`
- List mode reads `cell_index.json`
- ID mode reads `{id}.json`
- Validates positive integer IDs

### `routes/v2/features/mesocyclones.js`

- `GET /api/v2/features/mesocyclones`
- Optional query: `timestamp` (`YYYYMMDD-HHMMSS`)
- List mode scans `mesocyclones_*.json`
- Timestamp mode reads `mesocyclones_{timestamp}.json`

### `routes/v2/features/timestamps.js`

- `GET /api/v2/features/timestamps`
- Optional query: `timestamp` (`YYYYMMDD-HHMMSS`)
- List mode reads `stormcell_index.json`
- Timestamp mode reads `stormcells_{timestamp}.json`

### `routes/v2/features/alerts.js`

- `GET /api/v2/features/alerts/official`
- `GET /api/v2/features/alerts/edgewarn`
- Supports mutually exclusive query params: `id` or `timestamp`
- ID mode resolves filename-safe IDs in `ids/`
- Timestamp mode reads `{timestamp}.json` in `timestamps/` and returns `alerts` array
- List mode scans timestamp files and returns sorted timestamp keys

### `routes/v2/data/metar.js`

- `GET /api/v2/data/metar`
- Optional query: `timestamp`
- List mode scans hourly files `METAR_YYYYMMDD-HHz.json`
- Timestamp mode maps to hourly file and wraps response as `{ type, timestamp, data }`

### `routes/health.js`

- `GET /health` returns `{ status: "OK", timestamp }`

## Utilities

### `utils/fileReader.js`

Provides:

- `isSafeFilename(name)`
- `readJsonFileSafe(dir, name, options)` with traversal protection
- `readIndexFile(indexPath)`

Caching (`lru-cache`):

- max entries: `500`
- default TTL: `60s`
- max cache size per worker: `40MB`
- index-file TTL override: `5s`

### `utils/validation.js`

Provides validators for:

- timestamps (`YYYYMMDD-HHMMSS`)
- mutually exclusive query params
- cell IDs
- alert IDs

## Middleware and Security

### Helmet

- Enabled globally
- Includes HSTS and default CSP behavior from server config

### CORS

- Uses `ALLOWED_ORIGINS` when set
- Without `ALLOWED_ORIGINS`:
  - non-production: allows all origins
  - production: blocks cross-origin requests

### Rate Limiting

Two global limiters are applied:

- per-second limiter (defaults: `windowMs=1000`, `max=40`)
- per-minute limiter (defaults: `windowMs=60000`, `max=2000`)

Special behavior:

- `/health` can be skipped when header `x-internal-check: true` is present
- Key generation supports proxy and non-proxy deployment modes

## Error Handling

- Route handlers return `400`, `404`, or `500` as appropriate
- Global error middleware hides stack/detail in production (`Internal server error`)
- Legacy v1-style routes return `410 Gone`

## Environment Variables

- `PORT`
- `NODE_ENV`
- `ALLOWED_ORIGINS`
- `RATE_LIMIT_WINDOW_MS_SEC`
- `RATE_LIMIT_MAX_SEC`
- `RATE_LIMIT_WINDOW_MS_MIN`
- `RATE_LIMIT_MAX_MIN`
- `TRUST_PROXY`
- `TRUST_PROXY_IPS`
- `EDGEWARN_BASE_DIR`

## Runtime Modes

- Primary API server: `npm run api:edgewarn`
- Debug API server: `npm run debug:edgewarn`
- EWMRS API server: `npm run api:ewmrs`

See also:

- `docs/api/api_endpoints.md` (EdgeWARN API v2)
- `docs/api/ewmrs_api_endpoints.md` (EWMRS API)

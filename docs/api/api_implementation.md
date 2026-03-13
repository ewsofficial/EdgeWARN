# EdgeWARN API v2 Technical Implementation

This document describes the current implementation in `src/EdgeWARN/api`.

## Server Architecture

The API is an Express.js service with clustered workers (up to 4), file-backed data access, and defensive request validation.

### File Structure

```
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
│       │   ├── alerts.js
│       │   └── timestamps.js
│       └── data/
│           └── metar.js
└── utils/
    ├── fileReader.js
    └── validation.js
```

## Request Lifecycle

1. `server.js` loads environment variables (`dotenv`) and config.
2. If process is primary, Node cluster forks up to 4 workers.
3. Each worker applies middleware:
   - `helmet`
   - `compression`
   - `cors` (allowlist from `ALLOWED_ORIGINS`)
   - `express.json()`
   - `express-rate-limit`
4. Worker mounts routes:
   - `/`
   - `/health`
   - `/api/v2`
   - legacy guards for `/features/*` and `/data/*` returning `410`
5. File-backed routes fetch JSON from configured data directories using safe readers.

## Configuration (`config.js`)

`config.js` resolves `BASE_DIR` in this order:

1. CLI arg: `--base-dir` / `--base-dir=...`
2. `EDGEWARN_BASE_DIR`
3. Platform defaults (including `~/EdgeWARN_input` on non-Windows)

It then builds `DATA_DIR` subpaths (cells, stormcells, METAR, alerts, etc.) and creates missing directories at startup.

It also supports debug mode via `--debug_server`:

- default port: `5000`
- debug port: `3001`

## Routing

### `routes/v2/index.js`

Mounts:

- `/features/cells`
- `/features/timestamps`
- `/features/alerts`
- `/data/metar`

And provides `GET /api/v2` endpoint metadata.

### `routes/v2/features/cells.js`

- `GET /api/v2/features/cells`
- Optional query: `id`
- Reads `cell_index.json` for list mode.
- Reads `{id}.json` for single-cell mode.
- Validates positive integer IDs.

### `routes/v2/features/timestamps.js`

- `GET /api/v2/features/timestamps`
- Optional query: `timestamp` (`YYYYMMDD-HHMMSS`)
- Reads `stormcell_index.json` for list mode.
- Reads `stormcells_{timestamp}.json` for snapshot mode.

### `routes/v2/features/alerts.js`

- `GET /api/v2/features/alerts/official`
- `GET /api/v2/features/alerts/edgewarn`
- Supports mutually exclusive query params: `id` or `timestamp`
- ID mode resolves filename-safe IDs in `ids/`
- Timestamp mode reads `{timestamp}.json` from `timestamps/`
- List mode scans timestamp files and returns sorted timestamp keys

### `routes/v2/data/metar.js`

- `GET /api/v2/data/metar`
- Optional query: `timestamp`
- List mode scans `METAR_YYYYMMDD-HHz.json`
- Timestamp mode maps request to hourly METAR file and wraps result as `{ type, timestamp, data }`

### `routes/health.js`

- `GET /health` returns service status and server timestamp.

## Utilities

### `utils/fileReader.js`

Provides:

- `isSafeFilename(name)` filename hardening
- `readJsonFileSafe(dir, name, options)` safe JSON file reads with traversal protection
- `readIndexFile(path)` index-file reads with short cache TTL

Caching uses `lru-cache`:

- max entries: `500`
- default TTL: `60s`
- max worker cache size: `40MB`
- index TTL override: `5s`

### `utils/validation.js`

Centralized validation helpers for:

- resource type checks
- timestamp validation
- mutually exclusive query params
- cell ID and alert ID validation

## Middleware Details

### Security

`helmet` is enabled with HSTS and CSP defaults.

### CORS

- In non-production without env override: localhost allowlist
- In production: requires explicit `ALLOWED_ORIGINS`, otherwise cross-origin requests are blocked

### Rate Limiting

`express-rate-limit` defaults:

- `RATE_LIMIT_WINDOW_MS`: `60000`
- `RATE_LIMIT_MAX`: `60`

Special cases:

- Optional skip for `/health` with `x-internal-check: true`
- Custom key generation handles proxy/non-proxy deployments

## Error Handling

- Route-level handlers return `400/404/500` as appropriate.
- Global error middleware sanitizes output in production (`Internal server error`).
- Legacy v1 paths return `410 Gone` and include migration guidance.

## Environment Variables

- `PORT`
- `NODE_ENV`
- `ALLOWED_ORIGINS`
- `RATE_LIMIT_WINDOW_MS`
- `RATE_LIMIT_MAX`
- `TRUST_PROXY`
- `TRUST_PROXY_IPS`
- `EDGEWARN_BASE_DIR`

## Runtime Modes

- **Production**: `npm start`
- **Development watch**: `npm run dev`
- **Debug**: `npm run debug`

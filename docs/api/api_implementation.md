# EdgeWARN Unified API Technical Implementation

The live Express implementation is `src/api/`, started with `npm run api`
(or `npm run debug:api`). It serves `/api/v3` and retains selected `/api/v2`,
`/renders`, `/rap`, `/wpc`, and `/colormaps` endpoints as compatibility adapters.
The v2-specific implementation details below are historical route context;
`src/EdgeWARN/api` is not a live service tree.

## Server Architecture

The unified API is an Express.js service with file-backed responses, centralized
validation, and safe file reads. The following tree documents the retired v2
implementation for compatibility-reference purposes only.

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
   - per-second and per-minute `express-rate-limit`
   - `express.json({ limit: "16kb" })`
4. Routes are mounted:
   - `/`
   - `/health`
   - `/api/v2`
   - legacy guards for `/features/*`, `/data/*`, and `/api/v1*` returning `410`
5. Routes read file-backed JSON using guarded readers in `utils/fileReader.js`

## Configuration (`config.js`)

`BASE_DIR` resolution order:

1. CLI arg: `--base-dir` or `--base-dir=...`
2. `EDGEWARN_BASE_DIR`
3. Linux fallback chain: `~/EdgeWARN_input`, then `/home/EdgeWARN_input`, then `/workspaces/EdgeWARN_input`, then `./EdgeWARN_input`
4. Windows fallback: `C:\EdgeWARN_input`

EdgeWARN rate-limit CLI overrides:

- `--edgewarn-rate-limit-1s`
- `--edgewarn-rate-limit-1m`
- `0` disables the respective limiter window

Data directories are derived from `BASE_DIR/data/...`, including `cells`, `stormcells`, `METAR`, and alert directories.

At startup, required directories are created if missing.

Debug mode:

- Enabled with `--debug_server`
- Default port `5000`
- Debug port `3001`
- The current debug command is `npm run debug:api`.

## Routing

### `routes/v2/index.js`

Mounts:

- `/features/cells`
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
- Timestamp mode returns `[]` when the snapshot file is absent
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

Two global limiters are applied before JSON body parsing so abusive request bodies can be rejected before parsing work is performed:

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
- `TRUST_PROXY` — `false` explicitly disables Express trust-proxy. Other values (including `true`) only take effect when `TRUST_PROXY_IPS` is also set; the `TRUST_PROXY=true` value alone is consumed by the rate-limiter `keyGenerator` (so `req.ip` is used for rate-limit keys) but does not call `app.set('trust proxy', true)`.
- `TRUST_PROXY_IPS` — comma-separated allowlist; when present, sets Express `trust proxy` to that array. Only configure trust when a stripping reverse proxy removes client-supplied `X-Forwarded-For`/`X-Forwarded-Proto` headers before forwarding; on a directly exposed host, enabling trust lets clients spoof forwarded headers and bypass per-client rate limits.
- `EDGEWARN_BASE_DIR`

EWMRS-specific environment variables are documented in `docs/api/ewmrs_api_endpoints.md`.

## Runtime Modes

- Unified API server: `npm run api` (default port `5000`)
- Unified debug API server: `npm run debug:api` (default port `3001`)
- EWMRS compatibility routes are served by that same process.

See also:

- `docs/api/api_endpoints.md` (unified API and compatibility routes)
- `docs/api/ewmrs_api_endpoints.md` (EWMRS compatibility route mapping)
- `docs/core/goes_pipeline.md` (GOES ingest, readiness, rendering, and GUI output flow)

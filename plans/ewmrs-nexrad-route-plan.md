# Plan: Add `/nexrad` Route to EWMRS API

## Objective
Add EWMRS `/nexrad` routes that:
- list available NEXRAD sites
- list available timestamps for a selected site and variable
- download NEXRAD polar intermediate files (`<variable>.bin.gz`) from `<BASE_DIR>/gui/NEXRAD`

## Current State
- EWMRS currently mounts `/renders`, `/rap`, `/wpc`, and `/colormaps` in `src/EWMRS/api/server.js`.
- NEXRAD intermediates are already written to:
  - `<BASE_DIR>/gui/NEXRAD/<SITE>/<scan_timestamp>/<elevation>/<variable>.bin.gz`
- No dedicated HTTP route currently exposes these files.

## Proposed Route Contract
Use a focused `/nexrad` API surface:

- `GET /nexrad/sites?variable={variable}`
  - Returns unique site codes under `gui/NEXRAD/<SITE>/...` that contain the selected variable file
  - `variable` corresponds to the radar variable token (`DBZH`, `VRADH`, `WRADH`, `RHOHV`)

- `GET /nexrad/timestamps?site={site}&variable={variable}[&elevation={0.5|0.9}]`
  - Returns available scan timestamps (`YYYYMMDD-HHMMSS`) for selected `site` + `variable`
  - If `elevation` is omitted, route aggregates across matching elevation folders and de-duplicates timestamps

- `GET /nexrad/download?site={site}&variable={variable}&timestamp={YYYYMMDD-HHMMSS}&elevation={0.5|0.9}`
  - Resolves to `gui/NEXRAD/<SITE>/<timestamp>/<elevation>/<variable>.bin.gz`
  - Returns raw bytes (`application/octet-stream`) with `inline` content disposition

Common behavior:
- `400` for missing/invalid params
- `404` when no matching site/timestamp/file exists
- `500` for unexpected read failures

## Implementation Plan

### 1) Add route module
Create `src/EWMRS/api/routes/nexrad.js` with:
- Parameter validation (required params, strict timestamp format, safe site/variable/file/sweep values).
- Path construction rooted at `req.app.locals.GUI_DIR` + `NEXRAD`.
- Variable allowlist mapped directly to `<variable>.bin.gz` filenames.
- Endpoint handlers:
  - `GET /sites` (directory discovery and site de-duplication)
  - `GET /timestamps` (timestamp directory listing and sorting)
  - `GET /download` (binary file serving)
- Explicit allowlists for `variable` and `elevation` values.
- Binary response via `res.sendFile(...)` or `fs.readFile(...)` + headers.

### 2) Mount route in server
Update `src/EWMRS/api/server.js`:
- Import and mount `nexradRouter` at `/nexrad`.
- Add `/nexrad/sites`, `/nexrad/timestamps`, and `/nexrad/download` to root endpoint metadata list.

### 3) Add API tests
Update `tests/api/routes/test_ewmrs_api.js`:
- Add app wiring for `/nexrad`.
- Add `GET /nexrad/sites` tests:
  - returns sorted unique site list for variable
  - returns empty list when variable has no folders
- Add `GET /nexrad/timestamps` tests:
  - returns sorted timestamps for `site` + `variable`
  - handles optional `sweep`
  - returns empty list/404 behavior per final contract when site or variable is absent
- Add `GET /nexrad/download` happy-path tests for each file selector (`azimuths`, `ranges`, `data`).
- Add negative tests for:
  - missing params
  - invalid timestamp format
  - directory traversal attempts
  - invalid variable
  - invalid sweep format
  - invalid `file` selector
  - missing file (404)

Update `tests/api/test_ewmrs_server.js` (if needed) to assert route exposure remains consistent with server startup behavior.

### 4) Update documentation
Update `docs/api/ewmrs_api_endpoints.md`:
- Add `/nexrad/sites`, `/nexrad/timestamps`, and `/nexrad/download` contracts.
- Document `variable` parameter and accepted values.
- Document optional `elevation` behavior for timestamp listing.
- Add route to root endpoint list.

If route lists are duplicated elsewhere, update those references for consistency.

## Validation
1. Run EWMRS API Jest route tests (`tests/api/routes/test_ewmrs_api.js`).
2. Verify `/` includes `/nexrad/sites`, `/nexrad/timestamps`, and `/nexrad/download`.
3. Confirm `/nexrad/sites` and `/nexrad/timestamps` return expected ordered JSON arrays.
4. Confirm `/nexrad/download` returns expected `.bin.gz` bytes and headers.
5. Confirm invalid input and missing resources return expected status codes.

## Notes / Assumptions
- This plan intentionally exposes raw binary intermediates only, not decoded arrays.
- `scan_timestamp` format follows existing EWMRS conventions (`YYYYMMDD-HHMMSS`).
- `variable` is API-facing and mapped directly to the on-disk `<variable>.bin.gz` filename.

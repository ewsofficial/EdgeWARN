# Ingestion Architecture

EdgeWARN-Core uses shared ingest modules in `src/common/ingest` so EdgeWARN and EWMRS can consume the same staged data products.

## Module Layout

```text
src/common/ingest/
├── mrms/            # MRMS + GOES discovery/download and staging
├── nws/             # NWS active alert ingest + registry/snapshots
├── synoptic/        # RAP ingest
├── metar.py         # METAR ingest and parsing
└── wpc/             # WPC surface analysis ingest and GeoJSON conversion
```

## Staged Tandem Ingest

`src/common/pipeline/coordinator.py` drives staged readiness for tandem execution:

1. Detection inputs ready (MRMS detection subset)
2. EWMRS MRMS inputs ready (detection + MRMS integration subset)
3. EWMRS GOES inputs ready (separate GOES ABI render phase boundary)
4. EdgeWARN integration inputs ready (adds RAP and, when coupled, GOES)

This ordering preserves low-latency detection while allowing render and integration stages to proceed only when required data are staged.

For realtime tandem execution in `src/run.py`, GOES ingest remains decoupled from the shared MRMS ingest cycle. The runner performs a best-effort local GOES availability check and always releases the GOES render phase so MRMS rendering is never blocked. EWMRS GOES readiness is tied to the full configured ABI scalar render set, currently `GOES_ABI_C01` through `GOES_ABI_C16`, while EdgeWARN integration readiness still checks GLM availability separately.

## MRMS + GOES

`src/common/ingest/mrms/main.py` provides async-first ingestion with sync fallback paths.

Key entry points:

- `download_all_files_async(dt, max_entries=10, remove_old_files=True)`
- `download_detection_files_async(dt, ...)`
- `download_integration_files_async(dt, ...)`
- `download_ewmrs_files_async(dt, ...)`
- `download_all_files(dt, ...)` (async wrapper with sync fallback)

Notes:

- Detection and integration modifiers are staged separately
- GOES ingestion can run as part of the full ingest cycle or as a decoupled background loop in realtime mode
- GOES ABI staging uses `ABI-L1b-RadC` channel files from `noaa-goes19`; GLM staging remains a separate modifier path
- RGB render products are derived later from the staged ABI channel set and do not have separate remote ingest definitions
- Cleanup is constrained to configured runtime directories

See `docs/core/goes_pipeline.md` for the end-to-end GOES readiness and render flow.

## NWS Alerts

`src/common/ingest/nws/main.py` downloads active alerts from `https://api.weather.gov/alerts/active`, applies GeoMapper processing, and updates the alert registry.

Key behavior:

- Blocklist filtering for non-target event types
- Deduplicated alert tracking with `first_seen`, `last_seen`, and `expires`
- Timestamp snapshot generation for API serving
- TTL cleanup for stale registry and snapshot files

Primary entry points:

- `download_alerts(dt)`
- `download_alerts_async(dt)`

## RAP / Synoptic

`src/common/ingest/synoptic/main.py` stages RAP files for integration.

Entry points:

- `download_rap_async(dt)`
- `download_rap(dt)`

## METAR

`src/common/ingest/metar.py` ingests hourly METAR cycle files, parses reports, enriches station coordinates from the station cache, filters to CONUS bounds, and writes hourly JSON snapshots.

Entry points:

- `ingest_metars()`
- `ingest_metars_async()`

Output file pattern:

- `METAR_YYYYMMDD-HHz.json`

## WPC Surface Analysis

`src/common/ingest/wpc/main.py` fetches coded surface analysis, parses it, converts it to GeoJSON, and writes:

- `latest.geojson`
- timestamped `wpc_sfc_YYYYMMDD-HH0000.geojson` artifacts

Entry points:

- `fetch_surface_analysis(dt=None, save_timestamped=False)`
- `run_wpc_ingest()`

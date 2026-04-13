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
2. EWMRS render inputs ready (detection + MRMS integration subset)
3. EdgeWARN integration inputs ready (adds GOES + RAP)

This ordering preserves low-latency detection while allowing render and integration stages to proceed only when required data are staged.

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
- GOES ingestion runs as part of the full ingest cycle
- Cleanup is constrained to configured runtime directories

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

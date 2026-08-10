# NEXRAD ingest

- **Liveness:** bound per-site discovery, chunk-list, and ingest/download awaits, plus each full scan. On timeout, cancel/close it, log site/volume/stage/elapsed time, and continue; `gather()` must not stall future scans. Emit scan/output heartbeats and restart workers with stale heartbeats (process liveness alone is insufficient). Add hung-await and stale-worker regressions. [#89]

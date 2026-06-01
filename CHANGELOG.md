# Changelog

## [2.6.1] 2026-06-01

### Added

### Changed
- Reduced NEXRAD ingest polling latency and increased ingest concurrency for faster scan turnaround
- Streamlined NEXRAD ingest hot path and removed the xradar dependency
- Checkpointed NEXRAD startup parsing and compacted runtime buffers to lower startup latency
- Reduced NEXRAD worker RSS growth during incremental exports and overall ingest memory footprint
- Cached cost matrix entries, hoisted `AssignmentCostCalculator` out of single-candidate loops, and built detection centroid arrays once in `prefilter_candidates` for hybrid assignment
- Replaced linear track lookups with an O(1) `tracks_by_id` dict in `run_hybrid_assignment`
- Switched `latest_files` to `os.scandir` to reduce per-cycle filesystem overhead
- Hoisted `multiprocessing.Manager` out of per-cycle scope across tandem runs
- Context-managed `xr.open_dataset` calls to release file handles deterministically
- Gated `perf_tracker` instrumentation on the `EDGEWARN_PERF_TRACKER` env var
- Skipped gzip compression for `image/*` responses on both API servers
- Cached `_load_timestamp_tile_index` by `(path, mtime)` and pre-compiled timestamp regex patterns in `find_timestamp`
- Replaced the manual RLock colormap cache with `lru_cache`
- Bounded `express.json` body size and mounted the rate limiter before the body parser
- Removed the stale EWMRS scheduler fork, unused NEXRAD polling wrappers, NWS legacy ingest functions, the `run_ewmrs_pipeline` passthrough, the unused `realtime_pipeline` entry point, and manual `gc.collect` calls in the integrator

### Fixed
- Tightened `isSafeFilename` against control characters and Windows-reserved names, and added realpath containment to `readJsonFileSafe`
- Enforced `PRODUCT_MAPPING` allowlist on `/renders/fetch` and applied `resolveUnder` containment in WPC `/download`
- Blocked EWMRS query and WPC path escape attempts
- Used `path.relative` containment in `readJsonFileSafe` for cross-platform robustness
- Type-guarded `validateCellId`, `validateTimestamp`, and `validateTimestampV2`
- Used `fileURLToPath` for `mappings.json` path resolution on Windows
- Preserved NEXRAD elevation timing metadata in manifests and low elevations while trimming runtime files
- Restored NEXRAD pending chunk readiness and grouped AR2V rendering
- `--profile` now auto-enables `perf_tracker` and subsets netCDF before load
- Synced remote branch head before pushing to avoid spurious push failures

### Testing
- Covered NEXRAD worker RSS sampling and pool recycling
- Fixed a failing S3 test

## [2.6.0] 2026-05-28

### Added
- Realtime NEXRAD ingest pipeline with VCP-gated chunk ingest, scan coordinator, and timestamped retention logging
- Event-driven NEXRAD sweep rendering pipeline serving raw layer artifacts from GUI storage
- EWMRS NEXRAD discovery routes with gzipped data payloads, exposing all scan elevations through the API
- Full NEXRAD network downloads across available K* radars with downloaded sites returned from coordinator runs
- Consolidated NEXRAD bin payload format for render intermediates, with documentation of the layout

### Changed
- Streaming BZ2 ingest with mmap worker parsing, direct AR2V volume writes, and incremental offset tracking
- Reduced NEXRAD worker and pipeline peak memory via streaming writes, deferred imports, library cache clearing, record cleanup, and malloc_trim
- Refactored NEXRAD pipeline into service classes and a realtime subpackage with centralized timestamp and chunk helpers
- Aligned NEXRAD colormaps with official variable keys and added interpolation for VRADH and WRADH
- Parallelized NEXRAD latest-scan ingest with capped shared chunk concurrency and async S3 compatibility patches
- Doppler-angle elevation binning with grouped AR2V renders decoded directly and paired low sweeps preserved
- Polled local artifacts for GUI rendering and moved render serialization into the EWMRS render module
- Updated API, core, and plans documentation to reflect current source behavior and performance optimization work

### Fixed

### Testing
- Added coverage for streamed NEXRAD grouping regressions, bin payload serialization, AR2V-only storage ingest, and the realtime pipeline subpackage imports

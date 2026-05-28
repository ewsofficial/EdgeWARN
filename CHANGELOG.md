# Changelog

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

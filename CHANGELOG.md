# Changelog

## [2.6.2] 2026-06-07

### Added

### Changed
- Split `run.py` helpers into `util.runtime` package

### Fixed
- Route NEXRAD ingest process output through QueueWriter to prevent stdout pipe buffer blocking
- `order_recent_volume_ids` sorts numeric IDs by string value instead of integer value
- Render grouped NEXRAD elevations from AR2V sweeps only
- Adjust NEXRAD file retention to 2
- Render all available fields from grouped NEXRAD elevation artifacts

### Testing
- Add `StartedProcessRegistry` unit tests

# Changelog

## [2.6.3] 2026-06-17

### Added
- ``--disable-polygon-expansion``: skip ProbSevere polygon expansion and use the raw geometry only
- Added ``p90EchoTop30`` and ``p90EchoTop50`` keys as well as MRMS EchoTop50 ingestion

### Changed
- Update historical processing pipeline to use updated arguments

### Fixed
- Added heartbeat emission on NEXRAD parse workers to prevent them from timing out after a while
- Prune stale NEXRAD runtime volumes to prevent disk usage from ballooning

### Testing

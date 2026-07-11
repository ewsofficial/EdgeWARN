# Changelog

## [2.7.0] 2026-07-11

### Added
- ``--mrms-core-only``: run MRMS ingestion for EWMRS rendering without waiting on EdgeWARN detection outputs
- ``--disable-nexrad`` CLI flag to skip NEXRAD ingestion entirely
- StormCast alert outcome tracking and aggregated NEXRAD cleanup logging

### Changed
- Removed FLOHAR and Mesocyclone CTAM modules and related code, tests, API routes, and documentation
- Remediated runtime logging noise: condensed per-file summaries, cleaned up MRMS downloader log spam, reduced per-cycle output volume
- Condensed tracking debug output to reduce verbosity

### Fixed
- Release tandem MRMS ingestion phases (detection, render, integration) as soon as their respective inputs become available instead of waiting for all phases

### Testing
- Added regression coverage for logging remediation across MRMS ingest, synoptic downloader, scheduler, RAP pipeline, IO utilities, and runtime background workers


# Changelog

## [2.0.1] - 2026-03-23

### Fixed
- Preserve StormCast polygons during alert generation to prevent data loss.

### Changed
- Refactored code to extract shared S3 target-file selection, METAR helpers, and synoptic S3 parameters for better maintainability.
- Precompiled validation regexes to improve performance.
- Hardened performance benchmark calculations for more reliable testing.

### Added
- Added benchmark tests for validation regexes.

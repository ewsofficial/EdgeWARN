# Changelog

## [2.0.1] - 2026-03-23

### Fixed
- Preserve StormCast polygons during alert generation to prevent data loss.
- Updated EWMRS GUI cleanup to use the live render configuration so timestamp folders are deleted from the active filesystem root.

### Changed
- Refactored code to extract shared S3 target-file selection, METAR helpers, and synoptic S3 parameters for better maintainability.
- Precompiled validation regexes to improve performance.
- Hardened performance benchmark calculations for more reliable testing.
- Bumped the application version from `2.0.0` to `2.0.1` across package metadata, API responses, and release tests.

### Added
- Added benchmark tests for validation regexes.

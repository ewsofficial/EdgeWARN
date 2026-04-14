# Changelog

## [2.2.0] - 2026-04-14

### Added
- Added mesocyclone snapshot serving from API v2.
- Added NMDA-style mesocyclone CTAM sidecar output.
- Added MRMS MESH rendering for EWMRS

### Changed
- Reduced detection hot-path overhead with caching and vectorization.
- Lowered mesocyclone RSS with staged azshear processing.
- Parallelized mesocyclone stages and replaced full-grid gate scans with targeted component scanning.
- Removed WarmRainProbability integration.

### Fixed
- Used CompRefQC scan timestamps for FLOHAR output.
- Bound mesocyclone detection to component windows.
- Reduced mesocyclone grid harmonization overhead.
- Harmonized mesocyclone inputs across staggered MRMS grids.
- Aligned mesocyclone detection with 0.005 deg AzShear grid.
- Reduced FLOHAR to the strongest retained inputs.
- Rounded saved centroids to 3 decimals.
- Rounded integration stats to 2 decimals.
- Rounded saved polygon geometry to 3 decimals.
- Added RAP surface field aliases.

### Testing
- Added coverage for 2-decimal integration rounding.
- Updated integration benchmark.

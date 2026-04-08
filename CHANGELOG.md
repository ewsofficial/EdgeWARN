# Changelog

## [2.1.0] - 2026-04-07

### Fixed
- Fix dataset integration returning zeroes

### Changed
- Added STRtree filtering for alert matching.
- Precompiled RAP derived formulas once per field.
- Added spatial bins for GLM flash lookup.
- Switched `latest_files` selection to a top-k strategy.
- Made stormcell index updates incremental.
- Avoided meshgrid allocation in 1D bounding-box tracing.
- Refreshed AGENTS documentation for current codebase layout.
- Reduced per-cell integration hot-loop overhead.

### Added

### Documentation
- Updated AGENTS documentation.

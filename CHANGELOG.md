# Changelog

## [2.5.1] 2026-05-02

### Added
- CLI flags and environment-variable overrides for configuring EdgeWARN and EWMRS API rate limits
- Runtime controls to disable EWMRS processing and optional background ingest components when running the tandem pipeline
- API and unit test coverage for the new server controls, tandem coordinator behavior, and I/O helpers


### Changed
- Optimized integration processing by reusing spatial lookups, reducing worker copy overhead, and parallelizing selected pipeline work
- Improved storm-cell detection performance with parallel radar and precip-type loading plus more compact gate-expansion label reduction
- Updated tandem pipeline coordination and run-time flow to support the new component toggles and related API documentation


### Fixed
- RGB values in colormap causing render pipeline fails
- Clamped negative GOES reflectance values before masking in EWMRS rendering
- Removed redundant alert logging noise during processing
- Corrected StormCast package exports by dropping an unused compatibility import

### Testing
- Added regression coverage for EWMRS API behavior, API rate-limit configuration, gate-mapper connectivity, integration parallelism, tandem coordinator toggles, and utility I/O handling

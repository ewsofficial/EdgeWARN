# Changelog

## [2.6.4] 2026-06-21

### Added

### Changed
- Accelerated previous-stormcell lookup by switching detection scans to `os.scandir`.
- Reduced detection copy overhead by replacing a broad `deepcopy` path with shallow per-entry copies.
- Optimized Kalman filtering by vectorizing process-noise construction, avoiding covariance copies on read, and replacing matrix inversion with `np.linalg.solve` for gain computation.
- Improved EWMRS raster rendering with a single-pass RGBA LUT interpolation path.
- Reduced RAP pipeline memory pressure by reading RAP message values as `float32`.
- Removed the unused per-cell `RAPPointExtractor.extract` path from the GRIB loader.
- Improved EWMRS worker startup and cleanup throughput by warming render imports and parallelizing GUI cleanup.

### Fixed

### Testing

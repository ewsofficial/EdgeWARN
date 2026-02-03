# Changelog for Version ``1.4.0``

## Additions

- **MorphoWind CTAM Module**: New physics-based wind detection module using morphological analysis and Gaussian smoothing for scoring
- **Custom Fast GRIB Loader**: New `util/grib_loader.py` using direct `eccodes` bindings, bypassing `cfgrib` overhead (30x faster for AzShear files)
- **RAP Environmental Integration**: Config-driven RAP integration with environmental data (CAPE, CIN, wind profiles, etc.)
- **AzShear Integration**: Added AzShear Low/Mid to integration config for mesocyclone detection
- **Cell History Retrieval Utility**: New utility for retrieving storm cell history
- **Comprehensive Test Suite**: Added unit tests for MorphologyEngine, StormCellIntegrator, and physics-based MorphoWind logic
- **Performance Benchmark Tests**: New `tests/benchmarks/test_performance.py` measuring memory, CPU, and execution time for all pipeline components

## Changes

- **Detection Phase Optimization**: Switched to custom fast GRIB loader for radar data, achieving ~75% speedup in detection phase (16s → 4s)
- **RAP Loading Strategy**: Reverted to unfiltered `cfgrib.open_datasets` loading (fastest approach)
- **Morphology Engine Optimization**: Reduced overhead with early bailout for small cells and pre-allocated kernels
- **Integration I/O Optimization**: Grouped datasets to reduce file I/O operations
- **MorphoWind Refactor**: Moved into dedicated package folder, implemented Gaussian smoothing for scoring
- **Microphysics Metrics**: Moved from detection phase to integration phase
- **Integration Config Syntax**: Updated to use `p{percentile}` syntax (e.g., `p95AzShearLow`)
- **OpenCV Dependency**: Moved to pip dependencies to resolve conda conflicts

## Fixes

- **RAP File Cleanup**: Fixed bug where RAP files were not deleting due to symlink safety check and hardcoded limit
- **CVE-2025-55182**: Resolved critical vulnerability and updated dependencies
- **AzShear Performance Bottleneck**: Fixed 44s → 1.5s GRIB loading regression caused by `cfgrib` metadata parsing issues
- **RAP Integration Performance**: Fixed performance regression in RAP data loading
- **Morphology Metrics Bugs**: Fixed skeletonization and contour analysis issues
- **Test Failures**: Resolved various test failures in v1.4.0 test suite

## Documentation

- Added MorphoWind module documentation
- Updated CTAM, Detection, and Integration docs with MorphoWind references
- Added comprehensive RAP data structure documentation
- Added CVE-2025-55182 fix documentation
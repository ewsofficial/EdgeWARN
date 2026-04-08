# Changelog

## [2.1.0] - 2026-04-07

### Added
- Added modular AzShear mesocyclone analysis and split integration logic into dedicated `azshear`, `core`, `geometry`, `io`, and pipeline modules.
- Added three-worker parallel integration execution with hardened save boundaries for generated storm-cell outputs.
- Added EWMRS VIL render exposure in API listings, bundled assets, and colormap metadata.
- Added benchmark coverage for AzShear integration, the parallel integration pipeline, and EWMRS render performance.

### Changed
- Refined AzShear support feature analysis by tightening morphology inputs, ownership and pairing rules, minimum gate thresholds, and peak-component prioritization.
- Replaced AzShear support outputs with component summaries, weighted support centroids by gate value, and now return `null` support data when detections are missing.
- Disabled AzShear support feature integration in the main pipeline while keeping the updated mesocyclone analysis path available.
- Improved integration throughput with faster GRIB loading, pre-materialized AzShear arrays, and additional support-metric optimizations.
- Optimized EWMRS tiled rendering to reuse unchanged layers more aggressively.

### Fixed
- Hardened mesocyclone scoring and per-cell integration handling for missing or incomplete AzShear detections.
- Corrected EWMRS VIL and VIL Density mappings so each product resolves to the proper render layer.

### Documentation
- Updated integration and MorphoWind documentation to match the AzShear refactor and current runtime behavior.

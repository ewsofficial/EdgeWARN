# Changelog for Version ``1.5.2``

## Additions

- **Performance profiling and instrumentation** - Added TimingTracker utility in `src/util/performance.py` for granular timing of ingestion, detection, and integration modules
- **Profiling flag and optimization strategies** - Added `--profile` flag to `run.py` and `process_historical.py` with short-circuit logic for empty frames, state persistence (caching) for detection data, and pre-loaded objects support in DetectionDataHandler

## Changes

- **Finalize Optimizations** - Implemented NWS LRU Cache for geometry processing (-54% latency), optimized S3 listing with StartAfter (-75% lookup time), verified full system stability
- **Optimization of Ingestion Pipeline** - Implemented asynchronous filesystem cleanup, shared S3 client in scheduler, parallel decompression for GOES products, optimized METAR ingestion with async station DB load and session reuse
- **Vectorize draw_bbox coordinate extraction** - Refactored `draw_bbox` in `gatemapper.py` to use vectorized NumPy operations for ~8-10x speedup (0.26s vs 2.20s)
- **Optimize detection/integration pipeline hot paths** - Optimized hot paths in detection and integration pipelines

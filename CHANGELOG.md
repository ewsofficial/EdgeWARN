# Changelog

## [2.0.0-alpha] - 2026-03-12

### Added
- **Predictive Storm Tracking**: Implemented a core Kalman Filter motion tracking system to continuously predict and associate storm cells over time, replacing static overlap tracking. Includes configurable state dynamics to handle missed scans and a 6-minute Time-To-Live (TTL) for predictions.
- **Storm Lineage Detection Engine**: Introduced a storm merge/split tracking module to maintain continuity when storm cells intersect or fragment. Supported by comprehensive unit and integration tests.
- **Lineage Tracking Links**: Added `merged_cells` and `merged_to` keys to storm cell output to explicitly track which cells combined during merge events. Dissipated cells are now preserved in their final state with these links.
- **API v2**: New RESTful API endpoints for improved developer experience:
  - `GET /api/v2/features/cells` - List available cells or get specific cell data
  - `GET /api/v2/features/timestamps` - List available timestamps or get stormcell data
  - `GET /api/v2/features/alerts/official` - List active official (NWS) alerts or get snapshot/alert data
  - `GET /api/v2/features/alerts/edgewarn` - List active EdgeWARN internal alerts
  - `GET /api/v2/data/metar` - List METAR timestamps or get METAR data
- **FLOHAR (FLOod HAzaRds)**: New CTAM Grid Module for automated flash flood detection using MRMS FLASH products (CREST, HP, ARI, Soil Saturation).
- **Advanced Grid Indexing**: Added `GridIndex` factory with `RegularGridIndexer` (O(1)) and `KDTreeGridIndexer` (O(log N)) for highly optimized spatial lookups.
- **Warm Rain Probability Integration**: Added ingestion and processing logic for Warm Rain Probability, returning the maximum (`max`) expected probability value.
- **Unified Alert System**: Implemented `AlertManager` and `AlertPayload` for standardized internal alert emission across CTAM modules.
- **Legal Protection**: Added `robots.txt` policy with specific restrictions and "cheeky" legal warnings for OpenAI/LLM scrapers.
- **Automated Cleanup**: Added automatic disk cleanup for expired EdgeWARN alerts and old radar snapshots.

### Changed
- **Pipeline Optimization**: instituting concurrent execution for downloading integrations and moving METAR/NWS data ingestion to background processes, significantly reducing total cycle latency.
- **Memory Efficiency**: 
  - Implemented `RAPPointExtractor` using eccodes for zero-grid-memory point extraction from RAP files (eliminating ~3GB memory spikes).
  - Implemented lazy loading for NetCDF dataset integration.
  - Optimized FLOHAR module by destructively popping grids and limiting concurrent threads.
- **Security Hardening**:
  - Enabled HSTS and strengthened CORS/Trust Proxy configurations.
  - Implemented strict input validation and prototype pollution prevention for API endpoints.
  - Restricted detailed version exposure in production environments.
- **Metric Key Conventions**: Updated system-wide metric aggregation keys, replacing `p100` designations with the `max` convention.
- **Storage Restructuring**: Reorganized NWS and EdgeWARN alert storage into `ids/` and `timestamps/` subdirectories for better scalability.
- **Coordinate Precision**: Rounded polygon coordinates to 3 decimal points for improved storage efficiency and API performance.

### Fixed
- **Lineage Buffer Scan Termination**: Corrected logic for ending scans in the lineage buffer to ensure monotonic scan numbering.
- **Alert Matching Accuracy**: Improved alert-to-cell matching using high-precision polygon-to-polygon intersection (with `shapely.prepared`).
- **NWS Alert Deduplication**: Addressed code review issues within NWS data feed logic to accurately filter redundant event alerts.
- **StormCast Dropout Prevention**: Modified history mechanics to load historical state directly from persistent files.
- **TimingTracker Race Condition**: Resolved a thread-safety race condition within concurrent system tracking instances.
- **Precipitation Flag Logic**: Corrected hail core mask to use `PrecipFlag` 6 (Hail) instead of 7 (Snow).
- **Multi-message GRIB Support**: Enabled eccodes multi-message support for RAP wind fields (V-component).

### Removed
- **API v1**: All v1 endpoints (`/features/*`, `/data/*`) have been removed (now returning `410 Gone`).
- **Legacy Components**: Removed obsolete Footprint and CellAlert modules.
- **Cleanup**: Removed outdated project plan documents and legacy PRDs.

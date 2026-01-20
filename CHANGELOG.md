# Changelog for Version ``1.x.x``

## ``1.0.0`` (2025-12-26)

### Additions
- New data sources replacing ``MRMS_RotationTrack_30min``

``MRMS_MergedAzShear_0-2kmAGL``

``MRMS_MergedAzShear_3-6kmAGL``

- Added `remove_old_files` argument to `ingest.download_all_files` to control file cleanup.
- Added `remove_old_cells` argument to `index_manager` and `integration` to control cell history cleanup.

### Changes

### API
- API now follows RESTful API design
- Data fetching is now done via GET requests at 

``
/features/fetch/resources?type=[list | cell]
``

which returns a list of available timestamps for ``type=list`` and a list of cell IDs for ``type=cell``

- Data downloading is now done via GET requests at 

``
/features/download/resources?type=[list | cell]&[timestamp | id]=VALUE
``

where ``VALUE`` is the timestamp for ``type=list`` in ``YYYYMMDD-HHMM00`` format and the cell ID for ``type=cell``

### Cell JSONs
- Remove ``stormcell_test.json`` which contained all active cells and their histories
- A list of active cell JSONs are stored in ``STORMCELL_DIR``
- Individual cell JSONs are stored in ``CELL_DIR``

## 1.0.1 (2025-12-27)

### Fixes
- Fix DNS resolution errors on some machines
    - If you get this issue, remove ``aiodns`` from your environment
- Fix permission errors on Linux with the ``EdgeWARN_input`` path

### 1.0.2 (2026-01-0x)

### Additions
- Added downloading of RAP files from the ``noaa-rap-pds`` S3 bucket

### Fixes
- Cell detection pipeline crashes when ``PrecipType`` dataset is not loaded, despite ``CompRef`` and ``ProbSevere`` successfully loading. The fix will return an empty list to the hail core dict if ``PrecipType`` fails to load.

### Changes
- Offloaded the synchronous ``merge_glm_files`` operation in the async ingest module to a thread pool using ``run_in_executor``, preventing the event loop from blocking during heavy I/O and CPU-bound operations.
- Parallelized the MRMS update checks in the scheduler using ``ThreadPoolExecutor``, reducing latency when checking multiple data sources.

## 1.1.0 (2026-01-07)

### Additions
- Implemented RAP meteorological data integration for storm cells, incorporating U and V wind components at 850, 700, 500, and 250mb levels using a nearest-neighbor grid point mapping.
- Added CTAM module registry and engine to allow for dynamic registration and execution of CTAM modules.
- Added StormCast module to CTAM
- Add custom filesystem initialization for ``run.py``

### Changes
- Enhanced storm cell polygon fidelity by implementing an adaptive downsampling strategy that preserves more boundary detail for smaller and medium-sized cells.
- Use ``distance_transform_edt`` for gate expansion to improve performance.
- Cell data saving now loads coordinates lazily
- Add a 10 requests / second rate limit to the API to prevent abuse.

### Fixes
- Resolved a coordinate mismatch issue in RAP integration by normalizing storm cell centroids to the -180 to 180 longitude range.
- Fixed a crash in `xarray` dataset coordinate assignment by providing explicit dimension names for 2D coordinate data.

## 1.1.1 (2026-01-13)

### Additions
- Add security headers (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection) to API responses
- Add configurable CORS support via `CORS_ORIGINS` environment variable
- Add a ``--base_dir`` option to ``run.py`` to allow for custom base directories for data storage
- Add explicit checks for the existence of all input files (Radar, ProbSevere, PrecipType) at the start of the cell detection pipeline

### Changes
- Vectorize hailcore polygon creation for improved performance
- Improve robustness for missing input files in the cell detection pipeline, allowing for graceful degradation instead of crashing
- Update `DetectionDataHandler.find_timestamp` to log a warning instead of info when regex timestamp extraction fails and falls back to `utcnow`

### Fixes
- Fix StormCast module saving the uncertainty circle centers incorrectly
    - Problem: StormCast was using hardcoded values (35°N, -97°W) as the reference point for forecast cone calculations instead of each storm's actual centroid
- Add a safeguard to abort processing with a warning if no valid radar data is found, preventing downstream crashes

## 1.2.0 (2026-01-20)

### Additions
- Implement METAR ingestion module to fetch, parse, and save METAR data from NOAA cycle files
    - Parses location, time, wind, visibility, temperature, and altimeter from METAR strings
    - Processes the latest 3 hours of data automatically
    - **Now supports coordinate lookup for stations using Aviation Weather database**
- Add streaming ingest for NWS active alerts from `api.weather.gov`
    - Uses `ijson` for memory-efficient streaming JSON parsing
    - Filters for severe weather events (Tornado, Severe Thunderstorm, Flood, Winter Weather)
    - Outputs GeoJSON format compatible with mapping libraries
- Add `round_to_nearest_even_minute()` utility for consistent timestamp matching across scheduler and downloader
- Add data integrity check scripts: `check_data_integrity.py`, `validate_rounding.py`, `mock_download_test.py`
- **Add `--debug_server` flag to API (runs on port 3001)**
- **Add new API data endpoints for NWS and METAR access:**
    - `GET /data/fetch?type=[nws|metar]`
    - `GET /data/download?type=[nws|metar]&timestamp=YYYYMMDD-HHMM00`

### Changes
- Scheduler and downloader now use rounded even-minute timestamps for file matching
    - Fixes hour-boundary misalignment issues (e.g., 23:59 → 00:00)
    - Debug logging added when non-exact rounded matches are used
- Remove `NLDN_CG_005min_AvgDensity` from `check_modifiers` due to incompatible update cadence
- **Changed default run bounds to 20-55 N, 230-300 E (approx. -130 to -60 W) to cover continental US**
- **METAR parser now converts altimeter settings to decimal inHg (e.g. 30.12) and removes raw data field**
- **Refactored API endpoints to consolidate fetch and download routes by resource type**

### Fixes
- Fix timestamp misalignment causing missed downloads at hour boundaries for AzShear products
- **Fixed critical bare `except` clause in NWS ingestion that was suppressing `KeyboardInterrupt`**
- **Fixed NWS ingestion error with `Decimal` JSON serialization**
- **Fixed API `BASE_DIR` detection to properly search user home directory for `EdgeWARN_input`**
- **Fixed missing METAR ingestion in the main `run.py` pipeline**

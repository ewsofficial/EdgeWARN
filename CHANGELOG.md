# Changelog for Version ``1.0.x``

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

### 1.1.0 (2026-01-07)

### Additions
- Implemented RAP meteorological data integration for storm cells, incorporating U and V wind components at 850, 700, 500, and 250mb levels using a nearest-neighbor grid point mapping.

### Changes
- Enhanced storm cell polygon fidelity by implementing an adaptive downsampling strategy that preserves more boundary detail for smaller and medium-sized cells.
- Add a 10 requests / second rate limit to the API to prevent abuse.

### Fixes
- Resolved a coordinate mismatch issue in RAP integration by normalizing storm cell centroids to the -180 to 180 longitude range.
- Fixed a crash in `xarray` dataset coordinate assignment by providing explicit dimension names for 2D coordinate data.

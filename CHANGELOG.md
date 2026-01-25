# Changelog for Version ``1.3.0``

## Additions
- **Enhanced METAR parsing**
    - Added parsing for cloud layers, weather conditions, and remarks sections
    - **New Data Keys:**
        - `clouds`: List of cloud layers (e.g., `[{"code": "OVC", "altitude": 2000, "type": "CB"}]`)
        - `weather`: List of weather phenomena strings (e.g., `["+RA", "BR"]`)
        - `remarks`: Raw remarks string (e.g., `"AO2 SLP134"`)
- **Integrated GeoMapper into NWS Ingest**
    - Geocode-to-polygon mapping logic
    - Junk property removal logic
    - Runs during ingestion pipeline

## Changes
- **Filter METAR stations to CONUS bounds** (Lat: 24-50, Lon: -125 to -66)
- **Implement resilient async ingestion pipeline**
    - Added granular error handling and individual sync fallbacks for each data source (MRMS, RAP, NWS, METAR)
    - Prevents a single failed async source from triggering a slow synchronous download of all sources
- Remove redundant print statements in the CTAM processing pipeline
- **Transition GateMapper from Voronoi to Watershed expansion**
    - Ensures spatial connectivity (no "jumping" across gaps)
    - Constrained by reflectivity thresholds and adjacency
    - Optimized with cropped sub-grids and float16 maps for memory efficiency


## Fixes
- **Fix StormCast forecast cones appearing far from storm center**
    - Root cause: Absolute projected coordinates were passed to the engine instead of relative offsets
    - Fix: Now uses relative coordinates (current = 0,0; previous = -dx, -dy) based on storm centroid
- **Fix Centroid/Bbox mismatch and "Ghost" Cell hijacking** in detection engine
    - Root cause: Distance-based Voronoi expansion allowed weak ProbSevere polygons to "hijack" distant high-reflectivity storms
    - Fix: Implemented percentage-based seed filtering (min 40% coverage) to suppress weak seeds before expansion
- **Fix Storm Attribute misalignment (Centroid/Gates/MaxRefl)**
    - Root cause: Attributes were calculated on ProbSevere seeds instead of fully expanded masks
    - Fix: Unified all cell properties to scale with Watershed results
- **Resolve Numerical Instability in Weighted Centroids**
    - Root cause: Exponential weighting of high reflectivity ($e^{70}$) caused floating-point overflow
    - Fix: Implemented Log-Sum-Exp normalization to ensure stable centroids across all storm intensities
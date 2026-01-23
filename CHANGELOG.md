# Changelog for Version ``1.3.0``

## Additions
- **Add GeoMapper CTAM module** for NWS geocode-to-polygon mapping
    - Converts zone codes (e.g., `INZ021`) to precise polygon geometries using local asset library
    - Removes unnecessary properties from NWS alerts
    - Integrated into CTAM pipeline execution

## Changes
- **Filter METAR stations to CONUS bounds** (Lat: 24-50, Lon: -125 to -66)
- **Implement resilient async ingestion pipeline**
    - Added granular error handling and individual sync fallbacks for each data source (MRMS, RAP, NWS, METAR)
    - Prevents a single failed async source from triggering a slow synchronous download of all sources


## Fixes
- **Fix StormCast forecast cones appearing far from storm center**
    - Root cause: Absolute projected coordinates were passed to the engine instead of relative offsets
    - Fix: Now uses relative coordinates (current = 0,0; previous = -dx, -dy) based on storm centroid
- **Fix Centroid/Bbox mismatch and "Ghost" Cell hijacking** in detection engine
    - Root cause: Distance-based Voronoi expansion allowed weak ProbSevere polygons to "hijack" distant high-reflectivity storms
    - Fix: Implemented percentage-based seed filtering (min 40% coverage) to suppress weak seeds before expansion
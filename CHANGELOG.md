# Changelog

## Unreleased

### Features

- **NWS Alert Filter**: Implemented a "Drop List" to exclude specific NWS alert types (e.g., tests, administrative messages) from ingestion.
- **NWS Filter Removal**: Removed the previous "Allowed Events" filter; now all non-dropped events are ingested.

# Changelog for Version ``1.5.0``

## Additions

- **VIL Integration**: Added VIL (Vertically Integrated Liquid) integration with multiple percentiles (p100, p95, p90, p50)
- **Reflectivity Layers**: Added integration for Reflectivity at 0°C, -5°C, and -15°C isotherms
- **MRMS Downloads**: Added support for downloading MRMS Reflectivity at 0°C, -5°C, -10°C, -15°C, and -20°C
- **Verification Scripts**: Added scripts for verifying GLM and ProbSevere spatial alignment

## Changes

- **Wind Field Restructuring**: Grouped all isobaric wind components into a nested `wind_field` dictionary (e.g., `wind_field.u850`) for cleaner JSON output
- **RAP Wind Expansion**: Expanded RAP wind integration to include all 37 pressure levels (100 hPa to 1000 hPa)
- **Surface Winds**: Added extraction of 10m surface winds (`u10m`, `v10m`)
- **MorphoWind Cleanup**: Removed redundant `morphowind` key from cell properties (now exclusively in `modules.MorphoWind`)
- **Cleanup**: Removed unused directories and legacy verification/benchmarking scripts

## Fixes

- **Pipeline Stability**: Resolved pipeline unpacking crashes and added safety checks for integration
- **StormCast Module**: Fixed incorrect key usage for EchoTop30 in StormCast module

## Documentation

- **Data Keys Update**: Comprehensive update to `EdgeWARN_Data_Keys.md` reflecting the new 1.5.0 data structure, including `wind_field` and new reflectivity keys

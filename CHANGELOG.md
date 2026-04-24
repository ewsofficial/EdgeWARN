# Changelog

## [2.3.0] - TBD

### Added
- Added GOES-East satellite imagery products to the EWMRS service, including `GOES_ABI_C01` through `GOES_ABI_C16` and the RGB composites `GOES_RGB_TrueColor`, `GOES_RGB_Airmass`, `GOES_RGB_NighttimeMicrophysics`, `GOES_RGB_DayCloudPhase`, `GOES_RGB_SimpleWaterVapor`, and `GOES_RGB_Sandwich`, available through the EWMRS API endpoints `/renders/get-items`, `/renders/fetch`, `/renders/download`, `/renders/tile`, and `/renders/tile-info`

### Changed
- Reconcile NWS alerts with alerts from the NWS API
- Modified EWMRS tiles to be 350 x 350 pixels

### Fixed
- Added NWS fire zones that were missing from the zone catalog
- Tightened CTAM mesocyclone vertical association with footprint-overlap checks and filtered elongated azshear artifacts from detections and output records

### Testing

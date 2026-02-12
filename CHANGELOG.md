# Changelog for Version ``1.5.3``

## Additions

## Changes

- **Maximize Detection Sensitivity** - Removed PrecipType-based stratiform discrimination logic to prevent false negatives for misclassified mature storms.
- **Noise Reduction** - Implemented 5-gate minimum size filter in `GateMapper` to reject small radar noise artifacts.
- **Repository Cleanup** - Moved development utility scripts (`check_overlap.py`, `check_radar.py`, `profile_ingest.py`) to `scripts/` directory.

## Fixes

- **Fix Developing Cell Detection** - Resolved issue where developing storm cells (e.g., ID 6605) were missed by lowering reflectivity threshold to 37.5 dBZ and relaxing seed trigger logic to "any pixel".

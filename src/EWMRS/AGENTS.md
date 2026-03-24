# AGENTS Guide for `src/EWMRS/`

## Scope
EWMRS rendering service package.

## Major areas
- `api/`: Express routes for renders, tiles, WPC, and colormaps.
- `render/`: raster rendering, reprojection, tiling, and tools.
- `pipeline.py` / `scheduler.py`: orchestration helpers.

## Agent guidance
- Preserve GUI output structure and tile/index compatibility.
- Be careful with filesystem cleanup so it stays constrained to the configured runtime base directory.

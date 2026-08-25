# AGENTS Guide for `src/`

## Scope
This file applies to everything under `src/`.

## Purpose
`src/` contains the runtime application code for EdgeWARN and EWMRS:
- `run_edgewarn.py` is the supported primary service command (`run.py` is a
  deprecated thin alias forwarding to it).
- `run_ewmrs.py` runs EWMRS rendering, GOES ABI ingest/render, and
  METAR/NWS/WPC accessories as a standalone service.
- `run_nexrad.py` runs NEXRAD ingest and rendering as a standalone service.
- `process_historical.py` runs historical reprocessing.
- `common/` holds shared ingest and coordination code.
- `EdgeWARN/` contains the storm analysis, integration, CTAM, alerts, and API code.
- `EWMRS/` contains the rendering service and API.
- `NEXRAD/` contains NEXRAD GUI serialization, retention, and render loop.
- `util/` contains shared helpers used across Python modules.

## Agent guidance
- Prefer small, localized changes and keep imports compatible with `pythonpath = src`.
- Use the `EdgeWARN-dev` conda environment for Python tests.
- Treat the runtime base directory as the source of truth for generated outputs.
- Avoid repository-local output paths unless a test explicitly requires them.
- When changing pipeline behavior, consider impacts on ingest readiness, detection, integration, CTAM, alerts, and API indexes.

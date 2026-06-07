# CTAM Framework

CTAM (Convective Threat Analysis Module) is the modular analytics framework used during EdgeWARN integration.

It supports both cell-based and grid-based modules, publishes alert payloads, and writes timestamped alert snapshots.

## Framework Layout

```text
src/EdgeWARN/ctam/
├── __init__.py
├── interface.py
├── engine.py
├── registry.py
├── run.py
├── modules/
│   ├── StormCast/
│   ├── MorphoWind/
│   ├── FLOHAR/
│   └── Mesocyclone/
└── util/
    ├── history_cache.py
    ├── history.py
    └── json.py
```

## Module Types

- `AnalysisModule`: per-cell modules (for example: StormCast, MorphoWind)
- `GridAnalysisModule`: raster/grid modules (for example: FLOHAR, Mesocyclone)

## Registered Modules

`src/EdgeWARN/ctam/modules/__init__.py` currently registers:

- Cell modules:
  - `StormCast`
  - `MorphoWind`
- Grid modules:
  - `FLOHAR`
  - `Mesocyclone`

## Pipeline Entry Point

`src/EdgeWARN/ctam/run.py`:

```python
run_ctam(cells, timestamp=None)
```

Execution flow:

1. Cleanup expired EdgeWARN alerts
2. Run all registered cell modules on each cell
3. Collect and publish cell-module alerts
4. Run all registered grid modules
5. Collect and publish grid-module alerts
6. Attach grid outputs to cells only when modules mark results as attachable
7. Create timestamp snapshot when `timestamp` is provided

Per-module failures are isolated so other modules continue running.

## Alert Management

CTAM uses `EdgeWARN.alerts.AlertManager` to:

- publish ID-based EdgeWARN alerts to `data/Alerts/EdgeWARN/ids`
- clean expired alert files
- create timestamp snapshots in `data/Alerts/EdgeWARN/timestamps`

## Usage

```python
from EdgeWARN.ctam.run import run_ctam

processed_cells = run_ctam(storm_cells, timestamp="20260101-120000")
```

Module outputs are stored under each cell's `modules` key, with optional `_grid_outputs` for attachable grid products.

Grid modules can also write sidecar products outside storm-cell records. Current examples are FLOHAR GeoJSON-style flash-flood region output under the flash-flood runtime directory and Mesocyclone JSON snapshots under `data/Mesocyclones`. Mesocyclone results are intentionally not attached to storm-cell records and are consumed through `GET /api/v2/features/mesocyclones`.

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
│   └── MorphoWind/
└── util/
    ├── history_cache.py
    ├── history.py
    └── json.py
```

## Module Types

- `AnalysisModule`: per-cell modules (for example: StormCast, MorphoWind)

## Registered Modules

`src/EdgeWARN/ctam/modules/__init__.py` currently registers:

- Cell modules:
  - `StormCast`
  - `MorphoWind`

## Pipeline Entry Point

`src/EdgeWARN/ctam/run.py`:

```python
run_ctam(cells, timestamp=None)
```

Execution flow:

1. Cleanup expired EdgeWARN alerts
2. Run all registered cell modules on each cell
3. Collect and publish cell-module alerts
4. Create timestamp snapshot when `timestamp` is provided

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

Module outputs are stored under each cell's `modules` key.

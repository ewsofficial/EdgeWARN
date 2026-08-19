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
│   │   └── core/          # blending, config, core, diagnostics,
│   │                      # forecast, kalman, types, uncertainty
│   └── MorphoWind/
└── util/
    ├── history_cache.py
    ├── history.py
    └── json.py
```

## Module Types

`interface.py` defines two abstract bases:

- `AnalysisModule` — per-cell. `run(storm_entry, environment=None,
  history_cache=None)` returns nothing and mutates `storm_entry` in place,
  writing to `storm_entry['modules'][self.name]`. `alerts(storm_entry)` is
  optional and returns `None` by default.
- `GridAnalysisModule` — operates on raw raster files (GRIB/NetCDF) and needs no
  storm cells. `run()` takes no arguments and *returns* a dict with `features`
  (a GeoJSON `FeatureCollection`), `metadata`, and `timestamp`. Its
  `alerts(features)` receives the feature list, not a cell.

The two are independent classes, not a subclass relationship, and each has its
own registry.

## Registered Modules

`src/EdgeWARN/ctam/modules/__init__.py` currently registers:

- Cell modules (`CellModuleRegistry`):
  - `StormCast`
  - `MorphoWind`
- Grid modules (`GridModuleRegistry`): none. The grid path is fully implemented
  in `run.py` but no `GridAnalysisModule` is registered yet, so `grid_modules`
  is empty at runtime.

`registry.py` exports `CellModuleRegistry` and `GridModuleRegistry`;
`ModuleRegistry` is a backward-compatibility alias for `CellModuleRegistry`, so
the `ModuleRegistry.register(...)` calls in `modules/__init__.py` populate the
cell registry. Both registries hold module *instances* in a class-level dict
keyed by `module.name`, so registration is process-global.

## Pipeline Entry Point

`src/EdgeWARN/ctam/run.py`:

```python
run_ctam(cells, timestamp=None)
```

Execution flow:

1. Clean up expired EdgeWARN alerts
2. Read both registries; return `cells` unchanged if both are empty
3. Preload a `CellHistoryCache` for the IDs of the cells being processed
4. For each cell: initialize the `modules` namespaces, run every cell module,
   collect each module's `alerts(cell)`, then publish that cell's payloads with
   `AlertManager.publish_many`. Publishing happens once per cell inside the
   loop, not once for the whole batch.
5. Log the StormCast diagnostic summary — status, `can_generate_alerts`
   eligibility, `alert_outcome`, and `alert_blockers` tallies across all cells
6. Run every grid module, publishing alerts from any returned `features`
7. Attach grid results to `cells[0]['modules']['_grid_outputs']`, skipping any
   result that sets `attach_to_stormcells: false`
8. Create the timestamp snapshot when `timestamp` is provided

Per-module failures are isolated so other modules continue running. A cell
module that raises has `{"status": "error", "error": ...}` written to its
namespace, and its `alerts()` is still called afterward — a failed `run()` does
not suppress alert collection. A grid module that raises gets the same error
record in `grid_results`.

`run_ctam` normally returns the same list object it was given. The exception is
an empty input: if `cells` is empty and an attachable grid module produced
results, it returns a new single-element list holding only `_grid_outputs`, with
no `properties` key.

## Alert Management

CTAM uses `EdgeWARN.alerts.AlertManager` to:

- publish ID-based EdgeWARN alerts to `data/Alerts/EdgeWARN/ids`
- clean expired alert files, and prune aged timestamp snapshots in the same pass
  — `cleanup_expired` sweeps both directories, defaulting to a `120`-minute
  maximum age
- create timestamp snapshots as `{timestamp}.json` in
  `data/Alerts/EdgeWARN/timestamps`

## Usage

```python
from EdgeWARN.ctam.run import run_ctam

processed_cells = run_ctam(storm_cells, timestamp="20260101-120000")
```

Module outputs are stored under each cell's `modules` key.

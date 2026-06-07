# Detection Pipeline

Storm-cell detection is implemented under `src/EdgeWARN/process/detect`.

## Module Layout

```text
src/EdgeWARN/process/detect/
├── main.py                  # Orchestration entry point
├── detect.py                # Core cell extraction logic
├── track.py                 # Tracking + lineage updates
├── kalman/                  # Kalman tracking components
├── lineage/                 # Merge/split lineage logic
└── tools/                   # Save, vector math, alert matching, morphology helpers
```

## Main Entry Point

`src/EdgeWARN/process/detect/main.py`:

```python
main(
    radar_old,
    radar_new,
    ps_old,
    ps_new,
    pt_old,
    pt_new,
    lat_bounds,
    lon_bounds,
    json_output,
    radar_old_obj=None,       # cached prior-radar dataset, optional
    ps_old_obj=None,          # cached prior-ProbSevere dataset, optional
    pt_old_obj=None,          # cached prior-PrecipType dataset, optional
    disable_tracking=False,
    cleanup_stormcells=True,
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
)
```

`json_output` is accepted by the historical pipeline call path, but the detector currently writes its persisted runtime artifact to `<BASE_DIR>/data/stormcells/stormcells_{timestamp}.json`. It does not use `json_output` to relocate the final saved file.

## Detection Modes

- **Dual-frame mode**: runs detection on new scan, uses prior scan/context for tracking
- **Single-frame fallback**: runs detection without tracking when a full pair is unavailable

## Core Processing Steps

1. Validate input file availability
2. Resolve scan timestamp from radar input
3. Load prior stormcell state if available
4. Detect cells from radar/ProbSevere/PrecipType inputs
5. If tracking enabled:
   - run lineage event detection (merge/split)
   - run cell tracking updates and Kalman continuity
6. Compute vectors via `StormVectorCalculator`
7. Match NWS alerts to cells
8. Save `stormcells_YYYYMMDD-HHMMSS.json`
9. Update stormcell API index

## Tracking and Lineage

Tracking is handled by `StormCellTracker` in `track.py` with:

- merge/split lineage detection
- continuity support for temporary detection drops
- Kalman-assisted state evolution using `src/EdgeWARN/process/detect/kalman`

Tracking can be disabled via pipeline flags (`--disable-tracking`).

## Output Compatibility

Detection outputs feed integration, CTAM, alerting, and API index updates, so the saved stormcell schema and timestamp naming are treated as compatibility-sensitive interfaces.

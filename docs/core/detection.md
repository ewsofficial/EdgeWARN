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
    detection_config=None,    # DetectionConfig; loaded from detection.yaml when omitted
    radar_old_obj=None,       # cached prior-radar dataset, optional
    ps_old_obj=None,          # cached prior-ProbSevere dataset, optional
    pt_old_obj=None,          # cached prior-PrecipType dataset, optional
    disable_tracking=False,
    disable_polygon_expansion=False,
    cleanup_stormcells=True,
)
```

The detector writes its persisted runtime artifact to `<BASE_DIR>/data/stormcells/stormcells_{timestamp}.json`; callers cannot redirect it. It returns a `(path, cached_datasets)` tuple, or `(None, None)` when no radar frame is available.

`refl_threshold`, `min_seed_percentage` and `drop_offset` are carried on `detection_config` rather than passed individually.

`disable_polygon_expansion` skips the ProbSevere polygon-to-radar gate mapping and watershed-style expansion path, using the raw ProbSevere geometry directly instead.

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

# Integration Pipeline

Post-detection enrichment is implemented under `src/EdgeWARN/process/integrate`.

## Module Layout

```text
src/EdgeWARN/process/integrate/
├── main.py
├── pipeline.py
├── config.py
├── integrate.py
├── integrate_glm.py
├── integrate_rap.py
├── integrate_azshear.py
├── azshear/
├── core/
├── geometry/
├── io/
├── history.py
├── grid_index.py
└── utils.py
```

## Entry Point

`src/EdgeWARN/process/integrate/main.py` exports `pipeline.main`.

Primary call pattern:

```python
main(json_path=None, remove_old_cells=True, disable_ctam=False, mrms_core_only=False)
```

`json_path` defaults to `None` in the signature but is required at runtime — `main()` raises `ValueError` if it is not supplied. When `mrms_core_only=True`, GLM and RAP integration steps are skipped.

## Integration Stages

`pipeline.py` runs enrichment in parallel worker branches and merges property patches back onto the same storm-cell set.

Major stages:

1. Dataset stats integration (configured groups from `config.py`)
2. ProbSevere field integration
3. GLM integration (`GLM_FLASH_COUNT`, `GLM_TOTAL_ENERGY`) — skipped when `mrms_core_only=True`
4. RAP integration (wind/environment fields) — skipped when `mrms_core_only=True`
5. Optional AzShear support integration (currently feature-flagged)
6. CTAM execution unless `disable_ctam=True`
7. Save integrated stormcell JSON
8. Update cell history
9. Update API cell indexes and cleanup inactive cell files

## CTAM Handoff

When enabled, integration calls `EdgeWARN.ctam.run.run_ctam(cells, timestamp=...)` and persists module outputs under each cell's `modules` structure (plus grid outputs when applicable).

## API and History Side Effects

After save, integration updates:

- per-cell history files
- API cell index (`cell_index.json`)
- stale cell cleanup policy (default: remove inactive cells older than 2 hours)

These side effects are required for stable API behavior.

## Integrated Data Sources

The current integration configuration enriches storm cells with MRMS statistic groups such as reflectivity, NLDN density, echo tops, VIL/VIL density, VII, MESH, precipitation rate, RALA, and azimuthal shear summaries. It also copies selected ProbSevere fields, adds GLM flash count/energy when scan-time GLM files are available, and attaches RAP wind/environment fields used by CTAM modules such as StormCast and MorphoWind.

The optional AzShear support-feature integration path exists in `integrate_azshear.py` and `azshear/`, but the pipeline-level feature flag is currently disabled.

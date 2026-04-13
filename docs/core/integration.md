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
main(json_path, remove_old_cells=True, disable_ctam=False)
```

## Integration Stages

`pipeline.py` runs enrichment in parallel worker branches and merges property patches back onto the same storm-cell set.

Major stages:

1. Dataset stats integration (configured groups from `config.py`)
2. ProbSevere field integration
3. GLM integration (`GLM_FLASH_COUNT`, `GLM_TOTAL_ENERGY`)
4. RAP integration (wind/environment fields)
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

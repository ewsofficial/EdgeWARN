# AGENTS Guide for `src/EdgeWARN/`

## Scope
Primary EdgeWARN runtime package.

## Major areas
- `alerts/`: alert schema and persistence.
- `api/`: Express API service.
- `api_integration/`: index management for generated products.
- `ctam/`: analytics modules.
- `ingest/`: compatibility re-exports for shared ingest code.
- `process/`: detection and integration pipelines.
- `schedule/`: update-checking and scheduling helpers.
- `pipeline.py`: top-level orchestration.

## Agent guidance
- Preserve filesystem-first behavior and downstream JSON/API compatibility.
- Changes here often impact alert generation, CTAM, API indexes, and historical processing.

# EdgeWARN Core Runtime Overview

This document summarizes the current runtime architecture implemented under `src/`.

## Runtime Layout

```text
src/
├── run.py                           # Real-time tandem scheduler entry point
├── process_historical.py            # Historical reprocessing entry point
├── common/
│   ├── ingest/                      # Shared ingest implementations (MRMS/NWS/Synoptic/METAR/WPC)
│   └── pipeline/coordinator.py      # Shared staged-ingest coordination
├── EdgeWARN/
│   ├── process/detect/              # Storm-cell detection and tracking
│   ├── process/integrate/           # Per-cell data integration pipeline
│   ├── ctam/                        # CTAM framework + modules
│   ├── alerts/                      # EdgeWARN alert schema + manager
│   ├── api_integration/             # API index management for generated files
│   └── api/                         # EdgeWARN HTTP API service
└── EWMRS/
    ├── pipeline.py                  # Render pipeline orchestration
    ├── render/                      # Layer rendering and tile generation
    └── api/                         # EWMRS HTTP API service
```

## High-Level Flow

```mermaid
graph TD
    A[Shared Ingest] --> B[EdgeWARN Detection]
    A --> C[EWMRS MRMS Rendering]
    A --> D[EWMRS GOES Hook (No-op)]
    B --> E[EdgeWARN Integration]
    E --> F[CTAM Modules]
    F --> G[Alert Manager]
    E --> H[API Index Updates]
```

## Tandem Readiness Stages

- Detection inputs ready
- EWMRS MRMS inputs ready
- EWMRS GOES inputs ready
- EdgeWARN integration inputs ready

The GOES EWMRS stage is currently an explicit no-op hook. Current GUI rendering remains MRMS-only.

## Scheduling Modes

- `run.py` performs a staged shared ingest cycle, then runs EdgeWARN and EWMRS workers in tandem
- `process_historical.py` iterates through a requested UTC time range and runs the historical EdgeWARN flow

## Additional References

- `docs/core/ingestion.md`
- `docs/core/detection.md`
- `docs/core/integration.md`
- `docs/ctam/README.md`
- `docs/api/api_endpoints.md`

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
    A --> D[EWMRS GOES ABI Rendering]
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

The GOES EWMRS stage renders configured GOES ABI layers after local ABI readiness is met.
This now includes six derived RGB composites built from staged `ABI-L1b-RadC` channels:

- `GOES_RGB_TrueColor`
- `GOES_RGB_Airmass`
- `GOES_RGB_NighttimeMicrophysics`
- `GOES_RGB_DayCloudPhase`
- `GOES_RGB_SimpleWaterVapor`
- `GOES_RGB_Sandwich`

The RGB path reuses the existing GOES channel normalization and `EPSG:3857` reprojection flow, then writes the same tiled GUI layout and `index.json` contract used by the scalar products. If a required channel is missing or exceeds the allowed timestamp offset, only that RGB recipe is skipped.

## Scheduling Modes

- `run.py` performs a staged shared ingest cycle, then runs EdgeWARN and EWMRS workers in tandem
- `process_historical.py` iterates through a requested UTC time range and runs the historical EdgeWARN flow

## Additional References

- `docs/core/ingestion.md`
- `docs/core/detection.md`
- `docs/core/integration.md`
- `docs/ctam/README.md`
- `docs/api/api_endpoints.md`

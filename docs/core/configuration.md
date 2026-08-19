# Configuration reference

EdgeWARN loads a complete, schema-validated `config/` tree before either root
process starts work. Copy the entire tree when deploying; individual YAML files
are not standalone configuration units.

Discovery is `--config-dir`, then `EDGEWARN_CONFIG_DIR`, then the repository's
`config/` directory. Runtime base directories are independently resolved as
`--base-dir` / `--base_dir`, then `EDGEWARN_BASE_DIR`, then legacy `BASE_DIR`,
then `filesystem.yaml`. All catalog edits require a process restart.

| File | Owner and operator-facing scope |
| --- | --- |
| `runtime.yaml` | Realtime run bounds, feature switches, retry and supervisor timing. |
| `historical.yaml` | Historical scan bounds, cadence, and throttling. |
| `filesystem.yaml` | Platform base-directory defaults, cleanup retention, colormap lookup. |
| `detection.yaml` | Cell-detection thresholds, masks, expansion, and retention. |
| `lineage.yaml` | Tracking and lineage matching controls. |
| `integration.yaml` | Dataset sources, statistics, rounding, and RAP products. |
| `scheduler.yaml` | MRMS update-selection and scheduling policy. |
| `api_index.yaml` | Generated EdgeWARN index/snapshot retention. |
| `ingest.yaml` | MRMS/GOES ingest products, source keys, and retention. |
| `nexrad.yaml` | NEXRAD discovery, parsing, grouping, and output selection. |
| `synoptic_rap.yaml` | RAP source discovery, freshness, and request policy. |
| `wpc.yaml` | WPC surface-analysis sources and artifact naming. |
| `metar.yaml` | METAR source, parsing, and retention settings. |
| `nws.yaml` | NWS alert and zone-sync sources, headers, and retry policy. |
| `ewmrs_render.yaml` | MRMS/GOES render-layer inputs and render settings. |
| `ewmrs_pipeline.yaml` | EWMRS processing, cleanup, and render scheduling; the `rap_uint16` section holds RAP Uint16 conversion layers and encoding metadata. |
| `api.yaml` | Unified API network, security, limits, artifact, and query policy. |
| `kalman.yaml` | Kalman filter, assignment, and tracking parameters. |

Each file has a matching `config/schema/*.schema.json`; the schema gives the
accepted types and numeric ranges. Validate an installation before starting it:

```bash
npm run validate-config
PYTHONPATH=src python -m common.config.validate
```

The GUI renderer writes float16 chunk artifacts and JSON indexes under
`<BASE_DIR>/gui`; PNG routes are compatibility endpoints only where legacy PNG
artifacts exist.

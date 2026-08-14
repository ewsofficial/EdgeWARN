# YAML Configuration Grouping Plan

**Source:** `CONFIGURATION_AUDIT.md` (checked-out working tree audit)
**Package version:** `2.7.0`
**Status:** planning only; no YAML files created and no runtime behavior changed

## Objective

Group every configuration variable, constant, and tunable parameter catalogued
in `CONFIGURATION_AUDIT.md` into separate YAML files under `config/`, one file
per audit section so each subsystem is self-contained. The existing
`config/kalman.yaml` is already YAML-backed and is preserved as-is.

This plan only defines the file layout and content mapping. Wiring the files
into the Python and Node code (loader module, precedence with env/CLI,
validation) is the execution phase and is out of scope here.

## Scope boundaries

- **Included:** all rows from audit sections 1-9 that represent operator-facing
  or scientific tunables (timers, thresholds, buckets, URLs, retention,
  concurrency, catalogs, API policy).
- **Excluded by design** (already omitted from the audit or flagged
  non-configurable): scientific/geodetic constants, unit conversions,
  wire-format/parsing constants (NEXRAD record size, magics, msg types,
  `UINT16_NODATA`, etc.), colormap data, `WEB_MERCATOR_*` bounds/shape, chi²
  quantile maps.
- Env vars remain live overrides; each YAML file documents its applicable
  env overrides so existing deployments keep working. YAML becomes the
  replacement for the hardcoded defaults, not for env/CLI precedence.

## Proposed layout

| YAML file | Source (audit section) | Contents |
|---|---|---|
| `config/runtime.yaml` | §1 (run.py args) + §7 scheduler | `base_dir`, `profile`, `lat_limits`, `lon_limits`, `disable_{ctam,tracking,polygon_expansion,ewmrs,nws,metar,goes,nexrad}`, `mrms_core_only`, `refl_threshold`, `min_seed_percentage`, `drop_offset`; coordinator: `GOES_POLL_SECONDS`, `GOES_RENDER_WAIT_SECONDS`, `GOES_RENDER_WAIT_INTERVAL_SECONDS`, supervisor check loop (30 ticks × 0.5 s), `CycleRetryPolicy` (max_attempts 3, backoff 5.0, cap 30.0), `EDGEWARN_CYCLE_*` fallbacks, `GOES_RENDER_MAX_OFFSET_MINUTES`, `EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER` |
| `config/historical.yaml` | §1 (process_historical.py) | `start`, `end`, `lat` (`[20,55]`), `lon` (`[-130,-60]`), `output` (`stormcell_test.json`) |
| `config/detection.yaml` | §4 (`detect/main.py`, `gatemapper.py`, `morphology.py`, `save.py`, `track.py`) | `refl_threshold`, `min_seed_percentage`, `drop_offset`, old-stormcell cleanup age (120 min), fallback dt (120.0 s), single-frame tracking mode defaults; gatemapper: baseline reflectivity floor, crop pad, expansion seed rule, dynamic min threshold (40.0/37.5), max reflectivity clamp (52.0), per-cell dyn threshold, size rejection (5 gates), contour downsample (8, adaptive 1/4/8), coordinate rounding (3); morphology/save: `_MIN_PIXELS_FULL_ANALYSIS` (25), hail contour sampling step (5), hail class `preciptype` (6), contour level (0.5) |
| `config/kalman.yaml` | §3 (existing) | Unchanged. Kalman filter, tracking, assignment sections already consumed via `kalman/config.py from_yaml` |
| `config/lineage.yaml` | §4 (`detect/lineage/`) | `DEFAULT_OVERLAP_THRESHOLD` (0.15), `MIN_AREA` (1e-10 deg²), antimeridian normalization trigger, buffer `min_confirmations` (2), `max_pending` (100), `prune_after_scans` (5), `scan_interval_seconds` (120.0), buffer file name (`lineage_buffer.json`) |
| `config/integration.yaml` | §5 | `OUTPUT_DECIMALS` (2), `AZSHEAR_BUFFER_KM` (1.5), azshear thresholds (`8.0`/`6.0`), `AZSHEAR_MIN_GATE_COUNT` (5), `AZSHEAR_MAX_PAIR_SEPARATION_KM` (12.0), azshear history window (5), `GLM_BIN_SIZE_DEGREES` (1.0), cell history window (10 payloads), integration stats dataset catalog (VIL 95th, density 90th, AzShear 50th, ET30 90th, …) |
| `config/alerts.yaml` | §6 | alert `severity` default (`"warning"`), `MRMSUpdateChecker` max_entries (10), `APIIndexManager` remove_old_cells (true), index resync frequency (500 updates), `initialize_runtime(initialize_indexes)` realtime/historical |
| `config/mrms_goes.yaml` | §7 MRMS/GOES | MRMS bucket (`noaa-mrms-pds`), GOES bucket (`noaa-goes19`), ABI product (`ABI-L1b-RadC`), `_ABI_CHANNEL_DEFINITIONS` (16 channels, `max_files=2`), raw/decoded MRMS path patterns, `DECOMPRESS_CHUNK_SIZE` (1 MiB), NCEP HTTPS fallback URL, `GOES_MAX_ENTRIES` (96), detection modifiers, `download_all_files_async` max_entries (10, `remove_old_files=True`) |
| `config/nexrad.yaml` | §1 NEXRAD CLIs + §7 NEXRAD | Ingest CLI defaults (`--scan-interval-seconds` 20, `--completion-interval-seconds` 10, `--max-candidate-volumes-per-site` 3, `--max-volumes-per-site` 1); buckets (chunks + archive), station catalog URL, `ALLOWED_VCPS` {12,212,215}, `ANGLE_DEDUP_TOLERANCE_DEG` (0.1), `HIGH_MAX_ANGLE_DEG` (4.0), user agent, weather-api timeout/cache TTL, volume/chunk/ingest/scan/cancellation/heartbeat timeouts, `MIN_REQUIRED_VOLUME_CHUNKS` (25), min sweep angle (0.4°), canonical elevation bins, chunk download semaphore (8), `max_site_tasks` (24), scan/elevation dirs to keep (3/2), stale manifest max age (12 h), `NEXRAD_WORKER_POOL_SIZE` (4), `NEXRAD_WORKER_RECYCLE_INTERVAL` (24), `NEXRAD_WORKER_TIMEOUT_SECONDS` (40) |
| `config/synoptic_rap.yaml` | §7 RAP/Synoptic | `RAP_BUCKET` (`noaa-rap-pds`), `RAP_FILE_PATTERN`, `RAP_DIR_PATTERN`, `RAP_MAX_AGE_MINUTES` (180, env `EDGEWARN_RAP_MAX_AGE_MINUTES`), `RAP_MAX_FILES` (3) |
| `config/wpc.yaml` | §7 WPC | `WPC_CODED_SFC_BASE_URL`, `UPDATE_INTERVAL_HOURS` (3), `VALID_HOURS` [0,3,6,9,12,15,18,21], HTTP timeout (30 s), fallback GeoJSON color (`#000000`) |
| `config/metar.yaml` | §7 METAR | `STATION_DB_URL`, `CONUS_BOUNDS` (lat 24..50, lon -125..-67), per-request timeout (60 s), cache file (`stations_cache.json`) |
| `config/nws.yaml` | §1 zone_sync CLI + §7 NWS | `ZONE_TYPES`, zone sync defaults (timeout 30 s, max_retries 3, max_workers 16, pause 0.0), `DROPPED_EVENTS` blocklist, registry TTL (2 h) |
| `config/ewmrs_render.yaml` | §8 render/config.py, render/tiler/tools, nexrad.py GUI | `TILE_SIZE` (350), `TILE_GRID_ROWS/COLS` (10/20), MRMS layer catalog (15 entries), GOES single-channel layers (16) + per-channel mask windows, GOES RGB recipes (6), colormap cache `lru_cache(maxsize=128)`, tile worker upper bound (`min(tile_count, 8, cpu_count)`), timestamp format, PNG compress level (1), variable→colormap keys, manifest name pattern |
| `config/ewmrs_rgb.yaml` | §8 goes_rgb.py/goes_transform.py | `REFLECTANCE_CHANNELS` {C01..C06}, `TRUE_COLOR_TERMINATOR_START/END_DEGREES` (80.0/96.0), `_MAX_SOLAR_CACHE_ENTRIES` (32), RGB recipes + normalization windows, max layer-selection offset (20.0 min), `true_color_gamma` (2.2), green band blend weights, resampling (`bilinear`), CRS handling, radian detection rule |
| `config/ewmrs_rap_uint16.yaml` | §8 RAP uint16 | pressure levels (925,850,700,500,250 mb), 29-layer catalog (temp/RH/ThetaE/MSLP/CAPE/SRH/vorticity/wind ranges), `max_timestamps` (3), scale-to-uint16 rule, timestamp format |
| `config/ewmrs_pipeline.yaml` | §8 EWMRS pipeline.py/scheduler.py | GOES sub-extent, `run_render_pipeline` max_entries (10), GOES render cleanup age (120 min), NEXRAD GUI retention (120 min, poll 30 s, workers 8), `EWMRS_GOES_CLEANUP_MIN_INTERVAL_SECONDS` (300), worker budget (1200.0 GOES / 768.0 MRMS), `EWMRS_WORKER_RESERVE_MB` (1024.0), OMP/MKL/OPENBLAS/NUMEXPR thread caps |
| `config/api.yaml` | §9 Node APIs | ports (unified 5000, debug 3001, EWMRS 3003/3004), `requestTimeoutMs` (30000), rate limits (40/s, 2000/min, EWMRS 30/s, 1800/min, windows 1000/60000), CORS/trust-proxy defaults, `ALLOWED_ORIGINS`, artifact limits (json 8 MiB, binary 128 MiB, image 32 MiB, LRU max 256/maxSize 32 MiB), pagination (limit 100/cap 1000), `DEFAULT_GRID`, cache headers (5/60/31536000), route query constraints, `SLUG` regex, cluster `numCPUs = min(cpus,4)`, json body 16 kb |

## Key decisions

1. **One file per subsystem.** Files map 1:1 to audit sections so the audit
   table and the config tree stay cross-referencable, and each Python/Node
   component reads a single adjacent file.
2. **Ingestion CLIs bundled with their constants.** NEXRAD and NWS argparse
   defaults live in the same file as their module constants so each subsystem
   is self-contained.
3. **Env vars remain overrides.** Each file carries an `env_overrides`
   annotation listing the environment variables that may shade its keys
   (e.g. `EDGEWARN_RAP_MAX_AGE_MINUTES`, `NEXRAD_WORKER_POOL_SIZE`,
   `EWMRS_TILE_THREADS`). Precedence (execution phase): env > CLI > YAML >
   code fallback, matching the existing kalman pattern and documented overrides.
4. **`config/kalman.yaml` untouched** to preserve `kalman/config.py` loading
   compatibility; a future shared loader may absorb it.
5. **Cross-file derivation preserved.** Layer catalogs and product definitions
   (MRMS/GOES/RAP) remain authoritative in one place each; render, integration,
   and API views derive from them rather than duplicating lists.
6. **Code-constant allowlist.** Wire-format/parsing constants and geodetic/
   projection invariants stay in code and are recorded as intentional
   exclusions in each file's header comments.

## Execution phase (future, not done here)

1. Create the YAML files above with the audit baseline values and header
   comments citing audit section + source file/line.
2. Add `src/common/config/loader.py` (dataclass-backed, `pythonpath = src`
   compatible) with a `from_yaml` pattern like `kalman/config.py`, key-level
   defaults, and env/CLI precedence.
3. Wire the highest-impact consumers first: `run.py`/`process_historical.py`,
   detection, ingest coordinators, EWMRS pipeline.
4. Add pytest coverage asserting YAML values match the audited baseline and
   that files parse/validate.
5. Document authoritative file per setting in `docs/`.

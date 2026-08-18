# EdgeWARN-Core Configuration, Constants & Tunable Parameters Audit

> Current operator reference: [`docs/core/configuration.md`](docs/core/configuration.md).
> The complete 19-file `config/` tree is schema-validated before startup.
> `filesystem.yaml` owns runtime base defaults; precedence is CLI,
> `EDGEWARN_BASE_DIR`, legacy `BASE_DIR`, then YAML. EWMRS render artifacts are
> float16 chunks plus JSON metadata; PNG routes are legacy compatibility only.

Deep audit of every configuration variable, constant, and tunable parameter used in the EdgeWARN-Core codebase. Scientific/mathematical constants (physical constants, unit conversions, geodetic radii) are excluded by design.

Legend for "Override" column:
- `YAML` — set via `config/kalman.yaml`
- `CLI` — argparse flag (flag name given)
- `env` — environment variable (name given)
- `hardcoded` — only changeable by editing source
- `JSON` — editable data file

---

## 1. CLI Arguments (argparse)

### `src/util/io.py` — shared run-time & historical parsers
| Flag | Default | Used by |
|---|---|---|
| `--base_dir` / `--base-dir` | `None` (→ `~/EdgeWARN_input`) | run.py, process_historical.py (lines 71, 83) |
| `--profile` | off | run.py (line 84) |
| `--disable-ctam` | off | run.py (line 85) |
| `--disable-tracking` | off | run.py (line 86) |
| `--disable-polygon-expansion` | off | run.py (line 87) |
| `--refl-threshold` | `37.5` | run.py detection (line 88) |
| `--min-seed-percentage` | `0.001` | run.py gate expansion (line 89) |
| `--drop-offset` | `10.0` | run.py dynamic reflectivity drop (line 90) |
| `--lat_limits` | `[20, 55]` | run.py (line 94) |
| `--lon_limits` | `[230, 300]`, normalized `% 360` | run.py (lines 95, 120) |
| `--disable-ewmrs` | off | run.py (line 97) |
| `--disable-nws` | off | run.py (line 98) |
| `--disable-metar` | off | run.py (line 99) |
| `--disable-goes` | off | run.py (line 100) |
| `--disable-nexrad` | off | run.py (line 101) |
| `--mrms-core-only` | off | run.py (lines 102-109) |
| `--start` | *required* | process_historical.py (line 125) |
| `--end` | *required* | process_historical.py (line 126) |
| `--lat` | `[20, 55]` | process_historical.py (line 127) |
| `--lon` | `[-130, -60]` | process_historical.py (line 128) |

### `src/common/ingest/nexrad/pipeline/__init__.py` — NEXRAD pipeline CLI
| Flag | Default | Line |
|---|---|---|
| `--site` (append) | — | 387 |
| `--base-dir` | — | 388 |
| `--scan-interval-seconds` | `20` | 389 |
| `--completion-interval-seconds` | `10` | 390 |
| `--max-candidate-volumes-per-site` | `3` | 391 |

### `src/common/ingest/nexrad/main.py` — one-shot NEXRAD ingest CLI
| Flag | Default | Line |
|---|---|---|
| `--site` | — | 174 |
| `--volume-id` | — | 175 |
| `--base-dir` | — | 176 |
| `--max-volumes-per-site` | `1` | 177 |
| `--max-candidate-volumes-per-site` | `3` | 178 |

### `src/common/ingest/nws/zone_sync.py` — zone asset sync CLI
| Flag | Default | Line |
|---|---|---|
| `--assets-dir` | repo `assets/nws_zones` | 369 |
| `--zone-types` | `["forecast","fire","public","county","marine"]` | 376-378 |
| `--timeout-seconds` | `30` | 381-384 |
| `--max-retries` | `3` | 388-391 |
| `--max-workers` | `16` | 394-397 |
| `--pause-seconds` | `0.0` | 400-403 |
| `--no-progress` | off | 406-408 |
| `--apply` | off (dry-run) | 411-413 |
| `--report-path` | — | 416-418 |

---

## 2. Environment Variables (complete list)

| Variable | Default | File:Line | Purpose |
|---|---|---|---|
| `EDGEWARN_BASE_DIR` | — | EdgeWARN API + unified API | Base dir override |
| `BASE_DIR` | `~/EdgeWARN_input` / `C:\EdgeWARN_input` | EWMRS API, unified API config | Base dir override |
| `EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER` | `"0"` | `run.py:51-56` | Truthy set `{"1","true","yes","on"}` pauses GOES ingest while rendering |
| `EDGEWARN_CYCLE_MAX_ATTEMPTS` | `"3"` | `run.py:131` | Tandem cycle retry ceiling |
| `EDGEWARN_CYCLE_RETRY_BACKOFF_SECONDS` | `"5"` | `run.py:134` | Initial retry backoff |
| `EDGEWARN_CYCLE_MAX_BACKOFF_SECONDS` | `"30"` | `run.py:138` | Backoff cap |
| `EDGEWARN_RAP_MAX_AGE_MINUTES` | `180` (code default `RAP_MAX_AGE_MINUTES`) | `synoptic/config.py:7,14` | Max RAP file age (validated int ≥ 0) |
| `EDGEWARN_PERF_TRACKER` | off | `util/performance.py:15` | Enables TimingTracker profiler |
| `EWMRS_GOES_CLEANUP_MIN_INTERVAL_SECONDS` | `300` | `EWMRS/pipeline.py:55` | Min interval between GOES GUI cleanups |
| `EWMRS_TILE_THREADS` | unset (auto) | `EWMRS/render/render.py:100` | Caps tile writer thread count |
| `EWMRS_WORKER_BUDGET_MB` | `1200.0` (GOES) / `768.0` (MRMS) | `EWMRS/pipeline.py:137-138` | Per-render-worker memory budget |
| `EWMRS_WORKER_RESERVE_MB` | `1024.0` | `EWMRS/pipeline.py:139` | Reserved memory for OS/other procs |
| `OMP_NUM_THREADS` / `MKL_NUM_THREADS` / `OPENBLAS_NUM_THREADS` / `NUMEXPR_NUM_THREADS` | `"1"` (setdefault) | `EWMRS/pipeline.py:120-126` | Numerical thread caps for workers |
| `NEXRAD_WORKER_POOL_SIZE` | `"4"` | `nexrad/pipeline/worker_pool.py:194` | NEXRAD worker process pool size |
| `NEXRAD_WORKER_RECYCLE_INTERVAL` | `"24"` | `worker_pool.py:210` | Worker recycle interval (≤0 disables) |
| `NEXRAD_WORKER_TIMEOUT_SECONDS` | `"40"` | `nexrad/service.py:164` | Ingest worker timeout |
| `PORT` | `5000` (unified API) | `src/api/config/index.js` | Listen port |
| `NODE_ENV` | — | `src/api/config/index.js` | Controls production version exposure |
| `REQUEST_TIMEOUT_MS` | `30000` | `src/api/config/index.js:63` | Request timeout |
| `RATE_LIMIT_MAX_SEC` | `40` | unified API | Per-second rate limit |
| `RATE_LIMIT_MAX_MIN` | `2000` | unified API | Per-minute rate limit |
| `ALLOWED_ORIGINS` | `[]` (deny all) | unified API | Comma-separated CORS allowlist |
| `TRUST_PROXY` / `TRUST_PROXY_IPS` | `false` | unified API | Trust proxy configuration |
| `PROJ_DATA` / `PROJ_LIB` / `GDAL_DATA` | auto-detected | `EWMRS/render/tools.py:38-41` | Set by code for child processes |

---

## 3. YAML — `config/kalman.yaml` (loaded by `kalman/config.py from_yaml`)

| Key | Value | Purpose |
|---|---|---|
| `kalman_filter.process_noise.position` | `0.1` | Position process noise variance (deg²) |
| `kalman_filter.process_noise.velocity` | `0.5` | Velocity process noise (m/s)² |
| `kalman_filter.process_noise.acceleration` | `0.1` | Acceleration process noise |
| `kalman_filter.measurement_noise.position` | `0.5` | Position observation noise (km, ÷111 → deg²) |
| `tracking.max_prediction_time_minutes` | `6.0` | Max time storm stays in prediction mode (code fallback 10.0, `kalman/config.py:59`) |
| `tracking.reacquisition_radius_km` | `5.0` | Radius to reacquire a predicted storm |
| `tracking.confidence_threshold` | `0.4` | Min confidence before termination |
| `tracking.confidence_decay_factor` | `0.7` | Per-scan confidence decay multiplier |
| `assignment.prefilter_radius_km` | `16.0` | Spatial pre-filter radius before assignment |
| `assignment.gating_threshold` | `6.0` | Mahalanobis gate |
| `assignment.min_gating_radius_km` | `2.0` | Collapsed-covariance fallback gate |
| `assignment.weights.position` | `1.0` | Position cost weight |
| `assignment.weights.velocity_direction` | `2.0` | Velocity-consistency cost weight |
| `assignment.weights.size_similarity` | `0.5` | Shape/size cost weight |
| `assignment.method` | `greedy` | `hybrid` / `hungarian` / `greedy` |
| `assignment.covariance_regularization` | `1e-6` | Diagonal regularization for covariance S |

---

## 4. EdgeWARN — Detection & Tracking (`src/EdgeWARN/process/detect/`)

### `detect/main.py`, `detect.py`
| Name | Value | Line | Override |
|---|---|---|---|
| `refl_threshold` | `37.5` | main.py:100 | CLI `--refl-threshold` |
| `min_seed_percentage` | `0.001` | main.py:101 | CLI `--min-seed-percentage` |
| `drop_offset` | `10.0` | main.py:102 | CLI `--drop-offset` |
| old stormcell cleanup age | `120` min | main.py:105 | hardcoded |
| single-frame default tracking mode | `active`, prediction_count `0`, event_type `active` | main.py:338-339 | hardcoded |
| fallback dt | `120.0` s | main.py:358-368 | hardcoded |

### `detect/kalman/config.py` — dataclass fallbacks
| Field | Default | Notes |
|---|---|---|
| `process_noise_position/velocity/acceleration` | `0.1` / `0.5` / `0.1` | YAML-overridden |
| `measurement_noise_position` | `0.5` | |
| `max_prediction_time_minutes` | `10.0` | YAML → 6.0 |
| `reacquisition_radius_km` | `5.0` | |
| `confidence_threshold` / `confidence_decay_factor` | `0.4` / `0.7` | |
| `AssignmentConfig` prefilter/gate/min-gate/weights/method/reg | `16.0`/`6.0`/`2.0`/`1.0,2.0,0.5`/`greedy`/`1e-6` | lines 113-129 |

### `detect/kalman/filter.py`, `state.py`, `confidence.py`, `assignment.py`
| Param | Value | Line |
|---|---|---|
| Initial position uncertainty | `1.0` km std | filter.py:101 |
| Innovation-covariance regularization | `1e-6` | filter.py:409 |
| Mahalanobis singular-S retry regularization | `1e-4` | filter.py:479 |
| Gate threshold / min radius | `6.0` / `2.0` km | filter.py:490-492 |
| Init velocity/accel variance | `100.0` (m/s)² / `1.0` | state.py:134-144 |
| confidence decay | `base * conf_decay**scans` | confidence.py:46 |
| time penalty weight | `1 - (t/max_t) * 0.3` | confidence.py:50 |
| motion factor denominator | `2500.0` var_sum | confidence.py:61 |
| motion/position factor floor | `0.8` | confidence.py:62,72 |
| position soft-decay onset | avg_std `> 5.0` → `1/(1+(avg-5)/30)` | confidence.py:70-73 |
| confidence `high`/`medium` boundaries | `0.7` / `0.4` | confidence.py:116-123 |
| velocity cost dead-bands | pred_speed `< 1.0`; implied_speed `< 0.5` → 0 | assignment.py:227,250 |
| shape cost reflectivity cap | min(diff, 1.0) | assignment.py:293 |
| shape cost size ratio | `log2(ratio)/2` cap 1.0 | assignment.py:303-306 |
| single-candidate max cost | `w1·gating + w2·2 + w3·2` | assignment.py:414-416 |
| default `dt_seconds` | `120.0` | assignment.py:64,520 |

### `detect/tools/gatemapper.py` — watershed expansion
| Param | Value | Line |
|---|---|---|
| baseline reflectivity floor | `min(37.5, refl_threshold)` | 101 |
| crop pad | 2 px bottom / 3 px right | 117-122 |
| expansion seed | refl ≥ pixels·min_seed_percentage | 147 |
| dynamic min threshold | `40.0` if max_refl ≥ 45 else `37.5` | 174 |
| max reflectivity clamp | `52.0` | 175 |
| per-cell dyn threshold | `max(min_thresh, max_refl − drop_offset)` | 176 |
| final size rejection | clusters ≤ `5` gates dropped | 238-250 |
| contour downsample | `8`; adaptive `1/4/8` for n_pts <8/24/> | 287, 341-346 |
| coordinate rounding | 3 decimals | 368-369 |

### `detect/tools/morphology.py`, `save.py`
| Param | Value | Line |
|---|---|---|
| `_MIN_PIXELS_FULL_ANALYSIS` | `25` | morphology.py:14 |
| hail-core contour sampling step | `5` | save.py:148,182 |
| hail class `preciptype` | `6` | save.py:169 |
| contour level | `0.5` | |

### `detect/lineage/`
| Param | Default | Line |
|---|---|---|
| `DEFAULT_OVERLAP_THRESHOLD` | `0.15` | detector.py:15 |
| `MIN_AREA` | `1e-10` deg² | spatial.py:76 |
| antimeridian normalization trigger | max_lon>350, min_lon<10, span>180 | spatial.py:118 |
| buffer `min_confirmations` | `2` | buffer.py:172 |
| buffer `max_pending` | `100` | buffer.py |
| buffer `prune_after_scans` | `5` | buffer.py |
| buffer `scan_interval_seconds` | `120.0` | buffer.py |
| buffer file | `lineage_buffer.json` | buffer.py:138 |

### `detect/track.py`
- `LineageBuffer()` constructed with default kwargs (section above).

---

## 5. Edge Integration (`src/EdgeWARN/process/integrate/`)

| Param | Default | File:Line |
|---|---|---|
| `OUTPUT_DECIMALS` | `2` | core/stats.py:4 |
| `AZSHEAR_BUFFER_KM` | `1.5` | azshear/constants.py:1 |
| `AZSHEAR_LOW_THRESHOLD` | `8.0` | constants.py:2 |
| `AZSHEAR_MID_THRESHOLD` | `6.0` | constants.py:3 |
| `AZSHEAR_MIN_GATE_COUNT` | `5` | constants.py:4 |
| `AZSHEAR_MAX_PAIR_SEPARATION_KM` | `12.0` | constants.py:5 |
| azshear history window | `5` features | azshear/integration.py:73 |
| `GLM_BIN_SIZE_DEGREES` (flash gridding) | `1.0` | integrate_glm.py:7 |
| `cell_{id}.json` history window | last `10` payloads | integration.py:68 |
| integration stats | 7 datasets: VIL 95th, density 90th, AzShear 50th, ET30 90th… | config.py:20-133 |

---

## 6. Alerts, Scheduling, API Index (`src/EdgeWARN/`)

| Param | Value | File:Line |
|---|---|---|
| alert `severity` default | `"warning"` | alerts/schema.py |
| `APIIndexManager(remove_old_cells)` | `True` | api_integration/index_manager.py |
| index resync frequency | every `500` updates | index_manager.py |
| `initialize_runtime(initialize_indexes)` | `True` realtime / `False` historical | EdgeWARN/pipeline.py |

---

## 7. Common Ingestion (`src/common/ingest/`)

### Scheduler / coordinator (`run.py`, `cycle.py`, `goes_readiness.py`)
| Constant | Value | File |
|---|---|---|
| `GOES_POLL_SECONDS` | `60` | run.py:46 |
| `GOES_RENDER_WAIT_SECONDS` | `30` | run.py:47 |
| `GOES_RENDER_WAIT_INTERVAL_SECONDS` | `1.0` | run.py:48 |
| supervisor check loop | 30 ticks × `0.5`s (`15` s) | run.py:333-335 |
| cycle state | `data/runtime/cycle_state.json` | cycle.py:180 |
| `CycleStatus.COMPLETED/DISABLED/UNAVAILABLE/FAILED` | enum | cycle.py:27-33 |
| `CycleRetryPolicy` max_attempts/backoff/cap | `3` / `5.0` / `30.0` | cycle.py:127-129 |
| staged MRMS regex | `MRMS_MergedReflectivityQC_(\d{8})-(\d{6})` | manifest |
| `_GOES_RENDER_MAX_OFFSET_MINUTES` | `20.0` | goes_readiness.py:14 |

### Filesystem (`util/file.py`)
| Constant | Value | Line |
|---|---|---|
| default base dir | `~/EdgeWARN_input` (Linux/macOS), `C:\EdgeWARN_input` (Windows), fallback `/workspaces/EdgeWARN_input` | 203-208 |
| `clean_old_files` age | `max_age_minutes=60` | 280-303 |
| `clean_old_files` count | `max_files=10` | |
| scan/ignore | `.idx`, `.gz` skipped when base present; heapq by mtime | 211-256 |
| colormap search path | cwd → `src/EWMRS/colormaps.json` → `<gui>/colormaps.json` | 190-194 |

### MRMS & GOES
| Constant | Value | File |
|---|---|---|
| bucket | `noaa-mrms-pds` | mrms/config.py |
| GOES bucket | `noaa-goes19` | mrms/config.py |
| ABI product | `ABI-L1b-RadC` | config.py:28 |
| `_ABI_CHANNEL_DEFINITIONS` | 16 channels, `max_files=2` | config.py |
| raw/decoded MRMS patterns | `regional/`…`/{YYYYMMDD}` | parse.py |
| `DECOMPRESS_CHUNK_SIZE` | `1024 * 1024` bytes | s3_common.py:6 |
| NCEP HTTPS fallback | `https://mrms.ncep.noaa.gov/data/2D` | https_client.py:24 |
| `GOES_MAX_ENTRIES` | `96` | downloader.py:33 |
| detection modifiers | `MergedReflectivityQCComposite_00.50`, `PrecipFlag_00.00`, None | main.py |
| `download_all_files_async` max_entries | `10`, `remove_old_files=True` | MRMS main |

### NEXRAD (`common/ingest/nexrad/`)
| Constant | Value | File:Line |
|---|---|---|
| chunks bucket | `unidata-nexrad-level2-chunks` | config.py:1 |
| archive bucket | `unidata-nexrad-level2` | config.py:2 |
| station catalog | `https://api.weather.gov/api/stations` | config.py:3 |
| `ALLOWED_VCPS` | `{12, 212, 215}` | config.py:5 |
| `ANGLE_DEDUP_TOLERANCE_DEG` | `0.1` | config.py:6 |
| `HIGH_MAX_ANGLE_DEG` | `4.0` | config.py:7 |
| user agent | `(EdgeWARN/2.7.0, ewsbackend@gmail.com)` | config.py:9 |
| `WEATHER_API_TIMEOUT_SECONDS` | `15` | config.py:10 |
| `WEATHER_API_CACHE_TTL_SECONDS` | `0` | config.py:11 |
| `NEXRAD_VOLUME_DISCOVERY_TIMEOUT_SECONDS` | `20.0` | config.py:16 |
| `NEXRAD_CHUNK_LIST_TIMEOUT_SECONDS` | `20.0` | config.py:17 |
| `NEXRAD_INGEST_TIMEOUT_SECONDS` | `120.0` | config.py:18 |
| `NEXRAD_SCAN_TIMEOUT_SECONDS` | `180.0` | config.py:19 |
| `NEXRAD_CANCELLATION_GRACE_SECONDS` | `2.0` | config.py:20 |
| `NEXRAD_HEARTBEAT_STALE_SECONDS` | `240.0` | config.py:21 |
| `NEXRAD_HEARTBEAT_STARTUP_GRACE_SECONDS` | `60.0` | config.py:22 |
| `MIN_REQUIRED_VOLUME_CHUNKS` | `25` | models.py, s3_chunks.py:21 |
| min sweep angle | `0.4`° | parser.py |
| canonical elevation bins | `(0.5,0.9,1.3,1.8,2.4,3.1,4.0)` | grouping.py |
| chunk download semaphore | `8` across sites | service.py |
| `max_candidate_volumes_per_site` | `3` | coordinator |
| `max_site_tasks` | `24` | coordinator.py |
| scan dirs to keep | `3` | writer.py |
| elevation dirs to keep | `2` | writer.py |
| stale manifest max age | `12` h | writer.py |

### RAP / Synoptic
| Constant | Value | File:Line |
|---|---|---|
| `RAP_BUCKET` | `noaa-rap-pds` | synoptic/config.py:5 |
| `RAP_FILE_PATTERN` | `rap.t{hour:02d}z.awp130pgrbf00.grib2` | config.py:6 |
| `RAP_DIR_PATTERN` | `rap.{date}` | config.py:7 |
| `RAP_MAX_AGE_MINUTES` | `180` | config.py:8 |
| `RAP_MAX_FILES` | `3` | config.py:9 |
| `RAP_FILENAME_RE` | compiled | synoptic/main.py:11 |

### WPC
| Constant | Value | File:Line |
|---|---|---|
| `WPC_CODED_SFC_BASE_URL` | `https://ftp.wpc.ncep.noaa.gov/coded_sfc` | config.py:5 |
| `UPDATE_INTERVAL_HOURS` | `3` | config.py:8 |
| `VALID_HOURS` | `[0,3,6,9,12,15,18,21]` | config.py:11 |
| HTTP timeout | `30` s | downloader.py |
| fallback GeoJSON color | `#000000` | converter.py |

### METAR
| Constant | Default | Location |
|---|---|---|
| `STATION_DB_URL` | `https://api.aviationweather.gov/v1/stations/` | metar.py:19 |
| `CONUS_BOUNDS` | lat `24..50` / lon `-125..-67` | metar.py:306 |
| per-request timeout | `60` s | metar.py |
| cache file | `stations_cache.json` | metar.py |

### NWS ingest
| Constant | Default | Location |
|---|---|---|
| `DROPPED_EVENTS` | blocklist (public-info styles) | main.py:47 |
| registry TTL | 2 h | registry.py |
| `ZONE_TYPES` | `("forecast","fire","public","county","marine")` | zone_sync.py |

---

## 8. EWMRS

### render/config.py
| Constant | Value | Line |
|---|---|---|
| `TILE_SIZE` | `350` px | :5 |
| `TILE_GRID_ROWS` / `TILE_GRID_COLS` | `10` / `20` | :5-6 |
| MRMS layer catalog | 15 entries (colormap keys listed) | :10-102 |
| GOES single-channel layers | 6 reflectance + 10 brightness temp, per-channel mask windows (reflectance 0.0–1.2; BT 180–330 K, C08 300, C09 310, C10 320) | :104-177 |
| GOES RGB recipes | 6 (true_color, airmass, nighttime_microphysics, day_cloud_phase, simple_water_vapor, sandwich) | :180-225 |

### render.py / tiler.py / tools.py
| Constant | Value | File |
|---|---|---|
| colormap cache `lru_cache(maxsize=128)` | 128 | render.py:51 |
| `EWMRS_TILE_THREADS` worker cap | env | render.py:100 |
| tile worker upper bound | `min(tile_count, 8, cpu_count)` | render.py:96-109 |
| tile grid derivation | `shape[0]//350`, `shape[1]//350` | render.py:146-151 |
| timestamp format | `%Y%m%d-%H%M00` | render.py:142 |
| PNG compress level | `1` | render.py:167, tiler.py:27 |
| tile naming | `tile_{x}_{y}.png` | render.py:207 |
| `index.json` structure | tiles + tile_grid | render.py:283 |
| `_TRANSFORMER_4326_TO_3857` | legacy | tools.py:71 |
| find_timestamp fallback | `datetime.utcnow().isoformat()` | tools.py:137 |

### nexrad.py (GUI serialization)
| Constant | Value | Location |
|---|---|---|
| variable→colormap keys | DBZH→NWS_Reflectivity, VRADH, WRADH, PHIDP, RHOHV, ZDR | :53-60 |
| manifest name pattern | `<SITE>_<scan_timestamp>_<volume_id>.json` | :391/:441 |

### goes_rgb.py / goes_transform.py
| Constant | Value | Location |
|---|---|---|
| `REFLECTANCE_CHANNELS` | `{C01..C06}` | goes_rgb.py:22 |
| `TRUE_COLOR_TERMINATOR_START/END_DEGREES` | `80.0` / `96.0` | :24-25 |
| `_MAX_SOLAR_CACHE_ENTRIES` | `32` | :30 |
| GOES_RGB_RECIPES | 6 recipes w/ channel sets | :43-58 |
| RGB normalization windows (airmass, night, DCP, SWV, sandwich…) | e.g. airmass `-26.2..0.6`, `-43.2..6.7`, `-64.65..-29.25` | :636-692 |
| max offset for layer selection | `20.0` min | :364,404,445 |
| `true_color_gamma` | `2.2` | :615-618 |
| green band blend | `0.45*C02 + 0.10*C03 + 0.45*C01` | :624 |
| resampling | `Resampling.bilinear` | :539 |
| CRS handling | `CRS.from_cf` first, proj4 geos fallback | goes_transform.py:99 |
| radian detection | units == "rad" or max coord ≤ 2.0 | :242-244 |

### RAP uint16 (`EWMRS/rap/`)

| Constant | Value | File:Line |
|---|---|---|
| pressure levels wind/thermo | `(925, 850, 700, 500, 250)` mb | config.py:9-10 |
| layer catalog | 29 layers (temp K 180–330, RH % 0–100, ThetaE 250–390, MSLP 95000–105000 Pa, CAPE 0–6000, SRH ±1000, vorticity ±0.0002, wind ±80 m/s…) | config.py:73-330 |
| `max_timestamps` retained | `3` | uint16_pipeline.py:101,264,287 |
| scale-to-uint16 | `rint((v-min)/(max-min)*65534)` | :135-152 |
| timestamp format | `%Y%m%d-%H%M00` | :333-348 |

### EWMRS pipeline.py
| Constant | Value | Location |
|---|---|---|
| GOES sub-extent | `(-13914936.3, 2814454.7, -7402746.1, 6360130.7)` | :44-48 |
| `run_render_pipeline` max_entries | `10`, cleanup_after=True, phase "EWMRS" | :713 |
| `run_goes_render_pipeline` cleanup age | `max_age_minutes=120` | :1076-1140 |
| NEXRAD GUI retention | `120` min, poll `30.0` s, workers `8` | ~:58-75 |
| GOES cleanup interval | env (default 300 s) | :55 |
| worker budget/reserve | env (above) | :138-139 |

### EWMRS scheduler.py
- Scheduling helpers only; no numeric tunables beyond above.

---

## 9. Node.js / Express APIs

### Unified API (`src/api/`)

**server.js**: port `5000` (`PORT` env), debug `3001` (`--debug-server`), host `0.0.0.0`. `--compat=edgewarn|ewmrs` and the legacy entry points now launch the same unified service (deprecation notice).

**app.js**: version = `package.json` version, exposed as `'2.x'` in production; OpenAPI read from `openapi/v3.yaml`; middleware order requestId → access log → security (helmet+compression) → cors → rate limiters → requestTimeout; static root `src/EWMRS`; root endpoints `/`, `/robots.txt`, `/health/live`, `/health/ready`; legacy paths served by compatibility router.

**config/index.js**
| Key | Default | Override |
|---|---|---|
| `DEFAULT_BASE_DIR` | `C:\EdgeWARN_input` (win32) else `~/EdgeWARN_input` | `--base-dir`/`--base_dir`, `EDGEWARN_BASE_DIR`, `BASE_DIR`; conflicting values rejected |
| port | `5000` | `PORT` env |
| `requestTimeoutMs` | `30000` | `REQUEST_TIMEOUT_MS` |
| `rateLimits.perSecond` / `.perMinute` | `40` / `2000` | `RATE_LIMIT_MAX_SEC` / `RATE_LIMIT_MAX_MIN` |
| data/gui/wpc dirs | baseDir + suffixes | derived |
| `isProduction` | `NODE_ENV==='production'` | env |
| `allowedOrigins` | `[]` (deny-all) | `ALLOWED_ORIGINS` CSV |
| `trustProxy` | false | `TRUST_PROXY_IPS` (list) / `TRUST_PROXY` (`true` rejected in production) |

**middleware**: cors `methods GET/HEAD/OPTIONS`, `allowedHeaders Content-Type, X-Request-Id`, `maxAge 600`, `credentials false`; rateLimit `standardHeaders:true`, `legacyHeaders:false`, disabled when max=0; security `defaultSrc "self"` CSP; compression skips image/*; `requestTimeout` responds `503` on timeout.

**repositories/artifactRepository.js**: `DEFAULT_LIMITS={json:8MiB, binary:128MiB, image:32MiB}`; LRU `max:256, maxSize:32MiB`; `open()` with `O_NOFOLLOW`/symlink guards; `list({limit=1000})`; `readJson` cache keyed by ETag; ETag `W/"<size>-<mtimeMs>-<ino>"`; `ArtifactError` status map (NOT_FOUND 404, INVALID_ARTIFACT/IN_PROGRESS 503, else 400).

**services**: validation `timestamp` (`YYYYMMDD-HHMMSS`), `isCellId`, `isAlertId`, `isLayerId`; `ALERT_SOURCES={'official','edgewarn'}`; `RADAR_SITE /^[A-Z0-9]{4}$/`; `RADAR_PRODUCTS` set of 7; `ELEVATION` regex; `DEFAULT_GRID` `{rows:10, cols:20, tileSize:350}`; `page()` default `limit=100`, cap `1000`; render chunk schema v2 `float16` `.f16.gz` scalar/rgb (grid ≤100×100, tile ≤4096 px).

**routes/v3**: collection `Cache-Control max-age=5`, resource `max-age=60`, assets `max-age=31536000, immutable`; query params only `cursor`/`limit` (plus `source` for alert endpoints), max length `256`; `limit` regex `^(?:[1-9][0-9]{0,2}|1000)$`; unsupported methods → `405`; errors as `application/problem+json`.

**compatibility router** (`src/api/routes/compatibility/`): serves legacy `/api/v2`, `/renders/*`, `/nexrad/*`, `/rap/*`, `/wpc/*`, `/colormaps`, `/health`, `/healthz` with `Deprecation`/`Link` headers; `/features`, `/data`, `/api/v1` → `410`.

**product-catalog.json**: 31 entries `{id, legacyId, storageDirectory, legacyFilePrefix, representation:'png-tiles', colormapId}`; `SLUG` regex; unique id/legacyId/storageDirectory.

### Legacy EdgeWARN API (`src/EdgeWARN/api/server.js`)
- **Deprecated**: entry point now warns and launches the unified service. Retained legacy `createApp`: helmet HSTS `maxAge 31536000, includeSubDomains`; CSP `default-src 'self'`; compression skips image/*; CORS dev allow-all / prod deny unless `ALLOWED_ORIGINS` (`credentials:true`, methods GET/HEAD/OPTIONS, headers Content-Type/Authorization); trust proxy via `TRUST_PROXY`/`TRUST_PROXY_IPS`; rate limits `40`/s, `2000`/min (windows `RATE_LIMIT_WINDOW_MS_*` `1000`/`60000` ms); cluster `numCPUs = min(cpus,4)`; json body `16kb`; `/health`, `/api/v2`, `/features`+`/data`+`/api/v1` → `410`, `robots.txt`. BASE_DIR cascade `--base-dir` > `EDGEWARN_BASE_DIR` > defaults.

### Legacy EWMRS API (`src/EWMRS/api/server.js`)
- **Deprecated**: entry point now warns and launches the unified service. Retained legacy `createApp`: `cors()` allow-all; `morgan('tiny')`; `helmet()`; compression skips image/*; rate limits `EWMRS_RATE_LIMIT_MAX_SEC` = `30`, `MAX_MIN` = `1800` (windows fixed `1000`/`60000` ms); base dir `--base_dir` > `BASE_DIR` env > platform default; `DEFAULT_PORT`/`DEBUG_PORT` `3003`/`3004`; `listFilesInDir` limit `50` (excludes `.idx`); routes `/renders`, `/nexrad`, `/rap`, `/wpc`, `/colormaps`, `/healthz`.

### Root configs
- **package.json**: version 2.7.0; scripts `api`, `debug:api`, `api:edgewarn`, `debug:edgewarn`, `api:ewmrs`, `debug:ewmrs`, `audit:prod`, `sbom:prod`; test via jest + supertest.
- **pytest.ini**: `pythonpath=src`, `asyncio_mode=auto`, `testpaths=tests`, markers `unit,integration,e2e,slow,api,ingest,process,ctam`; filters DeprecationWarning.
- **environment.yml**: conda `EdgeWARN-dev`, python 3.13, per Section runtime deps.
- **jest.config.js**: `testEnvironment:node`, coverage 70% thresholds, `testTimeout:10000`.

---

## Omissions / notes
- `EARTH_RADIUS_KM`, `KM_TO_M`, `WEB_MERCATOR_RADIUS_METERS`, 111320 m/deg and other unit conversions are **scientific/geodetic constants and intentionally excluded** per task instruction.
- Ejected as non-configurable mathematical/geodetic/projection constants (tuning them breaks the pipeline):
  - Reference lat `35.0°` for deg↔km conversion (`detect/kalman/state.py:51`)
  - `WEB_MERCATOR_BOUNDS` `(-14471533.8, 2273030.9, -6679169.5, 7361866.1)` and `WEB_MERCATOR_SHAPE` `(3500, 7000)` (`EWMRS/pipeline.py:36-37`)
  - Overlay manifest bounds in EPSG:3857 (`EWMRS/render/tools.py:187-192`)
  - StormCast ellipse/cone chi² quantile maps (`uncertainty.py:44`, `core.py:227`)
- GOES colormap `GOES_IR` threshold values in `colormaps.json` are data, not code.
- `GOES_MAX_ENTRIES=96` and `max_entries=10` in multiple tasks are documented above.
- NEXRAD wire-format/parsing constants are **not tunable config** and are excluded from the tables: record size `2432`, volume magics `b"AR2V"`/`b"ARCHIVE2"`, msg types `{2,3,5,13,15,18}`, `NEXRAD_FIELD_MAGIC`, msg-31 struct, raw block names, mask values, sweep label lookups, `MIN_SWEEP_ANGLE_DEG`, DUALPOL/DOPPLER block sets, and `UINT16_NODATA=65535`/`UINT16_VALID_MAX=65534` in the RAP uint16 encoding. Retained as config/calibration: `ALLOWED_VCPS`, `min sweep angle`, and `canonical elevation bins`.
- All numeric values verified against current source at audit time.

---

*Generated by automated deep codebase audit. Values reflect the checked-out working tree.*

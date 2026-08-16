# YAML Configuration Grouping Plan

**Source:** `CONFIGURATION_AUDIT.md` (§ references below), with every value
re-verified against the working tree at commit `e12d407`.
**Package version:** `2.7.0`
**Status:** files written under `config/`; not yet consumed by code, so no runtime
behavior has changed

## Objective

Group every configuration variable, constant, and tunable parameter into
separate YAML files under `config/`, one file per subsystem so each component
is self-contained. The files now exist; **each file is authoritative for its own
keys and defaults**, and this document is the map of which file owns what.

Wiring the files into the Python and Node code (loader module, precedence with
env/CLI, validation) is out of scope here.

## Scope boundaries

- **Included:** operator-facing and scientific tunables — timers, thresholds,
  buckets, URLs, retention, concurrency, layer/product catalogs, API policy.
- **Excluded by design:** unit conversions and geodetic constants
  (`EARTH_RADIUS_KM`, `KM_TO_M`, 111320 m/deg, reference lat `35.0°`),
  `WEB_MERCATOR_BOUNDS`/`WEB_MERCATOR_SHAPE`, overlay manifest bounds,
  chi² quantile maps, NEXRAD wire-format constants (record size `2432`, volume
  magics, msg types, `NEXRAD_FIELD_MAGIC`, msg-31 struct, raw block names, mask
  values), `UINT16_NODATA`/`UINT16_VALID_MAX`, colormap data in
  `colormaps.json`, and root tooling configs (`package.json`, `pytest.ini`,
  `environment.yml`, `jest.config.js`).
- **Documented but frozen:** the EWMRS chunk wire format is written into
  `ewmrs_render.yaml` for visibility because API clients depend on it, but it is
  marked non-tunable.

## Conventions used in every file

- Each file opens with a header comment citing its audit section and the source
  `file:line` for each block.
- Each file carries an informational `env_overrides` mapping of key path to
  environment variable name.
- Precedence at execution time: **CLI > env > YAML**, with no code fallback.
  YAML is the base layer, and a missing key is a startup error. See
  `plans/source-configuration-extraction-plan.md` for the rationale and for the
  argparse `None`-sentinel work this requires.
- `null` means "no default; resolved at runtime" (required arg or derived path).
- Paths are repo-relative unless prefixed `<base_dir>`.
- Catalog entries keep the field names already used in code so the loader can
  hand dicts straight to existing consumers.

## File index

| File | Audit § | Owns |
|---|---|---|
| `config/runtime.yaml` | §1, §2, §7 | `run.py` flags, GOES coordination, cycle retry |
| `config/historical.yaml` | §1 | `process_historical.py` window and output |
| `config/filesystem.yaml` | §7 | base dir resolution, cleanup, colormap lookup |
| `config/detection.yaml` | §4 | detection, gatemapper, morphology, hail |
| `config/kalman.yaml` | §3, §4 | existing sections + filter/confidence/cost internals |
| `config/lineage.yaml` | §4 | merge/split detection and confirmation buffer |
| `config/integration.yaml` | §5 | stats catalog, azshear, GLM, RAP extraction |
| `config/scheduler.yaml` | §6 | MRMS S3 polling widths, lookback, slow-check gate |
| `config/api_index.yaml` | §6 | index bootstrap, cell expiry, resync cadence |
| `config/mrms_goes.yaml` | §7 | MRMS/GOES buckets and product catalogs |
| `config/nexrad.yaml` | §1, §7 | buckets, selection, timeouts, concurrency, retention |
| `config/synoptic_rap.yaml` | §7 | RAP source bucket and freshness |
| `config/wpc.yaml` | §7 | WPC surface analysis |
| `config/metar.yaml` | §7 | METAR station DB and bounds |
| `config/nws.yaml` | §1, §7 | alert blocklist, registry, zone sync |
| `config/ewmrs_render.yaml` | §8 | tile/chunk format, layer catalogs |
| `config/ewmrs_rap_uint16.yaml` | §8 | RAP uint16 layer catalog |
| `config/ewmrs_pipeline.yaml` | §8 | render orchestration, worker budgets |
| `config/api.yaml` | §9 | Node service ports, security, limits |

---

## `config/runtime.yaml`

Audit §1 (`src/util/io.py` shared parser), §2 env vars, §7 scheduler
(`src/run.py`, `src/util/runtime/cycle.py`, `src/common/pipeline/goes_readiness.py`).

Keys and defaults: [`config/runtime.yaml`](../config/runtime.yaml).

`refl_threshold`, `min_seed_percentage` and `drop_offset` are **not** duplicated
here — they live in `detection.yaml`; the `--refl-threshold`,
`--min-seed-percentage` and `--drop-offset` flags override those keys.

## `config/historical.yaml`

Audit §1 (`src/process_historical.py`).

Keys and defaults: [`config/historical.yaml`](../config/historical.yaml).

## `config/filesystem.yaml`

Audit §7 Filesystem (`src/util/file.py:183-186, 195-200, 282-342`). New file —
this subsection had no home in the previous layout.

Keys and defaults: [`config/filesystem.yaml`](../config/filesystem.yaml).

## `config/detection.yaml`

Audit §4 (`detect/main.py:29-31,100-105,338-368`, `tools/gatemapper.py`,
`tools/morphology.py:14`, `tools/save.py:148-182`).

Keys and defaults: [`config/detection.yaml`](../config/detection.yaml).

## `config/kalman.yaml`

Audit §3 (existing keys, reproduced unchanged) plus §4
(`kalman/filter.py`, `state.py`, `confidence.py`, `assignment.py`) which had no
home before. The three `from_yaml` loaders read only `kalman_filter`,
`tracking` and `assignment` via `.get()`, so the added top-level sections are
ignored by current code — the file stays backward compatible.

Keys and defaults: [`config/kalman.yaml`](../config/kalman.yaml).

## `config/lineage.yaml`

Audit §4 (`detect/lineage/detector.py:29`, `spatial.py:76,118`,
`buffer.py:138-159`).

Keys and defaults: [`config/lineage.yaml`](../config/lineage.yaml).

`detect/track.py` constructs `LineageBuffer()` with these defaults.

## `config/integration.yaml`

Audit §5 (`integrate/core/stats.py:4`, `azshear/constants.py:1-5`,
`azshear/integration.py:73`, `integrate_glm.py:7`, `integration.py:68`,
`integrate/config.py`).

Keys and defaults: [`config/integration.yaml`](../config/integration.yaml).

## `config/scheduler.yaml` and `config/api_index.yaml`

Audit §6, which grouped three unrelated subsystems under one heading. The
scheduler half (`schedule/scheduler.py`) and the index half
(`api_integration/index_manager.py`, `EdgeWARN/pipeline.py`,
`process_historical.py`) each own a file. The CTAM half (`alerts/schema.py`,
`alerts/manager.py`) owns no catalog: its literals stay in place until CTAM is
extracted deliberately, so nothing here concerns alert emission.

Keys and defaults: [`config/scheduler.yaml`](../config/scheduler.yaml),
[`config/api_index.yaml`](../config/api_index.yaml).

## `config/mrms_goes.yaml`

Audit §7 MRMS & GOES (`common/ingest/mrms/config.py`, `s3_common.py:6`,
`https_client.py:24`, `downloader.py:33`, `parse.py`).

Keys and defaults: [`config/mrms_goes.yaml`](../config/mrms_goes.yaml).

`outdir` values reference `util.file` attributes and stay derived — the loader
resolves the name, it is not a literal path.

## `config/nexrad.yaml`

Audit §1 NEXRAD CLIs (`nexrad/pipeline/__init__.py:387-391`,
`nexrad/main.py:174-178`) and §7 NEXRAD (`nexrad/config.py`, `s3_chunks.py:21`,
`grouping.py`, `parser.py`, `coordinator.py:25`, `service.py:164,237-238`,
`worker_pool.py:194,210`, `writer.py:15-18`).

Keys and defaults: [`config/nexrad.yaml`](../config/nexrad.yaml).

## `config/synoptic_rap.yaml`

Audit §7 RAP/Synoptic (`common/ingest/synoptic/config.py:4-9`,
`synoptic/main.py:11`).

Keys and defaults: [`config/synoptic_rap.yaml`](../config/synoptic_rap.yaml).

## `config/wpc.yaml`

Audit §7 WPC, plus the `FEATURE_TYPES` styling table the audit omitted. Read
through the accessors in `common/ingest/wpc/config.py`, which `downloader.py`,
`converter.py`, and `main.py` call per use.

Two of the audit's claims about WPC were wrong and are struck: the cleanup glob
never mismatched the writer, and TLS verification was never disabled. See
[Phase 0 findings §1 and §1b](source-configuration-extraction-phase0-findings.md).

Keys and defaults: [`config/wpc.yaml`](../config/wpc.yaml).

## `config/metar.yaml`

Audit §7 METAR (`common/ingest/metar.py:19,306`).

Keys and defaults: [`config/metar.yaml`](../config/metar.yaml).

## `config/nws.yaml`

Audit §1 zone_sync CLI (`nws/zone_sync.py:369-418`) and §7 NWS
(`nws/main.py:47`, `nws/registry.py`, `zone_sync.py:18`).

Keys and defaults: [`config/nws.yaml`](../config/nws.yaml).

## `config/ewmrs_render.yaml`

Audit §8 (`EWMRS/render/config.py`, `render.py:23,95-108,141,204`,
`tiler.py`, `tools.py`, `render/nexrad.py:53-60`). Note the render path now
emits float16 value chunks, not PNG tiles.

Keys and defaults: [`config/ewmrs_render.yaml`](../config/ewmrs_render.yaml).

There is no `config/ewmrs_rgb.yaml`. The GOES RGB recipe machinery the audit
described (`goes_rgb.py`, `GOES_RGB_RECIPES`, terminator angles, solar cache,
gamma, green-band blend) no longer exists in the tree — `render/config.py`
documents derived color products as a client-side concern.

## `config/ewmrs_rap_uint16.yaml`

Audit §8 RAP uint16 (`EWMRS/rap/config.py`, `uint16_pipeline.py:101,264,287,348`).
43 layers, not the 29 the audit reports.

Keys and defaults: [`config/ewmrs_rap_uint16.yaml`](../config/ewmrs_rap_uint16.yaml).

The four generated families (10 isobaric thermo + 10 isobaric wind layers) are
expanded by the loader from `pressure_levels_mb`, matching the current
`_pressure_thermo_layers()` / `_pressure_wind_layers()` behavior. `outdir`
stays derived (`GUI_RAP_DIR / name.removeprefix('RAP_')`).

## `config/ewmrs_pipeline.yaml`

Audit §8 EWMRS pipeline (`EWMRS/pipeline.py:44-65,120-140,551-585,707-794`).

Keys and defaults: [`config/ewmrs_pipeline.yaml`](../config/ewmrs_pipeline.yaml).

`WEB_MERCATOR_BOUNDS`, `WEB_MERCATOR_SHAPE` and `WEB_MERCATOR_TRANSFORM` stay in
code as projection invariants.

## `config/api.yaml`

Audit §9 (`src/api/config/index.js`, `middleware/`,
`repositories/artifactRepository.js:14-76`, `services/validation.js:18-19`,
`services/renders.js:5`, `routes/v3/`, `EdgeWARN/api/server.js`,
`EWMRS/api/server.js:17-18,77-79,145`).

Keys and defaults: [`config/api.yaml`](../config/api.yaml).

Route tables, deprecation headers, `ArtifactError` status mapping and ETag
format stay in code — they are protocol behavior, not tunables.

---

## Key decisions

1. **One file per subsystem.** Files map to audit sections so the audit table
   and the config tree stay cross-referencable, and each Python/Node component
   reads a single adjacent file.
2. **Single owner per key.** `refl_threshold`, `min_seed_percentage` and
   `drop_offset` live only in `detection.yaml`; `runtime.yaml` carries the
   enable/disable flags and the CLI flags override the detection keys. No key
   appears in two files.
3. **Ingestion CLIs bundled with their constants.** NEXRAD and NWS argparse
   defaults live in the same file as their module constants.
4. **Env vars remain overrides.** Every file carries an `env_overrides` block.
   Precedence: CLI > env > YAML, no code fallback.
5. **`config/kalman.yaml` is extended, not replaced.** All three `from_yaml`
   loaders read named sections with `.get()`, so the new `filter_internals`,
   `confidence` and `assignment_costs` sections are inert until a loader consumes
   them — no compatibility break, and no second Kalman file to keep in sync.
6. **New `config/filesystem.yaml`.** Base-dir resolution, cleanup defaults and
   colormap lookup had no owner in the previous layout.
7. **No `config/ewmrs_rgb.yaml`.** The code it would configure has been deleted.
8. **Catalogs are enumerated, not summarized.** Once a catalog moves to YAML the
   YAML is authoritative, so MRMS products, ABI channels, render layers, stats
   datasets and RAP layers are listed in full. Only `outdir`/`filepath` values
   stay derived from `util.file` attribute names.
9. **Wire formats are documented but frozen.** The EWMRS chunk contract is
   written into `ewmrs_render.yaml` because API clients depend on it, and marked
   non-tunable. NEXRAD parsing constants stay in code entirely.

## Audit corrections required

`CONFIGURATION_AUDIT.md` disagrees with the working tree in the following
places. The values above follow the source; the audit should be updated to
match.

| § | Audit says | Source says |
|---|---|---|
| §7 NEXRAD | station catalog `https://api.weather.gov/api/stations` | `https://api.weather.gov/radar/stations` |
| §7 NEXRAD | `WEATHER_API_CACHE_TTL_SECONDS` `0` | `30` |
| §7 NEXRAD | chunk download semaphore `8` | `max_chunk_downloads = 64` |
| §7 METAR | `https://api.aviationweather.gov/v1/stations/` | `https://aviationweather.gov/data/cache/stations.cache.json` |
| §7 METAR | `CONUS_BOUNDS` lon `-125..-67` | `-125.0..-66.0` |
| §5 | integration stats "7 datasets" | 25 datasets |
| §5 | — | `get_rap_products()` (37 isobaric levels × u/v, 10 m winds, 2 m t/d, freezing level, 2 derived) is undocumented |
| §8 RAP | 29-layer catalog, SRH `±1000` | 43 layers, SRH `-500..1000`; Dewpoint_2m, CIN/MLCIN/MUCIN, SnowWaterEquivalent, SnowDepth, WetBulbZeroHeight, FreezingLevelHeight and LiftedIndex are undocumented |
| §8 | `goes_rgb.py` recipes, terminator angles, solar cache, gamma, green-band blend | file and all symbols removed from the tree |
| §8 | PNG tiles, `tile_{x}_{y}.png`, compress level `1` | float16 chunks, `chunk_{x}_{y}.f16.gz`; `CHUNK_*` constants undocumented |
| §8 | GOES BT mask floor `180` for all channels | C10 is `185.0..320.0` |
| §7 WPC | fallback color only | `FEATURE_TYPES` (7 front/pressure types with colors) undocumented |
| §4 | `max_prediction_time_minutes` fallback `10.0` | dataclass default is `6.0`; only the inline `.get()` fallback is `10.0` |

Stale file paths in the audit: `cycle.py` → `src/util/runtime/cycle.py`;
`nexrad/pipeline/worker_pool.py` → `src/common/ingest/nexrad/worker_pool.py`;
`metar/metar.py` → `src/common/ingest/metar.py`;
`product-catalog.json` → `src/api/config/product-catalog.json`;
`_GLM_BIN_SIZE_DEGREES` and `DEFAULT_OVERLAP_THRESHOLD` line numbers have moved.

## Execution phase

1. ~~Create the 18 files above~~ — done; see `config/`. `config/kalman.yaml` was
   extended in place rather than rewritten.
2. Add `src/common/config/loader.py` (dataclass-backed, `pythonpath = src`
   compatible) with a `from_yaml` pattern like `kalman/config.py` — but with no
   inline literal fallbacks, since YAML is the base layer — and CLI > env > YAML
   precedence. Add a matching Node loader for `api.yaml`.
3. Wire the highest-impact consumers first: `run.py` / `process_historical.py`,
   detection, ingest coordinators, EWMRS pipeline.
4. Add pytest coverage asserting each YAML value matches the constant it
   replaces, that catalog lengths match (`28`/`12` MRMS, `16` ABI, `15` MRMS
   render, `16` GOES render, `25` stats, `43` RAP, `31` product catalog), and
   that every file parses and validates.
5. Update `CONFIGURATION_AUDIT.md` per the corrections table above.
6. Document the authoritative file per setting in `docs/`.







# Phase 0 findings — source configuration extraction

Phase 0 of `plans/source-configuration-extraction-plan.md` is complete. Phase 0
was value-preserving by design: nothing in `src/` was modified. The deliverable
is a characterization suite that freezes today's effective configuration, plus
the corrections below, which the plan needs before Phase 1 begins.

Audited against `HEAD` on `version-test/3.0.0`. The plan's own audit baseline is
commit `3afb88f`.

## What was added

| Path | Contents |
| --- | --- |
| `tests/core/config/baseline.py` | Snapshot harness. Normalizes dataclasses, sets, tuples, and `Path` values into stable JSON. Regenerate with `UPDATE_CONFIG_BASELINE=1`. |
| `tests/core/config/source_inspect.py` | `ast`-based reader for declared defaults, module constants, and `add_argument` flags. |
| `tests/core/config/test_catalog_baseline.py` | 38 tests over the product and data-source catalogs, including the Node `product-catalog.json` "API mappings" and its length-coincidence check against `EWMRS.render.config.get_file_list()`, plus the AzShear scientific-tunables snapshot. |
| `tests/core/config/test_known_drift.py` | 24 tests, one per known disagreement. |
| `tests/core/config/test_surface_baseline.py` | 20 tests over the filesystem, CLI, environment, and process-supervision surfaces, including the `AccessorySupervisor` restart/backoff timers and the NEXRAD stale-manifest retention constant. |
| `tests/config_baseline/*.json` | 25 committed snapshots. |

`python -m pytest tests/core/config` gives **82 passed, 0 skipped**. The
scientific dependencies (`numpy`, `xarray`, `scipy`, `rasterio`, `pyproj`,
`cfgrib`, etc.) were installed via `pip install` per `environment.yml`, which
resolved the environment gap described in the original draft of this document
and let the four remaining catalogs (`mrms_membership_lists`,
`integration_datasets`, `rap_integration_products`, `rap_transform_registry`)
get their baselines generated and committed.

Two design choices are worth knowing before touching these files.

**Snapshots record path *names*, not paths.** `util.file` exposes exactly 113
`Path` attributes with no two pointing at the same directory, so the harness can
render an absolute path back to `fs:ATTRIBUTE_NAME` unambiguously. Snapshots are
therefore identical on Windows, Linux, and a devcontainer. Uniqueness is
asserted by `test_path_attribute_count_and_uniqueness`, because a future
duplicate would silently alias two catalog entries onto one token.

**The drift tests read source text, not runtime values.** Most modules holding
the contested literals cannot be imported without the full scientific stack, and
a runtime read would show whatever the caller already overrode. Asserting on the
declaration is both runnable anywhere and closer to the actual question, which
is "how many places declare this."

## Corrections to the plan

Three rows of the plan's "Corrections required before the files can be loaded"
table need amending. Fifteen of the eighteen rows were verified correct against
`HEAD` and need no change.

### 1. The WPC glob mismatch does not exist — strike it

Plan lines 192-194 claim cleanup searches `surface_analysis_*.geojson` while the
writer emits a different prefix. It does not.

- `src/common/ingest/wpc/main.py:98` globs `wpc_sfc_*.geojson`
- `src/common/ingest/wpc/downloader.py:93` writes `f"wpc_sfc_{...}.geojson"`

`surface_analysis` is only a directory name (`src/util/file.py:176`).
`src/EWMRS/pipeline.py` contains no WPC references at all. Cleanup and the
writer already agree; there is nothing to reconcile. Pinned by
`test_wpc_cleanup_glob_matches_the_generated_filenames`.

### 2. `max_chunk_downloads` is in the wrong file

The plan attributes the chunk-download semaphore to `s3_chunks.py`. The value
`64` is correct, but `s3_chunks.py` does not mention `max_chunk_downloads` at
all — it holds the chunk-key regexes and `MIN_REQUIRED_VOLUME_CHUNKS = 25`. The
real declarations are:

- `src/common/ingest/nexrad/service.py:238` — `NexradIngestService.__init__`
- `src/common/ingest/nexrad/main.py:58` — re-declared by the CLI entry point

Consumed as `asyncio.Semaphore(self.max_chunk_downloads)` at `service.py:378`,
`:1075`, and `:1339`. Because the value is declared twice, a `runtime.yaml` key
has to displace both sites, not one. Pinned by
`test_max_chunk_downloads_is_declared_in_two_places_and_not_in_s3_chunks`.

Related: the earlier audit's value of `8` for this key is wrong.

### 3. The lineage overlap pair is a shadowing bug, not two policies

This is the correction most likely to cause harm if left as written. The plan
advises giving `0.15` and `0.10` distinct config names, on the assumption that
they express two deliberate policies. They do not — `0.15` is unreachable from
the tracked path.

There are three declarations, not two:

| Site | Default |
| --- | --- |
| `lineage/detector.py` `DEFAULT_OVERLAP_THRESHOLD` | `0.15` |
| `detect/track.py:65` `StormCellTracker.__init__` | `0.10` |
| `lineage/spatial.py` `find_overlapping_cells` | `0.0` |

`track.py:186` passes `overlap_threshold=self.overlap_threshold` into
`LineageDetector`, so the detector's `0.15` applies only when a detector is
constructed directly — which production code never does. Following the plan as
written would mint a `lineage.yaml` key that looks authoritative and has no
effect. The decision owed is whether `0.15` was ever intended to be live, not
what to name it. Pinned by
`test_lineage_overlap_default_is_shadowed_by_the_tracker`.

## New findings

These were not in the plan and each carries a decision.

**Detection thresholds are declared eight times each, so YAML can never win.**
`refl_threshold=37.5`, `min_seed_percentage=0.001`, and `drop_offset=10.0` each
appear as seven keyword-argument defaults plus one argparse flag: `pipeline.py`
(3), `detect/main.py` (2), `detect/detect.py` (1), `tools/gatemapper.py` (1), and
`util/io.py` (1, the flag). Every caller re-declares the literal, so introducing
`detection.yaml` without deleting the intermediate declarations changes nothing.
Phase 4 must drive this count to one. `test_detection_thresholds_are_declared_eight_times_each`
asserts the per-file breakdown, not just the total, so a partial cleanup is
visible.

**19 of 40 CLI flags cannot express "unset".** The plan's precedence rule is
CLI > env > YAML, which only works if a flag can decline to supply a value. A
flag with a non-`None` default always sends one. Current split, by module:

| Module | Shadows YAML | Can be unset |
| --- | --- | --- |
| `util/io.py` | 8 | 13 |
| `common/ingest/nws/zone_sync.py` | 6 | 3 |
| `common/ingest/nexrad/pipeline/__init__.py` | 3 | 2 |
| `common/ingest/nexrad/main.py` | 2 | 3 |

The 19 shadowing flags are the exact Phase 3 conversion list, enumerated in
`tests/config_baseline/cli_shadowing_*.json`. Exactly four modules define a CLI,
asserted by `test_only_four_modules_define_a_cli` so a fifth cannot appear
unnoticed.

**No `.get()` call in the Kalman config trusts its YAML.** All 19 `.get()` calls
in `kalman/config.py` pass an inline fallback; zero rely on the file supplying
the key. One fallback openly disagrees with the file it reads:
`max_prediction_time_minutes` is `6.0` in both the dataclass and
`config/kalman.yaml`, but the fallback says `10.0`. Today the effective value is
`6.0`; dropping the key from the YAML would silently switch to `10.0`.

**Half of `config/kalman.yaml` is inert.** Only `kalman_filter`, `tracking`, and
`assignment` are read. `confidence`, `assignment_costs`, and `filter_internals`
are not consumed by any code path. Note this contradicts the section names the
plan implies; the pinned set is in
`test_kalman_yaml_sections_consumed_versus_inert`.

**The base directory is bound before any config file could be read.**
`_define_paths()` runs at `util/file.py` module scope, so all 113 paths exist at
import. `EWMRS/render/config.py:221` then snapshots `file_list = get_file_list()`
at import as well, capturing pre-`--base-dir` paths. A `paths.yaml` cannot win
against either until Phase 1 defers them.

**Python and Node disagree on how to find the base directory.** Node reads
`EDGEWARN_BASE_DIR`. Python never reads it — `util/file.py` contains no
`environ` reference at all and resolves from `platform.system()` instead,
accepting an override only through `initialize_filesystem(base_dir=...)`.
Nothing keeps the two in agreement. Pinned by
`test_base_dir_alias_is_asymmetric_between_python_and_node`.

**16 environment variables are read, with no shared parser.** Thirteen are
EdgeWARN configuration; three (`GDAL_DATA`, `PROJ_DATA`, `PROJ_LIB`) are
third-party and should stay out of the `env:` allowlist. Only
`common/ingest/synoptic/config.py` validates its value and raises on malformed
input; every other site coerces or falls through, so identical bad input behaves
differently per variable. Full inventory in
`tests/config_baseline/environment_variables_python.json`, reader modules in
`environment_reader_modules.json`.

**Retention defaults are Python defaults.** `max_age_minutes=60` and
`max_files=10` are signature defaults on the four `util.file` cleanup functions,
so `retention.yaml` cannot reach them until they are parameterized. The `.idx`
and `.gz` skip rules are hardcoded in the same module.

**`src/EWMRS/mappings.json` is a derivable duplicate with one orphan.** All 44
entries equal `_with_colormap_key()` applied to the 43-layer RAP catalog, plus
`RAP_BestLiftedIndex_180_0mbAGL`, which has no Python producer. Either delete
the orphan or add a producer; keeping the file as a second authority guarantees
future drift.

**Every configured colormap key currently resolves.** The plan's proposed
cross-check will pass on day one — worth knowing so a failure during Phase 2 is
read as a real regression rather than pre-existing debt.

**Two RAP `outdir` naming asymmetries must survive transcription**, alongside the
already-documented `MRMS_DVIL_DIR` → `GUI_VILD_DIR` case: `RAP_SRH_0-1km` →
`SRH-0-1km` (hyphen) but `RAP_SRH_0-3km` → `SRH_0-3km` (underscore); also
`RAP_CAPE_0_3km` → `CAPE_0-3km` and `RAP_LiftedIndex_Surface_500_1000mb` →
`LiftedIndex_Surface_500-1000mb`. A transcriber who "fixes" these breaks the
output paths.

**Dead branch in `EWMRS/render/config.py:165`.** The reflectance `colormap_key`
ternary tests `channel_id in {"C01".."C06"}`, but `reflectance_specs`
(`:142-149`) holds exactly those six channels, so the `else` branch is
unreachable and the key is always `"GOES_RGB_Raw"`. Transcribing the ternary
into YAML would preserve a branch that cannot run. Pinned by
`test_reflectance_colormap_ternary_has_an_unreachable_branch`.

**`get_file_list()` returns 31 layers, and the Node `product-catalog.json` has
exactly 31 entries.** Whether that is a maintained correspondence or a
coincidence should be settled before either is moved, since Phase 2 would
otherwise fix the coincidence in place.

**Catalog modules are trapped behind a heavy package `__init__`.**
`EdgeWARN/__init__.py` imports `pipeline`, which imports `xarray`, so even the
otherwise-pure `EdgeWARN.process.integrate.config` cannot be imported without
the full stack. Phase 1 requires the loader to depend only on stdlib, `yaml`,
and `jsonschema`; `test_catalog_modules_import_standalone` guards the four
modules that already satisfy this.

## Silent fallbacks that a loader must reject

Each is pinned by a test and each is a decision the plan still owes.

- Unknown RAP transform names resolve to identity:
  `TRANSFORMS.get(product.get("transform"), lambda x: x)`. Once transform names
  live in YAML, a typo becomes a silent no-op.
- An unparseable derived formula sets the field to `None` per cell rather than
  failing at startup.
- `GateMapper` applies `min(37.5, self.refl_threshold)`, so raising
  `--refl-threshold` above `37.5` cannot change the baseline mask. A
  `detection.yaml` key would be inert across half its range. The adaptive
  literals `40.0`, `45.0`, and `52.0` are inline and not parameters.
- Integration output rounding is hardcoded `round(..., 2)` while
  `config/integration.yaml` records `output.decimals: 2` that nothing reads.
- `config/nexrad.yaml` records `cli.sites: []`, but the code treats `None` as
  "all sites" and a list as an explicit selection, so the transcribed sentinel
  currently means "no sites."
- `GUI_COLORMAP_JSON` resolution tries `Path.cwd() / "colormaps.json"` first, so
  the resolved file depends on the working directory.
- `src/run.py` calls `get_args()` at module scope, outside the `__main__` guard,
  which re-executes in every child process under Windows `spawn`.
- Two user agents disagree: `(EdgeWARN/1.0, contact@edgewarn.com)` in
  `nws/zone_sync.py` versus `(EdgeWARN/2.7.0, ewsbackend@gmail.com)` in
  `nexrad/config.py`. Only the second tracks `package.json`, and it does so by
  copy.
- `zone_sync` `pause_seconds` is `0.05` in the constructor and `0.0` on the
  flag, and the CLI always forwards its value, so the constructor default
  reaches only direct programmatic callers. `config/nws.yaml` recorded `0.0`,
  i.e. one side of a live disagreement.

## Environment gap — resolved

The active interpreter (`C:\Python313\python`, 3.13.3) was a partial
environment missing `numpy`, `pandas`, `xarray`, `scipy`, `shapely`,
`scikit-image`, `rasterio`, `rioxarray`, `netCDF4`, `cfgrib`, `eccodes`,
`psutil`, `requests`, `ijson`, `opencv-python-headless`, and `pyproj`. The
`EdgeWARN-dev` conda environment named in `AGENTS.md` is not present on this
machine (`conda` is not on `PATH`), so these were installed directly with
`pip install`, reading the package list from `environment.yml` (its conda
`dependencies:` plus its nested `pip:` list). `gputil` could not be installed —
PyPI returned an HTTP 403 on that one artifact at install time — but nothing in
`tests/core/config` depends on it.

With the real packages in place:

- **All four previously-skipped catalogs are now baselined and committed**:
  `mrms_membership_lists`, `integration_datasets`, `rap_integration_products`,
  `rap_transform_registry`.
- **Four additional coverage gaps, present in the plan's own wording but not
  in the first pass of this suite, are now closed**: the AzShear scientific
  tunables (`AZSHEAR_BUFFER_KM` and four siblings), the Node-side
  `product-catalog.json` "API mappings" catalog (31 entries, unique ids, and
  its length match against `EWMRS.render.config.get_file_list()` flagged as a
  DECISION OWED rather than assumed), the `AccessorySupervisor` restart/backoff
  timers named by the plan's "timers" category, and the NEXRAD stale-manifest
  retention constant (`STALE_MANIFEST_MAX_AGE_HOURS = 12`, pinned by asserting
  the parameter default is a reference to the module constant rather than a
  second inline literal). `python -m pytest tests/core/config` is
  **82 passed, 0 skipped**.
- Running the whole repository's test suite (`python -m pytest tests/`,
  excluding `tests/benchmarks`) now collects successfully apart from three
  files — `test_nexrad_parser.py`, `test_nexrad_worker.py`,
  `test_nexrad_worker_pool.py` — which `import resource`, a POSIX-only
  standard-library module with no Windows equivalent and no pip package to
  supply it. That is a platform gap, not a missing dependency.
- Of the tests that do collect, 51 fail on this machine for two Windows-only
  reasons, both pre-existing and unrelated to this session's changes:
  multiprocessing tests that rely on `fork` semantics (Windows uses `spawn`,
  which cannot pickle a locally-defined worker function — confirmed on
  `tests/util/test_runtime.py::test_supervisor_restarts_alive_process_with_stale_heartbeat`),
  and file-handling tests that hit `[Errno 9] Bad file descriptor` (confirmed
  on `tests/core/alerts/test_manager.py::test_publish_creates_file`), consistent
  with code written assuming POSIX file semantics. Neither category touches
  `tests/core/config`, which passes cleanly.
- Phase 0 step 4, config-sensitive benchmark baselines, was attempted against
  every script in `tests/benchmarks/`. All now import their dependencies, but
  a Windows dev laptop turned out to be the wrong environment for most of
  them, for three distinct reasons rather than one:
  - **Ran and produced a real number**: `benchmark_azshear_integration.py`
    (fully synthetic — `_make_cells`, a `tempfile.TemporaryDirectory`, and a
    mocked writer) and `benchmark_grid_index.py` (also synthetic; its
    hardcoded `sys.path.insert(0, '/home/yuchenwei/...')` is an *insert*, not
    a *replace*, so it's harmless as long as `src` is reachable another way,
    which it is here via `PYTHONPATH`).
    ```
    AzShear integration synthetic benchmark: mean 0.6348s, median 0.6443s
    over 8 runs (min 0.5837s, max 0.6647s).

    Grid index: 100x100/50 cells -> 3.6x speedup; 337x451/100 cells -> 62.8x;
    500x700/200 cells -> 167.3x (brute force vs. RegularGridIndexer).
    ```
  - **Blocked by missing production data on this machine** (skips cleanly or
    fails only because the input directory doesn't exist, not because of a
    code bug): `benchmark_integration_pipeline.py` needs `fs.STORMCELL_DIR`
    (`C:\EdgeWARN_input\data\stormcells`, absent); `benchmark_rap_uint16.py`
    calls `pytest.skip("No sample RAP file available in fs.RAP_DIR")` and
    skips cleanly; `test_performance.py` needs several live MRMS/GOES/RAP
    directories; `benchmark_goes_pipeline.py` runs to completion but every one
    of its 16 layers reports `"success": false, "output_count": 0"` because
    `fs.*_DIR` (e.g. `C:\EdgeWARN_input\data\ABI_RadC\VisibleBlue`) doesn't
    exist — the script itself never calls `pytest.skip`, so it looks like a
    result but isn't one.
  - **Blocked by a genuine Windows-specific bug in the source, not the
    benchmark**: `benchmark_ewmrs_render.py` fails with
    `OSError: [Errno 9] Bad file descriptor` inside
    `src/util/atomic.py:37` (`atomic_output_path`, at
    `os.fsync(handle.fileno())`) — the same failure category confirmed
    independently on `tests/core/alerts/test_manager.py::test_publish_creates_file`,
    so this is pre-existing POSIX-file-semantics code, not a benchmark-script
    problem. `benchmark_nexrad_memory.py` fails with
    `FileNotFoundError: [WinError 3] ... '/tmp/kilo\\nexrad_mem_1_...'`
    because it hardcodes `tempfile.TemporaryDirectory(..., dir="/tmp/kilo")`
    (line 175) — a Linux-only absolute path with no Windows equivalent.
    `benchmark_nexrad_pool_memory.py` fails to import at all:
    `ModuleNotFoundError: No module named 'resource'`, the same POSIX-only
    stdlib gap already blocking three files in `tests/`.
  - **Not attempted**: `benchmark_nexrad_live_pool_memory.py`,
    `benchmark_nexrad_memory_live.py`, `benchmark_nexrad_realtime_memory.py`,
    and `benchmark_lazy_loading.py` all require live network access (real
    radar station metadata, real AR2V volume downloads) or real GRIB/NetCDF
    sample files that don't exist on this machine.

  Net result: of ~15 benchmark scripts, 2 produced a usable number here
  (`benchmark_azshear_integration.py`, `benchmark_grid_index.py`); the rest
  are blocked by missing production data, missing live network access, or two
  newly-identified Windows-specific source bugs (`util/atomic.py`'s
  `os.fsync` call, and the hardcoded `/tmp/kilo` path in
  `benchmark_nexrad_memory.py`) that are independent of this migration and
  worth filing separately. Recording a number from a Windows dev laptop is in
  any case not a meaningful baseline for a Linux deployment target — capture
  the four whose throughput depends on catalog size or threshold values
  (`benchmark_integration_pipeline.py`, `benchmark_ewmrs_render.py`,
  `benchmark_rap_uint16.py`, `benchmark_azshear_integration.py`) on the actual
  target platform, with real sample data, before relying on any of them as a
  regression gate.

## Recommended sequencing change

The plan's Phase 4 (delete code fallbacks) is currently downstream of Phases 2-3
(move values into YAML, wire precedence). The eight-times-each detection
defaults and the 19 shadowing CLI flags mean that ordering produces a window in
which `detection.yaml` exists, appears authoritative, and has no effect —
exactly the inert-key failure the plan is trying to prevent. Per subsystem,
delete the redundant declarations in the same change that starts reading the
YAML key, rather than deferring all deletions to one late phase.

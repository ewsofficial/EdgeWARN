# Source Configuration Extraction Plan

**Audit baseline:** commit `28beff7242495170ad4cc34d22d74f0b3316e931`
on `version-test/3.0.0`  
**Package version:** `2.7.0`  
**Status:** planning only; this document does not move or change runtime
configuration

## Objective

Move deployable settings, product catalogs, scientific tunables, source
endpoints, path layout, retention policy, concurrency limits, and API policy
out of Python and JavaScript source files and into a validated configuration
tree under `config/`.

The extraction must include, at minimum, the complete MRMS ingest catalog and
its readiness/detection/integration/render memberships, the integration
dataset/statistic catalog, GOES ABI and RGB products, RAP integration and
render layers, NEXRAD products/VCP policy, CTAM tunables, filesystem layout,
runtime scheduling, and both API services.

This is not a request to turn every literal into configuration. Binary format
constants, mathematical and physical invariants, public wire-format
requirements, validation rules, and control-flow safety invariants remain in
code. The boundary is made explicit below so the implementation does not
create an unmaintainable “everything is YAML” system.

## Completion definition

The implementation is complete when:

- Every item in the source inventory below is either read from `config/`,
  derived from another configured value, or recorded in the intentional
  code-constant allowlist.
- There is one authoritative product definition for each MRMS, GOES, RAP, and
  NEXRAD product. Ingest, integration, rendering, filesystem, and API views
  are derived from those definitions rather than separately maintained lists.
- Python and Node load the same shared files, validate them before starting
  work, and report actionable key paths for invalid values.
- Existing CLI and environment overrides retain compatibility and have a
  documented precedence order.
- No production module silently substitutes a second hard-coded default when
  a config file or key is missing.
- Tests compare the new configuration against a checked-in current-behavior
  snapshot before any values are intentionally tuned.
- Documentation names the authoritative file for every operator-facing
  setting.
- A repository audit test prevents new configurable endpoints, product lists,
  timers, retention values, worker limits, and scientific thresholds from
  being added directly to production source.

## Configuration boundary

### Move to `config/`

Move values when at least one of these is true:

- Operators may need to change the value by deployment, domain, resource
  budget, data-source availability, or policy.
- The value selects a product, layer, statistic, event type, pressure level,
  source URL/bucket, output directory, colormap, or processing phase.
- The value is an empirically chosen scientific threshold, score weight,
  confidence cutoff, smoothing parameter, search window, or fallback policy.
- The value controls polling, retry, retention, cleanup, cache, timeout,
  concurrency, memory, logging, or server behavior.
- The same conceptual value or mapping appears in more than one source file.

### Keep in code

Keep these categories in code and cover them with named constants and tests:

- File-format and protocol invariants, such as NEXRAD record/header sizes,
  archive magic bytes, message status codes, binary block masks, Uint16
  no-data value/byte order, timestamp grammars, and safe-filename rules.
- Mathematical and physical invariants, such as metres per kilometre, Earth
  radius, Web Mercator radius, unit conversions, covariance equations,
  standard-atmosphere equations, and transform implementations.
- Public schema requirements and route contracts, such as required alert
  fields, HTTP status semantics, and route parameter validation.
- Derived values, such as an affine transform derived from configured bounds
  and shape, grid rows derived from height/tile size, and a worker count
  bounded by available CPUs and configured limits.
- Safety and orchestration invariants, including staged readiness order,
  atomic publication, path-containment checks, non-daemonic NEXRAD parser
  ownership, signal handling, and “do not advance on failed cycle” rules.
- Algorithms and registries that execute trusted behavior. Config may select
  a named transform or statistic, but may not contain executable expressions,
  imports, callbacks, or arbitrary formula strings.
- Package metadata. `package.json` remains the single version source;
  configuration and user-agent strings interpolate that version instead of
  copying `2.7.0`.

## Current-state findings

The configuration is currently split between:

- `config/kalman.yaml`, which is only partially authoritative because Python
  dataclass defaults and YAML fallback values duplicate it.
- Python functions and module constants named `config.py`, many of which are
  still source code rather than external configuration.
- `src/EWMRS/colormaps.json` and `src/EWMRS/mappings.json`, which are already
  data files but live below `src/` and are resolved independently by Python
  and Node.
- CLI defaults, environment fallbacks, function defaults, constructor
  defaults, and inline literals spread across the pipeline.
- Separately maintained Python and Node product/directory mappings.

The most important drift risks found in the baseline are:

- The 28-entry MRMS ingest catalog, 12-entry readiness list, three detection
  modifiers, derived integration membership, and EWMRS render membership are
  defined through separate functions.
- MRMS integration output/statistic definitions are separate from the ingest
  products they require.
- EWMRS product identity is repeated in Python render definitions, Python
  filesystem paths, Node `PRODUCT_MAPPING`, and Node `GUI_SUBDIRS`.
- GOES channel identity is repeated in ingest specs, render layers, RGB
  recipes, paths, and API mappings.
- RAP integration products, RAP Uint16 render layers, Python colormap mapping,
  and `mappings.json` do not share an authority.
- `src/EWMRS/mappings.json` exposes
  `RAP_BestLiftedIndex_180_0mbAGL`, but the Python RAP render catalog does not
  produce that layer.
- `TrackingConfig.max_prediction_time_minutes` is `6.0` in the dataclass and
  YAML but uses `10.0` as the loader fallback.
- `KalmanConfig.from_yaml()` exists, but the production tracker does not
  clearly inject the loaded Kalman filter parameters, allowing code defaults
  to remain effective.
- Detection threshold defaults (`37.5`, `0.001`, `10.0`) are copied through
  the CLI, EdgeWARN pipeline, detection entrypoints, and gate mapper.
- Lineage overlap is `0.15` in the detector but `0.10` in `CellTracker`.
  These may be intentionally distinct policies; configuration must give them
  distinct names rather than accidentally unifying them.
- Base-directory logic and directory names are independently implemented by
  Python, the EdgeWARN API, and the EWMRS API.
- EdgeWARN API version strings and the NEXRAD weather API user agent copy the
  package version instead of reading `package.json`.
- WPC cleanup searches `surface_analysis_*.geojson` while generated artifacts
  are named `wpc_sfc_*`; migration tests must expose and resolve this existing
  mismatch separately from value-preserving extraction.

## Target configuration tree

Use YAML for operator-edited settings and product catalogs, JSON for the
existing large colormap payload and JSON Schema files. Add a maintained YAML
parser to Node rather than introducing parallel JSON-only copies of shared
settings.

```text
config/
├── paths.yaml
├── runtime.yaml
├── api.yaml
├── detection.yaml
├── integration.yaml
├── kalman.yaml
├── ctam/
│   ├── modules.yaml
│   ├── morphowind.yaml
│   └── stormcast.yaml
├── ingest/
│   ├── sources.yaml
│   ├── metar.yaml
│   ├── nws.yaml
│   └── wpc.yaml
├── products/
│   ├── mrms.yaml
│   ├── goes.yaml
│   ├── rap.yaml
│   └── nexrad.yaml
├── render.yaml
├── colormaps.json
└── schema/
    ├── paths.schema.json
    ├── runtime.schema.json
    ├── api.schema.json
    ├── detection.schema.json
    ├── integration.schema.json
    ├── kalman.schema.json
    ├── ctam-modules.schema.json
    ├── morphowind.schema.json
    ├── stormcast.schema.json
    ├── ingest.schema.json
    ├── products.schema.json
    ├── render.schema.json
    └── colormaps.schema.json
```

Do not create generated copies of these files under `src/`. Python and Node
must resolve the repository configuration root through one loader contract.
Packaged/deployed installations must explicitly copy the `config/` tree.

## Canonical product model

Product files are the core of the extraction. A product entry receives a
stable, lowercase ID that is not an output directory or upstream filename.
References between files use that ID.

An MRMS entry should express this shape:

```yaml
schema_version: 1
products:
  - id: composite_reflectivity
    source:
      region: CONUS
      modifier: MergedReflectivityQCComposite_00.50
    storage:
      path_key: mrms.composite_reflectivity
    roles:
      ingest: true
      readiness: true
      detection: true
      integration: false
      render: true
    integration_outputs: []
    render:
      layer_id: comp_ref_qc
      output_prefix: CompRefQC
      colormap: Reflectivity
      range: [-10, 75]
    api:
      route_key: CompRefQC
```

The exact schema varies by family, but all product files must support:

- Stable ID and human-readable description.
- Upstream source selection fields.
- Logical path key, never a computed absolute path.
- Explicit role/phase membership.
- Product-specific freshness, search, or retention override only when it
  differs from the family default.
- Integration output names, statistic methods/percentiles, transforms, units,
  and precision where applicable.
- Render source variables/fallbacks, range, colormap, output prefix, and
  tiling/API exposure where applicable.
- Cross-reference validation for colormap and transform/statistic registry
  names.
- `enabled` and optional deprecation metadata without removing the entry
  silently.

From this model:

- `get_mrms_modifiers()` becomes a catalog query for `roles.ingest`.
- `get_check_modifiers()` becomes a query for `roles.readiness`.
- Detection, integration, and EWMRS modifier lists become role queries.
- Integration dataset configuration is derived from
  `integration_outputs`, not a separate source list.
- Render layers, output directories, Node route mappings, and API allowlists
  are built from `render` and `api` fields.
- Tests reject a product that names an integration or render role without the
  required corresponding fields.

## Loader, validation, and precedence

### Shared behavior

Add:

- `src/util/config/loader.py` for typed Python loading, caching, path
  resolution, schema validation, and domain accessors.
- `src/common/config/` dataclasses or immutable typed models for paths,
  runtime, ingest, products, detection, integration, CTAM, rendering, and API
  settings.
- `src/config/loader.js` for Node loading and the same schema validation.
- A `config/schema_version` check with explicit migration errors.

Load and validate configuration once in each root process before starting
threads, process pools, child workers, or HTTP listeners. Pass immutable
settings or a config-root argument to children. Do not repeatedly parse YAML
inside hot paths.

Both loaders must:

- Resolve `config/` relative to the repository/install root, not the current
  working directory.
- Accept an explicit `--config-dir` and `EDGEWARN_CONFIG_DIR`.
- Reject unknown keys by default so misspellings do not silently do nothing.
- Reject missing files/keys, duplicate product IDs, invalid ranges, invalid
  role combinations, unresolved path keys, unknown colormaps, and unknown
  transform/statistic names.
- Include filename plus dotted key/index in validation errors.
- Return immutable/deep-frozen data to prevent a worker from mutating global
  configuration.
- Expose sanitized effective configuration and provenance for diagnostics,
  excluding secrets.

### Precedence

Use this order, highest first:

1. Explicit CLI option.
2. Supported environment variable.
3. Value in the selected `config/` tree.

There is no fourth production fallback. The repository default configuration
contains the current values, so missing configuration is an early startup
error rather than an invitation to use a hidden literal.

Preserve existing aliases for one deprecation window:

- `--base_dir` and `--base-dir`.
- `EDGEWARN_BASE_DIR` for the EdgeWARN API and Python.
- `BASE_DIR` for the EWMRS API.
- Existing rate-limit, RAP-age, render-memory, tile-thread, GOES cleanup, and
  NEXRAD worker environment variables.

Document each alias and map it to one dotted key. Warn when deprecated names
are used, but do not change their precedence.

## Exhaustive source inventory and destination

The inventory below covers production Python and JavaScript at the audit
baseline. “Extract” includes values currently expressed as function defaults,
constructor defaults, inline conditions, and duplicated lists, not only
uppercase constants.

### Filesystem and artifact layout

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/util/file.py` | Default base directories, workspace fallback, `data`/`gui`/`wpc` roots, every MRMS/GOES/RAP/METAR/NWS/stormcell/cell/alert/NEXRAD directory and manifest/index/colormap location | `paths.yaml` |
| `src/EdgeWARN/api/config.js` | Base-dir fallback and all API data/index directory mappings and required-directory list | `paths.yaml` |
| `src/EWMRS/api/server.js` | Base-dir fallback, GUI root, and duplicated `GUI_SUBDIRS` | `paths.yaml` plus derived product catalog view |
| `src/EWMRS/api/routes/colormaps.js`, `src/EWMRS/api/routes/rap.js`, `src/EWMRS/pipeline.py` | Source-relative `colormaps.json` and `mappings.json` paths | Config-root resolver; `colormaps.json`; RAP mappings derived from `products/rap.yaml` |
| `src/EWMRS/colormaps.json` | Complete colormap thresholds/colors | Move unchanged to `config/colormaps.json` |
| `src/EWMRS/mappings.json` | RAP layer-to-colormap mapping | Remove as an independent authority; derive API response from `products/rap.yaml` |

`paths.yaml` stores relative path templates keyed by logical ID. The loader
joins them to the selected runtime base directory. It must reject absolute
artifact paths and `..` traversal in repository defaults.

### Product and data-source catalogs

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/common/ingest/mrms/config.py` | NOAA MRMS and GOES bucket names; all 28 MRMS region/modifier/output entries; 12 readiness entries; GLM and 16 ABI channel ingest specs; ABI source product; per-channel names/path keys/max-files | `ingest/sources.yaml`, `products/mrms.yaml`, `products/goes.yaml` |
| `src/common/ingest/mrms/main.py` | Three detection modifiers and derived integration/EWMRS memberships | Explicit `roles` in `products/mrms.yaml` |
| `src/EdgeWARN/process/integrate/config.py` | All MRMS output/statistic definitions and all RAP integration product definitions | MRMS outputs in `products/mrms.yaml`; RAP fields/derived products in `products/rap.yaml`; shared integration policy in `integration.yaml` |
| `src/EdgeWARN/process/integrate/core/integrator.py` | Complete ProbSevere source-field to output-field map | `integration.yaml` |
| `src/EdgeWARN/process/integrate/integrate_rap.py` | Names chosen from the trusted transform registry | Transform names in `products/rap.yaml`; implementations stay in code |
| `src/EWMRS/render/config.py` | Fifteen MRMS render layers, 16 GOES ABI layers, and six GOES RGB layers, including source variables, fallbacks, ranges, colormaps, transforms, prefixes, and paths | `products/mrms.yaml`, `products/goes.yaml` |
| `src/EWMRS/render/goes_rgb.py` | Reflectance-channel membership and complete RGB recipe catalog | `products/goes.yaml` |
| `src/EWMRS/rap/config.py` | Wind/thermodynamic pressure levels; full Uint16 RAP layer catalog; GRIB filters; variables/aliases; units; ranges; descriptions; output names; colormap rules | `products/rap.yaml` |
| `src/common/ingest/nexrad/config.py`, `grouping.py` | Source buckets/API, allowed VCPs, elevation dedup/range policy, canonical elevation bins/readiness IDs, and supported waveform policy | `ingest/sources.yaml`, `products/nexrad.yaml` |
| `src/EWMRS/render/nexrad.py` | VCP-to-elevation labels and NEXRAD variable-to-colormap mapping | `products/nexrad.yaml` |
| `src/EWMRS/api/routes/nexrad/validation.js` | Duplicated allowed NEXRAD product set | Derived from `products/nexrad.yaml`; regexes remain code |
| `src/EWMRS/api/routes/renders.js` | `PRODUCT_MAPPING` and duplicated tile-grid response defaults | Product catalogs and `render.yaml` |
| `src/EWMRS/api/server.js` | Duplicated GUI product directory list | Derived from all products with API/render exposure |
| `src/EWMRS/api/routes/wpc.js` | Supported WPC artifact types | `products` section of `ingest/wpc.yaml` |
| `src/EdgeWARN/process/detect/tools/alert_matcher.py` | Convective/flood event allowlist used for cell matching | `integration.yaml` |
| `src/common/ingest/nws/main.py` | NWS dropped-event blocklist | `ingest/nws.yaml` |
| `src/common/ingest/nws/zone_sync.py` | Zone-type catalog used by the maintenance sync | `ingest/nws.yaml` |
| `src/common/ingest/wpc/config.py` | WPC feature types and display styles | `ingest/wpc.yaml` |
| `src/common/ingest/wpc/converter.py` | Duplicated coded-front to GeoJSON feature-type mapping and output metadata labels | `ingest/wpc.yaml` |

The current MRMS catalog must be transcribed losslessly, including
EchoTop 18/30/50; all FLASH products; RQI; MESH; NLDN; precipitation/QPE;
low/mid AzShear; VIL/VIL density/VII; ProbSevere; RhoHV; PrecipFlag; RALA;
composite reflectivity; and reflectivity at 0, -5, and -15 °C.

The MRMS integration portion must preserve every current output and statistic:
reflectivity at 0/-5/-15 °C, NLDN, EchoTop 18/30/50, VIL, VIL density,
low/mid AzShear, precipitation rate, RALA, and VII, including every
max/percentile output name and percentile value. The migration test must also
make the currently implicit default statistic on the -15 °C reflectivity
entry explicit.

The RAP integration catalog must preserve pressure-level winds from 1000 to
100 mb in 25 mb increments, 10 m winds, 2 m temperature/dewpoint, freezing
level, and the currently configured derived values. The EWMRS RAP catalog must
preserve the separate display-layer set and scale ranges; it is not assumed to
be identical to integration needs.

### Remote ingest and selection policy

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/common/ingest/mrms/https_client.py` | MRMS 2D and ProbSevere base URLs and HTTP timeout | `ingest/sources.yaml` and `runtime.yaml` |
| `src/common/ingest/mrms/downloader.py`, `s3_common.py`, `s3_sync.py`, `s3_async.py` | Search-entry limits, GOES hour lookback, GLM one-minute window, per-product cleanup age/count, download concurrency policy, and decompression chunk size | `runtime.yaml` with product overrides in `products/goes.yaml` |
| `src/common/ingest/mrms/timestamp_utils.py` | MRMS nominal two-minute cadence and midpoint rounding/selection policy | `products/mrms.yaml` |
| `src/common/pipeline/coordinator.py`, `goes_readiness.py` | Max entries, GOES lookback, candidate count, and 20-minute source offset | `runtime.yaml` |
| `src/common/ingest/synoptic/config.py`, `downloader.py` | RAP bucket/path patterns, age/file limits, hourly lookback behavior, and environment alias | `ingest/sources.yaml`, `runtime.yaml` |
| `src/common/ingest/metar.py` | Station database and cycle URLs; request timeouts; user agent; station cache filename; CONUS bounds; lookback; rounding; retention | `ingest/metar.yaml` |
| `src/common/ingest/nws/main.py`, `registry.py`, `geomapper.py` | Alerts URL; user agent/contact; request/chunk settings; two-hour registry TTL; property drop list; geometry rounding/simplification | `ingest/nws.yaml` |
| `src/common/ingest/nws/zone_sync.py` | API URL templates, timeout, retries/backoff, worker count, pause, geometry precision, output path policy | `ingest/nws.yaml` maintenance section |
| `src/common/ingest/wpc/config.py`, `downloader.py`, `main.py` | Source URL, valid hours, source cadence, timeout, fallback-cycle count, file templates, and cleanup age | `ingest/wpc.yaml` |
| `src/common/ingest/nexrad/config.py`, `s3_chunks.py`, `s3_async.py`, `main.py` | Buckets, weather API URL/user agent/timeout/cache TTL, minimum volume chunks, volume candidate count and volumes/site | `ingest/sources.yaml`, `products/nexrad.yaml`, `runtime.yaml` |

TLS verification policy currently disabled in METAR/WPC code must be an
explicit boolean with a warning when false. Contact/user-agent values must not
contain a copied package version; the loader formats a configured template
using `package.json`.

### Detection, tracking, lineage, and alerts

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/util/io.py`, `src/EdgeWARN/pipeline.py`, `process/detect/detect.py`, `process/detect/main.py`, `tools/gatemapper.py` | Reflectivity threshold `37.5`, seed ratio `0.001`, drop offset `10`, processing bounds, and duplicated function/CLI defaults | `detection.yaml`, with CLI overrides |
| `tools/gatemapper.py` | Baseline floor/cap, adaptive 37.5/40/45/52 dBZ rules, crop padding, minimum retained gates, contour sampling steps/thresholds, coordinate precision, and `all_touched` policy | `detection.yaml` |
| `process/detect/morphology.py` | Minimum full-analysis pixels and contour-defect points | `detection.yaml` |
| `process/detect/tools/save.py` | Polygon/centroid precision, hail-core contour sampling step, and serialization choices that affect output geometry | `detection.yaml` |
| `process/detect/detect.py` | Detection executor worker count | `runtime.yaml` |
| `process/detect/track.py` | Tracker overlap, fallback scan interval, decay threshold/factor/floor, and diagnostic sample limits | `detection.yaml` and `runtime.yaml` |
| `process/detect/lineage/detector.py`, `lineage/buffer.py` | Lineage overlap, confirmations, pending limit, prune scans, scan interval, and buffer filename | `detection.yaml`, `paths.yaml`, `runtime.yaml` |
| `process/detect/kalman/config.py`, `config/kalman.yaml` | Process/measurement noise, tracking confidence/prediction/reacquisition, assignment gate/weights/method/covariance and assignment motion cutoffs | One validated `config/kalman.yaml` |
| `process/detect/kalman/confidence.py` | Confidence time penalty, motion-variance scale/floor, position-uncertainty threshold/scale/floor, and confidence-status display bands | `kalman.yaml` |
| `process/detect/kalman/filter.py` | Initial position uncertainty, reference origin if operational, innovation regularization, and direct gate defaults still copied from assignment configuration | `kalman.yaml`; matrix equations and unit conversions stay in code |
| `process/detect/kalman/assignment.py`, `state.py` | Near-stationary/implied-motion cutoffs, fallback interval, and reference origin if still operationally selectable | `kalman.yaml`; physical conversion constants stay in code |
| `process/detect/main.py` | Stormcell cleanup age and tracking fallback interval | `runtime.yaml` |
| `alerts/alert_manager.py`, `src/EdgeWARN/alerts/schema.py`, alert payload modules | Alert cleanup age, default severity, geometry precision | `runtime.yaml`, `integration.yaml`; required schema fields stay code |
| `api_integration/index_manager.py` | Resync update count and inactive-cell retention | `runtime.yaml` |

Do not flatten different overlap concepts into one setting. Use names such as
`tracking.lineage_overlap_ratio`, `lineage.event_overlap_ratio`, and
`lineage.spatial_query_overlap_ratio`, with descriptions and valid ranges.

### Integration and scientific policy

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `process/integrate/core/stats.py` | Output decimal precision and default statistic/percentile policy | `integration.yaml` |
| `process/integrate/core/integrator.py`, `geometry/cell_polygon.py`, and integration helpers | ProbSevere field map; coordinate-key precision; minimum fallback polygon size; duplicate-overlap and distance tolerance policies; chosen percentiles and buffers | `integration.yaml` |
| `process/integrate/azshear/constants.py`, `azshear/integration.py`, `azshear/metrics.py` | Buffer, low/mid thresholds, minimum gate count, maximum pair separation, five-entry history window, coordinate/output precision, p95 statistic, overlap/dedup tolerances, spacing multiplier/floor, and pairing/alignment policy | `integration.yaml` |
| `process/integrate/integrate_glm.py` | GLM spatial bin size and related matching/search policy | `integration.yaml` |
| `process/integrate/io/rap_files.py` | GRIB variable aliases and target pressure-level expectations currently repeated in fallback dataset scoring | Derive from `products/rap.yaml`; scoring mechanics stay code |
| `process/integrate/pipeline.py` | Enrichment concurrency cap/policy | `runtime.yaml` |
| `process/integrate/history.py` | No current configurable limits found; retain serialization mechanics in code | Intentional no-op |

Formula and statistic implementations remain trusted registries in Python.
Configuration may select `max`, `percentile`, `kelvin_to_celsius`, or another
registered name. It may not supply executable formulas. Existing derived RAP
formula strings must be replaced by named, tested derived-field
implementations during the extraction.

### CTAM MorphoWind

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `ctam/modules/MorphoWind/config.py` | All QLCS, microburst, collapse, environmental correction, and bookend thresholds/means/sigmas | `ctam/morphowind.yaml` |
| `ctam/modules/MorphoWind/morphowind.py` | History length, heavy-core minimum, lookback scans, collapse increments, notch bearing/motion/defect rules, partial-score multipliers, aspect fallback, bookend bonus, score weights/denominators, collapse risk floor, classification cutoff, and output precision | `ctam/morphowind.yaml` |
| `ctam/run.py`, module registries | Enabled cell/grid module IDs and deterministic execution order | `ctam/modules.yaml`; registration implementations stay code |

Preserve the one-line-per-threshold-dictionary formatting requirement when the
implementation touches EWMRS dictionaries; it does not constrain the YAML
layout.

### CTAM StormCast

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `ctam/modules/StormCast/core/config.py` | Pressure levels/heights, Gaussian activation parameters, base/shallow/mature blend weights, smoothing window, Bunkers depth/deviation parameters, Kalman/uncertainty parameters, lead times, reliability horizon, and velocity limits | `ctam/stormcast.yaml` |
| `core/blending.py` | Smoothing method/alpha/window/polyorder; maturity, depth, shear, and stratiform cutoffs; weight adjustments and floors | `ctam/stormcast.yaml` |
| `core/diagnostics.py` | Default shear levels, storm-height pressure-level selection thresholds, shallow cap, and raw Bunkers level/deviation choices | `ctam/stormcast.yaml` |
| `core/core.py`, `core/forecast.py`, `core/uncertainty.py`, `core/types.py`, `core/kalman.py` | Confidence-to-chi-square map, contour points, minimum radius, expansion factor, coordinate/output precision, default echo-top/history values, history-based noise scaling, and initial covariance values | `ctam/stormcast.yaml` |
| `ctam/modules/StormCast/__init__.py` | Alert cadence, expiry, MorphoWind severity cutoff, alert type, polygon/vector forecast cutoffs, and reference origin if operational | `ctam/stormcast.yaml` |

Standard-atmosphere and covariance equations stay in Python. Pressure levels,
weights, thresholds, and uncertainty coefficients move.

### Rendering and presentation

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/EWMRS/pipeline.py` | Web Mercator/GOES domain bounds and output shape; GUI cleanup ages; cleanup minimum interval; source freshness; render/process worker budgets; reserve memory; NEXRAD render poll/workers/retention; tile-index cache size | `render.yaml`, `runtime.yaml` |
| `src/EWMRS/render/config.py` | Tile size/grid and layer definitions | `render.yaml` and product catalogs |
| `src/EWMRS/render/render.py` | Tile thread default/override and colormap cache size | `runtime.yaml` |
| `src/EWMRS/render/tools.py` | Deprecated duplicate fixed render bounds | Remove and derive from `render.yaml`; CRS IDs and timestamp/file-format parsing remain code |
| `src/EWMRS/render/goes_rgb.py` | Terminator blend angles, solar cache size, file offset, gamma/normalization and RGB recipe values | `products/goes.yaml`, `render.yaml`, `runtime.yaml` |
| `src/EWMRS/rap/uint16_pipeline.py` | Number of retained RAP timestamps and force behavior default | `runtime.yaml`, `render.yaml` |
| `src/EWMRS/render/nexrad.py` | Product colormaps and VCP sweep labels | `products/nexrad.yaml` |

Grid rows and columns should be derived and validated from configured shape
and tile size. Preserve the current 350 px tiles over 3500×7000 output, but
remove separately editable `10×20` copies.

### Runtime scheduling, retention, and resources

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/run.py`, `src/util/runtime/cycle.py`, `goes.py` | GOES poll/wait intervals, optional ingest pause, render max entries, and cycle polling cadence | `runtime.yaml` |
| `src/util/runtime/background.py` | METAR 5-minute boundary, NWS 120-second poll, WPC 15-minute boundary, GOES poll, NEXRAD restart backoff, and interruptible-sleep granularity where operational | `runtime.yaml` |
| `src/util/runtime/processes.py` | Graceful join and forced-stop timeouts | `runtime.yaml` |
| `src/process_historical.py` | One-minute step, one-second throttle, historical bounds/output defaults | `runtime.yaml` plus CLI overrides |
| `src/EdgeWARN/schedule/scheduler.py` | MRMS search entries/lookback, worker cap, poll cadence, and slow-operation logging threshold | `runtime.yaml` |
| `src/EdgeWARN/pipeline.py` | Historical ingest directories retained per family | Product/path catalog plus `runtime.yaml` |
| `src/util/file.py` | Generic cleanup age and count defaults | `runtime.yaml` |
| `src/util/performance.py` | Performance tracker enablement and future thresholds | `runtime.yaml` with existing env alias |
| `src/common/ingest/nexrad/pipeline/__init__.py` | Scan/completion intervals and volume candidate defaults | `runtime.yaml` with CLI overrides |
| `src/common/ingest/nexrad/service.py`, `worker_pool.py` | Site/chunk concurrency, parse checkpoint, prefetch, pool size, recycle interval, timeout, and memory behavior | `runtime.yaml` with existing env aliases |
| `src/common/ingest/nexrad/writer.py` | Scan/elevation directories retained and stale-manifest age | `runtime.yaml` |
| `src/EWMRS/pipeline.py` | Render memory budget/reserve, thread-library caps, adaptive process limit, NEXRAD workers and source age | `runtime.yaml` |

Hardware-derived defaults may use a named strategy such as
`render.workers.strategy: adaptive_memory`. Its tunable caps, reserves, and
minimums belong in config; the CPU/memory calculation remains code.

### Explicitly classified source constants

The mechanical audit also finds the following files. They do not create
additional configuration authorities after the migrations above:

| Source | Classification |
| --- | --- |
| `src/common/ingest/mrms/parse.py`, `utils.py` | GOES/MRMS bucket and timestamp grammar, day-of-year conversion, NetCDF merge mechanics, and function parameters supplied by configured callers are protocol/implementation code. |
| `src/common/ingest/nexrad/parser.py`, `stream.py` | Record sizes, header lengths, archive magic, message/block/status codes, and stream overlap are Level-II format invariants. Elevation/product policy moves to config. |
| `src/common/ingest/nexrad/weather_api.py` | Consumes configured URL, timeout, user agent, and cache TTL; parsing and cache mechanics remain code. |
| `src/common/ingest/nexrad/worker.py` | Process memory cleanup and partial-volume parsing mechanics remain code; its pool/recycle/timeout settings move from the parent modules. |
| `src/common/ingest/wpc/parser.py` | Coded surface bulletin keywords and coordinate grammar are protocol vocabulary; feature enablement/styles move to config. |
| `src/EdgeWARN/process/detect/lineage/spatial.py` | Antimeridian handling, polygon minimum-point rules, floating-point degeneracy epsilon, and deterministic parent/child tie-breaking are geometry/safety mechanics. The caller's overlap policies move to config. |
| `src/EdgeWARN/process/integrate/grid_index.py` | Regular-grid floating-point tolerance and index arithmetic are numerical safety mechanics; processing domain and scientific matching tolerances move to config. |
| `src/EWMRS/render/tools.py` | CRS definitions, file timestamp regexes, and transformation mechanics remain code; duplicate domain bounds are removed as noted above. |
| `src/util/handler.py` | GRIB time-coordinate decoding is a format rule. |
| `src/util/release.py` | Package-version discovery stays code and reads `package.json`. |
| `src/util/runtime/timing.py` | Interruptible-sleep arithmetic stays code; operator-facing cadences and retry intervals come from `runtime.yaml`. |

Small numerical epsilons used solely to prevent division by zero, repair
invalid polygons, regularize a singular matrix, or clamp a probability are
algorithm safety constants unless they materially alter a configurable
scientific decision boundary. Named scientific regularization values already
present in `kalman.yaml` remain configurable; local machine-epsilon guards do
not.

### Node API services

| Current source | Values to extract | Destination |
| --- | --- | --- |
| `src/EdgeWARN/api/server.js` | Ports/host, rate windows/maxima, CORS origins/methods/headers/credentials, trust-proxy policy, HSTS/CSP policy, JSON body limit, health-check limiter bypass, compression policy, cluster worker cap, and logging mode | `api.yaml` |
| `src/EdgeWARN/api/config.js` | Debug/default port and path mappings | `api.yaml`, `paths.yaml` |
| `src/EdgeWARN/api/utils/fileReader.js` | Cache entries, default/index TTLs, per-worker byte budget | `api.yaml` |
| `src/EdgeWARN/api/routes/v2/data/metar.js`, `src/EdgeWARN/api/routes/v2/features/alerts.js`, `cells.js`, `timestamps.js` | Route-specific response cache-control TTLs, currently 5/60/3600 seconds | `api.yaml` |
| `src/EdgeWARN/api/routes/v2/index.js`, `server.js` | Copied package version | Read `package.json`; production redaction mode in `api.yaml` |
| `src/EWMRS/api/server.js` | Ports, host, rate windows/maxima, CORS, logging, compression, list limit, and cluster/server behavior | `api.yaml` |
| `src/EWMRS/api/routes/nexrad/index.js` | NEXRAD index/artifact response cache-control TTLs | `api.yaml` |
| `src/EWMRS/api/routes/renders.js` | Tile grid and product mapping | `render.yaml` and product catalogs |
| `src/EWMRS/api/routes/rap.js` | Mapping file location | Generated catalog view |

Route names, HTTP status meanings, path-validation regexes, and error schema
remain API contract code. Security settings must be validated conservatively:
production cannot silently broaden CORS or proxy trust because a config key is
missing.

## Migration phases

### Phase 0: Freeze and characterize current behavior

1. Add characterization tests that serialize the effective values returned by
   current MRMS/GOES/integration/render/RAP/NEXRAD config functions.
2. Snapshot current path keys, API mappings, colormap keys, CLI defaults,
   environment aliases, timers, retention, and scientific parameters.
3. Add explicit tests for the known drift points before deciding whether each
   is a bug or an intentional distinction.
4. Record config-sensitive benchmark baselines for ingest selection,
   detection, integration, EWMRS rendering, and NEXRAD parsing.

This phase is value-preserving. Do not combine extraction with scientific
retuning.

### Phase 1: Build config loading and validation

1. Add Python and Node dependencies for YAML and JSON Schema validation.
2. Implement config-root discovery, caching, immutability, provenance, schema
   versioning, and dotted-key errors.
3. Add `--config-dir` and `EDGEWARN_CONFIG_DIR` to Python and both Node
   entrypoints.
4. Implement CLI/environment overlay adapters without putting environment
   parsing inside domain modules.
5. Add a `validate-config` command usable in CI and deployment checks.

### Phase 2: Centralize paths and sources

1. Migrate `util/file.py` to construct paths from `paths.yaml`.
2. Make both Node APIs consume the same logical path layout.
3. Move endpoints, buckets, file templates, user agents, timeouts, and TLS
   policy to the ingest files.
4. Keep legacy base-directory names working through the overlay layer.
5. Test the same config against POSIX and Windows path construction.

### Phase 3: Create canonical product catalogs

1. Transcribe MRMS products and their role memberships first.
2. Generate all modifier/readiness/integration/render queries from the MRMS
   catalog and delete the old lists.
3. Transcribe GOES GLM/ABI and RGB recipes, then derive ingest, render, path,
   and API views.
4. Transcribe RAP integration fields and EWMRS layers, then generate the RAP
   API mapping response.
5. Transcribe NEXRAD VCP/elevation/product/render/API policy while leaving
   binary format definitions in code.
6. Move colormaps and cross-validate every configured colormap reference.
7. Add uniqueness and coverage tests for upstream modifier, output name,
   directory, API route key, and phase membership.

MRMS is the gate for this phase: no other family should copy a new catalog
pattern until MRMS proves that a single entry can drive ingest, readiness,
integration, rendering, and API behavior without circular imports.

### Phase 4: Extract scientific settings

1. Migrate detection and gate-mapping thresholds and thread one typed
   `DetectionConfig` through the pipeline.
2. Make `kalman.yaml` fully authoritative; remove dataclass and `.get()`
   fallback duplicates and inject the loaded filter configuration.
3. Migrate integration maps/statistics/AzShear/GLM policy.
4. Migrate MorphoWind and StormCast values, including inline scoring and
   maturity thresholds that are currently outside their `config.py` files.
5. Replace config formula strings with names from trusted registries.
6. Run output-equivalence tests against fixed meteorological fixtures at each
   step.

### Phase 5: Extract runtime and API settings

1. Migrate polling, readiness waits, search windows, retries, retention,
   cleanup, caches, worker limits, and memory budgets.
2. Remove duplicated function/constructor defaults; callers pass typed config
   or access one injected application settings object.
3. Migrate both API services to `api.yaml`, retaining environment and CLI
   compatibility.
4. Derive version strings from `package.json`.
5. Add effective-config summaries to startup logs and health diagnostics.

### Phase 6: Remove fallbacks and enforce the boundary

1. Delete obsolete source `config.py` catalogs or reduce them to typed adapter
   modules with no embedded values.
2. Delete `src/EWMRS/mappings.json` and move
   `src/EWMRS/colormaps.json` after all consumers use `config/`.
3. Search production source for every migrated literal/list and remove
   duplicates.
4. Add a CI audit that flags:
   - Production `http://` or `https://` literals outside the allowlist.
   - New `os.environ`/`process.env` reads outside overlay loaders.
   - Product/event/pressure-level catalogs outside typed registries.
   - Unapproved polling, timeout, retention, cache, or worker numeric
     literals.
   - Direct source-relative access to files in `config/`.
5. Maintain `config/code_constants_allowlist.yaml` only if the audit needs a
   machine-readable list. Each entry must name the source symbol, category,
   and reason; it must not become a second runtime configuration source.

## Validation plan

### Schema and loader tests

- Valid repository defaults load identically in Python and Node.
- Missing files, unknown keys, wrong types, invalid enum values, duplicate
  IDs, non-finite numbers, inverted ranges, negative intervals, and unsafe
  paths fail before service startup.
- Every config file has `schema_version: 1`.
- CLI and environment precedence is tested for each supported override.
- Config caches are process-local, immutable, and resettable in tests.
- Error messages include the file and exact dotted key.

### Product consistency tests

- Every MRMS readiness/detection/integration/render product is also ingestible.
- Every integration output has an available source product and unique output
  key.
- Every render/API product has one output directory/prefix and a valid
  colormap.
- Every GOES RGB recipe references defined ABI channels.
- Every RAP mapping corresponds to an actually produced layer.
- Every NEXRAD API product corresponds to a renderer/writer product.
- Derived Node and Python views contain the same IDs and path names.
- Product order is explicit where it affects readiness, output, or tests; no
  loader relies on unordered map iteration.

### Behavioral regression tests

- MRMS staged readiness order and required groups are unchanged.
- Detection masks/cells, tracking assignments/lineage, integration statistics,
  CTAM outputs, alerts, and API indexes match the baseline fixtures.
- MRMS, GOES, RAP, WPC, METAR, NWS, and NEXRAD selection/retention behavior
  matches the baseline at time boundaries.
- EWMRS PNG/tile metadata, colormap output, RAP Uint16 payloads, and API route
  results match the baseline.
- Node security middleware and rate-limit behavior retain existing defaults.
- Historical and real-time CLI defaults retain existing behavior.

### Operational tests

- Services start from a working directory outside the repository when given a
  config root and runtime base directory.
- A worker child receives the same effective configuration as its parent.
- Invalid config never leaves partial directory creation, listeners, or worker
  pools.
- Windows and POSIX default/override paths remain supported.
- Config reload is explicitly unsupported initially; changing files requires
  a restart and is reported as such.
- Startup diagnostics show config version, config root, source provenance,
  enabled product counts, and active overrides without exposing secrets.

## Documentation updates

Update:

- `INSTALLATION.md` with config discovery, `--config-dir`, environment
  precedence, validation, and deployment copying.
- `README.md` and `docs/core/README.md` with the target config tree and removal
  of source-local colormap/mapping files.
- Ingest/detection/integration/CTAM architecture docs with the authoritative
  product and scientific config files.
- `docs/api/api_endpoints.md` and
  `docs/api/ewmrs_api_endpoints.md` with configured product/mapping behavior
  and current endpoint paths.
- Environment variable tables, including aliases and deprecation notices.
- A new `docs/core/configuration.md` containing key descriptions, units,
  valid ranges, restart requirements, examples, and ownership.

Generated reference documentation may be built from JSON Schema descriptions,
but the schemas and checked-in config remain authoritative.

## Rollout and compatibility

- Ship repository defaults with values matching the audit baseline.
- Require an explicit opt-in only for custom config roots, not for the
  repository defaults.
- Keep public Python accessor function names temporarily as adapters where
  external imports may exist, but have them query typed loaded catalogs.
- Keep current CLI/environment names for one deprecation window.
- Do not support mixed old/new catalog authorities. Migrate one domain
  atomically and delete its source literals in the same change.
- Treat scientific output changes, source additions/removals, and retention
  changes as separate reviewed commits after extraction.
- Add `schema_version` migrations before changing config structure in a later
  release.

## Acceptance checklist

- [ ] `config/` contains every target file and passes both loaders.
- [ ] MRMS ingest, readiness, detection, integration, and render lists derive
  from `products/mrms.yaml`.
- [ ] GOES ingest/render/RGB/API views derive from `products/goes.yaml`.
- [ ] RAP integration/render/API views derive from `products/rap.yaml`.
- [ ] NEXRAD VCP/elevation/product/render/API policy derives from
  `products/nexrad.yaml`.
- [ ] Python and Node share path and API settings without duplicated defaults.
- [ ] `kalman.yaml` is actually injected and has no source fallback copies.
- [ ] All detection, integration, MorphoWind, and StormCast empirical
  parameters in this inventory are externalized.
- [ ] All endpoints, timers, retries, retention, caches, workers, memory
  budgets, and server policy in this inventory are externalized.
- [ ] Source-local colormaps and RAP mappings are removed.
- [ ] Package version is read from `package.json`.
- [ ] Intentional code constants are documented and covered by the audit.
- [ ] Characterization, schema, cross-language, product-consistency,
  behavioral, API, and operational tests pass.
- [ ] Documentation is synchronized.
- [ ] A final literal/catalog audit finds no unclassified configuration in
  production source.

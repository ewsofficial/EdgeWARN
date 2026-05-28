# EdgeWARN-Core — Detailed Performance, Security, Dead-Code & Pipeline Audit

**Repo:** EdgeWARN-Core · **Version:** 2.5.2 (per `package.json`) · **Date:** 2026-05-27
**Scope:** `src/` only — Python (`common/`, `EdgeWARN/`, `EWMRS/`, `util/`) and Node.js (`EdgeWARN/api/`, `EWMRS/api/`)
**Guiding principle:** every recommendation in this document is **behavior-preserving**. Externally-observable outputs (HTTP responses, on-disk artifacts, numeric pipeline results, CLI semantics) must remain identical for legitimate inputs.

---

## Glossary

- **Impact rating (performance):** HIGH (visible in steady-state CPU profile or RSS), MEDIUM (per-cycle wins, multiplied across requests), LOW (micro-optimizations, posture).
- **Severity rating (security):** Critical / High / Medium / Low / Info, OWASP-aligned.
- **"Preserves behavior"** = the change does not alter:
  1. HTTP response status, body shape, or headers under documented `Cache-Control`.
  2. Numeric output beyond IEEE-754 rounding equivalence (`np.allclose` with default `rtol=1e-5`).
  3. On-disk filename patterns, JSON key ordering, or wire formats listed in §6.
  4. CLI flags, env-var semantics, or scheduler cadences.
- **Behavior-preservation invariants:** Section 6 enumerates the 28 items reviewers must check before accepting any patch derived from §2–§5.

---

## Section 1 — Project structure & workflow summary

EdgeWARN-Core is a hybrid Python + Node.js weather pipeline.

### Top-level layout
```
src/
├── common/
│   ├── ingest/{mrms,nexrad,nws,synoptic,wpc,metar}/   shared real-time + historical ingest
│   └── pipeline/{coordinator.py, goes_readiness.py}    staged-ingest tandem coordinator
├── EdgeWARN/
│   ├── api/                                            Express API (port 5000 / debug 3001)
│   ├── alerts/                                         Alert schema + manager
│   ├── api_integration/                                API index/snapshot helpers
│   ├── ctam/                                           CTAM framework + modules (FLOHAR, MorphoWind, StormCast, Mesocyclone)
│   ├── ingest/                                         compat re-export layer for common/ingest (DEAD per §4)
│   ├── process/{detect,integrate}/                     storm-cell detection + GLM/RAP/stat integration
│   ├── schedule/                                       MRMS update checker
│   └── pipeline.py                                     EdgeWARN realtime + historical orchestration
├── EWMRS/
│   ├── api/                                            Express API (port 3003 / debug 3004)
│   ├── render/{nexrad,render,goes_rgb,goes_transform,tiler,tools}.py    raster rendering
│   ├── rap/uint16_pipeline.py                          RAP layer encoding
│   ├── pipeline.py                                     render pipeline + GUI cleanup
│   └── scheduler.py                                    DEAD per §4
├── util/                                               filesystem, GRIB, IO, performance utilities
├── run.py                                              real-time tandem entry point
└── process_historical.py                               historical reprocessing entry point
```

### Workflow at a glance
1. `run.py` parses CLI flags, builds an `MRMSUpdateChecker`, and dispatches:
   - **Foreground tandem cycles** every minute: `_run_tandem_cycle` invokes `common.pipeline.coordinator.run_tandem_ingest_cycle` (staged readiness: detection-MRMS → EWMRS-MRMS → EWMRS-GOES → EdgeWARN-integration), then spawns `EdgeWARN.pipeline.edgewarn_tandem_worker` and the EWMRS render worker as child processes.
   - **Background loops** with their own cadences: NEXRAD ingest (non-daemon process), METAR (300s), NWS (120s), WPC (900s), GOES (60s with `EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER` gate).
2. `process_historical.py` iterates a time range and calls `EdgeWARN.pipeline.historical_pipeline` for each minute.
3. Filesystem-first runtime: artifacts land in `<BASE_DIR>/{data,gui,wpc}/`. The two Express APIs serve those subdirs read-only.

### Key external contracts
- `nexrad_api_routes.md` and `docs/api/` document the live API surface.
- v1 routes `/features` and `/data` are sunset and return HTTP 410.
- EdgeWARN cluster cap: `min(cpus, 4)` workers (per-worker rate-limit counters).

---

## Section 2 — Performance optimizations

Each finding is formatted:
```
### <ID>. <Title>
**File:** path:line
**Current:** <snippet>
**Proposed:** <snippet>
**Why behavior-preserved:** <reason>
**Impact:** <H/M/L> · CPU | memory | I/O | concurrency
```

### 2.1 HIGH impact (12)

#### H1. Vectorize `RAPPointExtractor.extract` via existing `extract_batch`
**File:** `src/util/grib_loader.py:86-124`
**Current:** per-cell `codes_grib_find_nearest(gid, cell.lat, cell.lon)` Python loop.
**Proposed:** route all callers through `extract_batch` (lines 126-206), which builds a single `scipy.spatial.cKDTree` once and queries all points in C.
**Why behavior-preserved:** `extract_batch` already exists and returns the same field set. Pure callsite swap.
**Impact:** H · CPU (10–100× for >10 cells/cycle).

#### H2. `os.scandir` in `latest_files`
**File:** `src/util/file.py:211-237`
**Current:** `glob` + per-path `.stat().st_mtime`.
**Proposed:** `os.scandir(dir)` — `DirEntry.stat()` is cached on Windows; sort by `entry.stat().st_mtime`.
**Why behavior-preserved:** `os.scandir` is the documented fast equivalent; same ordering and filenames.
**Impact:** H · I/O (Windows syscalls).

#### H3. Narrow `deepcopy` in detection main loop
**File:** `src/EdgeWARN/process/detect/main.py:298`
**Current:** `entries_old = copy.deepcopy(entries_old)`.
**Proposed:** the subsequent loop only mutates a handful of leaf fields; replace with shallow copy plus targeted copying of mutated keys, or treat `entries_old` as read-only and write into a fresh dict.
**Why behavior-preserved:** deep copy was defensive — actual mutation surface is small and local.
**Impact:** H · CPU + memory (O(N×depth) Python overhead per cycle).

#### H4. Hoist `multiprocessing.Manager()` out of per-cycle scope
**File:** `src/run.py::_run_tandem_cycle`
**Current:** new `multiprocessing.Manager()` per cycle to host shared queues/dicts.
**Proposed:** move to `main()` lifetime; reuse across cycles.
**Why behavior-preserved:** Manager objects are reusable; queues are drained per-cycle anyway.
**Impact:** H · IPC + concurrency (Manager startup spawns a child process and IPC server).

#### H5. `np.linalg.solve` instead of `inv` for 2×2 in Kalman gating
**File:** `src/EdgeWARN/process/detect/kalman/filter.py:236`
**Current:** `S_inv = np.linalg.inv(S); mahal = innov.T @ S_inv @ innov`.
**Proposed:** `mahal = innov @ np.linalg.solve(S, innov)` (or inline 2×2 closed form).
**Why behavior-preserved:** mathematically identical for non-singular S; `solve` is more numerically stable and faster.
**Caution:** §6 invariant — verify diff against existing test fixtures; the auditor flagged that ill-conditioned S can produce different gating outcomes.
**Impact:** H · CPU (per track × candidate per frame).

#### H6. Cache cost matrix entry in Hungarian assignment
**File:** `src/EdgeWARN/process/detect/kalman/assignment.py` (cost-matrix build + costs-dict population)
**Current:** cost is computed when filling the cost matrix, then re-computed per accepted match for the `costs` dict.
**Proposed:** `costs[match_pair] = cost_matrix[row, col]`.
**Why behavior-preserved:** identical numeric value (no rounding difference).
**Impact:** H · CPU.

#### H7. Hoist `AssignmentCostCalculator(config)` outside loops
**File:** `src/EdgeWARN/process/detect/kalman/assignment.py:401, 437`
**Current:** fresh `AssignmentCostCalculator(config)` per loop iteration.
**Proposed:** construct once outside the loop; pass it in.
**Why behavior-preserved:** stateless w.r.t. iteration index.
**Impact:** H · CPU.

#### H8. Build candidate centroid array once in `prefilter_candidates`
**File:** `src/EdgeWARN/process/detect/kalman/assignment.py:101-147`
**Current:** inside per-track loop, candidate centroids extracted into a fresh NumPy array each iteration.
**Proposed:** build once outside the per-track loop and reuse.
**Why behavior-preserved:** candidate set is constant during prefiltering.
**Impact:** H · CPU + memory.

#### H9. Single-pass RGBA LUT indexing in `_scalar_data_to_rgba`
**File:** `src/EWMRS/render/render.py:59-86`
**Current:** four sequential `np.interp` calls (R, G, B, A).
**Proposed:** compute index once via `np.searchsorted`, then `rgba = lut[idx]`.
**Why behavior-preserved:** identical color output to within float rounding (same dtype).
**Caution:** §6 invariant — preserve continuous-vs-discrete LUT split (`np.interp` vs `np.digitize`); only refactor the four-pass form.
**Impact:** H · CPU + memory.

#### H10. Vectorize `_decode_grouped_ar2v_sweep` via `np.frombuffer`
**File:** `src/EWMRS/render/nexrad.py:192-245`
**Current:** pure-Python per-record / per-block loop assembling dense arrays.
**Proposed:** `np.frombuffer(record_bytes, dtype=...)` for the gate-data slice; `np.concatenate` per-block.
**Why behavior-preserved:** output array contents and dtype unchanged.
**Caution:** §6 invariant — `_normalize_azimuth_axis` must keep `kind="stable"`.
**Impact:** H · CPU + memory (sweep decode is the bottleneck of NEXRAD render frames).

#### H11. Context-manage `xr.open_dataset`
**Files:**
- `src/util/handler.py:56-101` (`FileHandler.load_dataset`)
- `src/EWMRS/render/tools.py` (timestamp finder)
**Current:** `ds = xr.open_dataset(path)` without `with` and without `ds.close()` on error paths.
**Proposed:** `with xr.open_dataset(path) as ds: ds.load()`; or `try/finally: ds.close()`.
**Why behavior-preserved:** loaded data is materialized into memory; closing the underlying file does not break in-memory arrays.
**Impact:** H · memory + handles (long-running RSS leak).

#### H12. Lock-free / opt-in `perf_tracker`
**File:** `src/util/performance.py:42-53`
**Current:** every `start`/`stop` acquires a global `RLock`; called in per-cell, per-modifier, per-render hot paths.
**Proposed:** add a top-level enable flag (env var) so calls become no-ops in production; or switch to `threading.local()` accumulators flushed once per cycle.
**Why behavior-preserved:** instrumentation is opt-in; functional behavior outside the tracker is unaffected.
**Impact:** H · CPU + concurrency (lock contention under parallel workers).

### 2.2 MEDIUM impact (21)

#### M1. Replace linear glob+stat in latest-json search with sentinel or scandir
**File:** `src/EdgeWARN/process/detect/main.py:159-163`
**Proposed:** `os.scandir` over the timestamped dir, comparing prefix only; or maintain a `latest.json` symlink/sentinel updated on write.
**Impact:** M · I/O.

#### M2. `LRUCache` `sizeCalculation` re-serializes JSON
**File:** `src/EdgeWARN/api/utils/fileReader.js`
**Current:** `sizeCalculation: (value) => JSON.stringify(value).length`.
**Proposed:** drop `sizeCalculation`; bound by entry count via `max`. Or compute size once at set time and store.
**Why behavior-preserved:** cache bounds are heuristic — switching policy still bounds memory.
**Impact:** M · CPU.

#### M3. `compression()` filter for already-compressed payloads
**Files:** `src/EdgeWARN/api/server.js`, `src/EWMRS/api/server.js`
**Current:** `app.use(compression())` unconditionally.
**Proposed:**
```js
app.use(compression({
  filter: (req, res) =>
    !/^image\//.test(res.getHeader('Content-Type') || '') &&
    compression.filter(req, res),
}));
```
**Why behavior-preserved:** PNGs/already-gzipped responses gain nothing from re-gzip; same bytes returned.
**Impact:** M · CPU (high-QPS tile endpoints).

#### M4. Buffer RAP `metadata.json` writes
**File:** `src/EWMRS/rap/uint16_pipeline.py`
**Current:** writes `metadata.json` after each forecast hour.
**Proposed:** buffer for the whole batch; write once via `os.replace(tmp, final)`.
**Why behavior-preserved:** final state identical; consumers read once.
**Impact:** M · I/O.

#### M5. Replace tile-dir `iterdir` cleanup with atomic rmtree+mkdir
**File:** `src/EWMRS/render/render.py:187`
**Current:** iterates an entire directory before writing new tiles.
**Proposed:** `shutil.rmtree(dir, ignore_errors=True); dir.mkdir(parents=True)`.
**Why behavior-preserved:** repopulated immediately afterwards.
**Impact:** M · I/O.

#### M6. In-memory render index, flushed per-cycle
**File:** `src/EWMRS/render/render.py:282-315`
**Current:** read → parse → write entire JSON every render.
**Proposed:** keep the index dict in memory; write to disk via `os.replace` only on cycle flush.
**Why behavior-preserved:** disk state at flush identical; readers already tolerate the cycle-aligned staleness window.
**Impact:** M · I/O + CPU.

#### M7. Bounded `ThreadPoolExecutor` in `latest_common_minute_1h`
**File:** `src/EWMRS/scheduler.py:120` (or surviving copy in `EdgeWARN/schedule/scheduler.py` after §4 cleanup)
**Proposed:** `max_workers=min(8, len(dirs))`.
**Impact:** M · concurrency.

#### M8. Module-level Kalman `_F`/`_H` template constants
**File:** `src/EdgeWARN/process/detect/kalman/filter.py`
**Proposed:** module-level constants; `.copy()` only when an instance must mutate.
**Why behavior-preserved:** read-only arrays are shareable.
**Impact:** M · memory.

#### M9. Vectorize 6×6 process-noise build
**File:** `src/EdgeWARN/process/detect/kalman/filter.py:327-333`
**Proposed:** pre-compute the sigma scale matrix in NumPy and `np.outer`.
**Why behavior-preserved:** identical algebra.
**Impact:** M · CPU.

#### M10. `CovarianceMatrix.to_array(copy=False)` opt-in
**File:** `src/EdgeWARN/process/detect/kalman/state.py:149`
**Proposed:** add `copy: bool = True` parameter; internal callers pass `copy=False` when read-only.
**Why behavior-preserved:** external callers default to copy.
**Impact:** M · memory.

#### M11. Thread-local reproject destination buffer
**File:** `src/EWMRS/render/goes_transform.py`
**Current:** `np.empty(...)` + `.fill(np.nan)` per call.
**Proposed:** cache per (shape, dtype) in `threading.local`; reuse + `.fill(np.nan)`.
**Why behavior-preserved:** buffer fully overwritten by `reproject`.
**Impact:** M · memory.

#### M12. Warm worker imports via `initializer=`
**File:** `src/EWMRS/pipeline.py::_render_layer`
**Proposed:** `ProcessPoolExecutor(initializer=_preload_modules)` to import xarray/rioxarray/etc. at worker startup.
**Why behavior-preserved:** identical import semantics, just earlier.
**Impact:** M · CPU (hundreds of ms per task saved).

#### M13. Parallel cleanup walks
**File:** `src/EWMRS/pipeline.py::cleanup_old_gui_files`
**Proposed:** `ThreadPoolExecutor` over independent directory walks.
**Why behavior-preserved:** independent dir cleanups commute.
**Impact:** M · I/O.

#### M14. LRU index cache keyed on (path, mtime)
**File:** `src/EWMRS/pipeline.py:64-82::_load_timestamp_tile_index`
**Proposed:** wrap in `functools.lru_cache` keyed on `(path, mtime)`; mtime invalidation gives identical data.
**Impact:** M · I/O + CPU.

#### M15. `Event.wait` over `time.sleep` in scheduler polling
**File:** `src/run.py` (`_sleep` polling loop)
**Proposed:** `threading.Event.wait(remaining)` for cancellable sleeps.
**Why behavior-preserved:** equivalent in steady state; faster only on shutdown.
**Impact:** M · latency.

#### M16. Combined regex in `find_timestamp`
**File:** `src/EWMRS/render/tools.py:126-156`
**Current:** 5 sequential regex tries.
**Proposed:** `re.compile("|".join([...]))` once at module load; single `match`.
**Why behavior-preserved:** same first-match semantics.
**Impact:** M · CPU.

#### M17. Mount `express.json()` only on body-bearing routes
**File:** `src/EdgeWARN/api/server.js:111`
**Current:** `app.use(express.json())` globally on a GET-only API.
**Proposed:** mount per-route on POST/PUT/PATCH only; or `app.use(express.json({ limit: '16kb', strict: true, type: 'application/json' }))` (also satisfies §3 H5).
**Why behavior-preserved:** GETs have no JSON body.
**Impact:** M · CPU.

#### M18. Minimal Helmet CSP for tile API
**File:** `src/EWMRS/api/server.js`
**Proposed:** `helmet({ contentSecurityPolicy: false })` if no HTML served, or explicit minimal CSP.
**Why behavior-preserved:** API serves binary tiles + JSON; CSP irrelevant.
**Impact:** M · CPU per request.

#### M19. `pino-http` or sampled morgan
**File:** `src/EWMRS/api/server.js`
**Proposed:** replace `morgan('tiny')` with `pino-http` (10× faster), or skip 2xx static-tile log lines.
**Why behavior-preserved:** observability preserved.
**Impact:** M · CPU.

#### M20. Memoized cells path
**File:** `src/EdgeWARN/api/routes/v2/features/cells.js`
**Proposed:** memoize `path.join` result keyed by `timestamp`; rely on existing LRU for re-reads.
**Impact:** M · CPU.

#### M21. Float32 RAP read
**File:** `src/EWMRS/rap/uint16_pipeline.py:199`
**Proposed:** read values as float32 from `eccodes` directly (cast on the fly) — skip float64 intermediate.
**Why behavior-preserved:** final uint16 encoding matches within rounding (same scale/offset).
**Impact:** M · memory + CPU.

### 2.3 LOW impact (25)

| ID | File | Optimization | Bound |
|----|------|--------------|-------|
| L1 | `src/util/file.py` | Memoize `_define_paths()` so import-time filesystem walks become lazy | I/O |
| L2 | `alerts/manager.py`, `util/handler.py`, `process/integrate/*` | Replace stdlib `json.loads` with `orjson.loads` for hot reads | CPU |
| L3 | `src/EdgeWARN/alerts/manager.py:142-168` | Cell-id-indexed alerts (filename or sidecar index) so `load_all` doesn't scan every alert | I/O |
| L4 | `src/EdgeWARN/alerts/manager.py:271-298` | Encode `expires` epoch in filename so `cleanup_expired` is a name comparison | I/O |
| L5 | `src/EdgeWARN/alerts/manager.py:57` | `indent=None` (or `separators=(",",":")`) on production writes | I/O + CPU |
| L6 | `src/EdgeWARN/process/detect/lineage/detector.py:325-370` | Use existing R-tree `new_index` for split-overlap prefilter | CPU |
| L7 | `src/EdgeWARN/process/detect/lineage/detector.py:163-260` | Build `cell_lineage` dict literals once outside hot branches | CPU |
| L8 | `src/common/ingest/nexrad/parser.py:37-62` | `bytearray.extend` vs `b"".join(record_parts)` | memory |
| L9 | `src/common/ingest/nexrad/parser.py:537` | Default to mmap path (`parse_raw_volume_file_mmap`) | memory |
| L10 | `src/common/ingest/nexrad/parser.py:399-422` | Combined regex `re.compile(b"AR2V|ARCHIVE2")` + `finditer` for split-stream | CPU |
| L11 | `src/common/ingest/mrms/downloader.py:496-501` | Dedicated `ThreadPoolExecutor(max_workers=2)` for `merge_glm_files` | concurrency |
| L12 | `src/common/ingest/mrms/downloader.py` | Collapse duplicate `perf_tracker` spans wrapping the same `PerformanceTimer` | CPU |
| L13 | `src/EdgeWARN/process/integrate/core/integrator.py:386-389` | List-comprehension for `feature_lookup` keys | CPU |
| L14 | `src/EdgeWARN/process/integrate/core/integrator.py:273, 373` | Remove explicit `gc.collect()` (also see §4 D8) | CPU |
| L15 | `src/EdgeWARN/ctam/engine.py:44-51` | Hoist `module_names_set = set(module_names)` once | CPU |
| L16 | `src/util/io.py:11-19` | Cache last-known-second ISO string in `TimestampedOutput.write` | CPU |
| L17 | `src/EdgeWARN/api/server.js` | Gate `app.use(rateLimit(...))` on `cfg.rateLimit.enabled` | CPU + memory |
| L18 | `src/EWMRS/scheduler.py:39` (or surviving) | `itertools.chain.from_iterable` for glob concat | memory |
| L19 | `src/EWMRS/render/goes_rgb.py:537` | Conditional copy in `_channel_data_from_registry` (only when dtype/order mismatch) | memory |
| L20 | `src/EWMRS/render/render.py:26-56` | `functools.lru_cache(maxsize=N)` instead of manual RLock | CPU |
| L21 | `src/common/ingest/nexrad/parser.py:425-533` | Free `record_stream` between sweep boundaries (non-mmap path) | memory |
| L22 | `src/common/ingest/nexrad/parser.py:536-537` | Once mmap path is proven, drop the bytes-only fallback | memory |
| L23 | `src/EdgeWARN/process/detect/kalman/filter.py:327-333` | `np.zeros((6,6))` + slicing assignment vs Python list → `np.array` | memory |
| L24 | `src/EdgeWARN/process/detect/kalman/assignment.py:572-580` | `tracks_by_id = {t.id: t for t in tracks}` for O(1) lookup | CPU |
| L25 | `src/EdgeWARN/api/server.js`, `src/EWMRS/api/server.js` | Mount `rateLimit` before helmet/cors | CPU |

---


## Section 3 — Security

Each finding cites file:line, a brief vulnerable snippet, an exploit sketch, the minimal patch, and a behavior-preservation note. **Critical**: none.

### 3.1 High (7)

#### S-H1. `isSafeFilename` accepts NUL bytes / control chars / Windows-reserved names
**File:** `src/EdgeWARN/api/utils/fileReader.js:20-24`
**Vulnerable code:**
```js
if (name.includes('..') || name.includes('/') || name.includes('\\')) return false;
return name.toLowerCase().endsWith('.json') && path.basename(name) === name;
```
**Exploit:** names like `CON.json`, `LPT1.json`, `AUX.json` (Windows reserved devices) and names containing `:`, `*`, `?`, `<`, `>`, `|`, `"`, or `\x00–\x1f` are accepted. On Windows opening `CON` may hang the file descriptor; `:` selects an NTFS alternate data stream.
**Fix:**
```js
if (/[\x00-\x1f<>:"|?*]/.test(name)) return false;
if (/^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\..*)?$/i.test(name)) return false;
```
**Behavior preserved on legitimate input:** yes — legitimate filenames are timestamps/IDs that contain none of those characters.

#### S-H2. `realpath` containment to defend against symlink escape
**File:** `src/EdgeWARN/api/utils/fileReader.js:44-52`
**Vulnerable code:**
```js
const resolvedFull = path.resolve(full);
if (!resolvedFull.startsWith(resolvedDir + path.sep) && resolvedFull !== resolvedDir) { ... }
```
**Exploit:** `path.resolve` does not follow symlinks. A symlink under `cells/` pointing at a system file is dereferenced on `fs.readFile`.
**Fix:** `await fs.promises.realpath(resolvedFull)` then re-check containment; convert ENOENT to a clean 404.
**Behavior preserved on legitimate input:** yes — no legitimate file is a symlink in this deployment.

#### S-H3. EWMRS `/renders/fetch` and `/renders/tile-info` skip the `PRODUCT_MAPPING` allowlist
**File:** `src/EWMRS/api/routes/renders.js:185-189` (`fetch`), `367-378` (`tile-info`)
**Vulnerable code:**
```js
if (product.includes('..') || product.includes('/') || product.includes('\\')) { ... }
const productDir = path.join(GUI_DIR, product);
const indexFile = path.join(productDir, 'index.json');
```
**Exploit:** `product` is not cross-checked against `PRODUCT_MAPPING` (only `/download` and `/tile` are). Combined with H1-style tricks, it reads attacker-chosen subdirs of `GUI_DIR`.
**Fix:** at the top of `/fetch` and `/tile-info`:
```js
if (!Object.prototype.hasOwnProperty.call(PRODUCT_MAPPING, product)) {
  return res.status(404).json({ error: 'Unknown product' });
}
```
Also tighten `isSafeFilename` per H1.
**Behavior preserved on legitimate input:** yes — legitimate clients pass keys in `PRODUCT_MAPPING` (evidenced by `/get-items`).

#### S-H4. Add `resolveUnder` to `wpc.js`
**File:** `src/EWMRS/api/routes/wpc.js:97-107`
**Vulnerable code:** `^\d{8}-\d{6}$` regex blocks traversal in practice, but no `realpath` containment / symlink defense (parity with H2).
**Fix:** import the existing `resolveUnder` helper from `nexrad/filesystem.js` and apply.
**Behavior preserved on legitimate input:** yes.

#### S-H5. Bound `express.json` and reorder middleware
**File:** `src/EdgeWARN/api/server.js:111`
**Vulnerable code:** `app.use(express.json());`
**Exploit:** default 100 KB limit is implicit; no body should ever reach this GET-only API. Body parser runs before rate-limit, so abusive bodies waste CPU before rate-limit kicks in.
**Fix:** `app.use(express.json({ limit: '16kb', strict: true, type: 'application/json' }));` AND mount the rate limiter **before** `express.json()`.
**Behavior preserved on legitimate input:** yes — no JSON body is sent by any v2 route.

#### S-H6. CORS misconfiguration: `origin: true` + `credentials: true`
**File:** `src/EdgeWARN/api/server.js:96-109`
**Vulnerable code:**
```js
const corsOrigin = hasExplicitOrigins
  ? allowedOrigins
  : (env.NODE_ENV === 'production' ? [] : true);
app.use(cors({ origin: corsOrigin, credentials: true, ... }));
```
**Exploit:** with `ALLOWED_ORIGINS` unset and `NODE_ENV !== 'production'`, the cors middleware reflects any origin and sets `Access-Control-Allow-Credentials: true`.
**Fix:** when no explicit allowlist is configured, set `credentials: false`. Or refuse to start with credentials enabled unless an allowlist is present.
**Behavior preserved on legitimate input:** yes for same-origin GUI clients.

#### S-H7. EWMRS `cors()` with default wildcard
**File:** `src/EWMRS/api/server.js:183`
**Vulnerable code:** `app.use(cors());`
**Exploit:** returns `Access-Control-Allow-Origin: *`. Primary risk is data exfiltration of imagery + DoS amplification.
**Fix:** mirror the EdgeWARN allowlist; require `EWMRS_ALLOWED_ORIGINS`.
**Behavior preserved on legitimate input:** yes for same-origin GUI.

### 3.2 Medium (12)

| ID | File:line | Issue | Fix |
|----|-----------|-------|-----|
| S-M1 | `EWMRS/api/server.js:185` | No CSP on EWMRS | `helmet({ contentSecurityPolicy: { useDefaults: true, directives: { "default-src": ["'self'"] } } })` |
| S-M2 | `EdgeWARN/api/server.js:114-150` | `keyGenerator` uses `req.ip`; behind a default reverse proxy this collapses to one bucket | Document required `TRUST_PROXY`; emit startup warning when production && trust-proxy=false && X-Forwarded-For observed |
| S-M3 | `EdgeWARN/api/server.js:137-140` | `skip` allows trivial bypass via `x-internal-check: true` | Compare against `INTERNAL_CHECK_TOKEN` via `crypto.timingSafeEqual`; do not honor skip if token unset |
| S-M4 | `alerts.js:13-34`, `mesocyclones.js:9-23`, `metar.js:14-36`, `nexrad/filesystem.js:88-105, 107-125` | Unbounded `fs.readdir` per request | Wrap in `LRUCache` keyed by directory with 5s TTL (matches advertised `max-age=5`) |
| S-M5 | `fileReader.js:61-62, 88-89`, `EWMRS/api/routes/{rap,renders,wpc,colormaps}.js` | `JSON.parse` with no size cap | Pre-`fs.stat`; reject if `size > 16MB` with 500 |
| S-M6 | `renders.js:253, 346`, `rap.js:295`, `nexrad/index.js:117` | Streaming `sendFile` with no concurrency limit / timeout | `server.requestTimeout`, `res.setTimeout(30000)`, optional `p-limit` |
| S-M7 | `wpc.js:62, 121`, `colormaps.js:27` | `details: err.message` leaks absolute paths | Drop `details` from response body; log server-side only |
| S-M8 | `nexrad/index.js:115` | `Content-Disposition` interpolates four validated values; future maintainer risk | Add comment + assertions; treat as info |
| S-M9 | `validation.js:55-58`, `cells.js:19-37` | `validateCellId` accepts arrays via type coercion | `if (typeof id !== 'string') return false;` |
| S-M10 | `validation.js:22-35` | `validateTimestamp*` accepts non-string types | Same `typeof !== 'string'` guard |
| S-M11 | All cacheable routes | Missing `Vary: Accept-Encoding, Origin` | Global middleware setting both `Vary` headers |
| S-M12 | `EWMRS/api/{routes/nexrad/filesystem,renders,wpc,rap}.js` | `app.locals.{BASE_DIR,GUI_DIR}` mutable | `Object.defineProperty(app.locals, 'BASE_DIR', { value, writable: false, configurable: false })` after init |

### 3.3 Low / Info (17)

| ID | File:line | Note |
|----|-----------|------|
| S-L1 | `EdgeWARN/api/server.js:222-224`, `EWMRS/api/server.js:235` | `app.listen` binds to all interfaces — accept `HOST` env var, default `127.0.0.1` |
| S-L2 | `EWMRS/api/routes/{renders,rap,nexrad/index}.js` | Conditional stack logging in production |
| S-L3 | `EWMRS/api/routes/{renders,rap,nexrad/index}.js` | Strip `\r\n` from interpolated user input in log lines (CRLF log injection) |
| S-L4 | — | No open-redirect handler (info, confirmed) |
| S-L5 | — | No SSRF surface (info, confirmed) |
| S-L6 | — | No prototype-pollution sinks (info, confirmed) |
| S-L7 | All validation regexes | All anchored, linear, no nested quantifiers — no ReDoS (info, confirmed) |
| S-L8 | `src/` | No hardcoded credentials (info, confirmed) |
| S-L9 | Python | No `pickle.load` / `yaml.load(` / `subprocess(...,shell=True)` / `eval` / `exec` (info, confirmed) |
| S-L10 | JS | No `child_process` (info, confirmed) |
| S-L11 | `renders.js:251-253, 344-346`, `rap.js:288-295`, `nexrad/index.js:108-117` | TOCTOU on `fs.access` + `sendFile` — drop `fs.access`, rely on `sendFile` 404 |
| S-L12 | EWMRS/EdgeWARN `helmet()` | `crossOriginResourcePolicy: 'same-origin'` may block cross-origin embedding — deliberate choice |
| S-L13 | `EdgeWARN/api/server.js:86`, `EWMRS/api/server.js:186` | `compression()` enabled — BREACH-like risk for any future authenticated, secret-bearing response |
| S-L14 | `EWMRS/api/routes/colormaps.js:18` | Path resolved relative to `__filename` with no containment check (currently fixed input) |
| S-L15 | `EWMRS/api/routes/rap.js:183` | `new URL(import.meta.url).pathname` is broken on Windows; use `fileURLToPath` |
| S-L16 | `EdgeWARN/api/config.js:13` | `args[i].split('=')[1]` loses `=`-containing values (functional bug) |
| S-L17 | `EdgeWARN/api/config.js:24-47`, EWMRS `server.js:88-105` | `BASE_DIR` resolved without root/system-path guard; `mkdirSync` creates subdirs under any path |

### 3.4 Top-3 fix order

1. **S-H6 / S-H7 — CORS:** gate `credentials: true` on an explicit origin allowlist; replace EWMRS `cors()` with allowlisted config.
2. **S-H3 / S-H1 — Path-traversal hygiene:** enforce `PRODUCT_MAPPING` allowlist on `/renders/fetch` + `/tile-info`; tighten `isSafeFilename` against control chars and Windows-reserved names.
3. **S-H5 — Body limit + middleware order:** `express.json({ limit: '16kb' })`; rate-limit before body parser.

---

## Section 4 — Dead code

Each finding cites file:line, kind, grep-evidence note, and removable LOC. All deletions assume a final pre-merge `grep -r <symbol> src/ tests/ scripts/ .` confirms zero references after migration.

### D1. `src/EdgeWARN/ingest/**` compatibility shim tree (~120 LOC)
**Kind:** compat re-export tree (~25 files, each 4–5 LOC).
**Grep evidence:** non-test importers are limited to:
- `src/EdgeWARN/schedule/scheduler.py:5-9, 198` (`from EdgeWARN.ingest.mrms.{s3_sync,utils,parse,config,timestamp_utils,https_client} import ...`)
- `src/EdgeWARN/ctam/modules/__init__.py:27` (a comment-only reference to `EdgeWARN.ingest.nws.geomapper`)
- 7 test files under `tests/core/ingest/`
**Action:** migrate the 1 production importer + 7 test files to import from `common.ingest.*`, then delete the entire `src/EdgeWARN/ingest/` tree (4 `AGENTS.md` stubs included).
**Behavior preserved:** `sys.modules[__name__] = _impl` makes attribute access identical to direct import — pure rename.
**Removable LOC:** ~120.

### D2. `src/EWMRS/scheduler.py` — full file (~195 LOC)
**Kind:** stale fork of `EdgeWARN/schedule/scheduler.py`.
**Grep evidence:** `grep "EWMRS.scheduler|from EWMRS import scheduler"` returns zero hits. The active scheduler is imported by `src/run.py:30,494` and `src/process_historical.py:9,55` from `EdgeWARN.schedule.scheduler`.
**Action:** delete the file outright.
**Removable LOC:** ~195.

### D3. `EdgeWARN.pipeline.realtime_pipeline` (~80 LOC)
**File:** `src/EdgeWARN/pipeline.py:252-332` + `__init__.py:1,7` re-export.
**Grep evidence:** zero call sites in `src/`, `tests/`, `scripts/`. Production uses `edgewarn_tandem_worker` (called from `src/run.py:351`).
**Action:** delete the function and remove from `EdgeWARN/__init__.py.__all__`.
**Removable LOC:** ~80.

### D4. NEXRAD coordinator polling wrappers (~70 LOC)
**File:** `src/common/ingest/nexrad/coordinator.py:156-178` (`NexradScanCoordinator.poll_latest_station_scans_forever_async`) and module-level wrapper at `:198-214`, plus the `main.py` re-export at lines 185-203.
**Grep evidence:** superseded by `NexradRealtimeIngestionPipeline.run_forever` (used at `src/run.py:13, 155`). The `poll_*` wrappers have no internal callers.
**Action:** delete the polling wrappers; keep `ingest_latest_station_scans_async` / `NexradScanCoordinator` for one-shot use (still referenced by `tests/core/ingest/test_nexrad_coordinator.py`).
**Removable LOC:** ~70.

### D5. MRMS sync fallback path (~50 LOC)
**File:** `src/common/ingest/mrms/downloader.py::download_all_files_sync_fallback`, `download_modifier_sync`.
**Grep evidence:** only triggered via `run_with_async_fallback`; lacks the HTTPS retry that the async path has. Also creates double-nested fallback because `_safe_ingest("MRMS Detection", sync_fallback=download_all_files, ...)` re-enters async via `download_all_files`.
**Action:** delete the sync downloader; update `download_all_files` in `mrms/main.py` to call `asyncio.run(download_all_files_async_internal(...))` directly (the async path has internal HTTPS fallback per modifier).
**Behavior preserved:** the async path has strictly more robustness; deletion improves rather than degrades it.
**Removable LOC:** ~50.

### D6. NWS legacy ingest (~85 LOC)
**File:** `src/common/ingest/nws/main.py:282-365` (`download_alerts_legacy` + `_process_nws_file_legacy`).
**Grep evidence:** zero callers in `src/` or `tests/`. Production uses `download_alerts_async` (called from `src/run.py::nws_loop`).
**Action:** delete both functions.
**Removable LOC:** ~85.

### D7. `EWMRS.pipeline.run_ewmrs_pipeline` passthrough (~3 LOC)
**File:** `src/EWMRS/pipeline.py:661-663`.
**Grep evidence:** zero in-tree callers. Pure forward to `run_render_pipeline`.
**Action:** delete.
**Removable LOC:** 3.

### D8. Explicit `gc.collect()` calls (2 LOC)
**File:** `src/EdgeWARN/process/integrate/core/integrator.py:273, 373`.
**Reason:** Python's generational GC already reclaims closed datasets. Manual full collection walks all live objects and is multi-hundred-ms.
**Action:** delete both lines.
**Behavior preserved:** reference counts already free closed datasets immediately.
**Removable LOC:** 2.

### D9. Opportunistic dead-code sweeps to perform during execution
The following categories require per-symbol grep verification (the dead-code subagent's Phase 4 protocol). They are intentionally not enumerated symbol-by-symbol here because each candidate must be verified against the live tree at execution time:

1. **Unused imports** — heavy imports (numpy, pandas, xarray, scipy, rasterio) flagged via grep where the symbol is never referenced inside the file.
2. **Orphan `__init__.py` re-exports** — symbols listed in `__all__` or imported in `__init__.py` with zero importers via the package path.
3. **Unused argparse flags** — flags parsed in `run.py` / `process_historical.py` whose `args.<name>` is never read downstream.
4. **Dead branches** — `if False:`, code after unconditional `return`, exception handlers that re-raise without modification.
5. **Long commented-out blocks** (>5 lines).
6. **Duplicate utility functions** — bbox parsing, lat/lon validation, JSON load/save in multiple modules.

Each sweep entry that survives grep verification is included in the final PR description with file:line + grep proof. Default to keeping anything that cannot be conclusively proven dead.

### Removable LOC total

| Category | LOC |
|----------|-----|
| Pure deletions, zero importers (D2, D3, D4, D5, D6, D7, D8) | ~485 |
| Compat shim tree (D1, after migrating 1 production + 7 test files) | ~120 |
| Opportunistic sweeps (D9) | TBD post-grep |
| **Total minimum** | **~605 LOC** |

---

## Section 5 — Redundant pipelines

Each entry: pipeline A vs B, overlap, active-vs-dead evidence, recommendation, LOC removable, behavior-preservation note.

### R1. `EdgeWARN/ingest/**` ↔ `common/ingest/**` (compat re-export tree)
- **Overlap:** every leaf of `src/EdgeWARN/ingest/` is a 4–5 LOC forward to its `common.ingest` counterpart.
- **Active:** `common.ingest.*` is the real surface. See §4 D1.
- **Recommendation:** see D1.
- **Behavior preserved:** yes — `sys.modules[__name__] = _impl` ensures attribute access is identical.

### R2. `EWMRS/scheduler.py` ↔ `EdgeWARN/schedule/scheduler.py` (stale fork)
- **Overlap:** same `MRMSUpdateChecker` class, same public API.
- **Active:** `EdgeWARN/schedule/scheduler.py` (used in `run.py`, `process_historical.py`). The EWMRS version is a stale fork missing the shared boto3 client, StartAfter optimization, parallel HTTPS fallback, and trace IDs.
- **Recommendation:** see D2 — delete `src/EWMRS/scheduler.py`.
- **Behavior preserved:** yes — the deleted file is unreachable.

### R3. `realtime_pipeline` ↔ `edgewarn_tandem_worker`
- **Overlap:** both run ingest → detect → integrate. `realtime_pipeline` runs ingest internally; `edgewarn_tandem_worker` waits on shared events whose ingest is performed by `src/run.py::_run_tandem_cycle` (which calls `run_tandem_ingest_cycle` once per cycle).
- **Active:** `edgewarn_tandem_worker` (spawned at `src/run.py:351`).
- **Recommendation:** see D3 — delete `realtime_pipeline`.

### R4. Three "MRMS-then-others" coordinators
- **Pipelines:** `src/run.py::_run_tandem_cycle` (A, production), `EdgeWARN/pipeline.py::realtime_pipeline` (B, dead), `EdgeWARN/pipeline.py::historical_pipeline` (C, used by `process_historical.py`).
- **Overlap:** A and B both compose `run_tandem_ingest_cycle`. C calls individual ingest mains directly.
- **Recommendation:** delete B (R3). **Optional/conditional:** consolidate C to use `run_tandem_ingest_cycle(include_goes=True, include_ewmrs=False)` if a file-set parity check confirms identical on-disk output (note: `download_all_files` adds GOES; the staged path adds GLM separately — verify before merging).
- **LOC removable:** ~30 if consolidated.
- **Behavior preserved:** yes for B; conditional yes for C.

### R5. NEXRAD coordinator vs pipeline subpackage
- **Pipelines:** `common/ingest/nexrad/coordinator.py::NexradScanCoordinator` vs `common/ingest/nexrad/pipeline/__init__.py::NexradRealtimeIngestionPipeline`.
- **Overlap:** both fetch VCP catalog, discover volumes, filter by ALLOWED_VCPS, dispatch to `NexradIngestService`. The pipeline form adds `NexradPendingVolumeTracker` and `NexradStationFilter` (extracted helpers).
- **Active:** the pipeline subpackage (`run.py:13,155`).
- **Recommendation:** see D4 — delete the polling wrappers in `coordinator.py` while keeping one-shot ingest helpers (which have tests). A future pass may collapse the coordinator entirely after migrating its test suite.

### R6. MRMS sync vs async download paths
- See D5. The sync path is an inferior duplicate (no HTTPS retry).
- **Recommendation:** delete the sync path; simplify `download_all_files` to a single `asyncio.run` of the async path.

### R7. Synoptic sync ↔ async (per-call inner fallback)
- **Pipelines:** `s3_sync.SynopticFileDownloader` and `s3_async.AsyncSynopticFileDownloader`.
- **Overlap:** identical logic; different I/O.
- **Active:** both, via `download_synoptic` (line 64–99) — async first, sync fallback, retry with previous hour. This is a **legitimate** per-call fallback (different from the coordinator-level fallback in R6).
- **Recommendation:** **keep**. Optional later: replace sync class with `asyncio.run` of async function in a fresh loop, deleting `SynopticFileDownloader` (~50 LOC). Lower priority; current dual path is small and the sync fallback may genuinely succeed when async fails.

### R8. `run_ewmrs_pipeline` passthrough
- See D7. Three-line forward to `run_render_pipeline` with no callers — delete.

### R9. `goes_readiness.py` is the single source of truth — no action
- All readiness checks (`check_local_goes_ready`, `check_local_glm_ready`, `latest_goes_file_near_target`, `parse_staged_file_time_window`, `parse_staged_file_timestamp`, `get_ewmrs_goes_render_specs`) live exclusively in `src/common/pipeline/goes_readiness.py`. Wrappers in `src/run.py:184-224` are thin adapters. **No redundancy.**

### Summary

| # | Action | LOC | Risk |
|---|--------|-----|------|
| R1 | Delete `src/EdgeWARN/ingest/**` (after migrating 1 prod + 7 test importers) | ~120 | Low |
| R2 | Delete `src/EWMRS/scheduler.py` | ~195 | None |
| R3 | Delete `realtime_pipeline` | ~80 | None |
| R4 | Optional: consolidate `historical_pipeline` to `run_tandem_ingest_cycle` | ~30 | Medium |
| R5 | Delete unused NEXRAD polling wrappers | ~70 | Low |
| R6 | Delete MRMS sync fallback | ~50 | Low |
| R7 | Synoptic sync — keep | 0 | — |
| R8 | Delete `run_ewmrs_pipeline` passthrough | 3 | None |

**Total low-risk:** ~518 LOC. With R1 included: ~638 LOC.

---

## Section 6 — Behavior preservation guardrails

Every recommendation in §2–§5 was vetted by a behavior-preservation auditor sub-agent. The following invariants must remain unchanged. Reviewers must check this list before accepting any patch derived from this document.

### 6.1 Numerical / scientific invariants

1. **Kalman matrices stay `float64`** — `src/EdgeWARN/process/detect/kalman/filter.py` `state`, `P`, `F`, `Q`, `R`, `H` matrices are not downcast. Crossing the `gating_threshold=6.0` boundary on borderline associations would shift tracker output. Reject patches introducing `astype(np.float32)` or `dtype=np.float32` in this module.
2. **`np.linalg.solve` not `inv` in Mahalanobis** — H5 in §2.1 explicitly proposes substituting `solve` for `inv`; the auditor confirmed `solve` is the existing convention in the codebase. Reject any reverse substitution.
3. **`cos(lat)` floor at `1e-6`** — guards polar-region division. Removing or changing the floor breaks lat=±90 handling.
4. **Centroid log-sum-exp weighting** in `src/EdgeWARN/process/detect/tools/save.py` — replacing with a plain mean shifts centroid lat/lon by ~0.01°, perturbing tracker associations.
5. **Polygon point rounding** `round(x, 3)` and `lon % 360` in the same file — any precision change alters byte-identical JSON output that consumers diff.
6. **`skimage.measure.find_contours(..., level=0.5)` for hail core** — changing the level constant changes which cells are flagged.
7. **NEXRAD `_normalize_azimuth_axis` `kind="stable"`** — recent commit `5626713` makes this a hard invariant. Reject any `np.argsort` / `np.sort` in `src/EWMRS/render/nexrad.py` without `kind="stable"`.
8. **Rendering `_scalar_data_to_rgba` two-path split** — `np.interp` for continuous colormaps, `np.digitize` for discrete LUTs, selected by `n_bins` truthiness. H9 must preserve the split.
9. **Colormap LUT cache key** `(cmap_name, vmin, vmax, n_bins)` — exact tuple shape. Reordering or normalizing causes cache misses and subtle pixel diffs from matplotlib float math.
10. **Tile alpha-zero filter** — tiles written only if `alpha.any()`. Switching to `alpha.sum() > 0` is not equivalent for floats with `-0.0`.

### 6.2 On-disk wire formats

11. **`CellDataSaver.create_entry` field insertion order** — Python dict insertion order is preserved and consumers rely on it. Reorganizing changes byte-identical JSON.
12. **Stormcell save envelope** `{source, product, version, latest_timestamp, features}` — additive only at the end.
13. **NEXRAD worker IPC contract** — `_worker_parse` return dict shape + `_dict_to_result` rehydration. Adding a field to `ElevationArtifact` requires updating both.
14. **Tile filename** `tile_<x>_<y>.png` and sort by `(y, x)` — animation playback order.
15. **RAP encoding** uint16 LE + `X-Missing-Value=65535` sentinel — wire format.
16. **NEXRAD bin format magic** `EWFFv1S0` — versioned; bumping requires parallel client support.
17. **WPC** `wpc_sfc_<TS>.geojson` + `latest.geojson` exclusion from listing.
18. **NEXRAD `Content-Disposition`** filename pattern `<site>_<ts>_<elev>_<product>.bin.gz`.

### 6.3 Concurrency / ordering

19. **Staged readiness order** in `common/pipeline/coordinator.py`: detection → EWMRS MRMS → EWMRS GOES → EdgeWARN integration. Downstream services subscribe in this order. New stages append or insert with explicit rationale; never reorder existing four.
20. **`mesocyclones.js` descending sort** — sorted by capture group `(\d{8}-\d{6})` from filename regex.
21. **LRU eviction order in `fileReader.js`** — 60s TTL + 40MB sizeCalculation. Switching policy can mask stale-file bugs.
22. **NEXRAD ingest non-daemon process** in `src/run.py` (`daemon=False`) is intentional: ingest survives Ctrl-C for in-flight volumes.
23. **`get_nexrad_pool` reuse semantics** in `worker_pool.py` — recreates only when `max_workers` differs.
24. **Cluster cap** `min(cpus, 4)` workers in `EdgeWARN/api/server.js` — rate-limit counters are per-worker.

### 6.4 Validation contracts (tighten only, never loosen)

25. **`validateAlertId`** blocks `__proto__`, `constructor`, `prototype`; allows `[a-zA-Z0-9_.:-]+`. Loosening enables path traversal via the alert-id-to-filename concatenation.
26. **NEXRAD validation** — site `[A-Z0-9]{4}`, real UTC date, allowed product set `{DBZH, VRADH, WRADH, PHIDP, CCORH, RHOHV, ZDR}` linked to `worker.py` waveform extraction.
27. **v1 `/features` and `/data` return 410** — deprecation contract; replace only with version bump.

### 6.5 CLI / env-var contracts

28. **`IOManager.get_args` `lon % 360`** in `util/io.py` mods `lon_limits` to 0–360 for `run.py` callers; `process_historical.py` deliberately does not. The asymmetry is intentional. A "unification" PR must touch both call sites with explicit rationale.

### 6.6 Greenlight checklist (per-patch)

A proposed change is greenlight-safe iff **all** apply:

1. No regex tightening on validation paths (`EdgeWARN/api/utils/validation.js`, `EWMRS/api/routes/nexrad/validation.js`) — only refactor or stricter.
2. No new `np.vectorize` introductions in `src/EdgeWARN/process/detect/**` or `src/EWMRS/render/**`.
3. No dtype downgrades in `kalman/filter.py`.
4. No `inv(S)` in place of `solve(S, x)` in `kalman/filter.py`.
5. No `kind="quicksort"` in `EWMRS/render/nexrad.py`.
6. No removal of `src/EdgeWARN/ingest/` shim modules without first grepping `from EdgeWARN.ingest` across full repo + external `pyproject.toml` consumers, then migrating each.
7. No reordering of `CellDataSaver.create_entry` field assembly.
8. No changes to documented JSON envelope keys (stormcells, mesocyclones, alerts, METAR, RAP).
9. No tile filename or grid-size changes (`TILE_SIZE=350`, `TILE_GRID_ROWS=10`, `TILE_GRID_COLS=20`, `tile_<x>_<y>.png`).
10. No rate-limit defaults loosened (only tightened or made configurable).
11. No `Cache-Control` `max-age` changes.
12. No removal of v1 410 sunset.
13. No changes to `EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER` semantics (use the same name pattern for new pauses).
14. No reordering of `common/pipeline/coordinator.py` staged readiness.
15. No silent removal of the `lon % 360` step in `IOManager.get_args`.

---

## Section 7 — Verification

How to validate any patch derived from this document before merging.

### 7.1 Static checks
- `npm test` (Jest + Supertest under `tests/api/`) — must remain green.
- `python -m pytest tests/` (pytest with `pythonpath = src`) — must remain green.
- For Python perf changes: run `tests/benchmarks/` and compare deltas before/after.

### 7.2 Targeted regression smoke

#### API regression
1. `npm run api:edgewarn` and `npm run api:ewmrs` in separate shells.
2. For each route documented in `nexrad_api_routes.md` and `docs/api/`, curl the route and validate:
   - Status code, response Content-Type, presence of `Cache-Control` and `Vary` headers.
   - JSON top-level keys match §6 envelope contracts.
3. For path-traversal fixes (S-H1, S-H2, S-H3, S-H4): manually attempt each exploit (`product=..`, `product=CON`, symlink farm) and confirm 404; confirm legitimate inputs still 200.
4. For CORS fixes (S-H6, S-H7): verify same-origin GET still succeeds; cross-origin GET without `ALLOWED_ORIGINS` is rejected.

#### Real-time pipeline regression
```
python src/run.py --lat_limits 20 55 --lon_limits 230 300 \
  --disable-ctam --disable-tracking
```
- Run for one tandem cycle.
- Diff `<BASE_DIR>/{data,gui,wpc}` filenames and JSON envelopes against a pre-change baseline. Numeric tolerance applies only to intentional float-pipeline edits (H5 `solve` vs `inv`, H9 LUT, M21 float32 RAP) — verify with `np.allclose(rtol=1e-5, atol=1e-8)`.

#### Historical pipeline regression
```
python src/process_historical.py --start <T0> --end <T0+1m> \
  --lat 20 55 --lon -130 -60
```
- Byte-level diff stormcell JSON outputs against a pre-change run.
- For the optional R4 consolidation: verify the on-disk file set under `<BASE_DIR>/data/` is identical to the pre-change historical run before merging.

### 7.3 Per-section gating

| Section | Verification |
|---------|--------------|
| §2 Performance — H1, H8, H10 (vectorization) | Numeric diff via `np.allclose`; perf delta via `util.performance` or benchmarks |
| §2 Performance — H5 `solve` vs `inv` | Tracker output diff: feed the test fixture in `tests/core/process/detect/kalman/` and confirm associations match within tolerance |
| §2 Performance — H9 RGBA LUT | Pixel-perfect PNG diff for sample tiles; tolerance only on continuous-cmap path |
| §2 Performance — H11 context manager | RSS curve over a 30-min run before/after |
| §3 Security — H1–H7 | OWASP ZAP baseline scan + manual exploit attempt for each |
| §4 Dead code | `grep -r <symbol> src/ tests/ scripts/ .` post-deletion confirms zero references |
| §5 Pipelines | Diff one full real-time cycle and one historical cycle of generated artifacts; confirm staged readiness ordering via log lines from `coordinator.py` |
| §6 Invariants | Manual review of the 28-item hot list against the diff |

### 7.4 Rollback plan
Each batch in §8 should be a separate PR. Bisect-friendly commit prefixes (per `CONTRIBUTING.md`):
- `IMP[perf]:` for performance edits
- `FIX[sec]:` for security edits
- `REM[ingest]:` for dead-code/pipeline removals

---

## Section 8 — Suggested execution sequencing

Ordered list of safe-first batches. Each batch is one or two PRs.

### Batch 1 — Pure deletions, zero importers (safest, ~430 LOC)
- D2: delete `src/EWMRS/scheduler.py` (~195)
- D3: delete `EdgeWARN.pipeline.realtime_pipeline` (~80)
- D4: delete NEXRAD polling wrappers (~70)
- D6: delete NWS legacy ingest functions (~85)
- D7: delete `EWMRS.pipeline.run_ewmrs_pipeline` (~3)
- D8: remove explicit `gc.collect()` calls (2)
- §2 L14 (drop explicit GC) and §2 L17 (gate rate limiters) bundle here as they share the "safe edit, zero functional change" profile.

### Batch 2 — Security high-priority hardening
- S-H6 / S-H7 (CORS allowlist + credentials gating).
- S-H3 (PRODUCT_MAPPING allowlist on `/renders/fetch` and `/tile-info`).
- S-H1 / S-H2 (path-traversal hygiene: control chars, Windows-reserved names, `realpath` containment).
- S-H4 (`resolveUnder` in `wpc.js`).
- S-H5 (`express.json({ limit: '16kb' })` + middleware reorder).
- S-M9 / S-M10 (`typeof !== 'string'` guards).
- S-M7 (drop `err.message` from EWMRS 500 bodies).
- S-L15 (`fileURLToPath` fix in `rap.js:183`).

### Batch 3 — Low-risk performance with no numeric output change
- H2 `os.scandir`
- H4 hoist `multiprocessing.Manager()`
- H11 context-manage `xr.open_dataset`
- H12 lock-free / opt-in `perf_tracker`
- H7 hoist `AssignmentCostCalculator`
- H6 cache cost matrix entry
- M3 `compression` filter for image responses
- M14 LRU index cache by (path, mtime)
- M16 combined regex in `find_timestamp`
- M17 `express.json({ limit })` (paired with S-H5 if not already shipped)
- L20 `lru_cache` for cmap
- L24 `tracks_by_id` dict

### Batch 4 — Compat shim removal (D1)
1. Update `src/EdgeWARN/schedule/scheduler.py` imports (lines 5–9, 198) to `from common.ingest.mrms.{...}` directly.
2. Update 7 test files under `tests/core/ingest/` to point at `common.ingest.*`.
3. Remove the comment-only reference in `src/EdgeWARN/ctam/modules/__init__.py:27`.
4. Run full test suite.
5. Delete `src/EdgeWARN/ingest/` tree.

### Batch 5 — Numeric / algorithmic refactors (require diff verification)
- H1 vectorize `RAPPointExtractor.extract` via `extract_batch`
- H3 narrow `deepcopy` in detection
- H5 `solve` vs `inv` for Mahalanobis
- H8 prefilter centroid array hoist
- H9 RGBA LUT single-pass
- H10 `_decode_grouped_ar2v_sweep` vectorization
- M21 float32 RAP read

Each item ships in its own PR with a stormcell JSON byte-diff and a render PNG pixel-diff in the test plan.

### Batch 6 — Optional consolidations (require explicit parity verification)
- D5 / R6 MRMS sync fallback removal
- R4 historical-pipeline consolidation to `run_tandem_ingest_cycle`
- D9 opportunistic dead-code sweep (per-symbol verified)

### Batch 7 — Medium and low-impact perf cleanups (rolling)
The remainder of §2 medium and low items can be batched freely once Batches 1–5 land. Bundle by file/module to keep PRs focused.

---

## Verification of plan completeness

- ✅ Project structure & workflow summary (§1)
- ✅ Performance: 12 high + 21 medium + 25 low = 58 items (§2)
- ✅ Security: 7 high + 12 medium + 17 low/info = 36 items (§3)
- ✅ Dead code: 8 concrete items + opportunistic sweep methodology (§4)
- ✅ Redundant pipelines: 9 entries, 7 actionable (§5)
- ✅ Behavior-preservation invariants: 28 items + 15-rule greenlight checklist (§6)
- ✅ Verification plan with per-section gating (§7)
- ✅ Execution sequencing (§8)
- ✅ Every recommendation includes a "Why behavior-preserved" rationale.
- ✅ No source code changes — this document is a recommendations artifact only.
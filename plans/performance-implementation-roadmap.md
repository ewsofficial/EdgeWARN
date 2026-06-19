# Performance / Security / Dead-Code Implementation Roadmap

**Source audit:** `plans/performance-optmization-plan.md` (re-audited 2026-06-19 against 2.6.3)
**Branch:** `version-test/2.6.4` (validated against package version `2.6.3`)
**Total scope (after subtracting items already shipped):** roughly high-60s actionable findings remain across performance, security, dead-code, and pipeline consolidation.

---

## Context

The audit document enumerates 58 performance, 36 security, 8 dead-code, and 9 redundant-pipeline findings. Each item already includes file:line, the proposed change, and a behavior-preservation rationale, so this roadmap does **not** re-derive the technical detail. Its job is to:

1. Skip items already shipped (H10, M8, L8, L13, L21, L23, S-H3, S-H5, S-L15, D2, D3, D4, D6, D7, D8/L14 fully; H7/L17 partially).
2. Group the remainder into three **parallelizable tracks** (Deletions, Security, Performance) so multiple PRs can land concurrently without merge friction.
3. Sequence within each track from "no behavior change" → "byte-identical numeric change" → "verified parity required."
4. Surface the cross-track dependencies (e.g. S-H5 + M17 share a middleware edit).
5. Keep §6 invariants and §7 verification gates wired to every batch.

The post-execution outcome: ~485 LOC of pure deletions, the high-severity security exposures (CORS reflection, path traversal, body parser unbounded) closed, the ~12 high-impact perf wins applied, and the compat shim tree retired — all behavior-preserving against the §6 hot list.

---

## Already shipped — do NOT re-PR

Per the 2026-06-19 re-audit + verification grep:

- **Fully done:** H10, M8, L8, L13, L21, L23, S-H3, S-H5, S-L15, D2, D3, D4, D6, D7, D8/L14
- **H7 line 437:** already hoisted; **only line 401 remains**
- **H5 tracker math:** Mahalanobis already uses `np.linalg.solve` at `filter.py:470`; **only Kalman gain `inv(S)` at filter.py:236 remains**

Treat these as guard rails — if a PR re-introduces the old form, reject it.

---

## Track A — Deletions (A1 shipped in 2.6.3; A2 still open)

**Track A checklist**

- [x] Complete A1 pure-deletion bundle
- [ ] Complete A2 compat shim removal
- [ ] Re-run Track A verification gates after each PR

Each item is a separate PR with prefix `REM[ingest]:` or `REM[dead]:`. Verification: post-deletion `grep -r <symbol> src/ tests/ scripts/` returns zero hits.

### A1 — Pure-deletion bundle (~435 LOC, shipped in 2.6.3)

**A1 checklist**

- [x] D2 delete stale EWMRS scheduler fork
- [x] D3 delete `realtime_pipeline` and related benchmark if now orphaned
- [x] D4 delete NEXRAD polling wrappers while keeping one-shot helpers and `NexradScanCoordinator`
- [x] D6 delete NWS legacy ingest functions
- [x] D7 delete `run_ewmrs_pipeline` passthrough
- [x] D8 / L14 remove explicit `gc.collect()` calls
- [ ] Re-run `pytest tests/` if replaying this bundle on another branch
- [ ] Re-run `npm test` if replaying this bundle on another branch
- [ ] Re-run the 1-cycle real-time smoke in §7.2 if replaying this bundle on another branch

This bundle is already present in the 2.6.3 tree. Keep the itemization below only as replay/cherry-pick guidance for other branches:

| ID | Action | File | LOC |
|----|--------|------|-----|
| D2 | Delete stale EWMRS scheduler fork | `src/EWMRS/scheduler.py` | ~195 |
| D3 | Delete `realtime_pipeline` (also remove from `EdgeWARN/__init__.py:1,7`); delete `tests/benchmarks/benchmark_realtime_pipeline_memory.py` if it only exercises that path | `src/EdgeWARN/pipeline.py:252-332` | ~80 |
| D4 | Delete NEXRAD polling wrappers (keep one-shot helpers and `NexradScanCoordinator`) | `src/common/ingest/nexrad/coordinator.py:156-178, 198-214`; `main.py:185-203` re-export | ~70 |
| D6 | Delete NWS legacy ingest functions | `src/common/ingest/nws/main.py:282-365` | ~85 |
| D7 | Delete `run_ewmrs_pipeline` passthrough | `src/EWMRS/pipeline.py:661-663` | 3 |
| D8 / L14 | Remove explicit `gc.collect()` calls | `src/EdgeWARN/process/integrate/core/integrator.py:273, 373` | 2 |

**Verification (if replayed elsewhere):** full `pytest tests/`, `npm test`, plus a 1-cycle real-time smoke per §7.2.

### A2 — Compat shim removal (D1 / R1, ~120 LOC, 1 PR)

**A2 checklist**

- [ ] Migrate scheduler imports from `EdgeWARN.ingest.mrms.*` to `common.ingest.mrms.*`
- [ ] Migrate 7 test files under `tests/core/ingest/` to `common.ingest.*`
- [ ] Drop the comment-only reference in `src/EdgeWARN/ctam/modules/__init__.py`
- [ ] Run full test suite before deletion
- [ ] Delete `src/EdgeWARN/ingest/` tree
- [ ] Re-run full test suite after deletion
- [ ] Run 1-cycle real-time smoke
- [ ] Verify `from EdgeWARN.ingest` grep returns zero hits

Multi-step; ordering matters:

1. Migrate `src/EdgeWARN/schedule/scheduler.py:5-9, 198` imports from `EdgeWARN.ingest.mrms.{...}` → `common.ingest.mrms.{...}`.
2. Migrate 7 test files under `tests/core/ingest/` to `common.ingest.*`.
3. Drop the comment-only reference at `src/EdgeWARN/ctam/modules/__init__.py:27`.
4. Run full test suite (must be green at this point — pre-deletion).
5. Delete `src/EdgeWARN/ingest/` tree (4 `AGENTS.md` stubs included).
6. Re-run full test suite + 1-cycle real-time smoke.

**Verification:** §6 invariant 6 (greenlight checklist) — grep `from EdgeWARN.ingest` across full repo returns zero post-step 5.

---

## Track B — Security (B2/B3 shipped in 2.6.3; B1/B4 still open)

**Track B checklist**

- [ ] Complete B1 CORS lockdown
- [x] Complete B2 path-traversal hygiene
- [x] Complete B3 request-body and middleware hardening
- [ ] Complete B4 medium-severity polish
- [ ] Run per-item exploit verification and end-of-phase ZAP baseline

Each item ships as its own PR with prefix `FIX[sec]:`. Verification: §7.2 manual exploit attempt for each H-item; OWASP ZAP baseline scan at end of phase.

### B1 — CORS lockdown (S-H6 + S-H7, 1 PR)

**B1 checklist**

- [ ] Force safe default when `ALLOWED_ORIGINS` is unset in `src/EdgeWARN/api/server.js`
- [ ] Replace permissive EWMRS CORS middleware with allowlist-driven config
- [ ] Introduce `EWMRS_ALLOWED_ORIGINS`
- [ ] Add `Vary: Origin, Accept-Encoding` middleware
- [ ] Verify same-origin success and cross-origin rejection when allowlist is unset

- `src/EdgeWARN/api/server.js:96-109`: when `ALLOWED_ORIGINS` unset, force `credentials: false` (or refuse to start in production).
- `src/EWMRS/api/server.js:183`: replace `app.use(cors())` with allowlist-driven config; introduce `EWMRS_ALLOWED_ORIGINS` env var.

**Side change:** add `Vary: Origin, Accept-Encoding` middleware (closes S-M11 cheaply).

### B2 — Path-traversal hygiene (S-H1, S-H2, S-H3 `/fetch`, S-H4, 1 PR)

**B2 checklist**

- [x] Tighten `isSafeFilename` against control characters and Windows-reserved names
- [x] Add `realpath` containment check with ENOENT mapped to 404
- [x] Add `PRODUCT_MAPPING` allowlist guard to EWMRS `/fetch`
- [x] Reuse `resolveUnder` in `wpc.js`
- [x] Verify manual traversal and reserved-name exploit attempts fail with 404

- `src/EdgeWARN/api/utils/fileReader.js:20-24`: tighten `isSafeFilename` against control chars + Windows-reserved names per audit snippet.
- `src/EdgeWARN/api/utils/fileReader.js:44-52`: add `await fs.promises.realpath(...)` containment check; ENOENT → 404.
- `src/EWMRS/api/routes/renders.js:185` (`/fetch`): add `PRODUCT_MAPPING` allowlist guard (mirror existing `/tile-info` form at line 372). `/tile-info` is already done; do NOT re-edit.
- `src/EWMRS/api/routes/wpc.js:97-107`: import `resolveUnder` from `nexrad/filesystem.js` and apply.

### B3 — Request-body + middleware hardening (S-H5 + M17 + S-L15 + S-M9/10, 1 PR)

**B3 checklist**

- [x] Replace `express.json()` with bounded strict JSON parsing
- [x] Mount rate limiting before JSON parsing
- [x] Optionally scope JSON parsing to mutating routes if that is the cleaner implementation
- [x] Add non-string guards in validation helpers
- [x] Replace `new URL(import.meta.url).pathname` with `fileURLToPath(import.meta.url)` in RAP route
- [x] Verify 17KB JSON returns 413
- [x] Verify abusive requests hit rate limiting before parsing

These edits cluster on `EdgeWARN/api/server.js` and validation helpers:

- `EdgeWARN/api/server.js:111`: replace `express.json()` with `express.json({ limit: '16kb', strict: true, type: 'application/json' })`.
- Reorder so `rateLimit` mounts **before** `express.json()` (covers S-H5 + L25).
- M17: alternative — mount `express.json` only on POST/PUT/PATCH routes if cleaner; the limit form satisfies both.
- `validation.js:22-58`: add `typeof !== 'string'` guards in `validateAlertId`, `validateCellId`, `validateTimestamp*` (S-M9/M10).
- `EWMRS/api/routes/rap.js:183`: replace `new URL(import.meta.url).pathname` with `fileURLToPath(import.meta.url)` (S-L15 — Windows correctness).

### B4 — Medium-severity polish (S-M3, S-M4, S-M7, 1 PR)

**B4 checklist**

- [ ] Document `keyGenerator` behavior and add startup warning for proxy misconfiguration
- [ ] Add short-TTL `LRUCache` wrappers around unbounded `fs.readdir` call sites
- [ ] Remove raw error-message details from EWMRS 500 responses while preserving server logs
- [ ] Defer internal-check token bypass unless coordinated client changes are ready

- S-M3: `keyGenerator` doc + production startup warning when `trust-proxy=false` and `X-Forwarded-For` observed.
- S-M4: wrap unbounded `fs.readdir` in `LRUCache(ttl: 5_000)` for `alerts.js:13-34`, `mesocyclones.js:9-23`, `metar.js:14-36`, `nexrad/filesystem.js:88-105, 107-125`.
- S-M7: drop `details: err.message` from EWMRS 500 bodies in `wpc.js`, `colormaps.js`; keep server-side log.

(S-M3 internal-check token bypass S-M3 — defer if it requires coordinated client changes.)

---

## Track C — Performance (A1 is already landed; numeric items still wait for Track C2)

**Track C checklist**

- [x] Complete C1 zero-numeric-change perf work
- [ ] Complete C2 numeric and algorithmic refactors with parity proof
- [ ] Complete C3 medium/low rolling cleanups
- [ ] Run Track C verification matrix per §7.3

Prefix `IMP[perf]:`. Verification matrix per §7.3.

### C1 — Zero-numeric-change perf (1-2 PRs)

**C1 checklist**

- [x] H2 `os.scandir` in `latest_files`
- [x] H4 hoist `multiprocessing.Manager()` to `main()` lifetime
- [x] H6 cache `cost_matrix[row, col]` into the `costs` dict
- [x] H7 hoist `AssignmentCostCalculator(config)` at line 401 only
- [x] H8 build candidate centroid array once outside the per-track loop
- [x] H11 context-manage `xr.open_dataset`
- [x] H12 gate `perf_tracker` behind env var
- [x] M3 add `compression()` filter for image responses in both API servers
- [x] M14 add `lru_cache((path, mtime))` for `_load_timestamp_tile_index`
- [x] M16 combine regex in `find_timestamp`
- [x] L20 cache colormap loading in `EWMRS/render/render.py`
- [x] L24 add `tracks_by_id` dict in `assignment.py`
- [x] Verify no filename or JSON envelope diffs versus baseline artifacts

Bundle by file/module to keep PRs focused. None of these alter byte output:

- **H2** `os.scandir` in `latest_files` (`src/util/file.py:211-237`).
- **H4** hoist `multiprocessing.Manager()` to `main()` lifetime (`src/run.py::_run_tandem_cycle`).
- **H6** cache `cost_matrix[row, col]` into the `costs` dict (`assignment.py`).
- **H7** hoist `AssignmentCostCalculator(config)` at line 401 (line 437 already done — do NOT re-edit).
- **H8** build candidate centroid array once outside per-track loop (`assignment.py:101-147`).
- **H11** context-manage `xr.open_dataset` (`util/handler.py:56-101`, `EWMRS/render/tools.py`).
- **H12** opt-in `perf_tracker` via env var (`util/performance.py:42-53`).
- **M3** `compression()` filter for image responses (both API servers).
- **M14** `lru_cache((path, mtime))` for `_load_timestamp_tile_index` (`EWMRS/pipeline.py:64-82`).
- **M16** combined regex in `find_timestamp` (`EWMRS/render/tools.py:126-156`).
- **L20** `lru_cache` cmap in `EWMRS/render/render.py:26-56`.
- **L24** `tracks_by_id` dict in `assignment.py:572-580`.

### C2 — Numeric/algorithmic refactors (separate PRs, each with diff verification)

**C2 checklist**

- [ ] H1 route `RAPPointExtractor.extract` callers through `extract_batch`
- [ ] H3 narrow `deepcopy(vector_previous_entries)`
- [ ] H5 replace Kalman gain `inv(S)` with a `solve` form only at `filter.py:236`
- [ ] H8 / H9 implement RGBA LUT single-pass while preserving the documented two-path split
- [ ] M21 switch RAP read to float32 at `rap/uint16_pipeline.py:199`
- [ ] Verify stormcell numeric diffs with `np.allclose(rtol=1e-5, atol=1e-8)`
- [ ] Verify render pixel diffs on sample tiles

Each ships in its own PR with stormcell JSON byte-diff + render PNG pixel-diff in the test plan. Strict §6 invariant compliance:

- **H1** route `RAPPointExtractor.extract` callers through `extract_batch` (`util/grib_loader.py:86-124`).
- **H3** narrow `deepcopy(vector_previous_entries)` (`detect/main.py:298`).
- **H5** Kalman gain: replace `inv(S)` with `solve` form at `filter.py:236`. Mahalanobis already done — do NOT re-edit. Verify against `tests/core/process/detect/kalman/` fixtures.
- **H8** + **H9** RGBA LUT single-pass via `searchsorted`, **preserving the `np.interp` vs `np.digitize` two-path split** (§6 invariant 8).
- **M21** float32 RAP read at `rap/uint16_pipeline.py:199`.

### C3 — Medium/Low rolling cleanups (rolling, batched by module)

**C3 checklist**

- [ ] Land M1, M2, M4, M5, M6, M7, M9, M10, M11, M12, M13, M15, M18, M19
- [ ] Land L1-L7, L9, L11, L12, L16, L18, L19, L22, L25
- [ ] Keep grouping by file/module so diffs remain reviewable
- [ ] Confirm no numeric impact for each batch

Items: M1, M2, M4, M5, M6, M7, M9, M10, M11, M12, M13, M15, M18, M19; plus L1–L7, L9, L11, L12, L16, L18, L19, L22, L25.

Group by file to keep diffs reviewable. No numeric impact on any of these. Land as bandwidth permits — not on the critical path.

---

## Track D — Optional / Gated (final phase)

**Track D checklist**

- [ ] Complete D1 MRMS sync fallback removal if parity proof passes
- [ ] Complete D2 historical pipeline consolidation if parity proof passes
- [ ] Complete D3 synoptic dual-path collapse if still worth review bandwidth
- [ ] Complete D4 opportunistic dead-code sweep category by category
- [ ] Run final gated parity checks before merge

Each requires explicit parity verification before merge. Land only after Tracks A/B/C are settled.

### D1 — MRMS sync fallback removal (D5 / R6, 1 PR)

**D1 checklist**

- [ ] Delete `download_all_files_sync_fallback`
- [ ] Delete `download_modifier_sync`
- [ ] Update `download_all_files` to call `asyncio.run(download_all_files_async_internal(...))` directly
- [ ] Produce one full real-time cycle artifact diff with no divergence

- Delete `download_all_files_sync_fallback`, `download_modifier_sync` (`common/ingest/mrms/downloader.py`).
- Update `download_all_files` in `mrms/main.py` to call `asyncio.run(download_all_files_async_internal(...))` directly.
- **Parity proof:** one full real-time cycle artifact diff before vs after.

### D2 — Historical pipeline consolidation (R4, 1 PR)

**D2 checklist**

- [ ] Switch `process_historical.py` to `run_tandem_ingest_cycle(include_goes=True, include_ewmrs=False)`
- [ ] Run fixed-window historical parity comparison
- [ ] Reject merge on any `<BASE_DIR>/data/` divergence

- Switch `process_historical.py` to `run_tandem_ingest_cycle(include_goes=True, include_ewmrs=False)`.
- **Mandatory parity proof:** byte-level diff `<BASE_DIR>/data/` over a fixed historical window (`process_historical.py --start <T0> --end <T0+1m>`) against pre-change run. Reject merge on any divergence.

### D3 — Synoptic dual-path collapse (R7, 1 PR, lowest priority)

**D3 checklist**

- [ ] Replace `SynopticFileDownloader` with `asyncio.run(...)` of the async function in a fresh loop
- [ ] Preserve async-first, sync-fallback, previous-hour retry semantics in `download_synoptic`
- [ ] Skip this PR if review bandwidth is tight

- Replace `SynopticFileDownloader` (sync class) with `asyncio.run(...)` of the async function in a fresh loop.
- Keep the per-call fallback semantics in `download_synoptic` (lines 64-99) — async first, sync fallback, retry previous hour.
- ~50 LOC saved. Skip if review bandwidth is tight.

### D4 — Opportunistic dead-code sweep (D9, rolling)

**D4 checklist**

- [ ] Split each sweep into one category per PR
- [ ] Keep any code that cannot be conclusively proven dead
- [ ] Include grep-proof in each PR before deletion

Per §4 D9 categories — each sweep entry survives only with grep-proof. Do **not** batch into one giant PR; one sweep category per PR (e.g. "remove unused heavy imports across `EdgeWARN/process`"). Default to keeping anything that cannot be conclusively proven dead.

---

## Cross-track dependencies

| If you ship | You must coordinate with |
|-------------|--------------------------|
| B3 (`express.json` limit + reorder) | M17 — same edit; pick one PR to own it |
| A1 D2 (delete `src/EWMRS/scheduler.py`) | Closed in the current tree; treat M7 as targeting only `EdgeWARN/schedule/scheduler.py` |
| C2 H5 (Kalman gain `solve`) | §6 invariants 1, 2, 4 — diff stormcell associations within `np.allclose(rtol=1e-5)` |
| C2 H9 (RGBA LUT) | §6 invariants 8, 9, 10 — preserve two-path split, exact cache key tuple, `alpha.any()` filter |
| A2 (compat shim removal) | All callers of `EdgeWARN.ingest.*` — none should remain; greenlight rule 6 |
| Any §3 fix touching validation regex | Greenlight rule 1 — only tighten, never loosen |

---

## Critical files (consolidated)

Modified across multiple tracks:

- `src/EdgeWARN/api/server.js` — Track B1, B3
- `src/EWMRS/api/server.js` — Track B1
- `src/EdgeWARN/api/utils/fileReader.js` — Track B2
- `src/EWMRS/api/routes/renders.js` — Track B2
- `src/EdgeWARN/process/detect/kalman/filter.py` — Track C2 H5
- `src/EdgeWARN/process/detect/kalman/assignment.py` — Track C1 (H6, H7-line-401, H8, L24)
- `src/util/file.py` — Track C1 H2
- `src/util/grib_loader.py` — Track C2 H1
- `src/EWMRS/render/render.py` — Track C2 H9
- `src/run.py` — Track C1 H4
- `src/EdgeWARN/pipeline.py` + `__init__.py` — Track A1 D3
- `src/common/ingest/nws/main.py` — Track A1 D6
- `src/common/ingest/nexrad/coordinator.py` + `main.py` — Track A1 D4
- `src/EdgeWARN/process/integrate/core/integrator.py` — Track A1 D8

Reusable helpers to leverage rather than reinvent:

- `extract_batch` (`src/util/grib_loader.py:126-206`) — already vectorized; H1 just routes to it.
- `resolveUnder` (`src/EWMRS/api/routes/nexrad/filesystem.js`) — reuse for S-H4.
- `PRODUCT_MAPPING` (`src/EWMRS/api/routes/renders.js`) — now consistently used by `/download`, `/tile`, `/tile-info`, and `/fetch`.
- `LRUCache` (already present in `fileReader.js`) — reuse for S-M4.
- `fileURLToPath` (Node stdlib) — already adopted for the RAP route; reuse it for any future `import.meta.url` path resolution.

---

## Verification (per phase)

Run before merging each PR. Cross-reference §7.

### Universal (every PR)

- `npm test` green
- `python -m pytest tests/` green
- 1 tandem cycle of `python src/run.py --lat_limits 20 55 --lon_limits 230 300 --disable-ctam --disable-tracking` succeeds with no new errors

### Track-specific

| Track | Additional verification |
|-------|-------------------------|
| A1, A2 | post-deletion `grep -r <symbol> src/ tests/ scripts/ .` returns zero hits |
| B1 (CORS) | same-origin GET succeeds; cross-origin GET without `ALLOWED_ORIGINS` rejected |
| B2 (paths) | manual exploit attempt: `product=..`, `product=CON`, symlink under `cells/` → 404 each; legitimate inputs still 200 |
| B3 (body) | POST 17KB JSON → 413; rate-limit triggers before parse on abusive bodies |
| C1 | `<BASE_DIR>/{data,gui,wpc}` filename + JSON envelope diff is empty against pre-change baseline |
| C2 | `np.allclose(rtol=1e-5, atol=1e-8)` on stormcell numeric fields; pixel diff on sample tiles |
| D1, D2 | full file-set parity diff over the relevant cycle window |
| D4 | per-symbol grep evidence in PR description |

### Final phase greenlight (before declaring roadmap complete)

Walk the §6.6 15-rule greenlight checklist against the cumulative diff. Sign off on the 28 invariants in §6.1–§6.5. Run OWASP ZAP baseline against both API servers.

---

## Out of scope for this roadmap

- Anything not in the audit doc.
- New features.
- Test additions beyond regression coverage for the items above (the audit deliberately doesn't touch test surface).
- Refactors of unmentioned modules.

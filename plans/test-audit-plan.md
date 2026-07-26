# Test Suite Audit: Edge Cases & Bloat Analysis

## 1. Bloat / Low-Value Tests (Remove or Consolidate)

### 1.1 Implementation-Detail Server Tests (3+1 tests)

| File | Test Name | Problem |
|------|-----------|---------|
| `tests/api/test_server.js` | `'starts a worker server with the requested host and port'` | Mocks `app.listen`, asserts call args — tests internal wiring, not behavior |
| `tests/api/test_server.js` | `'forks up to four workers in primary cluster mode and restarts on exit'` | Mocks `cluster.fork`/`cluster.on`, asserts call counts — white-box only |
| `tests/api/test_server.js` | `'starts the worker branch when cluster mode is not primary'` | Same pattern, tests `cluster.isPrimary` branching |
| `tests/api/test_ewmrs_server.js` | `'starts the server with the requested port'` | Same `app.listen` mock pattern as EdgeWARN version |

**Expected behavior:** Server actually binds to a random port and responds to real HTTP requests. Replace with `server.listen(0)` + real `fetch`/`supertest` assertions.

**Verdict: REMOVE** (4 tests) or rewrite as behavioral tests.

---

### 1.2 Tautological / No-Op Tests

| File | Test Name | Problem |
|------|-----------|---------|
| `tests/unit/test_lineage.py` | `test_full_overlap_returns_one` | `calculate_overlap_ratio(polygon, polygon)` is mathematically guaranteed to return 1.0. Tests identity, not logic. |
| `tests/util/test_release.py` | `test_get_release_version_matches_package_json` | Asserts hardcoded `"2.7.0"`. Fails on every version bump. Doesn't actually read `package.json`. |
| `tests/repro_morphology.py` | `test_morphology_logic` (standalone) | Uses `print("PASS"/"FAIL")` — no pytest assertions. Will never fail a test runner. |
| `tests/api/performance/test_benchmarks.js` | Both tests | Compute and `console.log` improvement percentages, assert no performance thresholds. The only assertions are correctness sanity checks duplicated from `test_validation.js`. |

**Expected behavior for `calculate_overlap_ratio`:** Two distinct polygons with partial overlap return a ratio in (0, 1). Already tested in `test_partial_overlap_returns_ratio`.

**Expected behavior for version:** `get_release_version()` matches the version in `package.json` at runtime. The test should read `package.json` dynamically.

**Verdict: REMOVE** `test_full_overlap_returns_one`, `test_repro_morphology.py` (or convert to real pytest). **CONVERT** `test_release` to read `package.json` dynamically. **REMOVE or CONVERT** benchmarks to assert a regression threshold.

---

### 1.3 Tests That Don't Test What They Claim

| File | Test Name | What It Actually Does |
|------|-----------|----------------------|
| `tests/api/routes/test_v2_data_metar.js` | `'should extract hour from timestamp correctly'` | Writes file at hour `09z`, queries `20231015-093000`, asserts `timestamp` field in response is `20231015-093000`. Tests echo-back, not hour extraction. If the route had a bug returning the wrong hour, this test would still pass because it only asserts the round-trip. |
| `tests/integration/test_ingest_to_detect.py` | `test_metar_ingest_to_detect` | Opens a JSON file, checks `len(data["stations"]) == 2`. Does not call any detection or ingest code. |
| `tests/integration/test_ingest_to_detect.py` | `test_full_ingest_pipeline` | Checks `radar_file.exists()`, `ps_file.exists()`, `xr.open_dataset(radar_file) is not None`. No pipeline runs. |

**Verdict:** REWRITE `test_v2_data_metar` to actually verify hour-specific file selection logic. REMOVE `test_metar_ingest_to_detect` (2 of 4 tests in that file are useless). REWRITE `test_full_ingest_pipeline` to actually run the pipeline or REMOVE.

---

### 1.4 Tests That Test the Runtime / Language Itself

| File | Test Name | Problem |
|------|-----------|---------|
| `tests/api/utils/test_fileReader.js` | `'should handle nested JSON structures'` | Tests Node.js `JSON.parse`/`JSON.stringify` behavior, not custom code. `readJsonFileSafe` does no structural transformation. |
| `tests/core/ctam/test_interface.py` | All 6 tests | Test Python's `abc.ABC` mechanism — that abstract classes can't be instantiated, that subclasses must implement abstract methods. Tests the language, not the application. |

**Verdict:** REMOVE both.

---

### 1.5 Duplicate / Consolidatable Pattern Tests

| File | Test Names | Issue |
|------|-----------|-------|
| `tests/unit/test_critical_fixes.py` | 3 `*_warns_on_missing_yaml` tests | Structurally identical: call `from_yaml("nonexistent")`, check for `"not found"` warning. Only the config key differs. |
| `tests/unit/test_kalman_filter.py` | `test_state_vector_initialization`, `test_covariance_initialization`, `test_prediction_state_initialization` | Each tests that a dataclass/class initializes with expected default values. Trivial getter tests. |
| `tests/unit/test_kalman_assignment.py` | `test_assignment_result_creation` | Tests a dataclass constructor. |
| `tests/api/routes/test_health.js` | Tests 1, 2, 5 | All overlap on asserting `status`/`timestamp` shape. Test 5 subsumes Test 1. |

**Verdict:** PARAMETERIZE the 3 YAML tests into one. REMOVE or CONSOLIDATE dataclass constructor tests. MERGE health.js tests 1+5.

---

## 2. Significant Overlaps / Redundancies

### 2.1 High: `test_kalman_tracking.py` ↔ `test_tracking_assignment.py`

Both test `StormCellTracker.update_cells` with near-identical scenarios:

| `test_kalman_tracking.py` | `test_tracking_assignment.py` |
|---|---|
| `test_update_cells_all_matched` | `test_simple_update_with_hybrid` |
| `test_update_cells_one_dropped_enters_prediction` | `test_prediction_mode_entry` |
| `test_reacquisition_within_radius` | `test_reacquisition_from_prediction` |
| `test_prediction_terminates_after_time_limit` | `test_termination_after_timeout` |

`test_tracking_assignment.py` uniquely covers crossed paths, splits, merges. `test_kalman_tracking.py` uniquely covers StormCast velocity and statistics logging.

**Verdict:** CONSOLIDATE — keep the unique scenarios from each, remove the 4 overlapping scenarios from one file (preferably `test_kalman_tracking.py` since `test_tracking_assignment.py` has the richer fixture setup).

### 2.2 Medium: `test_lineage.py` ↔ `test_lineage_integration.py` ↔ `test_high_fixes.py`

All three test merge/split detection, hysteresis buffer requiring two confirmations, and buffer persistence. The hysteresis "requires two scans" logic appears in all three.

**Verdict:** Keep in `test_lineage.py` (the unit-test level). Remove from `test_lineage_integration.py` and `test_high_fixes.py` (already covered + annotated as fix H2).

### 2.3 Medium: `test_ingest_to_detect.py` ↔ `test_detect_to_integrate.py`

Same mocked `detect_cells` setup. Weak pipeline coverage.

**Verdict:** After removing the 2 useless tests in `test_ingest_to_detect.py`, the remaining 2 tests (`test_mrms_ingest_to_detect`, `test_nws_ingest_to_detect`) still overlap heavily with `test_detect_flow.py`. Consider removing the redundant ones.

---

## 3. Missing Edge Cases

### 3.1 Detection

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **No cells detected (all reflectivity below threshold)** | `detect_cells` returns `[]`. No crash on `result[0]`. Downstream stages handle zero cells gracefully. | `test_detect_flow.py` |
| **Corrupted MRMS file (garbage bytes)** | `detect_cells` logs error via `IOManager.write_error`, returns `None` or empty result. No uncaught exception. | `test_detect_flow.py` |
| **Detection at domain boundaries (cells cut off by lat/lon edges)** | `GateMapper.draw_bbox` clamps to valid grid indices. No negative indices or out-of-bounds array access. | `test_gatemapper_bbox.py` |
| **Very large number of cells (100+)** | Detection completes in reasonable time. No O(n²) blowup. All cells have valid entries. | `test_main.py` (or `benchmarks/`) |

### 3.2 Integration

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **GLM file with zero flashes** | `integrate_glm` returns cells unchanged. No crash iterating empty dataset. Logs debug message. | `test_integrate_glm.py` |
| **Azimuthal shear with empty GRIB (no valid sweeps)** | `open_azshear_dataset` returns `None`. Integration skips, sets `azshear` to `None` on cells. No KeyError. | `test_integrate.py` |
| **Integration with empty cell list** | Returns `[]` immediately. No file I/O or spatial queries on zero cells. | `test_integrate.py` |
| **Timestamps crossing midnight boundary** | File paths and lookups work across day boundaries. No off-by-one on date rollover. | `test_integrate.py` |
| **RAP with zero valid cell data** | Returns cells with RAP fields set to `None`/`0`. No division-by-zero on wind calculations. | `test_integrate_rap.py` |

### 3.3 Tracking / Kalman

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Kalman with NaN measurements** | `update_step` rejects NaN measurement, logs warning. Covariance stays positive-definite. | `test_kalman_filter.py` |
| **All cells dissipated simultaneously** | `update_cells` marks all as `DISSIPATED`. Zero active tracks. No crash. | `test_track.py` |
| **Zero-velocity initialization** | `StateVector` initializes `vx=0, vy=0`. `get_bearing` returns `None` or `0`. No division-by-zero. | `test_kalman_filter.py` |
| **KF divergence (measurements increasingly inconsistent)** | Mahalanobis distance exceeds gating threshold → measurement rejected. Tracker enters prediction-only mode. After `max_missed` → terminated. | `test_kalman_filter.py` |
| **Long tracking sequence (100+ frames)** | No memory leak. Track IDs don't overflow. Buffer pruning keeps set bounded. | `test_kalman_tracking.py` |

### 3.4 Alerts

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Extremely long cell_id / 10K+ point geometry** | `publish` truncates or rejects. No unbounded memory allocation. JSON serialization does not produce MB-size strings. | `test_manager.py` |
| **Concurrent publish/load (race condition)** | Atomic temp+rename ensures consistent reads. No partial-file reads. | `test_manager.py` |
| **Partial write (crash mid-publish)** | On restart, partially-written file is detected as invalid JSON and cleaned up or skipped. | `test_manager.py` |

### 3.5 NWS Ingest

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **HTTP 429 rate limit** | Retries with backoff using `Retry-After` header. After exhaustion, logs error and returns gracefully. Ingest loop continues. | `test_main.py` (nws) |
| **Non-GeoJSON response format** | Parser detects invalid format, logs error, returns `None`/empty. No `response["features"]` KeyError. | `test_main.py` (nws) |
| **Network timeout** | `download` catches `requests.Timeout`. Registry retains previous state. Does not clear existing alerts. | `test_main.py` (nws) |
| **Empty DROPPED_EVENTS config** | No events are filtered. All events pass through. No crash iterating `None`. | `test_main.py` (nws) |

### 3.6 MRMS / GOES Ingest

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **MRMS all-NaN data** | Detection sees no valid reflectivity > threshold → zero cells. Render produces blank image. No NaN → image encode crash. | `test_downloader.py` (mrms), `test_ewmrs_render.py` |
| **GOES unexpected band naming** | Channel filter returns no files. `goes_ready` returns `False`. Pipeline skips GOES phase gracefully. | `test_async_compat.py` |
| **Network error mid-stream** | Partial file is cleaned up. Download retried on next cycle. No corrupted file left on disk. | `test_downloader.py` (mrms) |
| **Subsetting at extreme bounds** | `load_subset` clips to valid grid indices. Returns max valid slice. If no overlap, returns empty dataset. | `test_parse.py` (mrms) |

### 3.7 Geometry / Coordinates

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Cell at antimeridian (180°)** | Polygon expansion handles dateline crossing correctly. Longitudes stay in valid range consistently [-180, 180] or [0, 360]. | `test_gatemapper_bbox.py` |
| **Lat/lon at poles (±90°)** | No division-by-zero in lat→meter conversions. `cos(lat)` near pole is clamped to minimum value. | `test_gatemapper_bbox.py` |
| **Near-zero-area polygons** | `calculate_overlap_ratio` returns `0.0`. No division-by-zero on `polygon.area`. `GateMapper` skips degenerate polygons. | `test_gatemapper_connectivity.py` |

### 3.8 API Routes

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Concurrent requests under rate limit** | Rate limiter correctly blocks excess requests. Legitimate requests pass. No false positives. | `test_server.js` |
| **Very large response payloads** | API streams or paginates. Does not buffer multi-GB JSON in memory. Appropriate `Content-Length`. | `test_v2_features_cells.js` |
| **Malformed JSON bodies** | Returns 400 with descriptive error. No crash. `express.json()` error middleware catches parse failures. | `test_server.js` |
| **URL-encoded directory traversal** | `isSafeFilename`/`readJsonFileSafe` normalize and reject `%2e%2e%2f`, `%2e%2e%5c`. No path escape. | `test_v2_features_cells.js`, `test_fileReader.js` |

### 3.9 Scheduler / Pipeline

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Both S3 and HTTPS fallback fail** | Scheduler logs critical error, returns no new timestamps. Existing data preserved. Loop continues retrying (does not exit). | `test_scheduler_fallback.py` |
| **Zero MRMS timestamps in range** | `latest_common_minute_1h` returns `None`. Pipeline skips the cycle. No crash on `None` timestamp. | `test_scheduler.py` |
| **All downstream stages disabled** | Pipeline no-ops for each disabled stage. Returns clean success. No `None` module reference crashes. | `test_pipeline.py` (core) |

### 3.10 Filesystem / I/O

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Permission denied on write** | `write_json_atomic` catches `PermissionError`, logs error, returns failure. No stale temp file. | `test_file.py` |
| **Disk full** | Write raises `OSError("No space left on device")`. Caller handles gracefully, cleans up temp file. | `test_file.py` |
| **File locked by another process** | Read operation retries or fails with clear error. Does not deadlock. | `test_io.py` |

### 3.11 EWMRS Render

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **All-NaN data array** | Render produces blank/masked tile or returns error. No NaN→image encoding crash. | `test_ewmrs_pipeline.py` |
| **Invalid/negative zoom level** | Returns 400 Bad Request. No path traversal to parent directories. | `test_ewmrs_pipeline.py` |
| **GUI cleanup on locked file** | Skips locked file (logs warning). Removes other eligible files. Does not block. | `test_ewmrs_pipeline.py` |

### 3.12 NEXRAD

| Edge Case | Expected Behavior | File to Add To |
|-----------|------------------|----------------|
| **Volume with zero chunks (empty on S3)** | Coordinator returns `skipped_incomplete_remote`. No zero-byte files written. | `test_nexrad_pipeline.py` |
| **Corrupted chunk data (CRC mismatch)** | Parser detects invalid magic or truncated data. Skips chunk, logs error. Worker does not crash. | `test_nexrad_parser.py` |
| **Site disappears after catalog fetch but before ingest** | Failing site gets `site_error`. Other sites continue. No deadlock or hang. | `test_nexrad_coordinator.py` |

---

## 4. Summary: Action Items

| Priority | Action | Files Affected |
|----------|--------|---------------|
| **High** | Rewrite 4 server startup tests as real behavioral tests | `test_server.js`, `test_ewmrs_server.js` |
| **High** | Remove `test_metar_ingest_to_detect` and `test_full_ingest_pipeline` | `test_ingest_to_detect.py` |
| **High** | Remove `test_repro_morphology.py` or convert to real pytest | `repro_morphology.py` |
| **High** | Consolidate overlapping `StormCellTracker` scenarios between kalman/tracking | `test_kalman_tracking.py`, `test_tracking_assignment.py` |
| **Medium** | Parameterize 3 missing-YAML tests into 1 | `test_critical_fixes.py` |
| **Medium** | Remove tautological `test_full_overlap_returns_one` | `test_lineage.py` |
| **Medium** | Convert `test_release` to read `package.json` dynamically | `test_release.py` |
| **Medium** | Remove `test_interface.py` (tests Python ABC, not app code) | `test_interface.py` (ctam) |
| **Medium** | Rewrite `test_v2_data_metar` hour-extraction test to actually test extraction logic | `test_v2_data_metar.js` |
| **Medium** | Consolidate hysteresis buffer tests into one file | `test_lineage.py`, `test_lineage_integration.py`, `test_high_fixes.py` |
| **Low** | Merge overlapping health route tests 1+5 | `test_health.js` |
| **Low** | Add performance assertions to benchmarks (or convert to scripts) | `test_benchmarks.js` |
| **Low** | Remove Node.js JSON round-trip test | `test_fileReader.js` |
| **Low** | Remove dataclass constructor tests | `test_kalman_filter.py`, `test_kalman_assignment.py` |
| **Ongoing** | Add missing edge case tests (see Section 3) | ~25 files |

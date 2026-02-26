# TODO: Storm Cell Tracking System - Code Audit Findings

**Source Report:** `code_audit_report.md` (Generated: 2026-02-24)

This document tracks the resolution of issues found during the comprehensive code audit of the Kalman filter, Merger/Splitter logic, and Termination handling.

## 🔴 Critical Issues

- [x] **C1: Merge Processing Corrupts Cell ID**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Dominant parent's Kalman filter is orphaned because the merged cell takes the child's ID but the KF is kept under the dominant parent's ID.
  - **Fix:** Migrate KF from `dominant_parent` key to `child_id` key, or restore dominant parent's ID. Reset prediction state using `dominant_parent`.
- [x] **C2: Dead Code - `_check_reacquisition`**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Method is never called; reacquisition is handled by Hungarian/greedy assignment instead.
  - **Fix:** Remove dead code or integrate it. Update documentation.
- [x] **C3: Config Default Mismatch**
  - **Location:** `kalman.yaml` vs `config.py`
  - **Issue:** Python default for `max_prediction_time_minutes` is 10.0, but YAML sets it to 6. Silent fallback if YAML fails to load.
  - **Fix:** Align Python default with YAML value (6.0), add a warning log on fallback.

## 🟠 High Issues

- [x] **H1: Split Processing KF Migration**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Dominant child gets parent's ID initially, but KF updates use parent ID while cell is updated to child ID. Also, parent's KF is never removed.
  - **Fix:** Ensure KF state correctly follows the dominant child, clean up old KF states for the parent.
- [x] **H2: Hysteresis Buffer Fails to Enforce Consecutive Scans**
  - **Location:** `src/EdgeWARN/core/process/detect/lineage/buffer.py`
  - **Issue:** `count` increments unconditionally even if detections skip scans.
  - **Fix:** Reset count on non-consecutive detections (e.g., track `last_scan_number`).
- [x] **H3: Overlap Direction Asymmetry for Splits**
  - **Location:** `src/EdgeWARN/core/process/detect/lineage/spatial.py`, `detector.py`
  - **Issue:** Split detection calculates overlap relative to the *new* cell area rather than the *old* cell area. This can cause missed splits for fragmenting storms.
  - **Fix:** Review and correct the overlap ratio direction for split detection.
- [x] **H4: Bare `except` in KF Update**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Silently swallows all errors (including `KeyboardInterrupt`) when parsing timestamps.
  - **Fix:** Replace with `except (ValueError, TypeError)`.

## 🟡 Medium Issues

- [x] **M1: Process Noise Scaling**
  - **Location:** `src/EdgeWARN/core/process/detect/kalman/filter.py`
  - **Issue:** Linear scaling (`Q * dt`) underestimates positional noise for large `dt`.
  - **Fix:** Implement proper discrete-time Q matrix for constant-acceleration model.
- [x] **M2: Longitude Transition Matrix Error**
  - **Location:** `src/EdgeWARN/core/process/detect/kalman/filter.py`
  - **Issue:** Fails to adjust longitude change for latitude (`cos(lat)` correction missing), causing ~18% positional error.
  - **Fix:** Add `cos(lat)` correction for longitude updates.
- [x] **M3: Missing Reflectivity Decay Monitoring**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Unconditionally resets decay state upon ProbSevere match, ignoring the `< 30 dBZ` logic from the PRD.
  - **Fix:** Implement proper transition to DECAYING state based on reflectivity.
- [x] **M4: Shallow Copies on Cell Dicts**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Using `.copy()` on cells shares nested structures like `bbox`.
  - **Fix:** Use `copy.deepcopy()` or manually copy nested objects.
- [x] **M5: Missing Cost Check for Single Candidates**
  - **Location:** `src/EdgeWARN/core/process/detect/kalman/assignment.py`
  - **Issue:** Single-candidate assignments only check spatial gating, skipping total cost limits (velocity, shape).
  - **Fix:** Enforce maximum cost validation for single candidate paths.
- [x] **M6: Memory Leak for KF Objects**
  - **Location:** `src/EdgeWARN/core/process/detect/track.py`
  - **Issue:** Kalman filters are never cleaned up for merged parents or typical exits (only handled for explicitly terminated cells).
  - **Fix:** Clean up dictionaries for orphaned track IDs.

## 🔵 Low Issues

- [x] **L1: Buffer Scan Interval Mismatch**
  - **Location:** `src/EdgeWARN/core/process/detect/lineage/buffer.py`
  - **Issue:** `scan_interval_seconds` defaults to 300s, but scans are typically 120s, resulting in overly long pruning thresholds.
  - **Fix:** Update default parameter to match reality.
- [x] **L2: Hardcoded Latitude in Initialization**
  - **Location:** `src/EdgeWARN/core/process/detect/kalman/state.py`
  - **Issue:** Hardcodes latitude to 35° when bootstrapping longitudinal covariance.
  - **Fix:** Use actual cell latitude.
- [x] **L3: Greedy Assignment is Detection-Centric**
  - **Location:** `src/EdgeWARN/core/process/detect/kalman/assignment.py`
  - **Issue:** Assigns by max reflectivity rather than shortest distance first.
  - **Fix:** Change greedy sorting to distance/cost-based ranking.
- [x] **L4: Polynomial Coordinate Count**
  - **Location:** `src/EdgeWARN/core/process/detect/lineage/spatial.py`
  - **Issue:** Degenerate collinear 3-point polygons give zero area silently.
  - **Fix:** Document constraint or provide minimum dimension checks.

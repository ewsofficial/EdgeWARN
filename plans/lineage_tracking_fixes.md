# Lineage and Tracking Fixes Implementation Plan

This plan aims to address four key areas of improvement identified during the review of the EdgeWARN-Core detection pipeline's lineage and tracking modules: the asymmetric overlap ratio calculation, KF state drift in secondary children during splits, leftover `print()` statements, and expensive polygon instantiations.

## Proposed Changes

### 1. Asymmetric Overlap Ratio Calculation
*   **File:** `src/EdgeWARN/core/process/detect/lineage/spatial.py`
*   **Location:** `calculate_overlap_ratio` / `find_overlapping_cells`
*   **Issue:** The `calculate_overlap_ratio` currently divides the intersection area by the `parent_poly.area`. In symmetric 1-to-1 matching, a rapidly shrinking cell will yield a low overlap ratio (due to the large old area), potentially causing the tracker to lose it.
*   **Solution:** Modify `find_overlapping_cells` to pass a symmetric ratio (like Intersection over Min Area or Intersection over Union) when called from `detector.py`'s 1-to-1 match section, or adjust the `calculate_overlap_ratio` to accept an `overlap_mode` parameter (`'parent'`, `'child'`, `'min'`, `'iou'`).
    *   **Merge:** Use `'child'` relative area.
    *   **Split:** Use `'parent'` relative area.
    *   **1-to-1:** Use `'min'` area or `'iou'`.

### 2. KF State Drift in Secondary Children (Splits)
*   **File:** `src/EdgeWARN/core/process/detect/track.py`
*   **Location:** `StormCellTracker.update_cells` (Split Processing)
*   **Issue:** When a cell splits, the dominant child correctly inherits the parent's KF state. However, secondary children are treated as entirely new detections, losing their historical trajectory context.
*   **Solution:** Before calling `self._update_kalman_with_observation(new_entry, child_id)` for secondary children, initialize their KF using a deepcopy of the parent's KF state, adjusted for the new child's centroid. This preserves the velocity history.
    ```python
    # Draft logic for track.py
    elif split.parent_id in self._kalman_filters:
        # Clone parent's KF for the new secondary child
        parent_kf = self._kalman_filters[split.parent_id]
        new_kf = parent_kf.clone()  # Assuming a clone method exists or can be written
        # Update with new observation to shift position but keep velocity
        new_kf.update(KalmanObservation(lat=child_data['centroid'][0], ...))
        self._kalman_filters[child_id] = new_kf
    ```

### 3. Leftover Print Statements
*   **File:** `src/EdgeWARN/core/process/detect/track.py`, `src/EdgeWARN/core/process/detect/kalman/assignment.py`
*   **Issue:** Multiple `print()` statements remain from debugging, causing log spam.
*   **Solution:** Replace all instances of `print(...)` with `self.io_manager.write_debug(...)` or `self.io_manager.write_info(...)` using the existing `self.io_manager` or a passed logger.

### 4. Expensive Polygon Instantiations
*   **File:** `src/EdgeWARN/core/process/detect/lineage/spatial.py`
*   **Location:** `build_spatial_index`, `calculate_overlap_ratio`
*   **Issue:** `calculate_overlap_ratio` converts coordinate lists to Shapely `Polygon` objects on every call. This is highly inefficient when evaluating many potential matches.
*   **Solution:**
    1.  Update `build_spatial_index` to construct the `Polygon` once and cache it inside `cells_data[cell_id]['poly_geom']`.
    2.  Update `calculate_overlap_ratio` to accept pre-built `Polygon` objects. If lists are passed (fallback), build the polygon internally.
    3.  Ensure `detect_lineage_events` uses the cached polygons when traversing overlaps.

### 5. Review Kalman Filter Metric Inputs
*   **File:** `src/EdgeWARN/core/process/detect/filter.py`
*   **Issue:** Ensure that `control_u`/`control_v` (in m/s) are correctly converted to degrees when passed to `kf.predict()` to avoid unrealistic jumps in the prediction state.
*   **Solution:** Inspect `filter.py` and implement a lat/lon degree conversion inside the prediction step if it assumes uniform metrics.

## Verification Plan

### Automated Tests
1.  Run the existing tracking tests manually:
    ```bash
    pytest tests/integration/test_kalman_tracking.py
    pytest tests/integration/test_tracking_assignment.py
    pytest tests/unit/test_lineage.py
    pytest tests/integration/test_lineage_integration.py
    ```
2.  Add a specific unit test in `tests/unit/test_lineage.py` that verifies a shrinking geometry successfully retains its 1-to-1 match using the updated symmetric overlap formula.
3.  Add a test inside `tests/integration/test_kalman_tracking.py` to assert that secondary children inherit velocity traits during a simulated split.

### Manual Verification
1.  Run the pipeline iteratively on a known split event dataset.
2.  Verify the STDOUT logs are free of raw `print` debugging lines.
3.  Monitor execution time (via `util.performance`) to ensure the spatial indexing optimizations yield a reduction in the "Detection - Tracking" phase time.

## 2025-02-18 - Optimized Coordinate Extraction in Gatemapper
**Learning:** Python loops for coordinate transformation are significantly slower than NumPy vectorization, especially when processing large arrays of coordinates.
**Action:** Always prefer `numpy` vectorized operations for coordinate transformations and lookups. Use `np.clip` for bounds checking instead of `max(min())` in a loop.

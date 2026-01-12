## 2026-01-12 - [Vectorization of Coordinate Lookups]
**Learning:** Explicit Python loops for coordinate lookups in large grids (even when subsampled) significantly impact performance (~10x slower) compared to NumPy vectorization.
**Action:** Always prefer `np.column_stack` and vectorized indexing over list comprehensions when processing `scikit-image` contours or grid coordinates.

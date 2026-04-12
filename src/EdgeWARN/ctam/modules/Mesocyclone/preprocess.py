from typing import Dict

import numpy as np
from scipy import ndimage

from . import config as cfg


def _clean_input(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32)
    if not arr.flags.writeable:
        arr = arr.copy()
    arr[~np.isfinite(arr)] = np.float32(0.0)
    arr[arr < np.float32(cfg.NOISE_FLOOR)] = np.float32(0.0)
    return arr


def _post_smoothing_cleanup(smoothed: np.ndarray) -> np.ndarray:
    smoothed[smoothed < np.float32(cfg.NOISE_FLOOR)] = np.float32(0.0)
    if not cfg.ENABLE_MORPH_CLEANUP:
        return smoothed

    structure = np.ones((cfg.MORPH_STRUCTURE_SIZE, cfg.MORPH_STRUCTURE_SIZE), dtype=bool)
    binary = smoothed >= cfg.DETECTION_THRESHOLD
    binary = ndimage.binary_opening(binary, structure=structure)
    binary = ndimage.binary_closing(binary, structure=structure)
    smoothed[~binary] = np.float32(0.0)
    return smoothed


def _active_tiles(active_rows: np.ndarray, active_cols: np.ndarray, tile_size: int) -> list[tuple[int, int]]:
    if active_rows.size == 0:
        return []

    tile_rows = active_rows // tile_size
    tile_cols = active_cols // tile_size
    packed = tile_rows.astype(np.int64) << 32 | tile_cols.astype(np.int64)
    unique = np.unique(packed)
    return [(int(item >> 32), int(item & 0xFFFFFFFF)) for item in unique.tolist()]


def _preprocess_tiled(arr: np.ndarray) -> np.ndarray:
    active_rows, active_cols = np.where(arr >= np.float32(cfg.NOISE_FLOOR))
    if active_rows.size == 0:
        return np.zeros_like(arr, dtype=np.float32)

    tile_size = max(int(cfg.PREPROCESS_TILE_SIZE), 1)
    halo = max(int(cfg.PREPROCESS_HALO_CELLS), 0)
    result = np.zeros_like(arr, dtype=np.float32)
    tile_coords = _active_tiles(active_rows, active_cols, tile_size)

    for tile_row, tile_col in tile_coords:
        row_start = tile_row * tile_size
        col_start = tile_col * tile_size
        row_end = min(row_start + tile_size, arr.shape[0])
        col_end = min(col_start + tile_size, arr.shape[1])

        halo_row_start = max(row_start - halo, 0)
        halo_col_start = max(col_start - halo, 0)
        halo_row_end = min(row_end + halo, arr.shape[0])
        halo_col_end = min(col_end + halo, arr.shape[1])

        window = np.array(arr[halo_row_start:halo_row_end, halo_col_start:halo_col_end], copy=True)
        smoothed = np.empty_like(window, dtype=np.float32)
        ndimage.gaussian_filter(window, sigma=cfg.SMOOTHING_SIGMA, output=smoothed)
        smoothed = _post_smoothing_cleanup(smoothed)

        inner_row_start = row_start - halo_row_start
        inner_col_start = col_start - halo_col_start
        inner_row_end = inner_row_start + (row_end - row_start)
        inner_col_end = inner_col_start + (col_end - col_start)
        result[row_start:row_end, col_start:col_end] = smoothed[inner_row_start:inner_row_end, inner_col_start:inner_col_end]

    return result


def preprocess_azshear_grid(values: np.ndarray) -> np.ndarray:
    arr = _clean_input(values)
    if not cfg.ENABLE_TILED_PREPROCESS:
        smoothed = np.empty_like(arr, dtype=np.float32)
        ndimage.gaussian_filter(arr, sigma=cfg.SMOOTHING_SIGMA, output=smoothed)
        return _post_smoothing_cleanup(smoothed)

    return _preprocess_tiled(arr)


def preprocess_inputs(grids: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    return {
        "low": preprocess_azshear_grid(grids["low"]),
        "mid": preprocess_azshear_grid(grids["mid"]),
        "reflectivity": np.asarray(grids["reflectivity"], dtype=np.float32),
    }

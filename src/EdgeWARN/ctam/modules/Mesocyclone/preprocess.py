from typing import Dict

import numpy as np
from scipy import ndimage

from . import config as cfg


def preprocess_azshear_grid(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    arr = np.where(np.isfinite(arr), arr, np.nan)
    arr = np.where(arr >= cfg.NOISE_FLOOR, arr, 0.0)

    smoothed = ndimage.gaussian_filter(np.nan_to_num(arr, nan=0.0), sigma=cfg.SMOOTHING_SIGMA)
    smoothed = np.where(smoothed >= cfg.NOISE_FLOOR, smoothed, 0.0)

    if not cfg.ENABLE_MORPH_CLEANUP:
        return smoothed

    structure = np.ones((cfg.MORPH_STRUCTURE_SIZE, cfg.MORPH_STRUCTURE_SIZE), dtype=bool)
    binary = smoothed >= cfg.DETECTION_THRESHOLD
    binary = ndimage.binary_opening(binary, structure=structure)
    binary = ndimage.binary_closing(binary, structure=structure)
    return np.where(binary, smoothed, 0.0)


def preprocess_inputs(grids: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    return {
        "low": preprocess_azshear_grid(grids["low"]),
        "mid": preprocess_azshear_grid(grids["mid"]),
        "reflectivity": np.asarray(grids["reflectivity"], dtype=float),
    }

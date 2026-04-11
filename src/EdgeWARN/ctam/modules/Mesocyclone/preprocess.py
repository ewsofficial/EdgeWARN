from typing import Dict

import numpy as np
from scipy import ndimage

from . import config as cfg


def preprocess_azshear_grid(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32).copy()
    arr[~np.isfinite(arr)] = np.float32(0.0)
    arr[arr < np.float32(cfg.NOISE_FLOOR)] = np.float32(0.0)

    smoothed = np.empty_like(arr, dtype=np.float32)
    ndimage.gaussian_filter(arr, sigma=cfg.SMOOTHING_SIGMA, output=smoothed)
    smoothed[smoothed < np.float32(cfg.NOISE_FLOOR)] = np.float32(0.0)

    if not cfg.ENABLE_MORPH_CLEANUP:
        return smoothed

    structure = np.ones((cfg.MORPH_STRUCTURE_SIZE, cfg.MORPH_STRUCTURE_SIZE), dtype=bool)
    binary = smoothed >= cfg.DETECTION_THRESHOLD
    binary = ndimage.binary_opening(binary, structure=structure)
    binary = ndimage.binary_closing(binary, structure=structure)
    smoothed[~binary] = np.float32(0.0)
    return smoothed


def preprocess_inputs(grids: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    return {
        "low": preprocess_azshear_grid(grids["low"]),
        "mid": preprocess_azshear_grid(grids["mid"]),
        "reflectivity": np.asarray(grids["reflectivity"], dtype=np.float32),
    }

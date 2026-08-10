"""Chunk serialization helpers for EWMRS render outputs.

This module provides functionality to split rendered images into tiles
with the coordinate origin (0,0) at the bottom-left corner.
"""

import numpy as np
from pathlib import Path

from util.atomic import atomic_write_bytes


def save_rgba_chunk(chunk_data: np.ndarray, output_path: str | Path) -> Path:
    """Atomically publish one tightly-packed RGBA8 chunk.

    Chunks intentionally have no file header.  Their dimensions and
    orientation are supplied by the schema-versioned timestamp index.
    """
    if not isinstance(chunk_data, np.ndarray) or chunk_data.dtype != np.uint8:
        raise ValueError("RGBA chunk data must be a uint8 NumPy array")
    if chunk_data.ndim != 3 or chunk_data.shape[2] != 4:
        raise ValueError("RGBA chunk data must have shape (height, width, 4)")
    if not chunk_data.flags.c_contiguous:
        raise ValueError("RGBA chunk data must be C-contiguous")
    payload = chunk_data.tobytes(order="C")
    expected_length = chunk_data.shape[0] * chunk_data.shape[1] * 4
    if len(payload) != expected_length:
        raise ValueError("RGBA chunk payload length does not match its dimensions")
    return atomic_write_bytes(output_path, payload)

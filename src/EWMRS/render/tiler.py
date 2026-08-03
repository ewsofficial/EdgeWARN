"""Tile splitting module for render outputs.

This module provides functionality to split rendered images into tiles
with the coordinate origin (0,0) at the bottom-left corner.
"""

from typing import List, Tuple
import numpy as np
from PIL import Image
from util.atomic import atomic_output_path


class TileSplitter:
    # Deprecated: use GUILayerRenderer._save_tiles_from_image instead
    pass


def save_tile(tile_data: np.ndarray, output_path: str) -> None:
    """Save a tile as a PNG file.
    
    Args:
        tile_data: Tile as numpy array of shape (tile_size, tile_size, channels).
        output_path: Path to save the PNG file.
    """
    img = Image.fromarray(tile_data, mode="RGBA")
    with atomic_output_path(output_path) as temporary:
        img.save(temporary, format="PNG", compress_level=1)  # Fast compression

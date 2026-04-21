from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Tuple, List
import json
import numpy as np
from PIL import Image
from .tools import TransformUtils
from .tiler import TileSplitter, save_tile
import util.file as fs
from xarray import Dataset
from util.io import IOManager
from datetime import datetime
import threading

io_manager = IOManager("[Transform]")

# Colormap cache to avoid re-reading JSON on every render
_COLORMAP_CACHE = {}
_COLORMAP_CACHE_LOCK = threading.Lock()


class GUIRGBAWriter:
    def __init__(self, outdir: Path, file_name: str, timestamp):
        self.outdir = outdir
        self.file_name = file_name
        self.timestamp = timestamp

    def save_rgba(self, rgba: np.ndarray, tile_output: bool = True) -> Tuple[List[Path], str]:
        from .config import TILE_SIZE

        dt = self._coerce_timestamp(self.timestamp)
        timestamp = dt.strftime(r"%Y%m%d-%H%M00")
        self.outdir.mkdir(parents=True, exist_ok=True)

        if tile_output:
            tile_paths = self._save_tiles_from_array(rgba, timestamp)
            rows = rgba.shape[0] // TILE_SIZE
            cols = rgba.shape[1] // TILE_SIZE
            self._update_index(timestamp, tile_grid={"rows": rows, "cols": cols, "tile_size": TILE_SIZE})
            io_manager.write_debug(f"Saved {len(tile_paths)} tiles from RGBA image for {self.file_name} at {timestamp}")
            return tile_paths, timestamp

        png_file = self.outdir / f"{self.file_name}_{timestamp}.png"
        img = Image.fromarray(rgba, mode="RGBA")
        img.save(png_file, compress_level=1)
        self._update_index(timestamp, tile_grid=None)
        io_manager.write_debug(f"Saved {self.file_name} PNG file to {png_file}")
        return [png_file], timestamp

    def _coerce_timestamp(self, timestamp) -> datetime:
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            cleaned_ts = TransformUtils.find_timestamp(timestamp)
            return datetime.fromisoformat(cleaned_ts)

    def _save_tiles_from_array(self, rgba: np.ndarray, timestamp: str) -> List[Path]:
        from .config import TILE_SIZE

        height, width = rgba.shape[:2]
        grid_cols = width // TILE_SIZE
        grid_rows = height // TILE_SIZE

        tile_dir = self.outdir / timestamp
        tile_dir.mkdir(parents=True, exist_ok=True)

        tile_specs = []
        for tile_y in range(grid_rows):
            for tile_x in range(grid_cols):
                left = tile_x * TILE_SIZE
                right = left + TILE_SIZE
                top = (grid_rows - 1 - tile_y) * TILE_SIZE
                bottom = top + TILE_SIZE
                tile_filename = f"tile_{tile_x}_{tile_y}.png"
                tile_path = tile_dir / tile_filename
                tile_specs.append((rgba[top:bottom, left:right], tile_path))

        max_workers = min(8, len(tile_specs)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(lambda spec: save_tile(spec[0], spec[1]), tile_specs))

        return [tile_path for _, tile_path in tile_specs]

    def _update_index(self, new_timestamp, tile_grid=None):
        index_file = self.outdir / "index.json"
        timestamps = []
        existing_tile_grid = None

        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    data = json.load(f)

                if isinstance(data, list):
                    timestamps = data
                else:
                    timestamps = data.get("timestamps", [])
                    existing_tile_grid = data.get("tile_grid")
            except Exception as e:
                io_manager.write_warning(f"Failed to read index.json in {self.outdir}: {e}. Creating new one.")

        if new_timestamp not in timestamps:
            timestamps.append(new_timestamp)
            timestamps.sort(reverse=True)

            try:
                if tile_grid is not None:
                    output_data = {"timestamps": timestamps, "tile_grid": tile_grid}
                elif existing_tile_grid is not None:
                    output_data = {"timestamps": timestamps, "tile_grid": existing_tile_grid}
                else:
                    output_data = timestamps

                with open(index_file, 'w') as f:
                    json.dump(output_data, f, separators=(",", ":"))
            except Exception as e:
                io_manager.write_error(f"Failed to update index.json in {self.outdir}: {e}")

class GUILayerRenderer:
    def __init__(self, dataset: Dataset, outdir: Path, colormap_key, file_name, timestamp):
        """
        Args:
            filepath (xr.Dataset): Dataset being converted to GUI png
            outdir (Path): Output directory of the converted png file
            colormap_key (str): Key of the color map as stored under colormaps.json
            file_name (str): Key of .png file name
            timestamp (str): ISO formatted timestamp string or string to parse
        """
        self.ds = dataset
        self.outdir = outdir
        self.colormap_key = colormap_key
        self.file_name = file_name
        self.timestamp = timestamp

    def _get_cmap(self):
        """
        Returns cached colormap data to avoid re-reading JSON file.
        
        Returns:
            thresholds (np.ndarray): array of dBZ or value thresholds
            colors (np.ndarray): array of RGB colors corresponding to thresholds
            interpolate (bool): whether to interpolate between colors
        """
        # Check cache first
        if self.colormap_key in _COLORMAP_CACHE:
            return _COLORMAP_CACHE[self.colormap_key]
        
        with _COLORMAP_CACHE_LOCK:
            # Double-check after acquiring lock
            if self.colormap_key in _COLORMAP_CACHE:
                return _COLORMAP_CACHE[self.colormap_key]
            
            with open(fs.GUI_COLORMAP_JSON, 'r') as f:
                cmaps_json = json.load(f)

            # Iterate through all colormaps to find the matching key
            for source in cmaps_json:
                for cmap in source.get("colormaps", []):
                    if cmap.get("name") == self.colormap_key:
                        thresholds = np.array([t["value"] for t in cmap["thresholds"]], dtype=np.float32)
                        colors = np.array([t["rgb"] for t in cmap["thresholds"]], dtype=np.float32)
                        colors_uint8 = colors.astype(np.uint8)
                        interpolate = cmap.get("interpolate", True)
                        result = (thresholds, colors, colors_uint8, interpolate)
                        _COLORMAP_CACHE[self.colormap_key] = result
                        return result
            
            # If key not found, raise an error with the path we tried
            raise ValueError(f"Colormap '{self.colormap_key}' not found in {fs.GUI_COLORMAP_JSON}")

    def convert_to_png(self, tile_output: bool = True) -> Tuple[List[Path], str]:
        """
        Converts dataset to tiled PNG files or a single PNG file.
        
        Args:
            tile_output: If True, output tiles in timestamp subdirectory.
                        If False, output single PNG file (backward compatibility).
        
        Returns:
            Tuple of (list of tile paths or [single png path], timestamp string).
        """
        from .config import TILE_SIZE

        # Step 1: No Reprojection needed for 1km/pixel raw render
        # We will resize the output image based on physical domain size later
        data = self.ds['unknown'].values

        # Step 2: Get colormap
        thresholds, colors, colors_uint8, interpolate = self._get_cmap()

        # Step 2.5: Apply colormap
        # Use ravel() to avoid copy if possible, though digitize/interp might flatten anyway
        flat_data = np.asarray(data, dtype=np.float32).ravel()

        # Pre-allocate output array (N, 4) in uint8 to save memory
        N = flat_data.shape[0]
        rgba_flat = np.empty((N, 4), dtype=np.uint8)
        rgba_flat[:, 3] = 0
        valid_mask = np.isfinite(flat_data)
        safe_data = np.where(valid_mask, flat_data, thresholds[0])

        if interpolate:
            safe_data = np.clip(safe_data, thresholds[0], thresholds[-1])
            rgba_flat[:, 0] = np.interp(safe_data, thresholds, colors[:, 0]).astype(np.uint8)
            rgba_flat[:, 1] = np.interp(safe_data, thresholds, colors[:, 1]).astype(np.uint8)
            rgba_flat[:, 2] = np.interp(safe_data, thresholds, colors[:, 2]).astype(np.uint8)
        else:
            # Discrete color mapping
            indices = np.digitize(safe_data, thresholds) - 1
            indices = np.clip(indices, 0, len(colors_uint8) - 1)

            # Map directly into the output array
            rgba_flat[:, :3] = colors_uint8[indices]

        # Alpha channel: transparent for values < first threshold OR NaN
        # This ensures that empty space from reprojection is correctly transparent
        rgba_flat[valid_mask & (flat_data >= thresholds[0]), 3] = 255

        # Reshape to original grid
        # Note: Grib data is often (lat, lon), where lat is row (y), lon is col (x)
        # We want image to be (height, width) which corresponds to (lat, lon) shape
        rgba = rgba_flat.reshape((data.shape[0], data.shape[1], 4))

        writer = GUIRGBAWriter(self.outdir, self.file_name, self.timestamp)
        return writer.save_rgba(rgba, tile_output=tile_output)

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Tuple, List
import json
import os
import time
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


def _get_cached_cmap(colormap_key: str):
    if colormap_key in _COLORMAP_CACHE:
        return _COLORMAP_CACHE[colormap_key]

    with _COLORMAP_CACHE_LOCK:
        if colormap_key in _COLORMAP_CACHE:
            return _COLORMAP_CACHE[colormap_key]

        with open(fs.GUI_COLORMAP_JSON, 'r') as f:
            cmaps_json = json.load(f)

        for source in cmaps_json:
            for cmap in source.get("colormaps", []):
                if cmap.get("name") == colormap_key:
                    thresholds = np.array([t["value"] for t in cmap["thresholds"]], dtype=np.float32)
                    colors = np.array([t["rgb"] for t in cmap["thresholds"]], dtype=np.float32)
                    colors_uint8 = colors.astype(np.uint8)
                    interpolate = cmap.get("interpolate", True)
                    result = (thresholds, colors, colors_uint8, interpolate)
                    _COLORMAP_CACHE[colormap_key] = result
                    return result

        raise ValueError(f"Colormap '{colormap_key}' not found in {fs.GUI_COLORMAP_JSON}")


def _scalar_data_to_rgba(
    data: np.ndarray,
    thresholds: np.ndarray,
    colors: np.ndarray,
    colors_uint8: np.ndarray,
    interpolate: bool,
) -> np.ndarray:
    flat_data = np.asarray(data, dtype=np.float32).ravel()

    rgba_flat = np.empty((flat_data.shape[0], 4), dtype=np.uint8)
    rgba_flat[:, 3] = 0
    valid_mask = np.isfinite(flat_data)
    safe_data = np.where(valid_mask, flat_data, thresholds[0])

    if interpolate:
        safe_data = np.clip(safe_data, thresholds[0], thresholds[-1])
        rgba_flat[:, 0] = np.interp(safe_data, thresholds, colors[:, 0]).astype(np.uint8)
        rgba_flat[:, 1] = np.interp(safe_data, thresholds, colors[:, 1]).astype(np.uint8)
        rgba_flat[:, 2] = np.interp(safe_data, thresholds, colors[:, 2]).astype(np.uint8)
    else:
        indices = np.digitize(safe_data, thresholds) - 1
        indices = np.clip(indices, 0, len(colors_uint8) - 1)
        rgba_flat[:, :3] = colors_uint8[indices]

    rgba_flat[valid_mask & (flat_data >= thresholds[0]), 3] = 255
    return rgba_flat.reshape((data.shape[0], data.shape[1], 4))


def _resolve_tile_workers(tile_count: int) -> int:
    if tile_count <= 1:
        return 1

    env_value = os.environ.get("EWMRS_TILE_THREADS")
    if env_value:
        try:
            configured_cap = max(1, int(env_value))
            return min(tile_count, configured_cap)
        except ValueError:
            pass

    cpu_cap = max(1, os.cpu_count() or 1)
    return min(tile_count, 8, cpu_cap)


class GUIRGBAWriter:
    def __init__(self, outdir: Path, file_name: str, timestamp):
        self.outdir = outdir
        self.file_name = file_name
        self.timestamp = timestamp

    def save_rgba(
        self,
        rgba: np.ndarray,
        tile_output: bool = True,
        *,
        timing_context: dict | None = None,
    ) -> Tuple[List[Path], str]:
        from .config import TILE_SIZE

        render_start_s = time.perf_counter()
        dt = self._coerce_timestamp(self.timestamp)
        timestamp = dt.strftime(r"%Y%m%d-%H%M00")
        self.outdir.mkdir(parents=True, exist_ok=True)

        if tile_output:
            tile_paths = self._save_tiles_from_array(rgba, timestamp, timing_context=timing_context)
            rows = rgba.shape[0] // TILE_SIZE
            cols = rgba.shape[1] // TILE_SIZE
            self._update_index(timestamp, tile_grid={"rows": rows, "cols": cols, "tile_size": TILE_SIZE})
            total_render_s = time.perf_counter() - render_start_s
            io_manager.write_info(
                f"Render output for {self.file_name} completed in {total_render_s:.3f}s "
                f"({len(tile_paths)} tiles, timestamp={timestamp})"
            )
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

    def _save_tiles_from_array(
        self,
        rgba: np.ndarray,
        timestamp: str,
        *,
        timing_context: dict | None = None,
    ) -> List[Path]:
        from .config import TILE_SIZE

        tile_schedule_start_s = time.perf_counter()
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

        tile_schedule_s = time.perf_counter() - tile_schedule_start_s
        io_manager.write_info(
            f"Prepared tile schedule for {self.file_name} in {tile_schedule_s:.3f}s "
            f"({len(tile_specs)} tiles)"
        )

        max_workers = _resolve_tile_workers(len(tile_specs))
        tile_write_start_s = time.perf_counter()
        first_tile_lock = threading.Lock()
        first_tile_logged = False

        def _write_tile(spec):
            nonlocal first_tile_logged
            tile_data, tile_path = spec
            save_tile(tile_data, tile_path)
            completed_s = time.perf_counter()
            with first_tile_lock:
                if not first_tile_logged:
                    first_tile_logged = True
                    first_tile_latency_s = completed_s - tile_write_start_s
                    if timing_context is not None:
                        timing_context["tile_schedule_s"] = tile_schedule_s
                        timing_context["first_tile_latency_s"] = first_tile_latency_s
                        render_start_s = timing_context.get("render_start_s")
                        if render_start_s is not None:
                            timing_context["render_start_to_first_tile_s"] = completed_s - float(render_start_s)
                    render_start_s = None if timing_context is None else timing_context.get("render_start_s")
                    if render_start_s is not None:
                        render_to_first_tile_s = completed_s - float(render_start_s)
                        io_manager.write_info(
                            f"First tile written for {self.file_name}: {tile_path} "
                            f"({first_tile_latency_s:.3f}s tile-write latency, "
                            f"{render_to_first_tile_s:.3f}s from render start)"
                        )
                    else:
                        io_manager.write_info(
                            f"First tile written for {self.file_name}: {tile_path} "
                            f"({first_tile_latency_s:.3f}s tile-write latency)"
                        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(_write_tile, tile_specs))

        tile_write_s = time.perf_counter() - tile_write_start_s
        if timing_context is not None:
            timing_context["tile_write_s"] = tile_write_s
        io_manager.write_info(
            f"Tile writes for {self.file_name} completed in {tile_write_s:.3f}s using {max_workers} worker(s)"
        )

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
        return _get_cached_cmap(self.colormap_key)

    def _update_index(self, new_timestamp, tile_grid=None):
        writer = GUIRGBAWriter(self.outdir, self.file_name, self.timestamp)
        writer._update_index(new_timestamp, tile_grid=tile_grid)

    def convert_to_png(self, tile_output: bool = True, *, timing_context: dict | None = None) -> Tuple[List[Path], str]:
        """
        Converts dataset to tiled PNG files or a single PNG file.
        
        Args:
            tile_output: If True, output tiles in timestamp subdirectory.
                        If False, output single PNG file (backward compatibility).
        
        Returns:
            Tuple of (list of tile paths or [single png path], timestamp string).
        """
        # Step 1: No Reprojection needed for 1km/pixel raw render
        # We will resize the output image based on physical domain size later
        data = self.ds['unknown'].values

        # Step 2: Get colormap
        thresholds, colors, colors_uint8, interpolate = self._get_cmap()

        rgba_start_s = time.perf_counter()
        rgba = _scalar_data_to_rgba(data, thresholds, colors, colors_uint8, interpolate)
        scalar_to_rgba_s = time.perf_counter() - rgba_start_s
        if timing_context is not None:
            timing_context["scalar_to_rgba_s"] = scalar_to_rgba_s
        io_manager.write_info(f"Scalar-to-RGBA for {self.file_name} completed in {scalar_to_rgba_s:.3f}s")

        writer = GUIRGBAWriter(self.outdir, self.file_name, self.timestamp)
        return writer.save_rgba(rgba, tile_output=tile_output, timing_context=timing_context)


class GUIArrayRenderer:
    def __init__(self, values: np.ndarray, outdir: Path, colormap_key, file_name: str, timestamp):
        self.values = np.asarray(values, dtype=np.float32)
        self.outdir = outdir
        self.colormap_key = colormap_key
        self.file_name = file_name
        self.timestamp = timestamp

    def convert_to_png(self, tile_output: bool = True, *, timing_context: dict | None = None) -> Tuple[List[Path], str]:
        thresholds, colors, colors_uint8, interpolate = _get_cached_cmap(self.colormap_key)
        rgba_start_s = time.perf_counter()
        rgba = _scalar_data_to_rgba(self.values, thresholds, colors, colors_uint8, interpolate)
        scalar_to_rgba_s = time.perf_counter() - rgba_start_s
        if timing_context is not None:
            timing_context["scalar_to_rgba_s"] = scalar_to_rgba_s
        io_manager.write_info(f"Scalar-to-RGBA for {self.file_name} completed in {scalar_to_rgba_s:.3f}s")
        writer = GUIRGBAWriter(self.outdir, self.file_name, self.timestamp)
        return writer.save_rgba(rgba, tile_output=tile_output, timing_context=timing_context)

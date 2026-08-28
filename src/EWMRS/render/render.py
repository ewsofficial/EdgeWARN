from functools import lru_cache
from pathlib import Path
from typing import Tuple, List
import json
import shutil
import time
import numpy as np
from .tools import TransformUtils
from .tiler import save_float16_chunk
from EWMRS.pipeline_config import (
    colormap_cache_entries,
)
import util.file as fs
from util.atomic import atomic_write_json
from xarray import Dataset
from util.io import IOManager
from datetime import datetime

io_manager = IOManager("[Transform]")
_VALUES_FILENAME = "values.f16.gz"


@lru_cache(maxsize=colormap_cache_entries())
def _get_cached_cmap(colormap_key: str):
    """Cache parsed colormap arrays. lru_cache provides thread-safe
    insertion via the GIL and replaces the previous double-checked-lock
    dict. Cache key is just the colormap name, matching the original
    semantics."""
    with open(fs.GUI_COLORMAP_JSON, 'r') as f:
        cmaps_json = json.load(f)

    for source in cmaps_json:
        for cmap in source.get("colormaps", []):
            if cmap.get("name") == colormap_key:
                thresholds = np.array([t["value"] for t in cmap["thresholds"]], dtype=np.float32)
                # Use "rgba" for RAP colormaps, "rgb" for others
                color_key = "rgba" if colormap_key.startswith("RAP_") else "rgb"
                raw_colors = []
                for threshold in cmap["thresholds"]:
                    color = list(threshold[color_key])
                    if len(color) == 3:
                        color.append(255)
                    raw_colors.append(color)
                colors = np.array(raw_colors, dtype=np.float32)
                colors_uint8 = colors.astype(np.uint8)
                interpolate = cmap.get("interpolate", True)
                return (thresholds, colors, colors_uint8, interpolate)

    raise ValueError(f"Colormap '{colormap_key}' not found in {fs.GUI_COLORMAP_JSON}")


class _ColormapCacheView:
    """Tests call ``_COLORMAP_CACHE.clear()`` to force a re-read between
    cases. Expose that surface against the lru_cache without resurrecting
    the dict."""

    @staticmethod
    def clear():
        _get_cached_cmap.cache_clear()


_COLORMAP_CACHE = _ColormapCacheView()


def _scalar_data_to_rgba(
    data: np.ndarray,
    thresholds: np.ndarray,
    colors: np.ndarray,
    colors_uint8: np.ndarray,
    interpolate: bool,
) -> np.ndarray:
    flat_data = np.asarray(data, dtype=np.float32).ravel()

    rgba_flat = np.zeros((flat_data.shape[0], 4), dtype=np.uint8)
    valid_mask = np.isfinite(flat_data)
    safe_data = np.where(valid_mask, flat_data, thresholds[0])

    if interpolate:
        safe_data = np.clip(safe_data, thresholds[0], thresholds[-1])
        rgba_flat[:, 0] = np.interp(safe_data, thresholds, colors[:, 0]).astype(np.uint8)
        rgba_flat[:, 1] = np.interp(safe_data, thresholds, colors[:, 1]).astype(np.uint8)
        rgba_flat[:, 2] = np.interp(safe_data, thresholds, colors[:, 2]).astype(np.uint8)
        rgba_flat[:, 3] = np.interp(safe_data, thresholds, colors[:, 3]).astype(np.uint8)
        below_min_mask = valid_mask & (flat_data < thresholds[0])
        rgba_flat[below_min_mask] = 0
    else:
        indices = np.digitize(safe_data, thresholds) - 1
        indices = np.clip(indices, 0, len(colors_uint8) - 1)
        rgba_flat[:, :4] = colors_uint8[indices]

    rgba_flat[~valid_mask] = 0
    return rgba_flat.reshape((data.shape[0], data.shape[1], 4))


class GUIValueWriter:
    def __init__(self, outdir: Path, file_name: str, timestamp):
        self.outdir = outdir
        self.file_name = file_name
        self.timestamp = timestamp

    def save_values(
        self,
        values: np.ndarray,
        tile_output: bool = True,
        *,
        timing_context: dict | None = None,
    ) -> Tuple[List[Path], str]:
        render_start_s = time.perf_counter()
        dt = self._coerce_timestamp(self.timestamp)
        timestamp = dt.strftime(r"%Y%m%d-%H%M00")
        self.outdir.mkdir(parents=True, exist_ok=True)

        if tile_output:
            values = np.asarray(values, dtype=np.float32)
            if values.ndim != 2:
                raise ValueError("Rendered values must be scalar [height,width]")
            artifact_path = self._save_values_file(
                values,
                timestamp,
                timing_context=timing_context,
            )
            self._update_index(timestamp)
            total_render_s = time.perf_counter() - render_start_s
            io_manager.write_info(
                f"Render output for {self.file_name} completed in {total_render_s:.3f}s "
                f"({artifact_path.name}, timestamp={timestamp})"
            )
            io_manager.write_debug(f"Saved float16 values for {self.file_name} at {timestamp}: {artifact_path}")
            return [artifact_path], timestamp

        raise ValueError("EWMRS no longer writes flat PNG artifacts; use float16 value files")

    def _coerce_timestamp(self, timestamp) -> datetime:
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            cleaned_ts = TransformUtils.find_timestamp(timestamp)
            return datetime.fromisoformat(cleaned_ts)

    def _save_values_file(
        self,
        values: np.ndarray,
        timestamp: str,
        *,
        timing_context: dict | None = None,
    ) -> Path:
        timestamp_dir = self.outdir / timestamp
        timestamp_dir.mkdir(parents=True, exist_ok=True)
        output_path = timestamp_dir / _VALUES_FILENAME
        write_start_s = time.perf_counter()
        save_float16_chunk(np.ascontiguousarray(values, dtype=np.float16), output_path)
        write_s = time.perf_counter() - write_start_s
        if timing_context is not None:
            timing_context["value_write_s"] = write_s
        io_manager.write_info(
            f"Value file for {self.file_name} completed in {write_s:.3f}s: {output_path}"
        )
        self._write_timestamp_index(timestamp_dir, values.shape)
        legacy_chunks = timestamp_dir / "chunks"
        if legacy_chunks.is_dir():
            shutil.rmtree(legacy_chunks)
        return output_path

    def _write_timestamp_index(self, timestamp_dir: Path, shape: tuple[int, int]) -> None:
        from .config import chunk_format_descriptor, chunk_schema_version
        output_data = {
            "schema_version": chunk_schema_version(),
            "timestamp": timestamp_dir.name,
            "representation": "binary_file",
            "chunk_format": chunk_format_descriptor(),
            "file": _VALUES_FILENAME,
            "shape": list(shape),
        }

        index_file = timestamp_dir / "index.json"
        try:
            atomic_write_json(index_file, output_data)
        except Exception as e:
            io_manager.write_error(f"Failed to update index.json in {timestamp_dir}: {e}")

    def _update_index(self, new_timestamp):
        from .config import chunk_format_descriptor, chunk_schema_version
        index_file = self.outdir / "index.json"
        timestamps = []

        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    data = json.load(f)

                if isinstance(data, list):
                    timestamps = data
                else:
                    timestamps = data.get("timestamps", [])
            except Exception as e:
                io_manager.write_warning(f"Failed to read index.json in {self.outdir}: {e}. Creating new one.")

        if new_timestamp not in timestamps:
            timestamps.append(new_timestamp)
            timestamps.sort(reverse=True)

            try:
                output_data = {
                    "schema_version": chunk_schema_version(),
                    "timestamps": timestamps,
                    "representation": "binary_file",
                    "chunk_format": chunk_format_descriptor(include_media_type=True),
                }

                atomic_write_json(index_file, output_data)
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
        writer = GUIValueWriter(self.outdir, self.file_name, self.timestamp)
        writer._update_index(new_timestamp)

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

        render_start_s = time.perf_counter()
        values = np.asarray(data, dtype=np.float32)
        scalar_to_rgba_s = time.perf_counter() - render_start_s
        if timing_context is not None:
            timing_context["scalar_to_rgba_s"] = scalar_to_rgba_s
        io_manager.write_info(f"Value preparation for {self.file_name} completed in {scalar_to_rgba_s:.3f}s")

        writer = GUIValueWriter(self.outdir, self.file_name, self.timestamp)
        return writer.save_values(values, tile_output=tile_output, timing_context=timing_context)


class GUIArrayRenderer:
    def __init__(self, values: np.ndarray, outdir: Path, colormap_key, file_name: str, timestamp):
        self.values = np.asarray(values, dtype=np.float32)
        self.outdir = outdir
        self.colormap_key = colormap_key
        self.file_name = file_name
        self.timestamp = timestamp

    def convert_to_png(self, tile_output: bool = True, *, timing_context: dict | None = None) -> Tuple[List[Path], str]:
        scalar_to_rgba_s = 0.0
        if timing_context is not None:
            timing_context["scalar_to_rgba_s"] = scalar_to_rgba_s
        io_manager.write_info(f"Value preparation for {self.file_name} completed in {scalar_to_rgba_s:.3f}s")
        writer = GUIValueWriter(self.outdir, self.file_name, self.timestamp)
        return writer.save_values(self.values, tile_output=tile_output, timing_context=timing_context)

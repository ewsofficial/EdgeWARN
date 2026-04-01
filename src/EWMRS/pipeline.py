from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import rasterio.transform
import rioxarray  # noqa: F401  Ensures xarray .rio accessor is registered.
from rasterio.enums import Resampling

from EWMRS.render.config import TILE_GRID_COLS, TILE_GRID_ROWS, get_file_list
from EWMRS.render.tools import configure_proj_runtime
import util.file as fs
from util.io import IOManager, QueueWriter

RenderOutput = Optional[list[Path]]

EWMRS_COLORMAP_JSON = Path(__file__).resolve().with_name("colormaps.json")
fs.GUI_COLORMAP_JSON = EWMRS_COLORMAP_JSON

io_manager = IOManager("[Pipeline]")

WEB_MERCATOR_BOUNDS = (-14471533.8, 2273030.9, -6679169.5, 7361866.1)
WEB_MERCATOR_SHAPE = (3500, 7000)
WEB_MERCATOR_TRANSFORM = rasterio.transform.from_bounds(*WEB_MERCATOR_BOUNDS, WEB_MERCATOR_SHAPE[1], WEB_MERCATOR_SHAPE[0])
EXPECTED_TILE_COUNT = TILE_GRID_ROWS * TILE_GRID_COLS
_RUNTIME_CONFIGURED = False


def _ensure_dt(dt_in) -> datetime:
    if isinstance(dt_in, datetime):
        dt = dt_in
    elif isinstance(dt_in, str):
        dt = datetime.fromisoformat(dt_in)
    else:
        raise TypeError("dt must be a datetime or ISO-format string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _render_layer(layer) -> tuple[str, RenderOutput]:
    """Render a single layer. Returns (name, png_path or None)."""
    from EWMRS.render.render import GUILayerRenderer
    from EWMRS.render.tools import TransformUtils
    from util.io import IOManager

    io_mgr = IOManager("[Pipeline]")
    _ensure_runtime_configured()

    name = layer.get("name")
    colormap_key = layer.get("colormap_key")
    source_path = layer.get("filepath")
    output_path = layer.get("outdir")

    if source_path is None or output_path is None:
        io_mgr.write_error(f"Layer {name} is missing filepath/outdir configuration")
        return name, None

    src_dir = Path(source_path)
    out_dir = Path(output_path)

    io_mgr.write_debug(f"Processing layer {name}: src={src_dir}, out={out_dir}")

    try:
        if not src_dir.exists():
            io_mgr.write_warning(f"Source directory missing for {name}: {src_dir}")
            return name, None

        latest_file = _latest_source_file(src_dir)
        if latest_file is None:
            io_mgr.write_warning(f"No source files found for {name} in {src_dir}")
            return name, None

        timestamp_iso = TransformUtils.find_timestamp(str(latest_file))
        cached_render = _current_render_paths(out_dir, timestamp_iso)
        if cached_render is not None:
            io_mgr.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
            return name, cached_render

        io_mgr.write_info(f"Found latest file for {name}: {latest_file}")

        ds = TransformUtils.load_ds(latest_file)
        if ds is None:
            io_mgr.write_error(f"Failed to load dataset for {latest_file}")
            return name, None

        if "MergedAzShear" in name and ds.latitude.values.shape[0] > 3510:
            ds = ds.coarsen(latitude=2, longitude=2, boundary="trim", coord_func="mean").reduce(np.max)
            io_mgr.write_info(f"Downsampled {name} to 0.01 deg grid")

        if "latitude" in ds.coords and "longitude" in ds.coords:
            ds.rio.write_crs("EPSG:4326", inplace=True)
            ds = ds.rio.reproject(
                "EPSG:3857",
                shape=WEB_MERCATOR_SHAPE,
                transform=WEB_MERCATOR_TRANSFORM,
                resampling=Resampling.nearest,
            )
            io_mgr.write_info(f"Reprojected {name} to EPSG:3857 (Crisp nearest-neighbor, Precise bounds)")

        renderer = GUILayerRenderer(ds, out_dir, colormap_key, name, timestamp_iso)
        png_path, px_timestamp = renderer.convert_to_png(tile_output=True)

        return name, png_path

    except Exception as exc:
        io_mgr.write_error(f"Error processing layer {name}: {exc}")
        return name, None


def _ensure_runtime_configured() -> None:
    global _RUNTIME_CONFIGURED
    if not _RUNTIME_CONFIGURED:
        configure_proj_runtime()
        _RUNTIME_CONFIGURED = True


def _worker_initializer() -> None:
    _ensure_runtime_configured()


def _latest_source_file(src_dir: Path) -> Optional[Path]:
    latest = fs.latest_files(src_dir, 1)
    if not latest:
        return None
    return Path(latest[-1])


def _current_render_paths(out_dir: Path, timestamp_iso: str) -> RenderOutput:
    try:
        timestamp = _normalize_render_timestamp(timestamp_iso)
        tile_dir = out_dir / timestamp
        if not tile_dir.exists() or not tile_dir.is_dir():
            return None

        tile_paths = sorted(tile_dir.glob("tile_*.png"))
        if len(tile_paths) != EXPECTED_TILE_COUNT:
            return None

        index_file = out_dir / "index.json"
        if index_file.exists():
            with open(index_file, "r") as f:
                data = json.load(f)

            timestamps = data if isinstance(data, list) else data.get("timestamps", [])
            if timestamp not in timestamps:
                return None

        return tile_paths
    except Exception:
        return None


def _normalize_render_timestamp(timestamp_iso: str) -> str:
    dt = datetime.fromisoformat(timestamp_iso)
    return dt.strftime(r"%Y%m%d-%H%M00")


def cleanup_old_gui_files(max_age_minutes: int = 120):
    """Remove old files/folders from GUI output directories."""
    import shutil
    import time

    now = time.time()
    max_age_seconds = max_age_minutes * 60
    total_removed = 0

    for layer in get_file_list():
        output_path = layer.get("outdir")
        if output_path is None:
            continue

        out_dir = Path(output_path)
        if not out_dir.exists():
            continue

        existing_timestamps = set()

        for png_file in out_dir.glob("*.png"):
            try:
                file_age = now - png_file.stat().st_mtime
                if file_age > max_age_seconds:
                    png_file.unlink()
                    total_removed += 1
                else:
                    stem = png_file.stem
                    if "_" in stem:
                        existing_timestamps.add(stem.split("_")[-1])
            except Exception as exc:
                io_manager.write_warning(f"Failed to process {png_file}: {exc}")

        for ts_folder in out_dir.iterdir():
            if ts_folder.is_dir() and not ts_folder.name.startswith("."):
                try:
                    folder_age = now - ts_folder.stat().st_mtime
                    if folder_age > max_age_seconds:
                        shutil.rmtree(ts_folder)
                        total_removed += 1
                        io_manager.write_debug(f"Removed old timestamp folder: {ts_folder}")
                    else:
                        existing_timestamps.add(ts_folder.name)
                except Exception as exc:
                    io_manager.write_warning(f"Failed to process folder {ts_folder}: {exc}")

        index_file = out_dir / "index.json"
        if index_file.exists():
            try:
                with open(index_file, "r") as f:
                    data = json.load(f)

                if isinstance(data, list):
                    timestamps = data
                    tile_grid = None
                else:
                    timestamps = data.get("timestamps", [])
                    tile_grid = data.get("tile_grid")

                timestamps = [ts for ts in timestamps if ts in existing_timestamps]

                output_data = {"timestamps": timestamps, "tile_grid": tile_grid} if tile_grid is not None else timestamps

                with open(index_file, "w") as f:
                    json.dump(output_data, f)
            except Exception as exc:
                io_manager.write_warning(f"Failed to update index.json in {out_dir}: {exc}")

    if total_removed > 0:
        io_manager.write_info(f"Cleaned up {total_removed} old GUI files/folders (>{max_age_minutes} min)")


def run_render_pipeline(dt, max_entries: int = 10) -> Dict[str, RenderOutput]:
    """Render configured EWMRS layers from already staged local files."""
    from concurrent.futures import ProcessPoolExecutor, as_completed

    dt = _ensure_dt(dt)
    results: Dict[str, RenderOutput] = {}

    layers = get_file_list()
    max_workers = min(4, max(1, len(layers)))
    io_manager.write_info(f"Rendering {len(layers)} layers across {max_workers} CPU cores for {dt.isoformat()}...")
    with ProcessPoolExecutor(max_workers=max_workers, initializer=_worker_initializer) as executor:
        futures = {executor.submit(_render_layer, layer): layer for layer in layers}
        for future in as_completed(futures):
            name, png_path = future.result()
            results[name] = png_path

    cleanup_old_gui_files(max_age_minutes=120)
    return results


def run_ewmrs_pipeline(dt, max_entries: int = 10) -> Dict[str, RenderOutput]:
    """Run the EWMRS rendering pipeline using locally staged files only."""
    return run_render_pipeline(dt, max_entries=max_entries)


def _summarize_results(results: Dict[str, RenderOutput]) -> str:
    successful_layers = sum(1 for output_path in results.values() if output_path)
    total_layers = len(results)
    return f"{successful_layers}/{total_layers} layers succeeded"


def ewmrs_tandem_worker(log_queue, shared_state, ewmrs_ready_event, dt, max_entries: int = 10):
    """Process target for staged EWMRS rendering within the tandem runner."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    try:
        log(f"INFO: EWMRS worker waiting for render inputs for {dt}")
        ewmrs_ready_event.wait()

        if not shared_state.get("ewmrs_inputs_ready", False):
            log("ERROR: EWMRS inputs were not staged successfully; skipping render")
            return

        log("INFO: Starting EWMRS render phase")
        results = run_ewmrs_pipeline(dt, max_entries=max_entries)
        log(f"INFO: EWMRS render completed: {_summarize_results(results)}")
    except Exception as exc:
        log(f"ERROR: EWMRS tandem worker failed - {exc}")

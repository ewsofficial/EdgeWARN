from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import rasterio.transform
import rioxarray  # noqa: F401  Ensures xarray .rio accessor is registered.
from rasterio.enums import Resampling

from common.ingest.nexrad.s3_chunks import format_nexrad_timestamp, parse_nexrad_timestamp
from EWMRS.render.config import (
    get_file_list,
    get_goes_file_list,
    get_mrms_file_list,
)
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

# GOES previews/renders are clipped to a CONUS-focused Web Mercator extent.
# Derived from lon/lat bounds: -125.0..-66.5, 24.5..49.5 (EPSG:4326).
GOES_WEB_MERCATOR_BOUNDS = (
    -13914936.349159198,
    2814454.7323097703,
    -7402746.137752692,
    6360130.74092142,
)
GOES_WEB_MERCATOR_SHAPE = WEB_MERCATOR_SHAPE
GOES_WEB_MERCATOR_TRANSFORM = rasterio.transform.from_bounds(
    *GOES_WEB_MERCATOR_BOUNDS,
    GOES_WEB_MERCATOR_SHAPE[1],
    GOES_WEB_MERCATOR_SHAPE[0],
)
_RUNTIME_CONFIGURED = False
_GOES_CLEANUP_MIN_INTERVAL_SECONDS = max(0.0, float(os.environ.get("EWMRS_GOES_CLEANUP_MIN_INTERVAL_SECONDS", "300")))
_LAST_GOES_GUI_CLEANUP_S = 0.0
_LAST_GOES_GUI_CLEANUP_FUNC_ID: int | None = None
_NEXRAD_GUI_RETENTION_MINUTES = 120
_NEXRAD_SITE_DIR_PATTERN = re.compile(r"^[A-Z0-9]{4}$")
_NEXRAD_ELEVATION_DIR_PATTERN = re.compile(r"^\d{1,3}(?:\.\d{1,2})?$")
_NEXRAD_TIMESTAMP_PATTERN = re.compile(r"^\d{8}-\d{6}$")
_NEXRAD_POLL_INTERVAL_SECONDS = 30.0
_NEXRAD_RENDER_MAX_WORKERS = 8


@lru_cache(maxsize=512)
def _load_timestamp_tile_index_cached(
    index_path_str: str,
    mtime_ns: int,
) -> tuple[list[list[int]], dict | None] | None:
    """Cached read of a tile-dir index.json keyed on (path, mtime).

    The mtime is part of the cache key, so any rewrite of index.json
    invalidates the entry automatically and the next call re-reads from
    disk. ``index_path_str`` and ``mtime_ns`` are passed in to keep all
    cache key components hashable primitives.
    """
    with open(index_path_str, "r") as f:
        data = json.load(f)

    if isinstance(data, list):
        tiles = data
        tile_grid = None
    else:
        tiles = data.get("tiles", [])
        tile_grid = data.get("tile_grid")

    if not isinstance(tiles, list):
        return None

    return tiles, tile_grid if isinstance(tile_grid, dict) else None


def _load_timestamp_tile_index(tile_dir: Path) -> tuple[list[list[int]], dict | None] | None:
    index_file = tile_dir / "index.json"
    try:
        stat_result = index_file.stat()
    except FileNotFoundError:
        return None

    return _load_timestamp_tile_index_cached(str(index_file), stat_result.st_mtime_ns)


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


def _configure_numerical_thread_caps() -> None:
    for env_var in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ.setdefault(env_var, "1")


def _adaptive_process_worker_count(layer_count: int, phase_name: str) -> int:
    if layer_count <= 1:
        return 1

    cpu_cap = min(layer_count, max(1, os.cpu_count() or 1))
    if cpu_cap <= 1:
        return 1

    default_worker_budget_mb = 1200.0 if phase_name.upper().startswith("GOES") else 768.0
    worker_budget_mb = float(os.environ.get("EWMRS_WORKER_BUDGET_MB", default_worker_budget_mb))
    reserve_mb = float(os.environ.get("EWMRS_WORKER_RESERVE_MB", 1024.0))

    try:
        import psutil

        available_mb = psutil.virtual_memory().available / (1024.0 * 1024.0)
        usable_mb = max(0.0, available_mb - reserve_mb)
        memory_cap = max(1, int(usable_mb // max(1.0, worker_budget_mb)))
        return max(1, min(cpu_cap, memory_cap))
    except Exception:
        return max(1, min(cpu_cap, 2))


def _render_layer(layer) -> tuple[str, RenderOutput]:
    """Render a single layer. Returns (name, png_path or None)."""
    from EWMRS.render.goes_rgb import compose_goes_rgb, prepare_goes_rgb_render
    from EWMRS.render.render import GUIArrayRenderer, GUIRGBAWriter, GUILayerRenderer
    from EWMRS.render.goes_transform import (
        extract_goes_timestamp_iso,
        load_reproject_goes_abi_render_array,
    )
    from EWMRS.render.tools import TransformUtils
    from util.io import IOManager

    io_mgr = IOManager("[Pipeline]")
    _ensure_runtime_configured()

    name = layer.get("name")
    colormap_key = layer.get("colormap_key")
    source_path = layer.get("filepath")
    output_path = layer.get("outdir")
    source_type = str(layer.get("source_type", "mrms")).lower()

    if source_path is None or output_path is None:
        io_mgr.write_error(f"Layer {name} is missing filepath/outdir configuration")
        return name, None

    src_dir = Path(source_path)
    out_dir = Path(output_path)

    try:
        if not src_dir.exists():
            io_mgr.write_warning(f"Source directory missing for {name}: {src_dir}")
            return name, None

        if source_type == "goes_abi_rgb":
            prepared = prepare_goes_rgb_render(layer)
            if prepared is None:
                return name, None

            timestamp_iso = prepared["timestamp_iso"]
            cached_render = _current_render_paths(out_dir, timestamp_iso)
            if cached_render is not None:
                io_mgr.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
                return name, cached_render

            composed = compose_goes_rgb(
                prepared,
                web_mercator_shape=GOES_WEB_MERCATOR_SHAPE,
                web_mercator_transform=GOES_WEB_MERCATOR_TRANSFORM,
            )
            if composed is None:
                return name, None

            rgba, metadata = composed
            io_mgr.write_info(
                f"Composited {name} GOES RGB product with channels {', '.join(sorted(metadata['selected_files']))}"
            )
            renderer = GUIRGBAWriter(out_dir, name, timestamp_iso)
            png_path, px_timestamp = renderer.save_rgba(rgba, tile_output=True)
            return name, png_path

        latest_file = _latest_source_file(src_dir)
        if latest_file is None:
            io_mgr.write_warning(f"No source files found for {name} in {src_dir}")
            return name, None

        if source_type == "goes_abi":
            timestamp_iso = extract_goes_timestamp_iso(latest_file)
        else:
            timestamp_iso = TransformUtils.find_timestamp(str(latest_file))

        cached_render = _current_render_paths(out_dir, timestamp_iso)
        if cached_render is not None:
            io_mgr.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
            return name, cached_render

        io_mgr.write_info(f"Found latest file for {name}: {latest_file}")

        if source_type == "goes_abi":
            payload = load_reproject_goes_abi_render_array(
                latest_file,
                layer,
                shape=GOES_WEB_MERCATOR_SHAPE,
                transform=GOES_WEB_MERCATOR_TRANSFORM,
                resampling=Resampling.bilinear,
            )
            if payload is None:
                io_mgr.write_error(f"Failed to reproject GOES ABI dataset for {latest_file}")
                return name, None

            io_mgr.write_info(f"Reprojected {name} GOES ABI fixed grid to EPSG:3857 (CONUS clip)")
            renderer = GUIArrayRenderer(payload["data"], out_dir, colormap_key, name, timestamp_iso)
            png_path, _px_timestamp = renderer.convert_to_png(tile_output=True)
            return name, png_path
        else:
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
        png_path, _px_timestamp = renderer.convert_to_png(tile_output=True)

        return name, png_path

    except Exception as exc:
        io_mgr.write_error(f"Error processing layer {name}: {exc}")
        return name, None


def _ensure_runtime_configured() -> None:
    global _RUNTIME_CONFIGURED
    _configure_numerical_thread_caps()
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

        index_file = out_dir / "index.json"
        tile_grid = None
        if index_file.exists():
            with open(index_file, "r") as f:
                data = json.load(f)

            timestamps = data if isinstance(data, list) else data.get("timestamps", [])
            if timestamp not in timestamps:
                return None
            if not isinstance(data, list):
                tile_grid = data.get("tile_grid")

        timestamp_index = _load_timestamp_tile_index(tile_dir)
        if timestamp_index is None:
            return None

        indexed_tiles, timestamp_tile_grid = timestamp_index
        if timestamp_tile_grid is not None:
            tile_grid = timestamp_tile_grid

        tile_paths: list[tuple[int, int, Path]] = []
        for tile in indexed_tiles:
            if not isinstance(tile, list) or len(tile) != 2:
                continue

            tile_x, tile_y = tile
            if not isinstance(tile_x, int) or not isinstance(tile_y, int):
                continue

            if tile_grid is not None:
                rows = tile_grid.get("rows")
                cols = tile_grid.get("cols")
                if isinstance(rows, int) and isinstance(cols, int):
                    if tile_x < 0 or tile_x >= cols or tile_y < 0 or tile_y >= rows:
                        continue

            tile_path = tile_dir / f"tile_{tile_x}_{tile_y}.png"
            if not tile_path.is_file():
                continue

            tile_paths.append((tile_y, tile_x, tile_path))

        tile_paths.sort(key=lambda item: (item[0], item[1]))
        return [path for _, _, path in tile_paths]
    except Exception:
        return None
def _normalize_render_timestamp(timestamp_iso: str) -> str:
    dt = datetime.fromisoformat(timestamp_iso)
    return dt.strftime(r"%Y%m%d-%H%M00")


def _cleanup_old_nexrad_gui_files(max_age_minutes: int = 120) -> int:
    """Remove stale NEXRAD GUI files and empty site/elevation directories."""

    nexrad_root = Path(fs.GUI_NEXRAD_DIR)
    if not nexrad_root.exists():
        return 0

    now = time.time()
    max_age_seconds = max_age_minutes * 60
    total_removed = 0
    affected_sites: set[str] = set()
    files_removed = 0
    dirs_removed = 0

    for site_dir in nexrad_root.iterdir():
        if not site_dir.is_dir() or site_dir.name.startswith(".") or not _NEXRAD_SITE_DIR_PATTERN.fullmatch(site_dir.name):
            continue

        site_had_removals = False

        for child_dir in site_dir.iterdir():
            if not child_dir.is_dir() or child_dir.name.startswith("."):
                continue
            if child_dir.name != "render" and not _NEXRAD_ELEVATION_DIR_PATTERN.fullmatch(child_dir.name):
                continue

            try:
                for child_file in child_dir.iterdir():
                    if not child_file.is_file() or child_file.name.startswith("."):
                        continue
                    file_age = now - child_file.stat().st_mtime
                    if file_age <= max_age_seconds:
                        continue
                    child_file.unlink(missing_ok=True)
                    files_removed += 1
                    total_removed += 1
                    site_had_removals = True

                if not any(child_dir.iterdir()):
                    child_dir.rmdir()
                    dirs_removed += 1
                    total_removed += 1
                    site_had_removals = True
            except Exception as exc:
                io_manager.write_warning(f"Failed to process NEXRAD directory {child_dir}: {exc}")

        try:
            if any(site_dir.iterdir()):
                if site_had_removals:
                    affected_sites.add(site_dir.name)
                continue

            site_dir.rmdir()
            dirs_removed += 1
            total_removed += 1
            affected_sites.add(site_dir.name)
        except Exception as exc:
            io_manager.write_warning(f"Failed to process NEXRAD site folder {site_dir}: {exc}")

    if files_removed or dirs_removed:
        sites_str = ", ".join(sorted(affected_sites))
        io_manager.write_debug(
            f"Cleaned up NEXRAD GUI: {files_removed} file(s), {dirs_removed} dir(s) "
            f"across {len(affected_sites)} site(s): [{sites_str}]"
        )

    return total_removed


def _load_nexrad_artifact_metadata(artifact_path: Path) -> dict | None:
    sidecar_payload = {}
    sidecar_path = artifact_path.with_suffix(".json")
    if sidecar_path.exists():
        try:
            loaded = json.loads(sidecar_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                sidecar_payload = loaded
        except Exception:
            sidecar_payload = {}

    stem_prefix = f"{artifact_path.parent.parent.name}_{artifact_path.parent.name}_"
    filename_timestamp = artifact_path.stem[len(stem_prefix):] if artifact_path.stem.startswith(stem_prefix) else None
    normalized_filename_timestamp = _normalize_nexrad_timestamp(filename_timestamp)
    elevation_timestamp = _normalize_nexrad_timestamp(sidecar_payload.get("elevation_timestamp")) or normalized_filename_timestamp
    scan_timestamp = _normalize_nexrad_timestamp(sidecar_payload.get("scan_timestamp")) or elevation_timestamp
    if elevation_timestamp is None:
        return None

    return {
        "site": artifact_path.parent.parent.name,
        "elevation": artifact_path.parent.name,
        "scan_timestamp": scan_timestamp,
        "elevation_timestamp": elevation_timestamp,
        "volume_id": str(sidecar_payload.get("volume_id") or artifact_path.stem),
        "member_group_names": list(sidecar_payload.get("member_group_names") or []),
        "member_sweeps": list(sidecar_payload.get("member_sweeps") or []),
        "artifact_path": artifact_path,
    }


def _normalize_nexrad_timestamp(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if _NEXRAD_TIMESTAMP_PATTERN.fullmatch(text):
        return text
    return format_nexrad_timestamp(parse_nexrad_timestamp(text))


def _nexrad_source_artifact_is_fresh(artifact_path: Path, *, now: float, max_age_minutes: int) -> bool:
    try:
        return (now - artifact_path.stat().st_mtime) <= max_age_minutes * 60
    except OSError:
        return False


def _iter_latest_nexrad_artifacts():
    nexrad_root = Path(fs.NEXRAD_LEVEL2_DIR)
    if not nexrad_root.exists():
        return

    for site_dir in sorted(nexrad_root.iterdir(), key=lambda path: path.name):
        if (
            not site_dir.is_dir()
            or site_dir.name.startswith(".")
            or not _NEXRAD_SITE_DIR_PATTERN.fullmatch(site_dir.name)
        ):
            continue

        for elevation_dir in sorted(site_dir.iterdir(), key=lambda path: path.name):
            if (
                not elevation_dir.is_dir()
                or elevation_dir.name.startswith(".")
                or not _NEXRAD_ELEVATION_DIR_PATTERN.fullmatch(elevation_dir.name)
            ):
                continue

            preferred_by_stem: dict[str, Path] = {}
            for artifact_path in sorted(elevation_dir.iterdir(), key=lambda path: path.name):
                if not artifact_path.is_file() or artifact_path.suffix not in {".nc", ".ar2v"}:
                    continue
                existing = preferred_by_stem.get(artifact_path.stem)
                if existing is None or (existing.suffix != ".nc" and artifact_path.suffix == ".nc"):
                    preferred_by_stem[artifact_path.stem] = artifact_path

            for artifact_path in sorted(preferred_by_stem.values(), key=lambda path: path.name):
                metadata = _load_nexrad_artifact_metadata(artifact_path)
                if metadata is not None:
                    yield metadata


def _nexrad_gui_timestamp_exists(site: str, elevation: str, timestamp: str) -> bool:
    from EWMRS.render.nexrad import nexrad_render_elevation_dir

    elevation_dir = nexrad_render_elevation_dir(site, elevation)
    if not elevation_dir.exists():
        return False
    pattern = f"{str(site).upper()}_*_{elevation}_{timestamp}.bin.gz"
    return any(elevation_dir.glob(pattern))


def _render_pending_nexrad_gui_artifact(metadata: dict) -> bool:
    from EWMRS.render.nexrad import serialize_nexrad_elevation_artifacts
    from common.ingest.nexrad.models import ElevationArtifact

    site = str(metadata["site"]).upper()
    elevation = str(metadata["elevation"])
    timestamp = str(metadata["elevation_timestamp"])
    artifact_path = Path(metadata["artifact_path"])

    artifact = ElevationArtifact(
        site=site,
        volume_id=str(metadata["volume_id"]),
        volume_timestamp=str(metadata["scan_timestamp"]),
        scan_timestamp=str(metadata["scan_timestamp"]),
        elevation=elevation,
        elevation_timestamp=timestamp,
        first_sweep_index=0,
        last_sweep_index=0,
        first_sweep_timestamp=None,
        last_sweep_timestamp=None,
        member_group_names=list(metadata.get("member_group_names") or []),
        member_sweeps=list(metadata.get("member_sweeps") or []),
        waveforms_present=set(),
        supplemental=False,
        netcdf_path=str(artifact_path) if artifact_path.suffix == ".nc" else None,
        ar2v_path=str(artifact_path) if artifact_path.suffix == ".ar2v" else None,
    )
    serialize_nexrad_elevation_artifacts(site, str(metadata["volume_id"]), str(metadata["scan_timestamp"]), [artifact])
    return _nexrad_gui_timestamp_exists(site, elevation, timestamp)


def render_pending_nexrad_gui_files(*, base_dir=None, max_source_age_minutes: int = _NEXRAD_GUI_RETENTION_MINUTES) -> int:
    import concurrent.futures

    if base_dir:
        fs.initialize_filesystem(base_dir)

    now = time.time()
    pending_metadata = []
    for metadata in _iter_latest_nexrad_artifacts() or ():
        site = str(metadata["site"]).upper()
        elevation = str(metadata["elevation"])
        timestamp = str(metadata["elevation_timestamp"])
        artifact_path = Path(metadata["artifact_path"])
        if not _nexrad_source_artifact_is_fresh(artifact_path, now=now, max_age_minutes=max_source_age_minutes):
            continue
        if _nexrad_gui_timestamp_exists(site, elevation, timestamp):
            continue
        pending_metadata.append(metadata)

    if not pending_metadata:
        return 0

    max_workers = min(_NEXRAD_RENDER_MAX_WORKERS, len(pending_metadata))
    rendered_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_render_pending_nexrad_gui_artifact, metadata) for metadata in pending_metadata]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                rendered_count += 1

    return rendered_count


def run_nexrad_render_loop(*, base_dir=None, poll_interval_seconds: float = _NEXRAD_POLL_INTERVAL_SECONDS) -> None:
    poll_interval_seconds = max(1.0, float(poll_interval_seconds))
    while True:
        try:
            rendered = render_pending_nexrad_gui_files(base_dir=base_dir)
            if rendered > 0:
                io_manager.write_info(f"NEXRAD render poll cycle wrote {rendered} artifact(s)")
        except KeyboardInterrupt:
            return
        except Exception as exc:
            io_manager.write_warning(f"NEXRAD render poll cycle failed: {exc}")
        time.sleep(poll_interval_seconds)


def cleanup_old_gui_files(max_age_minutes: int = 120):
    """Remove old files/folders from GUI output directories."""
    import shutil

    now = time.time()
    max_age_seconds = max_age_minutes * 60
    total_removed = 0

    candidate_dirs = []
    for layer in get_file_list():
        output_path = layer.get("outdir")
        if output_path is None:
            continue

        candidate_dirs.append(Path(output_path))

    for out_dir in candidate_dirs:
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

    total_removed += _cleanup_old_nexrad_gui_files(max_age_minutes=max_age_minutes)

    if total_removed > 0:
        io_manager.write_info(f"Cleaned up {total_removed} old GUI files/folders (>{max_age_minutes} min)")


def run_render_pipeline(dt, max_entries: int = 10, layers=None, phase_name: str = "EWMRS", cleanup_after: bool = True) -> Dict[str, RenderOutput]:
    """Render configured EWMRS layers from already staged local files."""
    from concurrent.futures import ProcessPoolExecutor, as_completed

    dt = _ensure_dt(dt)
    results: Dict[str, RenderOutput] = {}

    layers = get_file_list() if layers is None else list(layers)
    if not layers:
        io_manager.write_info(f"{phase_name} render phase has no configured layers")
        return results

    max_workers = _adaptive_process_worker_count(len(layers), phase_name)
    io_manager.write_info(
        f"Rendering {len(layers)} {phase_name} layers across {max_workers} CPU cores for {dt.isoformat()}..."
    )
    rendered_layers: list[str] = []
    failed_layers: list[str] = []
    with ProcessPoolExecutor(max_workers=max_workers, initializer=_worker_initializer) as executor:
        futures = {executor.submit(_render_layer, layer): layer for layer in layers}
        for future in as_completed(futures):
            name, png_path = future.result()
            results[name] = png_path
            if png_path:
                rendered_layers.append(name)
                io_manager.write_info(f"Rendered layer: {name}")
            else:
                failed_layers.append(name)

    io_manager.write_info(
        f"{phase_name} rendered layers: {', '.join(rendered_layers) if rendered_layers else 'none'}"
    )
    if failed_layers:
        io_manager.write_warning(f"{phase_name} failed layers: {', '.join(failed_layers)}")

    if cleanup_after:
        cleanup_old_gui_files(max_age_minutes=120)
    return results


def run_mrms_render_pipeline(dt, max_entries: int = 10) -> Dict[str, RenderOutput]:
    """Run the MRMS-backed EWMRS render phase."""
    return run_render_pipeline(
        dt,
        max_entries=max_entries,
        layers=get_mrms_file_list(),
        phase_name="MRMS",
    )


def _goes_cycle_registry_key(file_path: Path, channel_id: str) -> tuple[str, str, tuple[int, int], tuple[float, ...]]:
    return (
        str(file_path),
        str(channel_id),
        GOES_WEB_MERCATOR_SHAPE,
        tuple(float(value) for value in tuple(GOES_WEB_MERCATOR_TRANSFORM)),
    )


def _load_goes_registry_entry(
    cycle_registry: dict[tuple[str, str, tuple[int, int], tuple[float, ...]], dict[str, object]],
    *,
    file_path: Path,
    channel_id: str,
    layer_config: dict,
) -> dict[str, object] | None:
    from EWMRS.render.goes_transform import extract_goes_timestamp_iso, load_reproject_goes_abi_render_array

    load_start_s = time.perf_counter()
    registry_key = _goes_cycle_registry_key(file_path, channel_id)
    cached = cycle_registry.get(registry_key)
    if cached is not None:
        io_manager.write_info(f"Reusing shared GOES registry entry for {channel_id} from {file_path}")
        return cached

    payload = load_reproject_goes_abi_render_array(
        file_path,
        layer_config,
        shape=GOES_WEB_MERCATOR_SHAPE,
        transform=GOES_WEB_MERCATOR_TRANSFORM,
        resampling=Resampling.bilinear,
    )
    if payload is None:
        return None

    entry: dict[str, object] = {
        "channel_id": str(channel_id),
        "file_path": file_path,
        "timestamp_iso": extract_goes_timestamp_iso(file_path),
        "data": payload["data"],
        "x": payload["x"],
        "y": payload["y"],
    }
    cycle_registry[registry_key] = entry
    io_manager.write_info(
        f"Built shared GOES registry entry for {channel_id} in {time.perf_counter() - load_start_s:.3f}s"
    )
    return entry


def _maybe_cleanup_goes_gui_files(max_age_minutes: int = 120) -> None:
    global _LAST_GOES_GUI_CLEANUP_S, _LAST_GOES_GUI_CLEANUP_FUNC_ID

    now_s = time.perf_counter()
    current_cleanup_func_id = id(cleanup_old_gui_files)
    if (
        _LAST_GOES_GUI_CLEANUP_FUNC_ID == current_cleanup_func_id
        and _GOES_CLEANUP_MIN_INTERVAL_SECONDS > 0
        and (now_s - _LAST_GOES_GUI_CLEANUP_S) < _GOES_CLEANUP_MIN_INTERVAL_SECONDS
    ):
        io_manager.write_debug(
            f"Skipping GOES GUI cleanup: last run was {(now_s - _LAST_GOES_GUI_CLEANUP_S):.1f}s ago"
        )
        return

    cleanup_old_gui_files(max_age_minutes=max_age_minutes)
    _LAST_GOES_GUI_CLEANUP_S = now_s
    _LAST_GOES_GUI_CLEANUP_FUNC_ID = current_cleanup_func_id


def _run_goes_unified_cycle(
    single_channel_layers: list[dict],
    rgb_layers: list[dict],
) -> Dict[str, RenderOutput]:
    from EWMRS.render.goes_rgb import iter_goes_rgb_batch, layer_config_for_channel, prepare_goes_rgb_batch
    from EWMRS.render.goes_transform import extract_goes_timestamp_iso
    from EWMRS.render.render import GUIArrayRenderer, GUIRGBAWriter

    results: Dict[str, RenderOutput] = {}
    _ensure_runtime_configured()
    cycle_start_s = time.perf_counter()

    prepare_rgb_start_s = time.perf_counter()
    prepared_batch = prepare_goes_rgb_batch(rgb_layers)
    prepare_rgb_batch_s = time.perf_counter() - prepare_rgb_start_s
    pending_recipes = []
    pending_selected_files: dict[str, Path] = {}
    rgb_layer_outdirs = {str(layer["name"]): Path(layer["outdir"]) for layer in rgb_layers}

    if prepared_batch is not None:
        for prepared in prepared_batch["recipes"]:
            layer = prepared["layer"]
            name = str(layer["name"])
            out_dir = Path(layer["outdir"])
            timestamp_iso = prepared["timestamp_iso"]
            cached_render = _current_render_paths(out_dir, timestamp_iso)
            if cached_render is not None:
                io_manager.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
                results[name] = cached_render
                continue

            pending_recipes.append(prepared)
            for channel_id, file_path in prepared["selected_files"].items():
                pending_selected_files.setdefault(str(channel_id), Path(file_path))
    else:
        for layer in rgb_layers:
            results.setdefault(str(layer["name"]), None)

    preferred_single_files = prepared_batch["selected_files"] if prepared_batch is not None else {}
    pending_single: list[dict[str, object]] = []
    for layer in single_channel_layers:
        name = str(layer.get("name"))
        source_path = layer.get("filepath")
        output_path = layer.get("outdir")
        channel_id = str(layer.get("channel_id", "")).strip()

        if source_path is None or output_path is None:
            io_manager.write_error(f"Layer {name} is missing filepath/outdir configuration")
            results[name] = None
            continue

        selected_file = None
        preferred_file = preferred_single_files.get(channel_id)
        if preferred_file is not None:
            preferred_path = Path(preferred_file)
            if preferred_path.exists():
                selected_file = preferred_path

        if selected_file is None:
            selected_file = _latest_source_file(Path(source_path))

        if selected_file is None:
            io_manager.write_warning(f"No source files found for {name} in {source_path}")
            results[name] = None
            continue

        timestamp_iso = extract_goes_timestamp_iso(selected_file)
        cached_render = _current_render_paths(Path(output_path), timestamp_iso)
        if cached_render is not None:
            io_manager.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
            results[name] = cached_render
            continue

        pending_single.append(
            {
                "name": name,
                "layer": layer,
                "file_path": Path(selected_file),
                "channel_id": channel_id,
                "timestamp_iso": timestamp_iso,
            }
        )

    cycle_registry: dict[tuple[str, str, tuple[int, int], tuple[float, ...]], dict[str, object]] = {}
    single_registry_preload_s = 0.0
    rgb_registry_preload_s = 0.0
    rgb_composition_s = 0.0

    io_manager.write_info(
        f"Starting unified GOES render cycle: {len(pending_single)} pending single-channel layer(s), "
        f"{len(pending_recipes)} pending RGB recipe(s), rgb_batch_prepare={prepare_rgb_batch_s:.3f}s"
    )

    for pending in pending_single:
        name = str(pending["name"])
        load_start_s = time.perf_counter()
        entry = _load_goes_registry_entry(
            cycle_registry,
            file_path=Path(pending["file_path"]),
            channel_id=str(pending["channel_id"]),
            layer_config=dict(pending["layer"]),
        )
        single_registry_preload_s += time.perf_counter() - load_start_s
        if entry is None:
            results[name] = None
            continue

        layer = dict(pending["layer"])
        out_dir = Path(layer["outdir"])
        render_timing_context = {"render_start_s": time.perf_counter(), "cycle_start_s": cycle_start_s}
        renderer = GUIArrayRenderer(
            np.asarray(entry["data"], dtype=np.float32),
            out_dir,
            layer.get("colormap_key"),
            name,
            str(pending["timestamp_iso"]),
        )
        png_path, _px_timestamp = renderer.convert_to_png(tile_output=True, timing_context=render_timing_context)
        results[name] = png_path

    io_manager.write_info(
        f"GOES unified cycle single-channel precompute completed in {single_registry_preload_s:.3f}s"
    )

    for channel_id, file_path in pending_selected_files.items():
        load_start_s = time.perf_counter()
        entry = _load_goes_registry_entry(
            cycle_registry,
            file_path=file_path,
            channel_id=str(channel_id),
            layer_config=layer_config_for_channel(str(channel_id)),
        )
        rgb_registry_preload_s += time.perf_counter() - load_start_s
        if entry is None:
            io_manager.write_warning(f"Skipping GOES RGB channel {channel_id}: failed to build shared registry entry")

    if pending_selected_files:
        io_manager.write_info(
            f"GOES unified cycle RGB shared-channel preload completed in {rgb_registry_preload_s:.3f}s"
        )

    rendered_rgb_layers: set[str] = set()
    if pending_recipes and prepared_batch is not None:
        rgb_registry: dict[str, dict[str, object]] = {}
        missing_channels: list[str] = []
        for channel_id, file_path in pending_selected_files.items():
            registry_key = _goes_cycle_registry_key(file_path, channel_id)
            entry = cycle_registry.get(registry_key)
            if entry is None:
                missing_channels.append(channel_id)
                continue
            rgb_registry[channel_id] = entry

        if missing_channels:
            io_manager.write_warning(
                f"Skipping GOES RGB batch: missing shared channel entries for {', '.join(sorted(missing_channels))}"
            )
        else:
            pending_batch = {
                **prepared_batch,
                "recipes": pending_recipes,
                "selected_files": pending_selected_files,
            }
            rgb_comp_start_s = time.perf_counter()
            for layer_name, rgba, metadata in iter_goes_rgb_batch(
                pending_batch,
                web_mercator_shape=GOES_WEB_MERCATOR_SHAPE,
                web_mercator_transform=GOES_WEB_MERCATOR_TRANSFORM,
                registry=rgb_registry,
            ) or ():
                out_dir = rgb_layer_outdirs[layer_name]
                timestamp_iso = metadata["timestamp_iso"]
                io_manager.write_info(
                    f"Composited {layer_name} GOES RGB product with channels {', '.join(sorted(metadata['selected_files']))}"
                )
                renderer = GUIRGBAWriter(out_dir, layer_name, timestamp_iso)
                png_path, _px_timestamp = renderer.save_rgba(
                    rgba,
                    tile_output=True,
                    timing_context={"render_start_s": time.perf_counter(), "cycle_start_s": cycle_start_s},
                )
                results[layer_name] = png_path
                rendered_rgb_layers.add(layer_name)
                del rgba
            rgb_composition_s = time.perf_counter() - rgb_comp_start_s
            io_manager.write_info(f"GOES unified cycle RGB composition completed in {rgb_composition_s:.3f}s")

    for layer in rgb_layers:
        name = str(layer["name"])
        if name in results:
            continue
        if name not in rendered_rgb_layers:
            results.setdefault(name, None)

    io_manager.write_info(
        f"Unified GOES render cycle completed in {time.perf_counter() - cycle_start_s:.3f}s "
        f"(single_precompute={single_registry_preload_s:.3f}s, rgb_preload={rgb_registry_preload_s:.3f}s, "
        f"rgb_comp={rgb_composition_s:.3f}s)"
    )
    return results


def run_goes_render_pipeline(dt, max_entries: int = 10) -> Dict[str, RenderOutput]:
    """Run the GOES-backed EWMRS render phase."""
    from EWMRS.render.goes_rgb import iter_goes_rgb_batch, prepare_goes_rgb_batch
    from EWMRS.render.render import GUIRGBAWriter

    layers = get_goes_file_list()
    if not layers:
        io_manager.write_info("GOES render phase is a no-op: no GOES layers configured")
        return {}

    pipeline_start_s = time.perf_counter()

    single_channel_layers = [layer for layer in layers if str(layer.get("source_type", "")).lower() != "goes_abi_rgb"]
    rgb_layers = [layer for layer in layers if str(layer.get("source_type", "")).lower() == "goes_abi_rgb"]

    results: Dict[str, RenderOutput] = {}

    if single_channel_layers and rgb_layers:
        results.update(_run_goes_unified_cycle(single_channel_layers, rgb_layers))
        _maybe_cleanup_goes_gui_files(max_age_minutes=120)
        io_manager.write_info(f"GOES render pipeline completed in {time.perf_counter() - pipeline_start_s:.3f}s")
        return results

    if single_channel_layers:
        results.update(
            run_render_pipeline(
                dt,
                max_entries=max_entries,
                layers=single_channel_layers,
                phase_name="GOES",
                cleanup_after=False,
            )
        )

    if rgb_layers:
        _ensure_runtime_configured()
        prepared_batch = prepare_goes_rgb_batch(rgb_layers)
        if prepared_batch is not None:
            pending_recipes = []
            pending_selected_files: dict[str, Path] = {}
            rgb_layer_outdirs = {str(layer["name"]): Path(layer["outdir"]) for layer in rgb_layers}

            for prepared in prepared_batch["recipes"]:
                layer = prepared["layer"]
                name = str(layer["name"])
                out_dir = Path(layer["outdir"])
                timestamp_iso = prepared["timestamp_iso"]
                cached_render = _current_render_paths(out_dir, timestamp_iso)
                if cached_render is not None:
                    io_manager.write_info(f"Reusing existing render for {name}: {timestamp_iso}")
                    results[name] = cached_render
                    continue

                pending_recipes.append(prepared)
                for channel_id, file_path in prepared["selected_files"].items():
                    pending_selected_files.setdefault(channel_id, file_path)

            rendered_rgb_layers: set[str] = set()
            if pending_recipes:
                pending_batch = {
                    **prepared_batch,
                    "recipes": pending_recipes,
                    "selected_files": pending_selected_files,
                }
                for layer_name, rgba, metadata in iter_goes_rgb_batch(
                    pending_batch,
                    web_mercator_shape=GOES_WEB_MERCATOR_SHAPE,
                    web_mercator_transform=GOES_WEB_MERCATOR_TRANSFORM,
                ) or ():
                    out_dir = rgb_layer_outdirs[layer_name]
                    timestamp_iso = metadata["timestamp_iso"]
                    io_manager.write_info(
                        f"Composited {layer_name} GOES RGB product with channels {', '.join(sorted(metadata['selected_files']))}"
                    )
                    renderer = GUIRGBAWriter(out_dir, layer_name, timestamp_iso)
                    png_path, _px_timestamp = renderer.save_rgba(rgba, tile_output=True)
                    results[layer_name] = png_path
                    rendered_rgb_layers.add(layer_name)
                    del rgba

            for layer in rgb_layers:
                name = str(layer["name"])
                if name in results:
                    continue
                if name not in rendered_rgb_layers:
                    results.setdefault(name, None)
                    continue
        else:
            for layer in rgb_layers:
                results.setdefault(str(layer["name"]), None)

    _maybe_cleanup_goes_gui_files(max_age_minutes=120)

    io_manager.write_info(f"GOES render pipeline completed in {time.perf_counter() - pipeline_start_s:.3f}s")

    return results
def run_rap_uint16_pipeline(rap_file, dt=None):
    """Run the EWMRS RAP Uint16Array conversion pipeline for one RAP GRIB2 file."""
    from EWMRS.rap.uint16_pipeline import run_rap_uint16_pipeline as _run_rap_uint16_pipeline

    return _run_rap_uint16_pipeline(rap_file, dt=dt)


def _summarize_results(results: Dict[str, RenderOutput]) -> str:
    successful_layers = sum(1 for output_path in results.values() if output_path is not None)
    total_layers = len(results)
    return f"{successful_layers}/{total_layers} layers succeeded"


def ewmrs_tandem_worker(
    log_queue,
    shared_state,
    ewmrs_mrms_ready_event,
    dt,
    max_entries: int = 10,
): 
    """Process target for staged EWMRS rendering within the tandem runner."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    try:
        log(f"INFO: EWMRS worker waiting for MRMS render inputs for {dt}")
        ewmrs_mrms_ready_event.wait()

        mrms_inputs_ready = shared_state.get(
            "ewmrs_mrms_inputs_ready",
            False,
        )
        if not mrms_inputs_ready:
            log("ERROR: EWMRS MRMS inputs were not staged successfully; skipping MRMS render")
        else:
            log("INFO: Starting EWMRS MRMS render phase")
            results = run_mrms_render_pipeline(dt, max_entries=max_entries)
            log(f"INFO: EWMRS MRMS render completed: {_summarize_results(results)}")
        log("INFO: EWMRS GOES render is decoupled from the tandem worker")
    except Exception as exc:
        log(f"ERROR: EWMRS tandem worker failed - {exc}")


def ewmrs_goes_worker(log_queue, dt, max_entries: int = 10):
    """Process target for decoupled GOES rendering outside tandem completion."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    try:
        log(f"INFO: Starting EWMRS GOES render phase for {dt}")
        results = run_goes_render_pipeline(dt, max_entries=max_entries)
        log(f"INFO: EWMRS GOES render completed: {_summarize_results(results)}")
    except Exception as exc:
        log(f"ERROR: EWMRS GOES worker failed - {exc}")

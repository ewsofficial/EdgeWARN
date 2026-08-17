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
from EWMRS.pipeline_config import (
    goes_cleanup_max_age_minutes,
    goes_cleanup_min_interval_seconds,
    gui_cleanup_max_age_minutes,
    nexrad_poll_interval_min_seconds,
    nexrad_poll_interval_seconds,
    nexrad_render_max_workers,
    nexrad_source_max_age_minutes,
    numeric_thread_cap_value,
    numeric_thread_cap_variables,
    render_cleanup_after,
    render_phase_name,
    tile_index_cache_entries,
    worker_budget_mb,
    worker_psutil_fallback_max,
    worker_reserve_mb,
)
import util.file as fs
from common.ingest.manifest import CycleInputManifest
from util.atomic import atomic_write_json
from util.io import IOManager, QueueWriter

RenderOutput = Optional[list[Path]]
_CHUNK_FILENAME_RE = re.compile(r"^chunk_(\d+)_(\d+)\.f16\.gz$")

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
_LAST_GOES_GUI_CLEANUP_S = 0.0
_LAST_GOES_GUI_CLEANUP_FUNC_ID: int | None = None
_NEXRAD_SITE_DIR_PATTERN = re.compile(r"^[A-Z0-9]{4}$")
_NEXRAD_ELEVATION_DIR_PATTERN = re.compile(r"^\d{1,3}(?:\.\d{1,2})?$")
_NEXRAD_TIMESTAMP_PATTERN = re.compile(r"^\d{8}-\d{6}$")


@lru_cache(maxsize=tile_index_cache_entries())
def _load_timestamp_chunk_index_cached(
    index_path_str: str,
    mtime_ns: int,
) -> tuple[list[list[int]], dict, dict] | None:
    """Cached read of a schema-versioned chunk index keyed on (path, mtime).

    The mtime is part of the cache key, so any rewrite of index.json
    invalidates the entry automatically and the next call re-reads from
    disk. ``index_path_str`` and ``mtime_ns`` are passed in to keep all
    cache key components hashable primitives.
    """
    with open(index_path_str, "r") as f:
        data = json.load(f)

    if not isinstance(data, dict) or data.get("schema_version") != 2 or data.get("representation") != "binary_chunks":
        return None
    chunks = data.get("chunks")
    tile_grid = data.get("tile_grid")
    chunk_format = data.get("chunk_format")
    if not isinstance(chunks, list) or not isinstance(tile_grid, dict) or not isinstance(chunk_format, dict):
        return None
    if chunk_format.get("encoding") != "float16" or chunk_format.get("file_suffix") != ".f16.gz" or chunk_format.get("compression") != "gzip" or chunk_format.get("bytes_per_component") != 2 or chunk_format.get("channels") not in {1, 3}:
        return None
    return chunks, tile_grid, chunk_format


def _load_timestamp_chunk_index(timestamp_dir: Path) -> tuple[list[list[int]], dict, dict] | None:
    index_file = timestamp_dir / "index.json"
    try:
        stat_result = index_file.stat()
    except FileNotFoundError:
        return None

    return _load_timestamp_chunk_index_cached(str(index_file), stat_result.st_mtime_ns)


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
    cap = str(numeric_thread_cap_value())
    for env_var in numeric_thread_cap_variables():
        os.environ.setdefault(env_var, cap)


def _adaptive_process_worker_count(layer_count: int, phase_name: str) -> int:
    if layer_count <= 1:
        return 1

    cpu_cap = min(layer_count, max(1, os.cpu_count() or 1))
    if cpu_cap <= 1:
        return 1

    budget_mb = worker_budget_mb(phase_name)
    reserve_mb = worker_reserve_mb()

    try:
        import psutil

        available_mb = psutil.virtual_memory().available / (1024.0 * 1024.0)
        usable_mb = max(0.0, available_mb - reserve_mb)
        memory_cap = max(1, int(usable_mb // max(1.0, budget_mb)))
        return max(1, min(cpu_cap, memory_cap))
    except Exception:
        return max(1, min(cpu_cap, worker_psutil_fallback_max()))


def _render_layer(layer) -> tuple[str, RenderOutput]:
    """Render a single layer. Returns (name, png_path or None)."""
    from EWMRS.render.render import GUIArrayRenderer, GUIValueWriter, GUILayerRenderer
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
    pinned_input_path = layer.get("input_path")

    if source_path is None or output_path is None:
        io_mgr.write_error(f"Layer {name} is missing filepath/outdir configuration")
        return name, None

    src_dir = Path(source_path)
    out_dir = Path(output_path)

    try:
        if not src_dir.exists():
            io_mgr.write_warning(f"Source directory missing for {name}: {src_dir}")
            return name, None

        if layer.get("input_manifest_bound"):
            latest_file = (
                Path(pinned_input_path)
                if pinned_input_path is not None
                else None
            )
        else:
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

        source_label = (
            "pinned source file"
            if layer.get("input_manifest_bound")
            else "latest source file"
        )
        io_mgr.write_info(f"Using {source_label} for {name}: {latest_file}")

        if source_type == "goes_abi":
            payload = load_reproject_goes_abi_render_array(
                latest_file,
                layer,
                shape=GOES_WEB_MERCATOR_SHAPE,
                transform=GOES_WEB_MERCATOR_TRANSFORM,
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
        timestamp_dir = out_dir / timestamp
        chunk_dir = timestamp_dir / "chunks"
        if not chunk_dir.is_dir():
            return None

        index_file = out_dir / "index.json"
        tile_grid = None
        if index_file.exists():
            with open(index_file, "r") as f:
                data = json.load(f)

            if not isinstance(data, dict) or data.get("schema_version") != 2 or data.get("representation") != "binary_chunks":
                return None
            timestamps = data.get("timestamps", [])
            if timestamp not in timestamps:
                return None
            if not isinstance(data, list):
                tile_grid = data.get("tile_grid")

        timestamp_index = _load_timestamp_chunk_index(timestamp_dir)
        if timestamp_index is None:
            return None

        indexed_tiles, timestamp_tile_grid, chunk_format = timestamp_index
        if timestamp_tile_grid is not None:
            tile_grid = timestamp_tile_grid

        tile_paths: list[tuple[int, int, Path]] = []
        for tile in indexed_tiles:
            if not isinstance(tile, list) or len(tile) != 2:
                return None

            tile_x, tile_y = tile
            if not isinstance(tile_x, int) or not isinstance(tile_y, int):
                return None

            if tile_grid is not None:
                rows = tile_grid.get("rows")
                cols = tile_grid.get("cols")
                if isinstance(rows, int) and isinstance(cols, int):
                    if tile_x < 0 or tile_x >= cols or tile_y < 0 or tile_y >= rows:
                        return None

            channels = chunk_format.get("channels")
            if not isinstance(channels, int) or channels not in {1, 3}:
                return None
            tile_path = chunk_dir / f"chunk_{tile_x}_{tile_y}.f16.gz"
            if not tile_path.is_file() or tile_path.stat().st_size <= 0:
                return None

            tile_paths.append((tile_y, tile_x, tile_path))

        tile_paths.sort(key=lambda item: (item[0], item[1]))
        return [path for _, _, path in tile_paths]
    except Exception:
        return None
def _normalize_render_timestamp(timestamp_iso: str) -> str:
    dt = datetime.fromisoformat(timestamp_iso)
    return dt.strftime(r"%Y%m%d-%H%M00")


def _cleanup_old_nexrad_gui_files(max_age_minutes: int | None = None) -> int:
    """Remove stale NEXRAD GUI files and empty site/elevation directories."""

    if max_age_minutes is None:
        max_age_minutes = gui_cleanup_max_age_minutes()

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
    manifest_path = serialize_nexrad_elevation_artifacts(
        site, str(metadata["volume_id"]), str(metadata["scan_timestamp"]), [artifact]
    )
    if manifest_path is not None and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            layers = manifest.get("layers") or []
            if layers:
                for layer in layers:
                    bin_path = layer.get("bin_path")
                    if bin_path is None or not Path(bin_path).exists():
                        return False
                return True
        except Exception:
            pass
    return _nexrad_gui_timestamp_exists(site, elevation, timestamp)


def render_pending_nexrad_gui_files(*, base_dir=None, max_source_age_minutes: int | None = None) -> int:
    import concurrent.futures

    if max_source_age_minutes is None:
        max_source_age_minutes = nexrad_source_max_age_minutes()

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

    max_workers = min(nexrad_render_max_workers(), len(pending_metadata))
    rendered_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_render_pending_nexrad_gui_artifact, metadata) for metadata in pending_metadata]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                rendered_count += 1

    return rendered_count


def run_nexrad_render_loop(*, base_dir=None, poll_interval_seconds: float | None = None) -> None:
    if poll_interval_seconds is None:
        poll_interval_seconds = nexrad_poll_interval_seconds()
    poll_interval_seconds = max(nexrad_poll_interval_min_seconds(), float(poll_interval_seconds))
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


def cleanup_old_gui_files(max_age_minutes: int | None = None):
    """Remove old files/folders from GUI output directories."""
    import shutil

    if max_age_minutes is None:
        max_age_minutes = gui_cleanup_max_age_minutes()

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

                if isinstance(data, dict) and data.get("schema_version") == 2 and data.get("representation") == "binary_chunks":
                    output_data = {**data, "timestamps": timestamps}
                else:
                    output_data = {"timestamps": timestamps, "tile_grid": tile_grid} if tile_grid is not None else timestamps
                atomic_write_json(index_file, output_data)
            except Exception as exc:
                io_manager.write_warning(f"Failed to update index.json in {out_dir}: {exc}")

    total_removed += _cleanup_old_nexrad_gui_files(max_age_minutes=max_age_minutes)

    if total_removed > 0:
        io_manager.write_info(f"Cleaned up {total_removed} old GUI files/folders (>{max_age_minutes} min)")


def run_render_pipeline(
    dt,
    max_entries: int | None = None,
    layers=None,
    phase_name: str | None = None,
    cleanup_after: bool | None = None,
    input_manifest: CycleInputManifest | None = None,
) -> Dict[str, RenderOutput]:
    """Render configured EWMRS layers from already staged local files.

    ``max_entries`` is accepted and ignored. It is part of the GOES render task
    tuple that ``util.runtime.cycle`` queues and ``util.runtime.background``
    unpacks, so the parameter has to stay, but nothing downstream of here reads
    it: layer selection comes from the catalogs, not a count. Its only owner is
    ``runtime.yaml goes_coordination.render_task_max_entries``, which is why
    ``ewmrs_pipeline.yaml`` does not carry a second copy.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    if phase_name is None:
        phase_name = render_phase_name()
    if cleanup_after is None:
        cleanup_after = render_cleanup_after()

    dt = _ensure_dt(dt)
    results: Dict[str, RenderOutput] = {}

    layers = get_file_list() if layers is None else list(layers)
    if input_manifest is not None:
        pinned_layers = []
        for layer in layers:
            pinned_layer = dict(layer)
            source_path = pinned_layer.get("filepath")
            if source_path is not None:
                record = input_manifest.latest_for_directory(source_path)
                pinned_layer["input_path"] = (
                    str(record.local_path) if record is not None else None
                )
                pinned_layer["input_manifest_bound"] = True
            pinned_layers.append(pinned_layer)
        layers = pinned_layers
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
        cleanup_old_gui_files(max_age_minutes=gui_cleanup_max_age_minutes())
    return results


def run_mrms_render_pipeline(
    dt,
    max_entries: int | None = None,
    input_manifest: CycleInputManifest | None = None,
) -> Dict[str, RenderOutput]:
    """Run the MRMS-backed EWMRS render phase."""
    return run_render_pipeline(
        dt,
        max_entries=max_entries,
        layers=get_mrms_file_list(),
        phase_name="MRMS",
        input_manifest=input_manifest,
    )


def _maybe_cleanup_goes_gui_files(max_age_minutes: int | None = None) -> None:
    global _LAST_GOES_GUI_CLEANUP_S, _LAST_GOES_GUI_CLEANUP_FUNC_ID

    if max_age_minutes is None:
        max_age_minutes = goes_cleanup_max_age_minutes()

    min_interval_s = goes_cleanup_min_interval_seconds()
    now_s = time.perf_counter()
    current_cleanup_func_id = id(cleanup_old_gui_files)
    if (
        _LAST_GOES_GUI_CLEANUP_FUNC_ID == current_cleanup_func_id
        and min_interval_s > 0
        and (now_s - _LAST_GOES_GUI_CLEANUP_S) < min_interval_s
    ):
        io_manager.write_debug(
            f"Skipping GOES GUI cleanup: last run was {(now_s - _LAST_GOES_GUI_CLEANUP_S):.1f}s ago"
        )
        return

    cleanup_old_gui_files(max_age_minutes=max_age_minutes)
    _LAST_GOES_GUI_CLEANUP_S = now_s
    _LAST_GOES_GUI_CLEANUP_FUNC_ID = current_cleanup_func_id


def run_goes_render_pipeline(
    dt,
    max_entries: int | None = None,
    input_manifest: CycleInputManifest | None = None,
) -> Dict[str, RenderOutput]:
    """Run the GOES-backed EWMRS render phase for raw ABI channels.

    EWMRS serves the raw ABI channel values; GoES RGB composites are a
    client-side derivation and are not rendered server-side.
    """
    layers = get_goes_file_list()
    if not layers:
        io_manager.write_info("GOES render phase is a no-op: no GOES layers configured")
        return {}

    pipeline_start_s = time.perf_counter()
    results = run_render_pipeline(
        dt,
        max_entries=max_entries,
        layers=layers,
        phase_name="GOES",
        cleanup_after=False,
        input_manifest=input_manifest,
    )
    _maybe_cleanup_goes_gui_files()

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
    max_entries: int | None = None,
):
    """Process target for staged EWMRS rendering within the tandem runner."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    def publish_stage(status, *, artifacts=(), errors=()):
        shared_state["ewmrs_stage"] = {
            "status": str(status),
            "produced_artifacts": [str(path) for path in artifacts],
            "errors": [str(error) for error in errors],
        }

    try:
        log(f"INFO: EWMRS worker waiting for MRMS render inputs for {dt}")
        ewmrs_mrms_ready_event.wait()

        mrms_inputs_ready = shared_state.get(
            "ewmrs_mrms_inputs_ready",
            False,
        )
        if not mrms_inputs_ready:
            message = "EWMRS MRMS inputs were not staged successfully"
            publish_stage("unavailable", errors=(message,))
            log(f"ERROR: {message}; skipping MRMS render")
        else:
            input_manifest = CycleInputManifest.from_dict(
                shared_state.get("input_manifest")
            )
            if input_manifest is None:
                message = "Cycle input manifest was not published for EWMRS"
                publish_stage("failed", errors=(message,))
                log(f"ERROR: {message}")
                return
            log("INFO: Starting EWMRS MRMS render phase")
            results = run_mrms_render_pipeline(
                dt,
                max_entries=max_entries,
                input_manifest=input_manifest,
            )
            failed_layers = sorted(
                str(layer_name)
                for layer_name, output in results.items()
                if output is None
            )
            artifacts = [
                str(path)
                for output in results.values()
                if output is not None
                for path in (output if isinstance(output, list) else [output])
            ]
            if not results or failed_layers:
                message = (
                    "EWMRS MRMS render did not produce the complete required layer set"
                    if not failed_layers
                    else f"EWMRS MRMS render missing required layers: {', '.join(failed_layers)}"
                )
                publish_stage(
                    "failed",
                    artifacts=artifacts,
                    errors=(message,),
                )
            else:
                publish_stage("completed", artifacts=artifacts)
            log(f"INFO: EWMRS MRMS render completed: {_summarize_results(results)}")
        log("INFO: EWMRS GOES render is decoupled from the tandem worker")
    except Exception as exc:
        publish_stage("failed", errors=(str(exc),))
        log(f"ERROR: EWMRS tandem worker failed - {exc}")
        raise


def ewmrs_goes_worker(
    log_queue,
    dt,
    max_entries: int | None = None,
    input_manifest=None,
):
    """Process target for decoupled GOES rendering outside tandem completion."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(msg: str):
        log_queue.put(str(msg))

    try:
        if isinstance(input_manifest, dict):
            input_manifest = CycleInputManifest.from_dict(input_manifest)
        if input_manifest is None:
            log("ERROR: EWMRS GOES render skipped because no pinned input manifest was provided")
            return
        log(f"INFO: Starting EWMRS GOES render phase for {dt}")
        results = run_goes_render_pipeline(
            dt,
            max_entries=max_entries,
            input_manifest=input_manifest,
        )
        log(f"INFO: EWMRS GOES render completed: {_summarize_results(results)}")
    except Exception as exc:
        log(f"ERROR: EWMRS GOES worker failed - {exc}")

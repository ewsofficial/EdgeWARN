"""Local readiness helpers for staged GOES/ABI render and GLM integration checks."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

_GOES_SCAN_WINDOW_PATTERN = re.compile(
    r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)"
    r"_e(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)"
)
_GOES_SCAN_START_PATTERN = re.compile(r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)")
_GOES_RENDER_MAX_OFFSET_MINUTES = 20.0


def get_ewmrs_goes_render_specs():
    """Return configured GOES ABI render layers with local source directories."""
    from EWMRS.render.config import get_goes_file_list

    specs = []
    for layer in get_goes_file_list():
        source_type = str(layer.get("source_type", "")).lower()
        source_path = layer.get("filepath")
        if source_type != "goes_abi" or source_path is None:
            continue

        specs.append(
            {
                "name": layer.get("name"),
                "filepath": Path(source_path),
            }
        )

    return specs


def _parse_goes_token(groups):
    year, day_of_year, hour, minute, second, _subsecond = groups
    return datetime.strptime(
        f"{year}{day_of_year}{hour}{minute}{second}",
        "%Y%j%H%M%S",
    ).replace(tzinfo=timezone.utc)


def _normalize_target_dt(target_dt):
    if target_dt.tzinfo is None:
        return target_dt.replace(tzinfo=timezone.utc)
    return target_dt.astimezone(timezone.utc)


def parse_staged_file_time_window(filepath):
    """Parse staged filename into `(start_dt, end_dt)` bounds in UTC.

    GOES ABI filenames encode an explicit scan start/end window as `s..._e...`.
    Readiness checks should treat that whole window as valid for a target cycle.
    Non-windowed filenames are interpreted as a point timestamp `(dt, dt)`.
    """
    filename = Path(filepath).name

    window_match = _GOES_SCAN_WINDOW_PATTERN.search(filename)
    if window_match is not None:
        groups = window_match.groups()
        try:
            start_dt = _parse_goes_token(groups[:6])
            end_dt = _parse_goes_token(groups[6:])
            return start_dt, end_dt
        except ValueError:
            pass

    file_dt = parse_staged_file_timestamp(filepath)
    if file_dt is None:
        return None

    return file_dt, file_dt


def parse_staged_file_timestamp(filepath):
    """Parse MRMS/GOES-style staged file timestamps into UTC datetimes."""
    filename = Path(filepath).name
    patterns = [
        r"MRMS_MergedReflectivityQC_(\d{8})-(\d{6})",
        r"(\d{8})-(\d{6})_renamed",
        r"(\d{8}-\d{6})",
        r".*(\d{8})-(\d{6}).*",
        _GOES_SCAN_START_PATTERN.pattern,
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match is None:
            continue

        groups = match.groups()
        try:
            if len(groups) == 2:
                date_str, time_str = groups
                return datetime.strptime(f"{date_str}-{time_str}", "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)

            if len(groups) == 1:
                return datetime.strptime(groups[0], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)

            if len(groups) == 6:
                return _parse_goes_token(groups)
        except ValueError:
            continue

    return None


def _window_distance_seconds(target_dt, start_dt, end_dt):
    if start_dt <= target_dt <= end_dt:
        return 0.0
    if target_dt < start_dt:
        return (start_dt - target_dt).total_seconds()
    return (target_dt - end_dt).total_seconds()


def latest_goes_file_near_target(directory, target_dt, *, max_offset_minutes=_GOES_RENDER_MAX_OFFSET_MINUTES):
    """Return the staged GOES file nearest ``target_dt`` within the allowed offset."""
    directory = Path(directory)
    try:
        candidates = [
            path
            for path in directory.iterdir()
            if path.is_file() and path.suffix.lower() != ".idx"
        ]
    except OSError:
        candidates = []
    if not candidates:
        return None

    target_dt = _normalize_target_dt(target_dt)
    max_offset_seconds = max(0.0, float(max_offset_minutes)) * 60.0
    best_path = None
    best_distance = None

    for candidate_path in candidates:
        file_window = parse_staged_file_time_window(candidate_path)
        if file_window is None:
            continue

        start_dt, end_dt = file_window
        if end_dt < start_dt:
            continue

        distance_seconds = _window_distance_seconds(target_dt, start_dt, end_dt)
        if distance_seconds > max_offset_seconds:
            continue

        if best_distance is None or distance_seconds < best_distance:
            best_path = candidate_path
            best_distance = distance_seconds
        elif distance_seconds == best_distance and best_path is not None:
            candidate_time = parse_staged_file_timestamp(candidate_path)
            best_time = parse_staged_file_timestamp(best_path)
            if (
                candidate_time is not None
                and best_time is not None
                and candidate_time > best_time
            ):
                best_path = candidate_path

    return best_path


def check_local_goes_ready(dt, *, specs=None):
    """Readiness for EWMRS GOES phase: requires the full configured ABI render set."""
    matched_paths = collect_local_goes_paths(dt, specs=specs)
    if not matched_paths:
        return False, None
    return True, str(matched_paths[0][1])


def collect_local_goes_paths(dt, *, specs=None):
    """Return every exact configured ABI path selected for ``dt``."""
    candidate_specs = get_ewmrs_goes_render_specs() if specs is None else specs
    if not candidate_specs:
        return ()

    matched_paths = []

    for spec in candidate_specs:
        source_path = spec.get("filepath") if isinstance(spec, dict) else getattr(spec, "filepath", None)
        if source_path is None:
            continue

        latest_path = latest_goes_file_near_target(source_path, dt)
        if latest_path is None:
            return ()
        product = (
            spec.get("name")
            if isinstance(spec, dict)
            else getattr(spec, "name", None)
        )
        matched_paths.append((str(product or Path(source_path).name), Path(latest_path)))

    return tuple(matched_paths)


def check_local_glm_ready(dt, *, specs):
    """Readiness for EdgeWARN integration: requires locally staged GLM input."""
    for spec in specs:
        is_glm = spec.get("is_glm", False) if isinstance(spec, dict) else getattr(spec, "is_glm", False)
        if not is_glm:
            continue

        outdir = spec.get("outdir") if isinstance(spec, dict) else getattr(spec, "outdir", None)
        if outdir is None:
            continue

        latest_path = latest_goes_file_near_target(outdir, dt)
        if latest_path is not None:
            return True, latest_path

    return False, None

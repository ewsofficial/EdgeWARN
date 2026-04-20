"""Local readiness helpers for staged GOES/ABI render and GLM integration checks."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import util.file as fs


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


def parse_staged_file_timestamp(filepath):
    """Parse MRMS/GOES-style staged file timestamps into UTC datetimes."""
    filename = Path(filepath).name
    patterns = [
        r"MRMS_MergedReflectivityQC_(\d{8})-(\d{6})",
        r"(\d{8})-(\d{6})_renamed",
        r"(\d{8}-\d{6})",
        r".*(\d{8})-(\d{6}).*",
        r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)",
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
                year, day_of_year, hour, minute, second, _subsecond = groups
                return datetime.strptime(
                    f"{year}{day_of_year}{hour}{minute}{second}",
                    "%Y%j%H%M%S",
                ).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def latest_goes_file_at_or_after(directory, target_dt):
    """Return the newest staged file path in directory if timestamp is >= target_dt."""
    latest = fs.latest_files(directory, 1)
    if not latest:
        return None

    latest_path = latest[-1]
    file_dt = parse_staged_file_timestamp(latest_path)
    if file_dt is None:
        return None

    if file_dt >= target_dt:
        return latest_path

    return None


def check_local_goes_ready(dt, *, specs=None):
    """Readiness for EWMRS GOES phase: requires configured ABI render inputs."""
    candidate_specs = get_ewmrs_goes_render_specs() if specs is None else specs
    if not candidate_specs:
        return False, None

    for spec in candidate_specs:
        source_path = spec.get("filepath") if isinstance(spec, dict) else getattr(spec, "filepath", None)
        if source_path is None:
            continue

        latest_path = latest_goes_file_at_or_after(source_path, dt)
        if latest_path is not None:
            return True, latest_path

    return False, None


def check_local_glm_ready(dt, *, specs):
    """Readiness for EdgeWARN integration: requires locally staged GLM input."""
    for spec in specs:
        is_glm = spec.get("is_glm", False) if isinstance(spec, dict) else getattr(spec, "is_glm", False)
        if not is_glm:
            continue

        outdir = spec.get("outdir") if isinstance(spec, dict) else getattr(spec, "outdir", None)
        if outdir is None:
            continue

        latest_path = latest_goes_file_at_or_after(outdir, dt)
        if latest_path is not None:
            return True, latest_path

    return False, None

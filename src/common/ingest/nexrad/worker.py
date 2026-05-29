"""Worker parse/export entrypoint for NEXRAD Level-II elevation artifacts.

Modeled after nexrad_overlap_scan_poc_low_rss.py: parent stays disk-backed
and stream-oriented, while the worker handles parse and export work.

Memory optimizations:
- mmap-based file reading (zero-copy, OS-paged on demand)
- Incremental parsing by offset (resume from last parsed position)
- Streaming BZ2 decompression (one block at a time)
- Deferred heavy imports (only when needed)
- Early cleanup of metadata_records + trailing_bytes after first elevation write
"""

from __future__ import annotations

import ctypes
import gc
import resource
import sys
from pathlib import Path

from common.ingest.nexrad.grouping import elevation_group_key, group_sweeps_by_elevation
from common.ingest.nexrad.models import ElevationArtifact, SweepRecord, WorkerParseResult
from common.ingest.nexrad.parser import parse_raw_volume_file_mmap
from common.ingest.nexrad.s3_chunks import format_nexrad_timestamp, parse_nexrad_timestamp
from common.ingest.nexrad.writer import write_elevation_artifacts


def _get_child_rss_kb() -> float:
    """Return peak RSS of the current process in KB."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss


def _clear_worker_caches() -> None:
    """Clear internal caches held by heavy libraries and return freed heap to OS."""
    dask_base = sys.modules.get("dask.base")
    if dask_base is not None:
        try:
            dask_base._seen.clear()
        except Exception:
            pass

    netcdf4 = sys.modules.get("netCDF4")
    if netcdf4 is not None:
        try:
            netcdf4.Dataset._cls_dict.clear()
        except Exception:
            pass

    gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        malloc_trim = getattr(libc, "malloc_trim", None)
        if malloc_trim is not None:
            malloc_trim(0)
    except Exception:
        pass


def _timestamp_sort_key(value: str | None) -> tuple[int, str]:
    if not value:
        return (0, "")
    parsed = parse_nexrad_timestamp(value)
    if parsed is not None:
        normalized = format_nexrad_timestamp(parsed)
        if normalized:
            return (1, normalized)
    return (0, str(value))


def _normalize_seen_elevation_exports(value: dict[str, str | None] | set[str] | None) -> dict[str, str | None]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): (str(timestamp) if timestamp else None) for key, timestamp in value.items()}
    return {str(key): None for key in value}


def _should_export_group(group_key: str, group_timestamp: str | None, seen_elevation_exports: dict[str, str | None]) -> bool:
    previous_timestamp = seen_elevation_exports.get(group_key)
    if previous_timestamp is None:
        return group_key not in seen_elevation_exports or group_timestamp is not None
    if group_timestamp is None:
        return False
    return _timestamp_sort_key(group_timestamp) > _timestamp_sort_key(previous_timestamp)


def parse_and_export(
    volume_path: str | Path,
    output_root: str | Path,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    seen_elevation_keys: dict[str, str | None] | set[str] | None = None,
    trim_buffer: bool = False,
    parse_offset: int = 0,
) -> WorkerParseResult:
    """Parse a partial .ar2v and emit newly-complete elevation artifacts.

    Args:
        volume_path: Path to the current partial .ar2v file.
        output_root: Root directory for emitted elevation artifacts.
        site: Radar site ID (upper-case).
        volume_id: Volume identifier.
        scan_timestamp: Scan-level timestamp for manifest tracking.
        seen_elevation_keys: Already-exported group keys or group-key timestamp metadata.
        parse_offset: Byte offset to resume parsing from (0 = full parse).

    Returns:
        WorkerParseResult with metadata about newly exported artifacts.
    """
    seen_elevation_exports = _normalize_seen_elevation_exports(seen_elevation_keys)

    saved_sweep_count = 0
    saved_elevations: list[ElevationArtifact] = []
    parse_error: str | None = None
    visible_sweeps = 0
    buffer_trimmed = False
    runtime_size: int | None = None

    try:
        raw_volume = parse_raw_volume_file_mmap(volume_path, parse_offset=parse_offset)

        visible_sweeps = len(raw_volume.sweeps)
        sweep_records = _extract_worker_sweep_records(raw_volume)

        elevation_groups = group_sweeps_by_elevation(sweep_records)
        dropped_group_names: set[str] = set()
        sweeps_by_group = {s.group_name: s for s in raw_volume.sweeps}

        first_elevation_written = False
        for group in elevation_groups:
            key = elevation_group_key(group)
            if not _should_export_group(key, group.first_timestamp, seen_elevation_exports):
                dropped_group_names.update(member.group_name for member in group.members)
                continue

            elevation_label = str(group.canonical_angle_deg)
            first_ts = group.first_timestamp

            artifacts = write_elevation_artifacts(
                group,
                raw_volume,
                site=str(site).upper(),
                volume_id=str(volume_id),
                scan_timestamp=scan_timestamp,
                elevation_label=elevation_label,
                elevation_timestamp=first_ts,
                output_root=output_root,
            )

            for artifact in artifacts:
                saved_elevations.append(artifact)
                seen_elevation_exports[key] = artifact.elevation_timestamp or artifact.scan_timestamp

            saved_sweep_count += len(group.members)
            dropped_group_names.update(member.group_name for member in group.members)

            for member in group.members:
                sweep = sweeps_by_group.get(member.group_name)
                if sweep is not None:
                    sweep.records.clear()

            if not first_elevation_written:
                first_elevation_written = True
                del raw_volume.metadata_records
                raw_volume.metadata_records = []
                del raw_volume.trailing_bytes
                raw_volume.trailing_bytes = b""

        if trim_buffer and raw_volume.compression_record_count == 0:
            with open(volume_path, "wb") as f:
                f.write(raw_volume.volume_header)
                for record in raw_volume.metadata_records:
                    f.write(record)
                for raw_sweep in raw_volume.sweeps:
                    if raw_sweep.group_name in dropped_group_names:
                        continue
                    for record in raw_sweep.records:
                        f.write(record)
                f.write(raw_volume.trailing_bytes)
            buffer_trimmed = bool(dropped_group_names)
            runtime_size = Path(volume_path).stat().st_size

        del raw_volume

    except Exception as exc:
        parse_error = str(exc)

    _clear_worker_caches()

    return WorkerParseResult(
        visible_sweeps=visible_sweeps,
        saved_sweep_count=saved_sweep_count,
        saved_elevations=saved_elevations,
        parse_error=parse_error,
        child_rss_kb=_get_child_rss_kb(),
        buffer_trimmed=buffer_trimmed,
        runtime_size=runtime_size,
    )


def _extract_worker_sweep_records(raw_volume) -> list[SweepRecord]:
    sweep_records: list[SweepRecord] = []
    for raw_sweep in raw_volume.sweeps:
        if raw_sweep.fixed_angle is None:
            continue

        azimuth_count = raw_sweep.radial_count
        if azimuth_count <= 0 or not raw_sweep.complete:
            continue

        sweep_index = len(sweep_records)

        sweep_records.append(SweepRecord(
            index=sweep_index,
            group_name=raw_sweep.group_name,
            fixed_angle=raw_sweep.fixed_angle,
            waveform=raw_sweep.waveform,
            timestamp=raw_sweep.last_timestamp,
            azimuth_count=azimuth_count,
            elevation_number=raw_sweep.elevation_number,
        ))

    return sweep_records

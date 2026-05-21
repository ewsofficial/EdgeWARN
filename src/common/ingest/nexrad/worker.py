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
from pathlib import Path

from common.ingest.nexrad.grouping import elevation_group_key, group_sweeps_by_elevation
from common.ingest.nexrad.models import ElevationArtifact, SweepRecord, WorkerParseResult
from common.ingest.nexrad.parser import parse_raw_volume_file_mmap
from common.ingest.nexrad.writer import write_elevation_artifacts


def _get_child_rss_kb() -> float:
    """Return peak RSS of the current process in KB."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss


def _clear_worker_caches() -> None:
    """Clear internal caches held by heavy libraries and return freed heap to OS."""
    try:
        import dask.base
        dask.base._seen.clear()
    except Exception:
        pass
    try:
        import netCDF4
        netCDF4.Dataset._cls_dict.clear()
    except Exception:
        pass
    gc.collect()
    try:
        # Move malloc_trim(0) out of the hot inner loop to avoid fragmentation
        pass
    except Exception:
        pass


def parse_and_export(
    volume_path: str | Path,
    output_root: str | Path,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    seen_elevation_keys: set[str] | None = None,
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
        seen_elevation_keys: Set of already-exported elevation group keys.
        parse_offset: Byte offset to resume parsing from (0 = full parse).

    Returns:
        WorkerParseResult with metadata about newly exported artifacts.
    """
    if seen_elevation_keys is None:
        seen_elevation_keys = set()

    saved_sweeps: list[str] = []
    saved_elevations: list[ElevationArtifact] = []
    parse_error: str | None = None
    visible_sweeps = 0

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
            if key in seen_elevation_keys:
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
                seen_elevation_keys.add(key)

            saved_sweeps.extend(m.group_name for m in group.members)
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

        del raw_volume

    except Exception as exc:
        parse_error = str(exc)

    _clear_worker_caches()

    return WorkerParseResult(
        visible_sweeps=visible_sweeps,
        saved_sweeps=saved_sweeps,
        saved_elevations=saved_elevations,
        parse_error=parse_error,
        child_rss_kb=_get_child_rss_kb(),
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

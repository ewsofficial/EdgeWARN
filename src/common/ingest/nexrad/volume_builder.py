from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

from common.ingest.nexrad.config import (
    LOW_BINS,
    LOW_CHECKPOINT_HINT,
)
from common.ingest.nexrad.models import NexradIngestResult, ParsedVolume, SweepInfo, SweepRecord
from common.ingest.nexrad.parser import (
    extract_azimuth_count,
    extract_sweep_angle,
    extract_sweep_timestamp,
    extract_waveform,
    parse_raw_volume_file,
)
from common.ingest.nexrad.sweep_classifier import canonical_angle_matches, classify_sweeps
from common.ingest.nexrad.writer import write_outputs
from util.io import IOManager

io_manager = IOManager("[NEXRAD]", include_timestamps=True)


def _sweep_group_sort_key(group_name: str):
    match = re.search(r"/sweep_(\d+)$", str(group_name))
    return int(match.group(1)) if match else 10**9


def _raw_sweeps_to_sweep_info(raw_sweeps: list) -> list[SweepInfo]:
    """Convert raw sweep list to SweepInfo list without xradar."""
    sweeps = []
    for raw_sweep in raw_sweeps:
        if raw_sweep.fixed_angle is None:
            continue
        azimuth_count = raw_sweep.radial_count
        waveform = raw_sweep.waveform
        sweeps.append(
            SweepInfo(
                index=raw_sweep.index,
                group_name=raw_sweep.group_name,
                fixed_angle=raw_sweep.fixed_angle,
                waveform=waveform,
                azimuth_count=azimuth_count,
                complete=raw_sweep.complete and azimuth_count > 0,
                supplemental=False,
                bucket="excluded",
            )
        )
    return sweeps


def extract_sweep_records(datatree, raw_sweeps_by_index: dict[int, object] | None = None) -> list[SweepRecord]:
    """Extract SweepRecord list from a parsed datatree."""
    records = []
    for group_name in sorted((g for g in datatree.groups if g.startswith("/sweep_")), key=_sweep_group_sort_key):
        node = datatree[group_name]
        dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
        raw_sweep = None if raw_sweeps_by_index is None else raw_sweeps_by_index.get(len(records))

        angle = raw_sweep.fixed_angle if raw_sweep is not None else extract_sweep_angle(dataset)
        if angle is None:
            continue

        azimuth_count = extract_azimuth_count(dataset)
        if azimuth_count <= 0 and raw_sweep is not None:
            azimuth_count = raw_sweep.radial_count
        waveform = extract_waveform(node)
        timestamp = (
            raw_sweep.last_timestamp
            if raw_sweep is not None and raw_sweep.last_timestamp is not None
            else extract_sweep_timestamp(dataset)
        )
        sweep_index = len(records)

        records.append(SweepRecord(
            index=sweep_index,
            group_name=group_name,
            fixed_angle=angle,
            waveform=waveform,
            timestamp=timestamp,
            azimuth_count=azimuth_count,
            elevation_number=None,
        ))
    return records


def _extract_parsed_volume(datatree, raw_sweeps_by_index: dict[int, object] | None = None) -> ParsedVolume:
    root_attrs = getattr(datatree, "attrs", {}) or {}
    scan_name = root_attrs.get("scan_name") or root_attrs.get("volume_scan_pattern")
    dynamic_scan_type = root_attrs.get("scan_strategy") or root_attrs.get("scan_name")
    sweeps = []

    for index, group_name in enumerate(sorted((group for group in datatree.groups if group.startswith("/sweep_")), key=_sweep_group_sort_key)):
        node = datatree[group_name]
        dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
        raw_sweep = None if raw_sweeps_by_index is None else raw_sweeps_by_index.get(index)
        fixed_angle = raw_sweep.fixed_angle if raw_sweep is not None else extract_sweep_angle(dataset)
        if fixed_angle is None:
            continue
        azimuth_count = extract_azimuth_count(dataset)
        if azimuth_count <= 0 and raw_sweep is not None:
            azimuth_count = raw_sweep.radial_count
        waveform = extract_waveform(node)
        sweeps.append(
            SweepInfo(
                index=index,
                group_name=group_name,
                fixed_angle=fixed_angle,
                waveform=waveform,
                azimuth_count=azimuth_count,
                complete=azimuth_count > 0,
                supplemental=False,
                bucket="excluded",
            )
        )

    return ParsedVolume(
        scan_name=scan_name,
        dynamic_scan_type=dynamic_scan_type,
        sweeps=sweeps,
        datatree=datatree,
        source_bucket="unidata-nexrad-level2-chunks",
    )


def _extract_parsed_volume_from_raw(raw_volume) -> ParsedVolume:
    """Build ParsedVolume from raw byte parsing only, no xradar."""
    sweeps = _raw_sweeps_to_sweep_info(raw_volume.sweeps)
    return ParsedVolume(
        scan_name=None,
        dynamic_scan_type=None,
        sweeps=sweeps,
        datatree=None,
        source_bucket="unidata-nexrad-level2-chunks",
    )


def parse_level2_volume_file(path: str | Path) -> ParsedVolume:
    raw_volume = parse_raw_volume_file(path)
    return _extract_parsed_volume_from_raw(raw_volume)


def parse_level2_volume_bytes(volume_bytes: bytes) -> ParsedVolume:
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".ar2v") as temp_file:
            temp_file.write(volume_bytes)
            temp_path = temp_file.name
        return parse_level2_volume_file(temp_path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def _has_required_low_bins(classified_sweeps: list[SweepInfo]):
    low_sweeps = [sweep for sweep in classified_sweeps if sweep.bucket == "low" and sweep.complete]
    return all(any(canonical_angle_matches(sweep.fixed_angle, target) for sweep in low_sweeps) for target in LOW_BINS)





def _required_low_chunks_available(chunks, low_checkpoint: int) -> bool:
    available = {chunk.chunk_number for chunk in chunks}
    return all(number in available for number in range(1, low_checkpoint + 1))


def build_low_high_outputs(
    probe,
    chunks,
    *,
    chunk_fetcher,
    parser=None,
    parser_file=parse_level2_volume_file,
    writer=write_outputs,
    base_dir=None,
):
    if not chunks:
        return NexradIngestResult(
            site=probe.site,
            volume_id=probe.volume_id,
            vcp=probe.vcp,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=None,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=0,
            complete=False,
        )

    max_chunk_number = chunks[-1].chunk_number
    low_checkpoint = min(LOW_CHECKPOINT_HINT, max_chunk_number)
    if not _required_low_chunks_available(chunks, low_checkpoint):
        io_manager.write_info(
            f"Volume {probe.site}/{probe.volume_id} is missing required low chunks through {low_checkpoint}; waiting before download"
        )
        return NexradIngestResult(
            site=probe.site,
            volume_id=probe.volume_id,
            vcp=probe.vcp,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=None,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=0,
            complete=False,
        )

    last_parsed = None
    classified = []
    chunks_downloaded = 0

    if parser is not None:
        low_bytes = b"".join(chunk_fetcher(chunk) for chunk in chunks if chunk.chunk_number <= low_checkpoint)
        chunks_downloaded = low_checkpoint
        try:
            last_parsed = parser(low_bytes)
            classified = classify_sweeps(last_parsed.sweeps, dynamic_scan_type=last_parsed.dynamic_scan_type)
            if _has_required_low_bins(classified):
                low_path, high_path, manifest_path = writer(
                    probe,
                    last_parsed,
                    classified,
                    chunks_downloaded,
                    base_dir=base_dir,
                )
                return NexradIngestResult(
                    site=probe.site,
                    volume_id=probe.volume_id,
                    vcp=probe.vcp,
                    dynamic_scan_type=last_parsed.dynamic_scan_type,
                    volume_path=None,
                    scan_timestamp=None,
                    low_path=low_path,
                    high_path=high_path,
                    manifest_path=manifest_path,
                    chunks_downloaded=chunks_downloaded,
                    complete=True,
                )
        except Exception as exc:
            io_manager.write_debug(
                f"Low-checkpoint parse failed at chunk {low_checkpoint} for {probe.site}/{probe.volume_id}: {exc}"
            )

        io_manager.write_warning(
            f"Unable to parse low checkpoint for {probe.site}/{probe.volume_id} after {chunks_downloaded} chunks"
        )
        return NexradIngestResult(
            site=probe.site,
            volume_id=probe.volume_id,
            vcp=probe.vcp,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=None,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=chunks_downloaded,
            complete=False,
        )
    else:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".ar2v") as volume_file:
                temp_path = volume_file.name
                low_chunks = [chunk for chunk in chunks if chunk.chunk_number <= low_checkpoint]
                for chunk in low_chunks:
                    volume_file.write(chunk_fetcher(chunk))
                volume_file.flush()
                chunks_downloaded = low_checkpoint

                try:
                    last_parsed = parser_file(temp_path)
                    classified = classify_sweeps(last_parsed.sweeps, dynamic_scan_type=last_parsed.dynamic_scan_type)
                    if _has_required_low_bins(classified):
                        low_path, high_path, manifest_path = writer(
                            probe,
                            last_parsed,
                            classified,
                            chunks_downloaded,
                            base_dir=base_dir,
                        )
                        return NexradIngestResult(
                            site=probe.site,
                            volume_id=probe.volume_id,
                            vcp=probe.vcp,
                            dynamic_scan_type=last_parsed.dynamic_scan_type,
                            volume_path=None,
                            scan_timestamp=None,
                            low_path=low_path,
                            high_path=high_path,
                            manifest_path=manifest_path,
                            chunks_downloaded=chunks_downloaded,
                            complete=True,
                        )
                except Exception as exc:
                    io_manager.write_debug(
                        f"Low-checkpoint parse failed at chunk {low_checkpoint} for {probe.site}/{probe.volume_id}: {exc}"
                    )

            io_manager.write_warning(
                f"Unable to parse low checkpoint for {probe.site}/{probe.volume_id} after {chunks_downloaded} chunks"
            )
            return NexradIngestResult(
                site=probe.site,
                volume_id=probe.volume_id,
                vcp=probe.vcp,
                dynamic_scan_type=None,
                volume_path=None,
                scan_timestamp=None,
                low_path=None,
                high_path=None,
                manifest_path=None,
                chunks_downloaded=chunks_downloaded,
                complete=False,
            )
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)

    low_path, high_path, manifest_path = writer(
        probe,
        last_parsed,
        classified,
        chunks_downloaded,
        base_dir=base_dir,
    )
    return NexradIngestResult(
        site=probe.site,
        volume_id=probe.volume_id,
        vcp=probe.vcp,
        dynamic_scan_type=last_parsed.dynamic_scan_type,
        volume_path=None,
        scan_timestamp=None,
        low_path=low_path,
        high_path=high_path,
        manifest_path=manifest_path,
        chunks_downloaded=chunks_downloaded,
        complete=False,
    )

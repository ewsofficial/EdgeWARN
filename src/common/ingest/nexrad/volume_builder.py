from __future__ import annotations

import os
import tempfile
import warnings
from pathlib import Path

from common.ingest.nexrad.config import (
    EXPECTED_HIGH_BINS,
    HIGH_CHECKPOINT_HINTS,
    LOW_BINS,
    LOW_CHECKPOINT_HINT,
)
from common.ingest.nexrad.models import NexradIngestResult, ParsedVolume, SweepInfo
from common.ingest.nexrad.sweep_classifier import canonical_angle_matches, classify_sweeps
from common.ingest.nexrad.writer import write_outputs
from util.io import IOManager

io_manager = IOManager("[NEXRAD]")


def _extract_parsed_volume(datatree) -> ParsedVolume:
    root_attrs = getattr(datatree, "attrs", {}) or {}
    scan_name = root_attrs.get("scan_name") or root_attrs.get("volume_scan_pattern")
    dynamic_scan_type = root_attrs.get("scan_strategy") or root_attrs.get("scan_name")
    sweeps = []

    for index, group_name in enumerate(sorted(group for group in datatree.groups if group.startswith("/sweep_"))):
        dataset = datatree[group_name].to_dataset()
        angle_var = dataset.get("sweep_fixed_angle")
        if angle_var is None:
            continue
        fixed_angle = float(angle_var.values.item())
        azimuth_count = int(dataset.sizes.get("azimuth", dataset.sizes.get("time", 0)))
        waveform = (
            dataset.attrs.get("waveform_type")
            or dataset.attrs.get("prt_mode")
            or dataset.attrs.get("sweep_mode")
        )
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


def _parse_level2_datatree(path: str | Path):
    try:
        import xradar as xd
    except ImportError as exc:
        raise RuntimeError("xradar is required for live NEXRAD volume parsing") from exc

    opener = getattr(xd.io.backends.nexrad_level2, "open_nexradlevel2_datatree", None)
    if opener is None:
        raise RuntimeError("xradar nexrad Level-II DataTree opener is unavailable")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            return opener(str(path), loaddata=False)
        except TypeError:
            return opener(str(path))


def parse_level2_volume_file(path: str | Path) -> ParsedVolume:
    datatree = _parse_level2_datatree(path)
    return _extract_parsed_volume(datatree)


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


def _has_required_high_bins(classified_sweeps: list[SweepInfo], vcp: int):
    high_sweeps = [sweep for sweep in classified_sweeps if sweep.bucket == "high" and sweep.complete]
    expected_bins = EXPECTED_HIGH_BINS.get(vcp, ())
    return all(any(canonical_angle_matches(sweep.fixed_angle, target) for sweep in high_sweeps) for target in expected_bins)


def _checkpoint_numbers(vcp: int, max_chunk_number: int):
    return {
        min(LOW_CHECKPOINT_HINT, max_chunk_number),
        max_chunk_number,
    }


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
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=0,
            complete=False,
        )

    max_chunk_number = chunks[-1].chunk_number
    checkpoint_numbers = _checkpoint_numbers(probe.vcp, max_chunk_number)
    last_parsed = None
    classified = []
    chunks_downloaded = 0

    if parser is not None:
        payload_parts = []
        for chunk in chunks:
            payload_parts.append(chunk_fetcher(chunk))
            chunks_downloaded = chunk.chunk_number
            if chunk.chunk_number not in checkpoint_numbers:
                continue

            try:
                last_parsed = parser(b"".join(payload_parts))
            except Exception as exc:
                io_manager.write_debug(
                    f"Partial parse not ready at chunk {chunk.chunk_number} for {probe.site}/{probe.volume_id}: {exc}"
                )
                continue
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
                    low_path=low_path,
                    high_path=high_path,
                    manifest_path=manifest_path,
                    chunks_downloaded=chunks_downloaded,
                    complete=True,
                )
        if last_parsed is None:
            try:
                last_parsed = parser(b"".join(payload_parts))
            except Exception as exc:
                io_manager.write_warning(
                    f"Unable to parse volume {probe.site}/{probe.volume_id} after {chunks_downloaded} chunks: {exc}"
                )
                return NexradIngestResult(
                    site=probe.site,
                    volume_id=probe.volume_id,
                    vcp=probe.vcp,
                    dynamic_scan_type=None,
                    low_path=None,
                    high_path=None,
                    manifest_path=None,
                    chunks_downloaded=chunks_downloaded,
                    complete=False,
                )
            classified = classify_sweeps(last_parsed.sweeps, dynamic_scan_type=last_parsed.dynamic_scan_type)
    else:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".ar2v") as volume_file:
                temp_path = volume_file.name
                for chunk in chunks:
                    volume_file.write(chunk_fetcher(chunk))
                    chunks_downloaded = chunk.chunk_number
                    if chunk.chunk_number not in checkpoint_numbers:
                        continue
                    volume_file.flush()

                    try:
                        last_parsed = parser_file(temp_path)
                    except Exception as exc:
                        io_manager.write_debug(
                            f"Partial parse not ready at chunk {chunk.chunk_number} for {probe.site}/{probe.volume_id}: {exc}"
                        )
                        continue

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
                            low_path=low_path,
                            high_path=high_path,
                            manifest_path=manifest_path,
                            chunks_downloaded=chunks_downloaded,
                            complete=True,
                        )

            if last_parsed is None:
                try:
                    last_parsed = parser_file(temp_path)
                except Exception as exc:
                    io_manager.write_warning(
                        f"Unable to parse volume {probe.site}/{probe.volume_id} after {chunks_downloaded} chunks: {exc}"
                    )
                    return NexradIngestResult(
                        site=probe.site,
                        volume_id=probe.volume_id,
                        vcp=probe.vcp,
                        dynamic_scan_type=None,
                        low_path=None,
                        high_path=None,
                        manifest_path=None,
                        chunks_downloaded=chunks_downloaded,
                        complete=False,
                    )
                classified = classify_sweeps(last_parsed.sweeps, dynamic_scan_type=last_parsed.dynamic_scan_type)
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
        low_path=low_path,
        high_path=high_path,
        manifest_path=manifest_path,
        chunks_downloaded=chunks_downloaded,
        complete=False,
    )

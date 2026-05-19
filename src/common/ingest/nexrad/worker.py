"""Worker parse/export entrypoint for NEXRAD Level-II elevation artifacts.

Modeled after nexrad_overlap_scan_poc_low_rss.py: parent stays disk-backed
and stream-oriented, while the worker handles heavy xradar parse and export.
"""

from __future__ import annotations

import json
import resource
from pathlib import Path

from common.ingest.nexrad.config import ANGLE_DEDUP_TOLERANCE_DEG
from common.ingest.nexrad.grouping import elevation_group_key, group_sweeps_by_elevation
from common.ingest.nexrad.models import ElevationArtifact, SweepRecord, WorkerParseResult
from common.ingest.nexrad.writer import write_elevation_artifacts
from common.ingest.nexrad.xradar_helpers import (
    extract_azimuth_count,
    extract_sweep_angle,
    extract_sweep_timestamp,
    extract_waveform,
    open_partial_volume,
)


def _get_child_rss_kb() -> float:
    """Return peak RSS of the current process in KB."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_maxrss


def parse_and_export(
    volume_path: str | Path,
    output_root: str | Path,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    seen_elevation_keys: set[str] | None = None,
) -> WorkerParseResult:
    """Parse a partial .ar2v and emit newly-complete elevation artifacts.

    Args:
        volume_path: Path to the current partial .ar2v file.
        output_root: Root directory for emitted elevation artifacts.
        site: Radar site ID (upper-case).
        volume_id: Volume identifier.
        scan_timestamp: Scan-level timestamp for manifest tracking.
        seen_elevation_keys: Set of already-exported elevation group keys.

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
        datatree = open_partial_volume(volume_path)

        sweep_records: list[SweepRecord] = []
        for group_name in sorted(g for g in datatree.groups if g.startswith("/sweep_")):
            node = datatree[group_name]
            dataset = node.ds if hasattr(node, "ds") else node.to_dataset()

            angle = extract_sweep_angle(dataset)
            if angle is None:
                continue

            azimuth_count = extract_azimuth_count(dataset)
            if azimuth_count <= 0:
                continue

            waveform = extract_waveform(node)
            timestamp = extract_sweep_timestamp(dataset)
            sweep_index = len(sweep_records)

            sweep_records.append(SweepRecord(
                index=sweep_index,
                group_name=group_name,
                fixed_angle=angle,
                waveform=waveform,
                timestamp=timestamp,
                azimuth_count=azimuth_count,
            ))

        visible_sweeps = len(sweep_records)

        elevation_groups = group_sweeps_by_elevation(sweep_records)

        for group in elevation_groups:
            key = elevation_group_key(group)
            if key in seen_elevation_keys:
                continue

            elevation_label = str(group.canonical_angle_deg)
            first_ts = group.first_timestamp

            artifacts = write_elevation_artifacts(
                group,
                datatree,
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

    except Exception as exc:
        parse_error = str(exc)

    return WorkerParseResult(
        visible_sweeps=visible_sweeps,
        saved_sweeps=saved_sweeps,
        saved_elevations=saved_elevations,
        parse_error=parse_error,
        child_rss_kb=_get_child_rss_kb(),
    )


def worker_main():
    """CLI entrypoint for subprocess worker invocation."""
    import argparse

    parser = argparse.ArgumentParser(description="NEXRAD elevation export worker")
    parser.add_argument("--volume-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--site", required=True)
    parser.add_argument("--volume-id", required=True)
    parser.add_argument("--scan-timestamp", default=None)
    parser.add_argument("--seen-keys", default="")
    args = parser.parse_args()

    seen = set(args.seen_keys.split(",")) if args.seen_keys else set()
    seen.discard("")

    result = parse_and_export(
        volume_path=args.volume_path,
        output_root=args.output_root,
        site=args.site,
        volume_id=args.volume_id,
        scan_timestamp=args.scan_timestamp,
        seen_elevation_keys=seen,
    )

    payload = {
        "visible_sweeps": result.visible_sweeps,
        "saved_sweeps": result.saved_sweeps,
        "saved_elevations": [
            {
                "site": a.site,
                "volume_id": a.volume_id,
                "scan_timestamp": a.scan_timestamp,
                "elevation": a.elevation,
                "elevation_timestamp": a.elevation_timestamp,
                "first_sweep_index": a.first_sweep_index,
                "last_sweep_index": a.last_sweep_index,
                "member_group_names": a.member_group_names,
                "waveforms_present": list(a.waveforms_present),
                "supplemental": a.supplemental,
                "netcdf_path": a.netcdf_path,
            }
            for a in result.saved_elevations
        ],
        "parse_error": result.parse_error,
        "child_rss_kb": result.child_rss_kb,
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    worker_main()

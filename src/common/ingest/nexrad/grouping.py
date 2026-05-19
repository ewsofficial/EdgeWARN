"""Elevation grouping for NEXRAD sweeps.

Modeled after nexrad_elevation_grouping_poc.py structural grouping algorithm.
"""

from common.ingest.nexrad.config import ANGLE_DEDUP_TOLERANCE_DEG
from common.ingest.nexrad.models import ElevationGroup, SweepRecord


def _canonical_angle(fixed_angle: float, tolerance: float = ANGLE_DEDUP_TOLERANCE_DEG) -> float:
    """Canonicalize a fixed angle by rounding to the nearest tolerance bucket."""
    return round(fixed_angle / tolerance) * tolerance


def _waveform_key(waveform: str | None) -> str:
    return str(waveform or "").strip().lower()


def _first_non_null_timestamp(members: list[SweepRecord]) -> str | None:
    """Return the first non-null timestamp from members in order."""
    for member in members:
        if member.timestamp is not None:
            return member.timestamp
    return None


def group_sweeps_by_elevation(
    sweeps: list[SweepRecord],
    *,
    tolerance: float = ANGLE_DEDUP_TOLERANCE_DEG,
) -> list[ElevationGroup]:
    """Group complete sweeps into elevation groups.

    Rules:
    - Sort sweeps by sweep index
    - Ignore incomplete sweeps (azimuth_count <= 0)
    - Canonicalize angle by tolerance
    - Merge same-angle sweeps into one elevation
    - Preserve waveform membership
    - Mark repeated low tilts after higher tilts as supplemental

    The emitted filename timestamp uses the first sweep timestamp in the group.
    """
    valid = [s for s in sweeps if s.azimuth_count > 0]
    valid.sort(key=lambda s: s.index)

    groups: dict[float, ElevationGroup] = {}
    max_angle_seen = 0.0

    for sweep in valid:
        canon = _canonical_angle(sweep.fixed_angle, tolerance)
        waveform = _waveform_key(sweep.waveform)

        if canon not in groups:
            groups[canon] = ElevationGroup(
                elevation_id=str(canon),
                canonical_angle_deg=canon,
            )

        group = groups[canon]
        group.members.append(sweep)
        group.waveforms_present.add(waveform)

        if len(group.members) == 1:
            group.first_sweep_index = sweep.index
        group.last_sweep_index = sweep.index

        if canon > max_angle_seen:
            max_angle_seen = canon

    for group in groups.values():
        group.first_timestamp = _first_non_null_timestamp(group.members)
        group.last_timestamp = group.members[-1].timestamp if group.members else None
        group.complete = len(group.members) > 0

        is_low_tilt = group.canonical_angle_deg <= 1.0
        is_repeated_after_high = group.last_sweep_index > 0 and group.canonical_angle_deg < max_angle_seen
        if is_low_tilt and is_repeated_after_high:
            group.supplemental = True

    result = sorted(groups.values(), key=lambda g: g.first_sweep_index)
    return result


def elevation_group_key(group: ElevationGroup) -> str:
    """Create a dedup key for an elevation group."""
    members_key = ",".join(m.group_name for m in group.members)
    return f"{group.elevation_id}:{members_key}"

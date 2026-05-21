"""Elevation grouping for NEXRAD sweeps."""

from common.ingest.nexrad.models import ElevationGroup, SweepRecord

MAX_ELEVATION_DEG = 4.0
FIXED_ELEVATION_BINS = (0.5, 0.9, 1.3, 1.8, 2.4, 3.1, 4.0)
SURVEILLANCE_WAVEFORM = "contiguous_surveillance"
DOPPLER_WAVEFORM = "contiguous_doppler"
SINGLE_ELEVATION_WAVEFORMS = {"staggered_pulse_pair", "batch"}
RECOGNIZED_WAVEFORMS = SINGLE_ELEVATION_WAVEFORMS | {SURVEILLANCE_WAVEFORM, DOPPLER_WAVEFORM}


def _canonical_angle(fixed_angle: float) -> float:
    """Return the elevation angle used for the grouped output label."""
    angle = float(fixed_angle)
    return min(FIXED_ELEVATION_BINS, key=lambda candidate: (abs(candidate - angle), candidate))


def _waveform_key(waveform: str | None) -> str:
    return str(waveform or "").strip().lower()


def _first_non_null_timestamp(members: list[SweepRecord]) -> str | None:
    """Return the first non-null timestamp from members in order."""
    for member in members:
        if member.timestamp is not None:
            return member.timestamp
    return None


def _finalize_group(group: ElevationGroup | None, result: list[ElevationGroup]) -> None:
    if group is None:
        return
    group.first_timestamp = _first_non_null_timestamp(group.members)
    group.last_timestamp = group.members[-1].timestamp if group.members else None
    group.complete = len(group.members) > 0
    result.append(group)


def _start_group(sweep: SweepRecord, waveform: str) -> ElevationGroup:
    canon = _canonical_angle(sweep.fixed_angle)
    return ElevationGroup(
        elevation_id=str(canon),
        canonical_angle_deg=canon,
        members=[sweep],
        waveforms_present={waveform},
        first_sweep_index=sweep.index,
        last_sweep_index=sweep.index,
    )


def _group_by_waveform(valid: list[SweepRecord]) -> list[ElevationGroup]:
    result: list[ElevationGroup] = []
    current_group: ElevationGroup | None = None

    for sweep in valid:
        if sweep.fixed_angle > MAX_ELEVATION_DEG:
            break

        waveform = _waveform_key(sweep.waveform)

        if waveform == SURVEILLANCE_WAVEFORM:
            _finalize_group(current_group, result)
            current_group = _start_group(sweep, waveform)
            continue

        if waveform == DOPPLER_WAVEFORM:
            if current_group is None:
                continue
            current_group.members.append(sweep)
            current_group.waveforms_present.add(waveform)
            current_group.last_sweep_index = sweep.index
            continue

        if waveform in SINGLE_ELEVATION_WAVEFORMS:
            _finalize_group(current_group, result)
            current_group = None
            single_group = _start_group(sweep, waveform)
            _finalize_group(single_group, result)
            continue

    _finalize_group(current_group, result)
    return result


def _same_raw_elevation(left: SweepRecord, right: SweepRecord) -> bool:
    if left.elevation_number is not None and right.elevation_number is not None:
        return left.elevation_number == right.elevation_number
    return left.fixed_angle == right.fixed_angle


def _group_without_waveforms(valid: list[SweepRecord]) -> list[ElevationGroup]:
    result: list[ElevationGroup] = []
    current_group: ElevationGroup | None = None
    current_sweep: SweepRecord | None = None

    for sweep in valid:
        if sweep.fixed_angle > MAX_ELEVATION_DEG:
            break

        waveform = _waveform_key(sweep.waveform)
        if current_group is None:
            current_group = _start_group(sweep, waveform)
            current_sweep = sweep
            continue

        if current_sweep is not None and _same_raw_elevation(current_sweep, sweep):
            current_group.members.append(sweep)
            current_group.waveforms_present.add(waveform)
            current_group.last_sweep_index = sweep.index
            current_sweep = sweep
            continue

        _finalize_group(current_group, result)
        current_group = _start_group(sweep, waveform)
        current_sweep = sweep

    _finalize_group(current_group, result)
    return result


def group_sweeps_by_elevation(
    sweeps: list[SweepRecord],
) -> list[ElevationGroup]:
    """Group complete sweeps into elevation groups using waveform sequencing.

    Rules:
    - Sort sweeps by sweep index
    - Ignore incomplete sweeps (azimuth_count <= 0)
    - Force grouped elevations into the fixed levels 0.5, 0.9, 1.3, 1.8, 2.4, 3.1, 4.0
    - Stop processing the volume once the sweep angle exceeds 4.0 degrees
    - A grouped elevation starts with `contiguous_surveillance`
    - Following contiguous `contiguous_doppler` sweeps join that elevation
    - `staggered_pulse_pair` and `batch` each become single-sweep elevations
    - `contiguous_doppler` sweeps without a leading surveillance sweep are ignored
    """
    valid = [s for s in sweeps if s.azimuth_count > 0]
    valid.sort(key=lambda s: s.index)
    has_recognized_waveforms = any(_waveform_key(sweep.waveform) in RECOGNIZED_WAVEFORMS for sweep in valid)
    if has_recognized_waveforms:
        return _group_by_waveform(valid)
    return _group_without_waveforms(valid)


def elevation_group_key(group: ElevationGroup) -> str:
    """Create a dedup key for an elevation group."""
    members_key = ",".join(m.group_name for m in group.members)
    return f"{group.elevation_id}:{members_key}"

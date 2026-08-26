"""Elevation grouping for NEXRAD sweeps."""

from common.ingest.nexrad import config as nexrad_config
from common.ingest.nexrad.models import ElevationGroup, SweepRecord


def canonical_elevation_ids() -> tuple[str, ...]:
    """The canonical bins as the string labels that reach grouped output."""
    return tuple(str(angle) for angle in nexrad_config.canonical_elevation_bins())


def ingest_readiness_elevation_ids() -> tuple[str, ...]:
    """Elevations a volume must carry before readiness is satisfied.

    The same tuple as :func:`canonical_elevation_ids` today. It keeps its own
    name because readiness and output labelling are separate contracts that
    happen to coincide, and a caller reading one should not be made to reason
    about the other.
    """
    return canonical_elevation_ids()


# Backwards-compatible snapshot for benchmark consumers that import the
# readiness set as a module constant. Runtime pipeline code should call the
# function above so configuration changes are observed on each cycle.
INGEST_READINESS_ELEVATION_IDS = ingest_readiness_elevation_ids()


def _canonical_angle(fixed_angle: float) -> float:
    """Return the elevation angle used for the grouped output label."""
    angle = float(fixed_angle)
    bins = nexrad_config.canonical_elevation_bins()
    return min(bins, key=lambda candidate: (abs(candidate - angle), candidate))


def _waveform_key(waveform: str | None) -> str:
    return str(waveform or "").strip().lower()


def _first_non_null_timestamp(members: list[SweepRecord]) -> str | None:
    """Return the first non-null timestamp from members in order."""
    for member in members:
        if member.timestamp is not None:
            return member.timestamp
    return None


def _group_representative_angle(group: ElevationGroup) -> float:
    """Prefer the doppler angle when present, otherwise the first member angle."""
    doppler = nexrad_config.doppler_waveform()
    for member in group.members:
        if _waveform_key(member.waveform) == doppler:
            return float(member.fixed_angle)
    return float(group.members[0].fixed_angle)


def _group_has_required_waveforms(group: ElevationGroup) -> bool:
    """Only export surveillance groups once their doppler mate is present."""
    waveforms_present = {_waveform_key(member.waveform) for member in group.members}
    if nexrad_config.surveillance_waveform() in waveforms_present:
        return nexrad_config.doppler_waveform() in waveforms_present
    return len(group.members) > 0


def _finalize_group(group: ElevationGroup | None, result: list[ElevationGroup]) -> None:
    if group is None:
        return
    if not _group_has_required_waveforms(group):
        return
    representative_angle = _group_representative_angle(group)
    if representative_angle > nexrad_config.max_elevation_deg():
        return
    canonical_angle = _canonical_angle(representative_angle)
    group.elevation_id = str(canonical_angle)
    group.canonical_angle_deg = canonical_angle
    group.first_timestamp = _first_non_null_timestamp(group.members)
    group.last_timestamp = group.members[-1].timestamp if group.members else None
    group.complete = len(group.members) > 0
    result.append(group)


def _start_group(sweep: SweepRecord, waveform: str) -> ElevationGroup:
    return ElevationGroup(
        elevation_id="",
        canonical_angle_deg=0.0,
        members=[sweep],
        waveforms_present={waveform},
        first_sweep_index=sweep.index,
        last_sweep_index=sweep.index,
    )


def _group_by_waveform(valid: list[SweepRecord]) -> list[ElevationGroup]:
    result: list[ElevationGroup] = []
    current_group: ElevationGroup | None = None

    # Resolved before the loop, not inside it: every accessor re-resolves the
    # config root, which costs a stat call, and this loop runs once per sweep.
    surveillance = nexrad_config.surveillance_waveform()
    doppler = nexrad_config.doppler_waveform()
    single_elevation = nexrad_config.single_elevation_waveforms()

    for sweep in valid:
        waveform = _waveform_key(sweep.waveform)

        if waveform == surveillance:
            _finalize_group(current_group, result)
            current_group = _start_group(sweep, waveform)
            continue

        if waveform == doppler:
            if current_group is None:
                continue
            current_group.members.append(sweep)
            current_group.waveforms_present.add(waveform)
            current_group.last_sweep_index = sweep.index
            continue

        if waveform in single_elevation:
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
    - Snap grouped elevations onto `selection.canonical_elevation_bins`
    - Continue parsing the full volume, including any low-level revisits after higher sweeps
    - Do not export grouped elevations above `selection.high_max_angle_deg`
    - A grouped elevation starts with the surveillance waveform
    - Following contiguous doppler sweeps join that elevation
    - Each single-elevation waveform becomes a one-sweep elevation of its own
    - Doppler sweeps without a leading surveillance sweep are ignored

    The waveform names are `selection.waveforms` in nexrad.yaml; they are not
    restated here so that this list cannot drift from the catalog.
    """
    valid = [s for s in sweeps if s.azimuth_count > 0]
    valid.sort(key=lambda s: s.index)
    recognized = nexrad_config.recognized_waveforms()
    has_recognized_waveforms = any(_waveform_key(sweep.waveform) in recognized for sweep in valid)
    if has_recognized_waveforms:
        return _group_by_waveform(valid)
    return _group_without_waveforms(valid)


def elevation_group_key(group: ElevationGroup) -> str:
    """Create a dedup key for an elevation group."""
    members_key = ",".join(m.group_name for m in group.members)
    return f"{group.elevation_id}:{members_key}"

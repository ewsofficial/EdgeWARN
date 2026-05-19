import pytest

from common.ingest.nexrad.grouping import (
    _canonical_angle,
    _first_non_null_timestamp,
    elevation_group_key,
    group_sweeps_by_elevation,
)
from common.ingest.nexrad.models import SweepRecord


def _sweep(index, angle, waveform="surveillance", timestamp=None, azimuth_count=720):
    return SweepRecord(
        index=index,
        group_name=f"/sweep_{index:02d}",
        fixed_angle=angle,
        waveform=waveform,
        timestamp=timestamp,
        azimuth_count=azimuth_count,
    )


def test_canonical_angle_rounds_to_tolerance():
    assert _canonical_angle(0.5) == 0.5
    assert _canonical_angle(0.51) == 0.5
    assert _canonical_angle(0.49) == 0.5
    assert _canonical_angle(0.9) == 0.9
    assert _canonical_angle(1.3) == 1.3


def test_first_non_null_timestamp_returns_first_valid():
    members = [
        _sweep(0, 0.5, timestamp=None),
        _sweep(1, 0.5, timestamp="2026-01-01T00:00:00Z"),
        _sweep(2, 0.5, timestamp="2026-01-01T00:01:00Z"),
    ]
    assert _first_non_null_timestamp(members) == "2026-01-01T00:00:00Z"


def test_first_non_null_timestamp_all_none():
    members = [
        _sweep(0, 0.5, timestamp=None),
        _sweep(1, 0.5, timestamp=None),
    ]
    assert _first_non_null_timestamp(members) is None


def test_group_sweeps_groups_same_angle():
    sweeps = [
        _sweep(0, 0.5, timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.5, timestamp="2026-01-01T00:01:00Z"),
        _sweep(2, 0.9, timestamp="2026-01-01T00:02:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert groups[0].canonical_angle_deg == 0.5
    assert groups[1].canonical_angle_deg == 0.9
    assert len(groups[0].members) == 2
    assert len(groups[1].members) == 1


def test_group_sweeps_ignores_incomplete():
    sweeps = [
        _sweep(0, 0.5, azimuth_count=720),
        _sweep(1, 0.5, azimuth_count=0),
        _sweep(2, 0.9, azimuth_count=720),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert len(groups[0].members) == 1


def test_group_sweeps_first_timestamp():
    sweeps = [
        _sweep(0, 0.5, timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.5, timestamp="2026-01-01T00:01:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert groups[0].first_timestamp == "2026-01-01T00:00:00Z"
    assert groups[0].last_timestamp == "2026-01-01T00:01:00Z"


def test_group_sweeps_marks_supplemental_low_after_high():
    sweeps = [
        _sweep(0, 0.5, timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 1.8, timestamp="2026-01-01T00:01:00Z"),
        _sweep(2, 0.5, timestamp="2026-01-01T00:02:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    low_05_groups = [g for g in groups if g.canonical_angle_deg == 0.5]
    assert len(low_05_groups) == 1
    assert low_05_groups[0].supplemental is True


def test_group_sweeps_preserves_waveforms():
    sweeps = [
        _sweep(0, 0.5, waveform="surveillance"),
        _sweep(1, 0.5, waveform="contiguous_doppler"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert "surveillance" in groups[0].waveforms_present
    assert "contiguous_doppler" in groups[0].waveforms_present


def test_elevation_group_key_is_deterministic():
    sweeps = [
        _sweep(0, 0.5),
        _sweep(1, 0.5),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    key1 = elevation_group_key(groups[0])
    key2 = elevation_group_key(groups[0])
    assert key1 == key2


def test_group_sweeps_ordered_by_first_sweep_index():
    sweeps = [
        _sweep(2, 0.9),
        _sweep(0, 0.5),
        _sweep(4, 1.3),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert groups[0].canonical_angle_deg == 0.5
    assert groups[1].canonical_angle_deg == 0.9
    assert groups[2].canonical_angle_deg == 1.3

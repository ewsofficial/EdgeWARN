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


def test_canonical_angle_preserves_angle():
    assert _canonical_angle(0.5) == 0.5
    assert _canonical_angle(0.51) == 0.5
    assert _canonical_angle(0.49) == 0.5
    assert _canonical_angle(0.9) == 0.9
    assert _canonical_angle(1.3) == 1.3
    assert _canonical_angle(1.23) == 1.3
    assert _canonical_angle(3.78) == 4.0


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


def test_group_sweeps_groups_surveillance_then_doppler_into_one_elevation():
    sweeps = [
        _sweep(0, 0.68, waveform="contiguous_surveillance", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.47, waveform="contiguous_doppler", timestamp="2026-01-01T00:01:00Z"),
        _sweep(2, 0.84, waveform="staggered_pulse_pair", timestamp="2026-01-01T00:02:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert groups[0].canonical_angle_deg == 0.5
    assert groups[1].canonical_angle_deg == 0.9
    assert len(groups[0].members) == 2
    assert len(groups[1].members) == 1


def test_group_sweeps_assigns_pair_using_doppler_angle_when_available():
    sweeps = [
        _sweep(0, 0.84, waveform="contiguous_surveillance", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.47, waveform="contiguous_doppler", timestamp="2026-01-01T00:01:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 1
    assert groups[0].canonical_angle_deg == 0.5


def test_group_sweeps_ignores_incomplete():
    sweeps = [
        _sweep(0, 0.5, waveform="contiguous_surveillance", azimuth_count=720),
        _sweep(1, 0.5, waveform="contiguous_doppler", azimuth_count=0),
        _sweep(2, 0.9, waveform="batch", azimuth_count=720),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert len(groups[0].members) == 1


def test_group_sweeps_first_timestamp():
    sweeps = [
        _sweep(0, 0.5, waveform="contiguous_surveillance", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.5, waveform="contiguous_doppler", timestamp="2026-01-01T00:01:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert groups[0].first_timestamp == "2026-01-01T00:00:00Z"
    assert groups[0].last_timestamp == "2026-01-01T00:01:00Z"


def test_group_sweeps_ignores_doppler_without_leading_surveillance():
    sweeps = [
        _sweep(0, 0.5, waveform="contiguous_doppler", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.9, waveform="staggered_pulse_pair", timestamp="2026-01-01T00:01:00Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 1
    assert groups[0].canonical_angle_deg == 0.9


def test_group_sweeps_preserves_waveforms():
    sweeps = [
        _sweep(0, 0.5, waveform="contiguous_surveillance"),
        _sweep(1, 0.5, waveform="contiguous_doppler"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert "contiguous_surveillance" in groups[0].waveforms_present
    assert "contiguous_doppler" in groups[0].waveforms_present


def test_group_sweeps_treats_batch_and_staggered_pulse_pair_as_single_elevations():
    sweeps = [
        _sweep(0, 0.5, waveform="batch"),
        _sweep(1, 0.9, waveform="staggered_pulse_pair"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert len(groups[0].members) == 1
    assert len(groups[1].members) == 1


def test_group_sweeps_assigns_single_sweep_waveforms_to_nearest_target_bin():
    sweeps = [
        _sweep(0, 1.19, waveform="batch"),
        _sweep(1, 3.08, waveform="staggered_pulse_pair"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert [group.canonical_angle_deg for group in groups] == [1.3, 3.1]


def test_elevation_group_key_is_deterministic():
    sweeps = [
        _sweep(0, 0.5, waveform="contiguous_surveillance"),
        _sweep(1, 0.5, waveform="contiguous_doppler"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    key1 = elevation_group_key(groups[0])
    key2 = elevation_group_key(groups[0])
    assert key1 == key2


def test_group_sweeps_ordered_by_first_sweep_index():
    sweeps = [
        _sweep(2, 0.84, waveform="batch"),
        _sweep(0, 0.68, waveform="contiguous_surveillance"),
        _sweep(1, 0.47, waveform="contiguous_doppler"),
        _sweep(4, 1.19, waveform="staggered_pulse_pair"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert groups[0].canonical_angle_deg == 0.5
    assert groups[1].canonical_angle_deg == 0.9
    assert groups[2].canonical_angle_deg == 1.3


def test_group_sweeps_stops_once_angle_reaches_four_degrees():
    sweeps = [
        _sweep(0, 0.68, waveform="contiguous_surveillance"),
        _sweep(1, 0.47, waveform="contiguous_doppler"),
        _sweep(2, 3.78, waveform="batch"),
        _sweep(3, 4.94, waveform="staggered_pulse_pair"),
        _sweep(4, 1.3, waveform="staggered_pulse_pair"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert [group.canonical_angle_deg for group in groups] == [0.5, 4.0]


def test_group_sweeps_keeps_revisit_groups_separate_but_in_same_canonical_folder():
    sweeps = [
        _sweep(0, 0.68, waveform="contiguous_surveillance", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.47, waveform="contiguous_doppler", timestamp="2026-01-01T00:00:10Z"),
        _sweep(2, 3.08, waveform="staggered_pulse_pair", timestamp="2026-01-01T00:01:00Z"),
        _sweep(3, 0.70, waveform="contiguous_surveillance", timestamp="2026-01-01T00:02:00Z"),
        _sweep(4, 0.45, waveform="contiguous_doppler", timestamp="2026-01-01T00:02:10Z"),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    low_groups = [group for group in groups if group.canonical_angle_deg == 0.5]
    assert len(low_groups) == 2
    assert low_groups[0].first_timestamp == "2026-01-01T00:00:00Z"
    assert low_groups[1].first_timestamp == "2026-01-01T00:02:00Z"
    assert [member.group_name for member in low_groups[0].members] == ["/sweep_00", "/sweep_01"]
    assert [member.group_name for member in low_groups[1].members] == ["/sweep_03", "/sweep_04"]


def test_group_sweeps_uses_doppler_angle_for_revisit_bin_selection():
    sweeps = [
        _sweep(0, 0.73333740234375, waveform="contiguous_surveillance", timestamp="2026-01-01T00:00:00Z"),
        _sweep(1, 0.8349609375, waveform="contiguous_doppler", timestamp="2026-01-01T00:00:10Z"),
        _sweep(2, 1.08489990234375, waveform="contiguous_surveillance", timestamp="2026-01-01T00:01:00Z"),
        _sweep(3, 1.23046875, waveform="contiguous_doppler", timestamp="2026-01-01T00:01:10Z"),
    ]

    groups = group_sweeps_by_elevation(sweeps)

    assert [group.canonical_angle_deg for group in groups] == [0.9, 1.3]


def test_group_sweeps_falls_back_to_raw_elevation_number_when_waveform_missing():
    sweeps = [
        SweepRecord(index=0, group_name="/sweep_00", fixed_angle=0.68, waveform=None, timestamp="2026-01-01T00:00:00Z", azimuth_count=720, elevation_number=1),
        SweepRecord(index=1, group_name="/sweep_01", fixed_angle=0.47, waveform=None, timestamp="2026-01-01T00:00:10Z", azimuth_count=720, elevation_number=1),
        SweepRecord(index=2, group_name="/sweep_02", fixed_angle=0.84, waveform=None, timestamp="2026-01-01T00:01:00Z", azimuth_count=720, elevation_number=2),
        SweepRecord(index=3, group_name="/sweep_03", fixed_angle=4.94, waveform=None, timestamp="2026-01-01T00:02:00Z", azimuth_count=720, elevation_number=3),
    ]
    groups = group_sweeps_by_elevation(sweeps)
    assert len(groups) == 2
    assert [group.canonical_angle_deg for group in groups] == [0.5, 0.9]
    assert [len(group.members) for group in groups] == [2, 1]

from types import SimpleNamespace

import common.ingest.nexrad.worker as worker_module


def _raw_sweep(index, *, angle, elevation_number, complete=True, radial_count=720, timestamp="2026-01-01T00:00:00Z"):
    return SimpleNamespace(
        index=index,
        group_name=f"/sweep_{index}",
        elevation_number=elevation_number,
        fixed_angle=angle,
        first_timestamp=timestamp,
        last_timestamp=timestamp,
        radial_count=radial_count,
        waveform=None,
        complete=complete,
        records=[],
    )


def test_extract_worker_sweep_records_preserves_raw_waveforms():
    raw_volume = SimpleNamespace(
        sweeps=[
            _raw_sweep(0, angle=0.483, elevation_number=1),
            _raw_sweep(1, angle=0.483, elevation_number=1),
        ]
    )

    raw_volume.sweeps[0].waveform = "contiguous_surveillance"
    raw_volume.sweeps[1].waveform = "contiguous_doppler"

    records = worker_module._extract_worker_sweep_records(raw_volume)

    assert [record.waveform for record in records] == ["contiguous_surveillance", "contiguous_doppler"]
    assert [record.elevation_number for record in records] == [1, 1]


def test_extract_worker_sweep_records_filters_incomplete_raw_records():
    raw_volume = SimpleNamespace(
        sweeps=[
            _raw_sweep(0, angle=0.632, elevation_number=1),
            _raw_sweep(1, angle=0.483, elevation_number=2),
            _raw_sweep(2, angle=19.0, elevation_number=3, complete=False, radial_count=240),
        ]
    )

    records = worker_module._extract_worker_sweep_records(raw_volume)

    assert [record.fixed_angle for record in records] == [0.632, 0.483]
    assert [record.elevation_number for record in records] == [1, 2]
    assert all(record.waveform is None for record in records)

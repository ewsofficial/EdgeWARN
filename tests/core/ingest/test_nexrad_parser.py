import struct
from pathlib import Path
import bz2

import pytest

from common.ingest.nexrad import parser
from common.ingest.nexrad.worker import parse_and_export


def _volume_header(site="KTLH"):
    header = bytearray(24)
    header[:4] = b"AR2V"
    header[20:24] = site.encode("ascii")[:4]
    return bytes(header)


def _msg31_record(*, radial_status, elevation_number=1, elevation_angle=0.5, collect_ms=1000, collect_date=1):
    prefix = struct.pack(
        ">4sIHHfBBHBBBBf",
        b"TEST",
        collect_ms,
        collect_date,
        1,
        90.0,
        0,
        0,
        parser.MSG_31_PREFIX_LEN,
        1,
        radial_status,
        elevation_number,
        0,
        elevation_angle,
    )
    size_words = (12 + parser.MSG_HEADER_LEN + len(prefix) - 12) // 2
    msg_header = struct.pack(
        ">HBBHHIHH",
        size_words,
        0,
        31,
        1,
        collect_date,
        collect_ms,
        1,
        1,
    )
    return (b"\x00" * 12) + msg_header + prefix


def test_split_stream_into_volumes_handles_inline_boundaries():
    first = _volume_header("KTLH") + _msg31_record(radial_status=0)
    second = _volume_header("KDGX") + _msg31_record(radial_status=0)
    volumes = parser.split_stream_into_volumes(first + second)
    assert len(volumes) == 2
    assert volumes[0].startswith(b"AR2V")
    assert volumes[1].startswith(b"AR2V")


def test_parse_raw_volume_bytes_extracts_complete_sweep_metadata():
    volume_bytes = _volume_header("KTLH") + _msg31_record(radial_status=0) + _msg31_record(radial_status=2, collect_ms=2000)
    parsed = parser.parse_raw_volume_bytes(volume_bytes)

    assert parsed.site == "KTLH"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].group_name == "/sweep_0"
    assert parsed.sweeps[0].complete is True
    assert parsed.sweeps[0].radial_count == 2
    assert parsed.sweeps[0].fixed_angle == pytest.approx(0.5)
    assert parsed.sweeps[0].first_timestamp == "1970-01-01T00:00:01Z"
    assert parsed.sweeps[0].last_timestamp == "1970-01-01T00:00:02Z"


def test_parse_raw_volume_bytes_supports_bzip2_compressed_record_stream():
    record_stream = _msg31_record(radial_status=0) + _msg31_record(radial_status=2, collect_ms=2000)
    payload = bytearray(_volume_header("KTLH"))
    payload.extend(struct.pack(">I", 1))
    payload.extend(bz2.compress(record_stream))
    parsed = parser.parse_raw_volume_bytes(bytes(payload))
    assert parsed.site == "KTLH"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].complete is True


def test_parser_surface_exposes_runtime_helpers():
    assert hasattr(parser, "open_partial_volume")
    assert hasattr(parser, "extract_sweep_timestamp")
    assert hasattr(parser, "parse_raw_volume_file")


def test_worker_parse_and_export_writes_ar2v_elevation_artifacts(tmp_path):
    volume_bytes = (
        _volume_header("KTLH")
        + _msg31_record(radial_status=0, elevation_number=1, elevation_angle=0.5, collect_ms=1000)
        + _msg31_record(radial_status=2, elevation_number=1, elevation_angle=0.5, collect_ms=2000)
        + _msg31_record(radial_status=0, elevation_number=2, elevation_angle=0.9, collect_ms=3000)
        + _msg31_record(radial_status=2, elevation_number=2, elevation_angle=0.9, collect_ms=4000)
    )
    volume_path = tmp_path / "scan.ar2v"
    volume_path.write_bytes(volume_bytes)

    result = parse_and_export(
        volume_path=volume_path,
        output_root=tmp_path,
        site="KTLH",
        volume_id="999",
        scan_timestamp="20260507-150000",
        seen_elevation_keys=set(),
    )

    assert result.parse_error is None
    assert result.visible_sweeps == 2
    assert len(result.saved_elevations) == 2
    ar2v_paths = [Path(artifact.ar2v_path) for artifact in result.saved_elevations if artifact.ar2v_path]
    assert len(ar2v_paths) == 2
    assert all(path.exists() for path in ar2v_paths)
    assert all(path.suffix == ".ar2v" for path in ar2v_paths)

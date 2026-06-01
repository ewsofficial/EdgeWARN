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


def _msg31_record(*, radial_status, elevation_number=1, elevation_angle=0.5, collect_ms=1000, collect_date=1, block_names=None):
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

    if block_names is None:
        block_names = []

    pointer_base = parser.MSG_31_PREFIX_LEN
    block_pointer_table = bytearray(struct.pack(">HH", 0, len(block_names)))
    block_payload = bytearray()
    next_offset = pointer_base + 4 + parser.MSG_31_BLOCK_POINTERS * 4
    offsets = []
    for name in block_names[: parser.MSG_31_BLOCK_POINTERS]:
        offsets.append(next_offset)
        block_payload.extend(name.encode("ascii")[:4].ljust(4, b" "))
        next_offset += 4
    offsets.extend([0] * (parser.MSG_31_BLOCK_POINTERS - len(offsets)))
    block_pointer_table.extend(struct.pack(">" + "I" * parser.MSG_31_BLOCK_POINTERS, *offsets))
    body = prefix + bytes(block_pointer_table) + bytes(block_payload)

    size_words = (12 + parser.MSG_HEADER_LEN + len(body) - 12) // 2
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
    return (b"\x00" * 12) + msg_header + body


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


def test_parse_raw_volume_bytes_drops_sub_point_four_degree_sweeps():
    volume_bytes = (
        _volume_header("KTLH")
        + _msg31_record(radial_status=0, elevation_number=1, elevation_angle=0.3, collect_ms=1000)
        + _msg31_record(radial_status=2, elevation_number=1, elevation_angle=0.3, collect_ms=2000)
        + _msg31_record(radial_status=0, elevation_number=2, elevation_angle=0.5, collect_ms=3000)
        + _msg31_record(radial_status=2, elevation_number=2, elevation_angle=0.5, collect_ms=4000)
    )
    parsed = parser.parse_raw_volume_bytes(volume_bytes)

    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].elevation_number == 2
    assert parsed.sweeps[0].fixed_angle == pytest.approx(0.5)


def test_parse_raw_volume_bytes_classifies_waveform_from_message31_blocks():
    volume_bytes = (
        _volume_header("KTLH")
        + _msg31_record(radial_status=0, elevation_number=1, elevation_angle=0.5, block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=2, elevation_number=1, elevation_angle=0.5, collect_ms=2000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=0, elevation_number=2, elevation_angle=0.5, collect_ms=3000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "])
        + _msg31_record(radial_status=2, elevation_number=2, elevation_angle=0.5, collect_ms=4000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "])
        + _msg31_record(radial_status=0, elevation_number=3, elevation_angle=1.8, collect_ms=5000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW ", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=2, elevation_number=3, elevation_angle=1.8, collect_ms=6000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW ", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=0, elevation_number=4, elevation_angle=4.2, collect_ms=7000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW ", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=2, elevation_number=4, elevation_angle=4.2, collect_ms=8000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW ", "DZDR", "DPHI", "DRHO", "DCFP"])
    )
    parsed = parser.parse_raw_volume_bytes(volume_bytes)

    assert [sweep.waveform for sweep in parsed.sweeps] == [
        "contiguous_surveillance",
        "contiguous_doppler",
        "staggered_pulse_pair",
        "batch",
    ]


def test_filter_msg31_blocks_removes_dref_from_doppler_record():
    record = _msg31_record(
        radial_status=0,
        elevation_number=2,
        elevation_angle=0.5,
        block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "],
    )

    filtered = parser.filter_msg31_blocks(record, {"DREF"})

    assert parser._message31_block_names(record) == ["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "]
    assert parser._message31_block_names(filtered) == ["RVOL", "RELV", "RRAD", "DVEL", "DSW "]


def test_parse_raw_volume_bytes_supports_bzip2_compressed_record_stream():
    record_stream = _msg31_record(radial_status=0) + _msg31_record(radial_status=2, collect_ms=2000)
    payload = bytearray(_volume_header("KTLH"))
    payload.extend(struct.pack(">I", 1))
    payload.extend(bz2.compress(record_stream))
    parsed = parser.parse_raw_volume_bytes(bytes(payload))
    assert parsed.site == "KTLH"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].complete is True


def test_parse_raw_volume_bytes_treats_non_bzip_runtime_stream_as_uncompressed():
    first_record = bytearray(_msg31_record(radial_status=0))
    first_record[:4] = b"\x00\x00\x00\x02"
    volume_bytes = _volume_header("KTLH") + bytes(first_record) + _msg31_record(radial_status=2, collect_ms=2000)

    parsed = parser.parse_raw_volume_bytes(volume_bytes)

    assert parsed.site == "KTLH"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].complete is True


def test_parser_surface_exposes_runtime_helpers():
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


def test_worker_parse_and_export_drops_dref_from_contiguous_doppler_raw_ar2v(tmp_path):
    volume_bytes = (
        _volume_header("KTLH")
        + _msg31_record(radial_status=0, elevation_number=1, elevation_angle=0.68, collect_ms=1000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=2, elevation_number=1, elevation_angle=0.68, collect_ms=2000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"])
        + _msg31_record(radial_status=0, elevation_number=2, elevation_angle=0.47, collect_ms=3000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "])
        + _msg31_record(radial_status=2, elevation_number=2, elevation_angle=0.47, collect_ms=4000, block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "])
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
    assert len(result.saved_elevations) == 1
    saved_path = Path(result.saved_elevations[0].ar2v_path)
    reparsed = parser.parse_raw_volume_bytes(saved_path.read_bytes())

    assert len(reparsed.sweeps) == 2
    first_record = parser.materialize_record_range(reparsed, reparsed.sweeps[0].record_ranges[0])
    second_record = parser.materialize_record_range(reparsed, reparsed.sweeps[1].record_ranges[0])
    assert parser._message31_block_names(first_record) == ["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"]
    assert parser._message31_block_names(second_record) == ["RVOL", "RELV", "RRAD", "DVEL", "DSW "]


def test_worker_parse_and_export_trims_exported_sweeps_from_runtime_file(tmp_path):
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
        trim_buffer=True,
    )

    trimmed = parser.parse_raw_volume_file_mmap(volume_path)

    assert result.parse_error is None
    assert result.buffer_trimmed is True
    assert result.runtime_size == volume_path.stat().st_size
    assert result.runtime_size < len(volume_bytes)
    assert len(trimmed.sweeps) == 0


def test_worker_parse_and_export_needs_full_reparse_for_split_low_elevation_group(tmp_path):
    first_pass = (
        _volume_header("KTLH")
        + _msg31_record(
            radial_status=0,
            elevation_number=1,
            elevation_angle=0.5,
            collect_ms=1000,
            block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"],
        )
        + _msg31_record(
            radial_status=2,
            elevation_number=1,
            elevation_angle=0.5,
            collect_ms=2000,
            block_names=["RVOL", "RELV", "RRAD", "DREF", "DZDR", "DPHI", "DRHO", "DCFP"],
        )
    )
    second_pass_tail = (
        _msg31_record(
            radial_status=0,
            elevation_number=1,
            elevation_angle=0.5,
            collect_ms=3000,
            block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "],
        )
        + _msg31_record(
            radial_status=2,
            elevation_number=1,
            elevation_angle=0.5,
            collect_ms=4000,
            block_names=["RVOL", "RELV", "RRAD", "DREF", "DVEL", "DSW "],
        )
    )
    volume_path = tmp_path / "scan.ar2v"
    volume_path.write_bytes(first_pass)

    first_result = parse_and_export(
        volume_path=volume_path,
        output_root=tmp_path,
        site="KTLH",
        volume_id="999",
        scan_timestamp="20260507-150000",
        seen_elevation_keys=set(),
        trim_buffer=False,
    )
    volume_path.write_bytes(first_pass + second_pass_tail)

    full_reparse_result = parse_and_export(
        volume_path=volume_path,
        output_root=tmp_path,
        site="KTLH",
        volume_id="999",
        scan_timestamp="20260507-150000",
        seen_elevation_keys=set(),
        trim_buffer=False,
    )

    assert first_result.saved_sweep_count == 0
    assert full_reparse_result.saved_sweep_count == 2
    assert [artifact.elevation for artifact in full_reparse_result.saved_elevations] == ["0.5"]

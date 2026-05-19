import sys
import types

from common.ingest.nexrad.models import ChunkKey, ParsedVolume, SweepInfo, VolumeProbe
from common.ingest.nexrad.volume_builder import build_low_high_outputs, parse_level2_volume_bytes


def _probe():
    return VolumeProbe("KDDC", "468", "VCP-215", 215, None, None, True, "weather.gov/radar/stations")


def _chunk(number):
    return ChunkKey("KDDC", "468", number, "I", f"KDDC/468/{number:03d}")


def _chunks_with_terminal(last_number):
    chunks = [_chunk(number) for number in range(1, last_number)]
    chunks.append(ChunkKey("KDDC", "468", last_number, "E", f"KDDC/468/{last_number:03d}"))
    return chunks


def _sweep(index, angle, bucket="excluded", complete=True):
    return SweepInfo(index, f"/sweep_{index:02d}", angle, "surveillance", 720, complete, False, bucket)


def test_volume_builder_marks_low_ready_at_low_checkpoint():
    chunks = _chunks_with_terminal(25)
    parsed = ParsedVolume(
        scan_name="VCP-215",
        dynamic_scan_type="standard",
        sweeps=[_sweep(0, 0.5), _sweep(1, 0.9), _sweep(2, 1.2)],
        datatree=None,
        source_bucket="chunks",
    )
    writes = []

    result = build_low_high_outputs(
        _probe(),
        chunks,
        chunk_fetcher=lambda chunk: f"{chunk.chunk_number}".encode(),
        parser=lambda payload: parsed,
        writer=lambda *args, **kwargs: writes.append(args[3]) or (None, None, None),
    )

    assert writes == [25]
    assert result.complete is True
    assert result.chunks_downloaded == 25


def test_volume_builder_waits_for_high_bins_through_chunk_61():
    chunks = _chunks_with_terminal(61)

    def parser(payload):
        marker = int(payload.decode()[-2:]) if len(payload.decode()) >= 2 else int(payload.decode())
        if marker < 61:
            sweeps = [_sweep(0, 0.5), _sweep(1, 0.9), _sweep(2, 1.2), _sweep(3, 1.8), _sweep(4, 2.4)]
        else:
            sweeps = [_sweep(0, 0.5), _sweep(1, 0.9), _sweep(2, 1.2), _sweep(3, 1.8), _sweep(4, 2.4), _sweep(5, 3.0), _sweep(6, 4.0)]
        return ParsedVolume("VCP-215", "SAILS x 1", sweeps, None, "chunks")

    writes = []
    result = build_low_high_outputs(
        _probe(),
        chunks,
        chunk_fetcher=lambda chunk: f"{chunk.chunk_number:02d}".encode(),
        parser=parser,
        writer=lambda *args, **kwargs: writes.append(args[3]) or (None, None, None),
    )

    assert writes == [25]
    assert result.complete is True
    assert result.chunks_downloaded == 25


def test_volume_builder_waits_for_required_low_chunks_before_downloading():
    chunks = [_chunk(number) for number in range(1, 26) if number != 10]
    fetched = []

    result = build_low_high_outputs(
        _probe(),
        chunks,
        chunk_fetcher=lambda chunk: fetched.append(chunk.chunk_number) or b"x",
        parser=lambda payload: ParsedVolume("VCP-215", "standard", [_sweep(0, 0.5), _sweep(1, 0.9)], None, "chunks"),
        writer=lambda *args, **kwargs: (None, None, None),
    )

    assert fetched == []
    assert result.complete is False
    assert result.chunks_downloaded == 0


def test_parse_level2_volume_bytes_uses_temp_file_path(monkeypatch):
    captured = {}

    class _RawSweep:
        def __init__(self):
            self.index = 0
            self.group_name = "/sweep_00"
            self.fixed_angle = 0.5
            self.waveform = "contiguous_surveillance"
            self.radial_count = 720
            self.complete = True

    class _RawVolume:
        def __init__(self, path):
            self.site = "KTLX"
            self.volume_header = b"AR2V\x00\x00\x00\x00" + b"\x00" * 16
            self.metadata_records = []
            self.sweeps = [_RawSweep()]
            self.trailing_bytes = b""
            self.compression_record_count = 0

    def fake_parse(path):
        captured["arg_type"] = type(path)
        captured["path"] = str(path)
        with open(path, "rb") as handle:
            captured["payload"] = handle.read()
        return _RawVolume(path)

    monkeypatch.setattr(
        "common.ingest.nexrad.volume_builder.parse_raw_volume_file",
        fake_parse,
    )

    parsed = parse_level2_volume_bytes(b"example-level2-bytes")

    assert captured["arg_type"] is str
    assert captured["path"].endswith(".ar2v")
    assert captured["payload"] == b"example-level2-bytes"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].azimuth_count == 720
    assert parsed.sweeps[0].waveform == "contiguous_surveillance"

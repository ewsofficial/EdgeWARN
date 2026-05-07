import sys
import types

from common.ingest.nexrad.models import ChunkKey, ParsedVolume, SweepInfo, VolumeProbe
from common.ingest.nexrad.volume_builder import build_low_high_outputs, parse_level2_volume_bytes


def _probe():
    return VolumeProbe("KDDC", "468", "VCP-215", 215, None, None, True, "weather.gov/radar/stations")


def _chunk(number):
    return ChunkKey("KDDC", "468", number, "I", f"KDDC/468/{number:03d}")


def _sweep(index, angle, bucket="excluded", complete=True):
    return SweepInfo(index, f"/sweep_{index:02d}", angle, "surveillance", 720, complete, False, bucket)


def test_volume_builder_marks_low_ready_at_low_checkpoint():
    chunks = [_chunk(number) for number in range(1, 26)]
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
    assert result.complete is False
    assert result.chunks_downloaded == 25


def test_volume_builder_waits_for_high_bins_through_chunk_61():
    chunks = [_chunk(number) for number in range(1, 62)]

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

    assert writes == [61]
    assert result.complete is True
    assert result.chunks_downloaded == 61


def test_parse_level2_volume_bytes_uses_temp_file_path(monkeypatch):
    captured = {}

    class _Angle:
        def __init__(self, value):
            self.values = self
            self._value = value

        def item(self):
            return self._value

    class _Dataset:
        def __init__(self):
            self.sizes = {"azimuth": 720}
            self.attrs = {"prt_mode": "contiguous_surveillance"}

        def get(self, key):
            if key == "sweep_fixed_angle":
                return _Angle(0.5)
            return None

    class _Group:
        def to_dataset(self):
            return _Dataset()

    class _Tree:
        attrs = {"scan_name": "VCP-215", "scan_strategy": "SAILS x 1"}
        groups = ["/sweep_00"]

        def __getitem__(self, key):
            assert key == "/sweep_00"
            return _Group()

    def fake_opener(path):
        captured["arg_type"] = type(path)
        captured["path"] = path
        with open(path, "rb") as handle:
            captured["payload"] = handle.read()
        return _Tree()

    fake_xradar = types.SimpleNamespace(
        io=types.SimpleNamespace(
            backends=types.SimpleNamespace(
                nexrad_level2=types.SimpleNamespace(
                    open_nexradlevel2_datatree=fake_opener,
                )
            )
        )
    )
    monkeypatch.setitem(sys.modules, "xradar", fake_xradar)

    parsed = parse_level2_volume_bytes(b"example-level2-bytes")

    assert captured["arg_type"] is str
    assert captured["path"].endswith(".ar2v")
    assert captured["payload"] == b"example-level2-bytes"
    assert parsed.scan_name == "VCP-215"
    assert parsed.dynamic_scan_type == "SAILS x 1"
    assert len(parsed.sweeps) == 1
    assert parsed.sweeps[0].azimuth_count == 720

from common.ingest.nexrad.models import ChunkKey, ParsedVolume, SweepInfo, VolumeProbe
from common.ingest.nexrad.volume_builder import build_low_high_outputs


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

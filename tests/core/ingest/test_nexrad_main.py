from pathlib import Path
from unittest.mock import patch

import util.file as fs
from common.ingest.nexrad.models import ChunkKey
from common.ingest.nexrad.main import ingest_allowed_vcp_volume, list_allowed_vcp_sites


def test_ingest_downloads_chunks_to_timestamped_site_chunks_dir(tmp_path):
    fs.initialize_filesystem(tmp_path)

    chunks = [
        ChunkKey(
            "KTLH",
            "999",
            number,
            "S" if number == 1 else "I",
            f"KTLH/999/20260507-150000-{number:03d}-{'S' if number == 1 else 'I'}",
        )
        for number in range(1, 31)
    ]

    def _chunk_bytes(chunk, **_kwargs):
        return f"chunk{chunk.chunk_number}".encode("utf-8")

    with patch("common.ingest.nexrad.main.probe_volume_vcp", return_value=type("Probe", (), {
        "accepted": True,
        "site": "KTLH",
        "volume_id": "999",
        "vcp": 212,
    })()), \
         patch("common.ingest.nexrad.main.list_volume_chunks", return_value=chunks), \
         patch("common.ingest.nexrad.main.get_chunk_bytes", side_effect=_chunk_bytes):
        result = ingest_allowed_vcp_volume("KTLH", "999", base_dir=tmp_path, s3_client=object())

    assert result.site == "KTLH"
    assert result.volume_id == "999"
    assert result.vcp == 212
    assert result.chunks_downloaded == 25
    assert result.complete is True
    assert result.low_path is None
    assert result.high_path is None
    assert result.manifest_path is None
    outdir = Path(tmp_path) / "data" / "NEXRAD_Level2" / "KTLH" / "20260507-150000" / "chunks"
    assert (outdir / "20260507-150000-001-S").read_bytes() == b"chunk1"
    assert (outdir / "20260507-150000-025-I").read_bytes() == b"chunk25"
    assert not (outdir / "20260507-150000-026-I").exists()


def test_list_allowed_vcp_sites_filters_and_sorts():
    station = lambda vcp: type("Station", (), {"vcp": vcp})()
    stations = {
        "KBBB": station(99),
        "KCCC": station(212),
        "KAAA": station(12),
        "KDDD": station(None),
    }

    with patch("common.ingest.nexrad.main.fetch_radar_station_vcps", return_value=stations):
        sites = list_allowed_vcp_sites()

    assert sites == ["KAAA", "KCCC"]

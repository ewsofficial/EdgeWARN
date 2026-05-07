from unittest.mock import patch

from common.ingest.nexrad.models import ChunkKey, RadarStationVcp
from common.ingest.nexrad.vcp_probe import probe_volume_vcp


def test_probe_volume_vcp_accepts_allowed_vcps_and_lists_chunks_after_gate():
    station = RadarStationVcp("KDDC", 215, "R215", None, None, {})
    chunk = ChunkKey("KDDC", "468", 1, "I", "KDDC/468/001")

    with patch("common.ingest.nexrad.vcp_probe.get_station_vcp", return_value=station), \
         patch("common.ingest.nexrad.vcp_probe.list_volume_chunks", return_value=[chunk]) as mock_list:
        probe = probe_volume_vcp("KDDC", "468")

    assert probe.accepted is True
    assert probe.vcp == 215
    assert probe.first_chunk_key == chunk
    mock_list.assert_called_once_with("KDDC", "468", s3_client=None)


def test_probe_volume_vcp_rejects_disallowed_vcp_without_chunk_listing():
    station = RadarStationVcp("KTLX", 35, "R35", None, None, {})

    with patch("common.ingest.nexrad.vcp_probe.get_station_vcp", return_value=station), \
         patch("common.ingest.nexrad.vcp_probe.list_volume_chunks") as mock_list:
        probe = probe_volume_vcp("KTLX", "999")

    assert probe.accepted is False
    assert probe.first_chunk_key is None
    mock_list.assert_not_called()


def test_probe_volume_vcp_rejects_unknown_station_without_chunk_listing():
    with patch("common.ingest.nexrad.vcp_probe.get_station_vcp", return_value=None), \
         patch("common.ingest.nexrad.vcp_probe.list_volume_chunks") as mock_list:
        probe = probe_volume_vcp("KXXX", "999")

    assert probe.accepted is False
    assert probe.vcp is None
    mock_list.assert_not_called()

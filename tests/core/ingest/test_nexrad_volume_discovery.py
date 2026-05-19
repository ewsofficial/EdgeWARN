import util.file as fs
from common.ingest.nexrad.models import ChunkKey
from common.ingest.nexrad.pipeline.volume_discovery import is_newer_volume_stamp, local_volume_complete
from common.ingest.nexrad.writer import elevation_netcdf_path, volume_output_path


def _chunks(site="KTLH", volume_id="999", stamp="20260507-150000", last_number=25):
    return [
        ChunkKey(
            site=site,
            volume_id=volume_id,
            chunk_number=number,
            chunk_type="S" if number == 1 else "I",
            key=f"{site}/{volume_id}/{stamp}-{number:03d}-{'S' if number == 1 else 'I'}",
        )
        for number in range(1, last_number + 1)
    ]


def test_is_newer_volume_stamp_prefers_newer_timestamp_strings_when_parseable():
    assert is_newer_volume_stamp("20260508-173100", "20260508-172500") is True
    assert is_newer_volume_stamp("20260508-172500", "20260508-173100") is False


def test_local_volume_complete_accepts_volume_or_operational_elevation_outputs(tmp_path):
    fs.initialize_filesystem(tmp_path)
    chunks = _chunks()

    assert local_volume_complete("KTLH", "999", chunks) is False

    volume_path = volume_output_path("KTLH", "999", chunks)
    volume_path.parent.mkdir(parents=True, exist_ok=True)
    volume_path.write_bytes(b"volume")
    assert local_volume_complete("KTLH", "999", chunks) is True

    volume_path.unlink()
    for elev in ("0.5", "0.9"):
        nc_path = elevation_netcdf_path("KTLH", elev, "20260507-150000")
        nc_path.parent.mkdir(parents=True, exist_ok=True)
        nc_path.write_bytes(b"elevation")

    assert local_volume_complete("KTLH", "999", chunks) is True

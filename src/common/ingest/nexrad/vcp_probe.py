from common.ingest.nexrad.config import ALLOWED_VCPS
from common.ingest.nexrad.models import VolumeProbe
from common.ingest.nexrad.s3_chunks import list_volume_chunks
from common.ingest.nexrad.weather_api import get_station_vcp


def probe_volume_vcp(site, volume_id, s3_client=None, weather_session=None) -> VolumeProbe:
    station = get_station_vcp(site, session=weather_session)
    if station is None or station.vcp not in ALLOWED_VCPS:
        return VolumeProbe(
            site=str(site).upper(),
            volume_id=str(volume_id),
            scan_name=None,
            vcp=None if station is None else station.vcp,
            dynamic_scan_type=None,
            first_chunk_key=None,
            accepted=False,
            vcp_source="weather.gov/radar/stations",
        )

    chunks = list_volume_chunks(site, volume_id, s3_client=s3_client)
    return VolumeProbe(
        site=str(site).upper(),
        volume_id=str(volume_id),
        scan_name=f"VCP-{station.vcp}",
        vcp=station.vcp,
        dynamic_scan_type=None,
        first_chunk_key=chunks[0] if chunks else None,
        accepted=True,
        vcp_source="weather.gov/radar/stations",
    )

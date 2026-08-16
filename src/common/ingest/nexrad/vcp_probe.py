from common.ingest.nexrad.config import allowed_vcps
from common.ingest.nexrad.models import VolumeProbe
from common.ingest.nexrad.s3_chunks import list_volume_chunks
from common.ingest.nexrad.weather_api import get_station_vcp


class VolumeVcpProber:
    def __init__(self, *, station_lookup=None, chunk_lister=None):
        self.station_lookup = station_lookup or get_station_vcp
        self.chunk_lister = chunk_lister or list_volume_chunks

    def probe_volume_vcp(self, site, volume_id, s3_client=None, weather_session=None) -> VolumeProbe:
        station = self.station_lookup(site, session=weather_session)
        if station is None or station.vcp not in allowed_vcps():
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

        chunks = self.chunk_lister(site, volume_id, s3_client=s3_client)
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


def probe_volume_vcp(site, volume_id, s3_client=None, weather_session=None) -> VolumeProbe:
    prober = VolumeVcpProber()
    return prober.probe_volume_vcp(site, volume_id, s3_client=s3_client, weather_session=weather_session)

from common.ingest.nexrad.models import RadarStationVcp
from common.ingest.nexrad.pipeline_models import VolumeDiscoveryResult
from common.ingest.nexrad.s3_async import async_list_recent_volume_ids, async_list_volume_chunks
from common.ingest.nexrad.s3_chunks import extract_volume_timestamp, parse_nexrad_timestamp
from common.ingest.nexrad.writer import local_low_chunks_complete


class NexradVolumeDiscovery:
    def __init__(
        self,
        *,
        async_volume_lister=None,
        async_chunk_lister=None,
        max_candidate_volumes_per_site=3,
    ):
        self.async_volume_lister = async_volume_lister or async_list_recent_volume_ids
        self.async_chunk_lister = async_chunk_lister or async_list_volume_chunks
        self.max_candidate_volumes_per_site = max_candidate_volumes_per_site

    async def discover_latest(self, site: str, station: RadarStationVcp, *, s3_client=None):
        latest_volume_id = None
        latest_chunks = ()
        latest_stamp = None
        volume_ids = await self.async_volume_lister(
            site,
            limit=self.max_candidate_volumes_per_site,
            s3_client=s3_client,
        )
        for volume_id in volume_ids:
            chunks = tuple(await self.async_chunk_lister(site, volume_id, s3_client=s3_client))
            if not chunks:
                continue
            stamp = extract_volume_timestamp(volume_id, chunks)
            if self._is_newer(stamp, latest_stamp):
                latest_volume_id = volume_id
                latest_chunks = chunks
                latest_stamp = stamp

        return VolumeDiscoveryResult(
            site=str(site).upper(),
            station=station,
            volume_id=latest_volume_id,
            chunks=latest_chunks,
            latest_scan_time=latest_stamp or station.level_two_last_received_time,
        )

    @staticmethod
    def _is_newer(stamp: str | None, latest_stamp: str | None) -> bool:
        if latest_stamp is None:
            return stamp is not None
        if stamp is None:
            return False
        current_dt = parse_nexrad_timestamp(stamp)
        latest_dt = parse_nexrad_timestamp(latest_stamp)
        if current_dt is None or latest_dt is None:
            return stamp > latest_stamp
        return current_dt > latest_dt

    @staticmethod
    def local_complete(site: str, volume_id: str, chunks) -> bool:
        return local_low_chunks_complete(site, volume_id, chunks)

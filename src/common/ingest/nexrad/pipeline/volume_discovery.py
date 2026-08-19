import json
from pathlib import Path

from common.ingest.nexrad import config as nexrad_config
from common.ingest.nexrad.grouping import ingest_readiness_elevation_ids
from common.ingest.nexrad.models import RadarStationVcp
from common.ingest.nexrad.pipeline.models import VolumeDiscoveryResult
from common.ingest.nexrad.s3_async import async_list_recent_volume_ids, async_list_volume_chunks
from common.ingest.nexrad.s3_chunks import extract_volume_timestamp, parse_nexrad_timestamp
from common.ingest.nexrad.writer import elevation_dir, local_volume_file_complete


def _manifest_artifact_exists(payload: dict) -> bool:
    for key in ("netcdf_path", "ar2v_path"):
        value = payload.get(key)
        if value and Path(value).exists():
            return True
    return False


def _local_elevation_complete_for_volume(site: str, volume_id: str, elevation: str) -> bool:
    elev_dir = elevation_dir(site, elevation)
    if not elev_dir.exists():
        return False
    for manifest_path in sorted(elev_dir.glob("*.json"), reverse=True):
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("volume_id")) != str(volume_id):
            continue
        if _manifest_artifact_exists(payload):
            return True
    return False


def is_newer_volume_stamp(stamp: str | None, latest_stamp: str | None) -> bool:
    if latest_stamp is None:
        return stamp is not None
    if stamp is None:
        return False
    current_dt = parse_nexrad_timestamp(stamp)
    latest_dt = parse_nexrad_timestamp(latest_stamp)
    if current_dt is None or latest_dt is None:
        return stamp > latest_stamp
    return current_dt > latest_dt


def local_volume_complete(site: str, volume_id: str, chunks) -> bool:
    if local_volume_file_complete(site, volume_id, chunks):
        return True
    _ = extract_volume_timestamp(volume_id, chunks)
    return all(
        _local_elevation_complete_for_volume(site, volume_id, elevation)
        for elevation in ingest_readiness_elevation_ids()
    )


class NexradVolumeDiscovery:
    def __init__(
        self,
        *,
        async_volume_lister=None,
        async_chunk_lister=None,
        max_candidate_volumes_per_site=None,
    ):
        if max_candidate_volumes_per_site is None:
            max_candidate_volumes_per_site = nexrad_config.max_candidate_volumes_per_site()
        self.async_volume_lister = async_volume_lister or async_list_recent_volume_ids
        self.async_chunk_lister = async_chunk_lister or async_list_volume_chunks
        self.max_candidate_volumes_per_site = max_candidate_volumes_per_site

    async def discover_latest(self, site: str, station: RadarStationVcp, *, s3_client=None, await_operation=None):
        latest_volume_id = None
        latest_chunks = ()
        latest_stamp = None
        volume_list = self.async_volume_lister(
            site,
            limit=self.max_candidate_volumes_per_site,
            s3_client=s3_client,
        )
        if await_operation is None:
            volume_ids = await volume_list
        else:
            volume_ids = await await_operation(volume_list, stage="volume-discovery")
        for volume_id in volume_ids:
            chunk_list = self.async_chunk_lister(site, volume_id, s3_client=s3_client)
            if await_operation is None:
                chunks = tuple(await chunk_list)
            else:
                chunks = tuple(await await_operation(chunk_list, stage="chunk-list", volume_id=volume_id))
            if not chunks:
                continue
            stamp = extract_volume_timestamp(volume_id, chunks)
            if is_newer_volume_stamp(stamp, latest_stamp):
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
    def local_complete(site: str, volume_id: str, chunks) -> bool:
        return local_volume_complete(site, volume_id, chunks)

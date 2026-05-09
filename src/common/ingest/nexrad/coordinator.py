import asyncio
import time

import util.file as fs
from common.ingest.nexrad.config import ALLOWED_VCPS
from common.ingest.nexrad.models import NexradCoordinatorResult, NexradCoordinatorRunResults
from common.ingest.nexrad.service import NexradIngestService
from common.ingest.nexrad.s3_chunks import extract_volume_timestamp, parse_nexrad_timestamp, required_low_chunks
from common.ingest.nexrad.writer import local_low_chunks_complete
from common.ingest.nexrad.s3_async import async_list_recent_volume_ids, async_list_volume_chunks
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from util.io import IOManager

io_manager = IOManager("[NEXRAD-COORD]", include_timestamps=True)


class NexradScanCoordinator:
    def __init__(
        self,
        *,
        station_fetcher=None,
        async_volume_lister=None,
        async_chunk_lister=None,
        async_ingest_trigger=None,
        max_site_tasks=16,
        max_candidate_volumes_per_site=3,
    ):
        self.station_fetcher = station_fetcher or fetch_radar_station_vcps
        self.async_volume_lister = async_volume_lister or async_list_recent_volume_ids
        self.async_chunk_lister = async_chunk_lister or async_list_volume_chunks
        self.max_site_tasks = max_site_tasks
        self.max_candidate_volumes_per_site = max_candidate_volumes_per_site
        self._ingest_service = NexradIngestService()
        self.async_ingest_trigger = async_ingest_trigger or self._ingest_service.ingest_allowed_vcp_volume_async

    @staticmethod
    def _format_perf_ms(started_at: float) -> float:
        return (time.perf_counter() - started_at) * 1000

    async def _find_latest_volume(self, site, *, s3_client):
        latest_volume_id = None
        latest_chunks = None
        latest_stamp = None
        volume_ids = await self.async_volume_lister(
            site,
            limit=self.max_candidate_volumes_per_site,
            s3_client=s3_client,
        )
        for volume_id in volume_ids:
            chunks = await self.async_chunk_lister(site, volume_id, s3_client=s3_client)
            if not chunks:
                continue

            stamp = extract_volume_timestamp(volume_id, chunks)
            if latest_stamp is None:
                latest_volume_id = volume_id
                latest_chunks = chunks
                latest_stamp = stamp
                continue

            current_dt = parse_nexrad_timestamp(stamp)
            latest_dt = parse_nexrad_timestamp(latest_stamp)
            if current_dt is None or latest_dt is None:
                if stamp > latest_stamp:
                    latest_volume_id = volume_id
                    latest_chunks = chunks
                    latest_stamp = stamp
                continue

            if current_dt > latest_dt:
                latest_volume_id = volume_id
                latest_chunks = chunks
                latest_stamp = stamp

        return latest_volume_id, latest_chunks

    async def _process_site(self, site, station, *, base_dir=None, s3_client=None, weather_session=None):
        if not str(site).upper().startswith("K"):
            return NexradCoordinatorResult(site=str(site).upper(), latest_scan_time=None, vcp=station.vcp, volume_id=None, action="skipped_non_us_site")

        if station.vcp not in ALLOWED_VCPS:
            return NexradCoordinatorResult(site=str(site).upper(), latest_scan_time=station.level_two_last_received_time, vcp=station.vcp, volume_id=None, action="skipped_invalid_vcp")

        volume_id, chunks = await self._find_latest_volume(site, s3_client=s3_client)
        if volume_id is None:
            return NexradCoordinatorResult(site=str(site).upper(), latest_scan_time=station.level_two_last_received_time, vcp=station.vcp, volume_id=None, action="skipped_no_matching_volume")

        latest_scan_stamp = extract_volume_timestamp(volume_id, chunks)

        if local_low_chunks_complete(site, volume_id, chunks):
            return NexradCoordinatorResult(site=str(site).upper(), latest_scan_time=latest_scan_stamp, vcp=station.vcp, volume_id=volume_id, action="skipped_already_downloaded")

        if not required_low_chunks(chunks):
            return NexradCoordinatorResult(site=str(site).upper(), latest_scan_time=latest_scan_stamp, vcp=station.vcp, volume_id=volume_id, action="skipped_incomplete_remote")

        result = await self.async_ingest_trigger(
            site,
            volume_id,
            base_dir=base_dir,
            s3_client=s3_client,
            weather_session=weather_session,
            station_vcp=station,
        )
        chunks_downloaded = 0 if result is None else result.chunks_downloaded
        action = "downloaded" if result is not None else "skipped_invalid_vcp"
        return NexradCoordinatorResult(
            site=str(site).upper(),
            latest_scan_time=latest_scan_stamp,
            vcp=station.vcp,
            volume_id=volume_id,
            action=action,
            chunks_downloaded=chunks_downloaded,
        )

    async def ingest_latest_station_scans_async(
        self,
        sites=None,
        *,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        max_candidate_volumes_per_site=None,
    ):
        started_at = time.perf_counter()
        if base_dir:
            fs.initialize_filesystem(base_dir)

        if max_candidate_volumes_per_site is not None:
            self.max_candidate_volumes_per_site = max(1, int(max_candidate_volumes_per_site))

        station_started_at = time.perf_counter()
        station_vcps = await asyncio.to_thread(self.station_fetcher, session=weather_session)
        io_manager.write_perf(
            f"[RUN] station_catalog_fetch: {self._format_perf_ms(station_started_at):.2f}ms "
            f"(stations={len(station_vcps)})"
        )

        if sites is None:
            site_items = sorted(station_vcps.items())
        else:
            site_items = [(str(site).upper(), station_vcps.get(str(site).upper())) for site in sites]

        site_items = [(site, station) for site, station in site_items if station is not None]
        if not site_items:
            return NexradCoordinatorRunResults()

        site_semaphore = asyncio.Semaphore(self.max_site_tasks)

        async def _run(site, station, *, active_s3_client):
            async with site_semaphore:
                try:
                    result = await self._process_site(
                        site,
                        station,
                        base_dir=base_dir,
                        s3_client=active_s3_client,
                        weather_session=weather_session,
                    )
                    io_manager.write_info(
                        f"[SITE {site}] action={result.action} latest_scan={result.latest_scan_time} "
                        f"volume_id={result.volume_id} chunks_downloaded={result.chunks_downloaded}"
                    )
                    return result
                except Exception as exc:
                    io_manager.write_warning(f"[SITE {site}] coordinator failure: {exc}")
                    return NexradCoordinatorResult(
                        site=site,
                        latest_scan_time=station.level_two_last_received_time,
                        vcp=station.vcp,
                        volume_id=None,
                        action="site_error",
                    )

        async with self._ingest_service._async_s3_client(s3_client) as active_s3_client:
            results = await asyncio.gather(
                *(
                    _run(site, station, active_s3_client=active_s3_client)
                    for site, station in site_items
                )
            )
        downloaded = sum(1 for result in results if result.action == "downloaded")
        downloaded_sites = [result.site for result in results if result.action == "downloaded"]
        io_manager.write_perf(
            f"[RUN] latest_station_scans_async_total: {self._format_perf_ms(started_at):.2f}ms "
            f"(sites={len(site_items)}, downloaded={downloaded}, skipped={len(results) - downloaded})"
        )
        return NexradCoordinatorRunResults(results, downloaded_sites=downloaded_sites)

    async def poll_latest_station_scans_forever_async(
        self,
        sites=None,
        *,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        max_candidate_volumes_per_site=None,
        poll_interval_seconds=60,
    ):
        interval = max(1.0, float(poll_interval_seconds))
        while True:
            try:
                await self.ingest_latest_station_scans_async(
                    sites,
                    base_dir=base_dir,
                    s3_client=s3_client,
                    weather_session=weather_session,
                    max_candidate_volumes_per_site=max_candidate_volumes_per_site,
                )
            except Exception as exc:
                io_manager.write_warning(f"[RUN] periodic station scan poll failed: {exc}")
            await asyncio.sleep(interval)


async def ingest_latest_station_scans_async(
    sites=None,
    *,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    max_candidate_volumes_per_site=3,
):
    coordinator = NexradScanCoordinator(max_candidate_volumes_per_site=max_candidate_volumes_per_site)
    return await coordinator.ingest_latest_station_scans_async(
        sites,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
    )


async def poll_latest_station_scans_forever_async(
    sites=None,
    *,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    max_candidate_volumes_per_site=3,
    poll_interval_seconds=60,
):
    coordinator = NexradScanCoordinator(max_candidate_volumes_per_site=max_candidate_volumes_per_site)
    await coordinator.poll_latest_station_scans_forever_async(
        sites,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        poll_interval_seconds=poll_interval_seconds,
    )

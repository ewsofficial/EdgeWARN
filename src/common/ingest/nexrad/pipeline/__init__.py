import argparse
import asyncio
import time
from pathlib import Path

import util.file as fs
from common.ingest.nexrad.models import NexradCompletionRecord, NexradIngestResult
from common.ingest.nexrad.service import NexradIngestService
from common.ingest.nexrad.s3_async import async_list_recent_volume_ids, async_list_volume_chunks
from common.ingest.nexrad.s3_chunks import required_volume_chunks
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from common.ingest.nexrad.pipeline.models import PendingVolume
from common.ingest.nexrad.pipeline.pending import NexradPendingVolumeTracker
from common.ingest.nexrad.pipeline.station_filter import NexradStationFilter
from common.ingest.nexrad.pipeline.volume_discovery import NexradVolumeDiscovery
from util.io import IOManager

io_manager = IOManager("[NEXRAD-PIPE]", include_timestamps=True)


def _completion_record_from_result(result: NexradIngestResult) -> NexradCompletionRecord:
    return NexradCompletionRecord(
        site=str(result.site).upper(),
        volume_id=str(result.volume_id),
        scan_timestamp=result.scan_timestamp,
        volume_path=None if result.volume_path is None else Path(result.volume_path),
        manifest_path=None if result.manifest_path is None else Path(result.manifest_path),
    )


class NexradRealtimeIngestionPipeline:
    def __init__(
        self,
        *,
        station_fetcher=None,
        async_volume_lister=None,
        async_chunk_lister=None,
        async_ingest_trigger=None,
        base_dir=None,
        sites=None,
        max_site_tasks=24,
        max_candidate_volumes_per_site=3,
        scan_interval_seconds=20,
        completion_interval_seconds=10,
        sleeper=asyncio.sleep,
        monotonic=time.monotonic,
    ):
        self.base_dir = base_dir
        self.sites = None if sites is None else [str(site).upper() for site in sites]
        self.max_site_tasks = max_site_tasks
        self.scan_interval_seconds = max(1.0, float(scan_interval_seconds))
        self.completion_interval_seconds = max(1.0, float(completion_interval_seconds))
        self.sleeper = sleeper
        self.monotonic = monotonic
        self._ingest_service = NexradIngestService(max_site_tasks=max_site_tasks)
        self.station_filter = NexradStationFilter(
            station_fetcher=station_fetcher or fetch_radar_station_vcps,
        )
        self.volume_discovery = NexradVolumeDiscovery(
            async_volume_lister=async_volume_lister or async_list_recent_volume_ids,
            async_chunk_lister=async_chunk_lister or async_list_volume_chunks,
            max_candidate_volumes_per_site=max_candidate_volumes_per_site,
        )
        self.async_ingest_trigger = (
            async_ingest_trigger or self._ingest_service.ingest_allowed_vcp_volume_async
        )
        self.pending_tracker = NexradPendingVolumeTracker()
        self.last_seen_by_site: dict[str, str] = {}

    async def scan_for_new_volumes_once(self, *, s3_client=None, weather_session=None):
        if self.base_dir:
            fs.initialize_filesystem(self.base_dir)
        downloaded_records = []
        site_semaphore = asyncio.Semaphore(self.max_site_tasks)
        allowed_sites = await self.station_filter.fetch_allowed_stations(
            sites=self.sites,
            weather_session=weather_session,
        )
        if not allowed_sites:
            return []

        async with self._ingest_service._async_s3_client(s3_client) as active_s3_client:
            async def _scan_site(site, station):
                async with site_semaphore:
                    discovery = await self.volume_discovery.discover_latest(
                        site,
                        station,
                        s3_client=active_s3_client,
                    )
                    if discovery.volume_id is None:
                        return None

                    self.last_seen_by_site[site] = discovery.volume_id
                    stale_pending = [
                        pending
                        for (pending_site, _pending_volume_id), pending in self.pending_tracker.items()
                        if pending_site == site and pending.volume_id != discovery.volume_id and not pending.ingest_started
                    ]
                    for pending in stale_pending:
                        self.pending_tracker.remove(pending.site, pending.volume_id)
                    if self.volume_discovery.local_complete(
                        site,
                        discovery.volume_id,
                        discovery.chunks,
                    ):
                        self.pending_tracker.remove(site, discovery.volume_id)
                        return None

                    if not required_volume_chunks(discovery.chunks):
                        self.pending_tracker.upsert(
                            PendingVolume(
                                site=site,
                                volume_id=discovery.volume_id,
                                station=station,
                                latest_scan_time=discovery.latest_scan_time,
                                ingest_started=False,
                            )
                        )
                        return None

                    result = await self.async_ingest_trigger(
                        site,
                        discovery.volume_id,
                        base_dir=self.base_dir,
                        s3_client=active_s3_client,
                        weather_session=weather_session,
                        station_vcp=station,
                    )
                    if result is None:
                        return None
                    if not result.complete:
                        self.pending_tracker.upsert(
                            PendingVolume(
                                site=site,
                                volume_id=discovery.volume_id,
                                station=station,
                                latest_scan_time=discovery.latest_scan_time,
                                ingest_started=True,
                            )
                        )
                        return None
                    self.pending_tracker.remove(site, discovery.volume_id)
                    return _completion_record_from_result(result)

            results = await asyncio.gather(
                *(_scan_site(site, station) for site, station in allowed_sites),
                return_exceptions=True,
            )

        for result in results:
            if isinstance(result, Exception):
                io_manager.write_warning(f"[SCAN] site failure: {result}")
                continue
            if result is not None:
                downloaded_records.append(result)
        return downloaded_records

    async def check_pending_once(self, *, s3_client=None, weather_session=None):
        if self.base_dir:
            fs.initialize_filesystem(self.base_dir)
        downloaded_records = []
        async with self._ingest_service._async_s3_client(s3_client) as active_s3_client:
            for (site, volume_id), pending_volume in self.pending_tracker.items():
                if self.last_seen_by_site.get(site) not in (None, volume_id):
                    if not pending_volume.ingest_started:
                        self.pending_tracker.remove(site, volume_id)
                        continue
                chunks = tuple(
                    await self.volume_discovery.async_chunk_lister(
                        site,
                        volume_id,
                        s3_client=active_s3_client,
                    )
                )
                if not chunks:
                    continue
                if self.volume_discovery.local_complete(site, volume_id, chunks):
                    self.pending_tracker.remove(site, volume_id)
                    continue
                if not required_volume_chunks(chunks):
                    continue
                result = await self.async_ingest_trigger(
                    site,
                    volume_id,
                    base_dir=self.base_dir,
                    s3_client=active_s3_client,
                    weather_session=weather_session,
                    station_vcp=pending_volume.station,
                )
                if result is None or not result.complete:
                    continue
                self.pending_tracker.remove(site, volume_id)
                downloaded_records.append(_completion_record_from_result(result))

        return downloaded_records

    async def run_forever(self, *, s3_client=None, weather_session=None):
        next_scan_at = self.monotonic()
        next_completion_at = self.monotonic()
        while True:
            now = self.monotonic()
            if now >= next_scan_at:
                try:
                    await self.scan_for_new_volumes_once(
                        s3_client=s3_client,
                        weather_session=weather_session,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    io_manager.write_warning(f"[RUN] scan cycle failed: {exc}")
                next_scan_at = now + self.scan_interval_seconds
            if now >= next_completion_at:
                try:
                    await self.check_pending_once(
                        s3_client=s3_client,
                        weather_session=weather_session,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    io_manager.write_warning(f"[RUN] pending cycle failed: {exc}")
                next_completion_at = now + self.completion_interval_seconds
            delay = max(0.0, min(next_scan_at, next_completion_at) - self.monotonic())
            await self.sleeper(delay)


async def run_realtime_ingestion_pipeline_async(**kwargs):
    pipeline = NexradRealtimeIngestionPipeline(**kwargs)
    await pipeline.run_forever()


def run_realtime_ingestion_pipeline(**kwargs):
    asyncio.run(run_realtime_ingestion_pipeline_async(**kwargs))


def _build_parser():
    parser = argparse.ArgumentParser(description="Run the NEXRAD real-time ingestion pipeline")
    parser.add_argument("--site", action="append")
    parser.add_argument("--base-dir")
    parser.add_argument("--scan-interval-seconds", type=float, default=20)
    parser.add_argument("--completion-interval-seconds", type=float, default=10)
    parser.add_argument("--max-candidate-volumes-per-site", type=int, default=3)
    return parser


def main():
    args = _build_parser().parse_args()
    run_realtime_ingestion_pipeline(
        sites=args.site,
        base_dir=args.base_dir,
        scan_interval_seconds=args.scan_interval_seconds,
        completion_interval_seconds=args.completion_interval_seconds,
        max_candidate_volumes_per_site=args.max_candidate_volumes_per_site,
    )

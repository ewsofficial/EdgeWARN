import argparse
import asyncio
import time
from pathlib import Path

import util.file as fs
from common.ingest.nexrad.models import NexradCompletionRecord, NexradIngestResult
from common.ingest.nexrad import config as nexrad_config
from common.ingest.nexrad.service import NexradIngestService
from common.ingest.nexrad.s3_async import async_list_recent_volume_ids, async_list_volume_chunks
from common.ingest.nexrad.s3_chunks import required_volume_chunks
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from common.ingest.nexrad.pipeline.models import PendingVolume
from common.ingest.nexrad.pipeline.pending import NexradPendingVolumeTracker
from common.ingest.nexrad.pipeline.station_filter import NexradStationFilter
from common.ingest.nexrad.pipeline.volume_discovery import NexradVolumeDiscovery
from common.config import loader as config_loader
from common.config import overlay
from util.io import IOManager

io_manager = IOManager("[NEXRAD-PIPE]")


class NexradOperationTimeout(TimeoutError):
    """An operation exceeded its bounded realtime deadline."""

    def __init__(self, *, site, stage, elapsed_seconds, volume_id=None):
        self.site = str(site).upper()
        self.stage = stage
        self.volume_id = None if volume_id is None else str(volume_id)
        self.elapsed_seconds = elapsed_seconds
        target = self.site if self.volume_id is None else f"{self.site}/{self.volume_id}"
        super().__init__(f"{target} {stage} timed out after {elapsed_seconds:.2f}s")


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
        max_site_tasks=None,
        max_candidate_volumes_per_site=None,
        scan_interval_seconds=None,
        completion_interval_seconds=None,
        volume_discovery_timeout_seconds=None,
        chunk_list_timeout_seconds=None,
        ingest_timeout_seconds=None,
        scan_timeout_seconds=None,
        cancellation_grace_seconds=None,
        heartbeat_callback=None,
        sleeper=asyncio.sleep,
        monotonic=time.monotonic,
    ):
        # Every tunable defaults to None and is resolved here rather than in the
        # signature above. Production reaches this constructor from
        # util/runtime/background.py with only base_dir and heartbeat_callback,
        # so a signature default is what the whole run would use -- and a
        # signature default calling an accessor would evaluate at import, before
        # EDGEWARN_CONFIG_DIR exists. None means "take the catalog value"; an
        # explicit argument still wins, which is how the tests pin cadences.
        if max_site_tasks is None:
            max_site_tasks = nexrad_config.max_site_tasks()
        if max_candidate_volumes_per_site is None:
            max_candidate_volumes_per_site = nexrad_config.max_candidate_volumes_per_site()
        if scan_interval_seconds is None:
            scan_interval_seconds = nexrad_config.scan_interval_seconds()
        if completion_interval_seconds is None:
            completion_interval_seconds = nexrad_config.completion_interval_seconds()
        if volume_discovery_timeout_seconds is None:
            volume_discovery_timeout_seconds = nexrad_config.volume_discovery_timeout_seconds()
        if chunk_list_timeout_seconds is None:
            chunk_list_timeout_seconds = nexrad_config.chunk_list_timeout_seconds()
        if ingest_timeout_seconds is None:
            ingest_timeout_seconds = nexrad_config.ingest_timeout_seconds()
        if scan_timeout_seconds is None:
            scan_timeout_seconds = nexrad_config.scan_timeout_seconds()
        if cancellation_grace_seconds is None:
            cancellation_grace_seconds = nexrad_config.cancellation_grace_seconds()

        self.base_dir = base_dir
        self.sites = None if sites is None else [str(site).upper() for site in sites]
        self.max_site_tasks = max_site_tasks
        # The clamps guard explicit arguments, not the catalog: the schema now
        # pins each of these at or above the floor the clamp applies, so a
        # configured value can no longer be silently rewritten.
        self.scan_interval_seconds = max(1.0, float(scan_interval_seconds))
        self.completion_interval_seconds = max(1.0, float(completion_interval_seconds))
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.volume_discovery_timeout_seconds = max(0.01, float(volume_discovery_timeout_seconds))
        self.chunk_list_timeout_seconds = max(0.01, float(chunk_list_timeout_seconds))
        self.ingest_timeout_seconds = max(0.01, float(ingest_timeout_seconds))
        self.scan_timeout_seconds = max(0.01, float(scan_timeout_seconds))
        self.cancellation_grace_seconds = max(0.0, float(cancellation_grace_seconds))
        self.heartbeat_callback = heartbeat_callback
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

    def _timeout_for_stage(self, stage):
        if stage == "volume-discovery":
            return self.volume_discovery_timeout_seconds
        if stage == "chunk-list":
            return self.chunk_list_timeout_seconds
        return self.ingest_timeout_seconds

    async def _await_operation(self, awaitable, *, site, stage, volume_id=None):
        """Await one network/ingest boundary without allowing it to wedge a cycle."""
        started_at = self.monotonic()
        task = asyncio.create_task(awaitable)
        try:
            done, _pending = await asyncio.wait({task}, timeout=self._timeout_for_stage(stage))
        except asyncio.CancelledError:
            # A scan-wide deadline cancels the parent site task.  Propagate
            # that cancellation to this child task as well; otherwise an
            # unresolved lister/download can outlive the scan indefinitely.
            task.cancel()
            if self.cancellation_grace_seconds:
                await asyncio.wait({task}, timeout=self.cancellation_grace_seconds)
            raise
        if done:
            return task.result()

        task.cancel()
        if self.cancellation_grace_seconds:
            await asyncio.wait({task}, timeout=self.cancellation_grace_seconds)
        elapsed = self.monotonic() - started_at
        raise NexradOperationTimeout(
            site=site,
            volume_id=volume_id,
            stage=stage,
            elapsed_seconds=elapsed,
        )

    @staticmethod
    def _log_timeout(timeout, *, cycle):
        target = timeout.site if timeout.volume_id is None else f"{timeout.site}/{timeout.volume_id}"
        io_manager.write_warning(
            f"[{cycle}] timeout site={target} stage={timeout.stage} elapsed={timeout.elapsed_seconds:.2f}s"
        )

    def _emit_heartbeat(self, cycle, records, *, timed_out=0):
        if self.heartbeat_callback is None:
            return
        try:
            self.heartbeat_callback({
                "cycle": cycle,
                "completed_records": records,
                "output_count": len(records),
                "timed_out": timed_out,
            })
        except Exception as exc:
            io_manager.write_warning(f"[HEARTBEAT] failed to emit {cycle} progress: {exc}")

    async def scan_for_new_volumes_once(self, *, s3_client=None, weather_session=None):
        if self.base_dir:
            fs.initialize_filesystem(self.base_dir)
        downloaded_records = []
        site_semaphore = asyncio.Semaphore(self.max_site_tasks)
        try:
            allowed_sites = await self._await_operation(
                self.station_filter.fetch_allowed_stations(sites=self.sites, weather_session=weather_session),
                site="ALL",
                stage="volume-discovery",
            )
        except NexradOperationTimeout as timeout:
            self._log_timeout(timeout, cycle="SCAN")
            return []
        if not allowed_sites:
            self._emit_heartbeat("scan", [])
            return []

        timed_out = 0
        async with self._ingest_service._async_s3_client(s3_client) as active_s3_client:
            async def _scan_site(site, station):
                async with site_semaphore:
                    try:
                        discovery = await self.volume_discovery.discover_latest(
                            site,
                            station,
                            s3_client=active_s3_client,
                            await_operation=lambda awaitable, **context: self._await_operation(
                                awaitable, site=site, **context,
                            ),
                        )
                    except NexradOperationTimeout as timeout:
                        self._log_timeout(timeout, cycle="SCAN")
                        return None
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

                    try:
                        result = await self._await_operation(
                            self.async_ingest_trigger(
                                site,
                                discovery.volume_id,
                                base_dir=self.base_dir,
                                s3_client=active_s3_client,
                                weather_session=weather_session,
                                station_vcp=station,
                            ),
                            site=site,
                            volume_id=discovery.volume_id,
                            stage="ingest-download",
                        )
                    except NexradOperationTimeout as timeout:
                        self._log_timeout(timeout, cycle="SCAN")
                        return None
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

            tasks = {
                asyncio.create_task(_scan_site(site, station)): site
                for site, station in allowed_sites
            }
            done, pending = await asyncio.wait(tasks, timeout=self.scan_timeout_seconds)
            results = []
            # ``asyncio.wait`` returns a set, whose iteration order is not a
            # result contract.  Preserve the already deterministic allowed-site
            # order so callers receive stable completion records.
            for task in tasks:
                if task not in done:
                    continue
                if task.cancelled():
                    continue
                try:
                    results.append(task.result())
                except Exception as exc:
                    results.append(exc)
            for task in pending:
                site = tasks[task]
                task.cancel()
                timed_out += 1
                io_manager.write_warning(
                    f"[SCAN] timeout site={site} stage=scan elapsed={self.scan_timeout_seconds:.2f}s"
                )
            if pending and self.cancellation_grace_seconds:
                await asyncio.wait(pending, timeout=self.cancellation_grace_seconds)

        for result in results:
            if isinstance(result, Exception):
                io_manager.write_warning(f"[SCAN] site failure: {result}")
                continue
            if result is not None:
                downloaded_records.append(result)
        self._emit_heartbeat("scan", downloaded_records, timed_out=timed_out)
        return downloaded_records

    async def check_pending_once(self, *, s3_client=None, weather_session=None):
        if self.base_dir:
            fs.initialize_filesystem(self.base_dir)
        downloaded_records = []
        timed_out = 0
        pending_items = self.pending_tracker.items()
        if not pending_items:
            self._emit_heartbeat("pending", downloaded_records, timed_out=timed_out)
            return downloaded_records
        async with self._ingest_service._async_s3_client(s3_client) as active_s3_client:
            for (site, volume_id), pending_volume in pending_items:
                if self.last_seen_by_site.get(site) not in (None, volume_id):
                    if not pending_volume.ingest_started:
                        self.pending_tracker.remove(site, volume_id)
                        continue
                try:
                    chunks = tuple(await self._await_operation(
                        self.volume_discovery.async_chunk_lister(site, volume_id, s3_client=active_s3_client),
                        site=site,
                        volume_id=volume_id,
                        stage="chunk-list",
                    ))
                except NexradOperationTimeout as timeout:
                    timed_out += 1
                    self._log_timeout(timeout, cycle="PENDING")
                    continue
                if not chunks:
                    continue
                if self.volume_discovery.local_complete(site, volume_id, chunks):
                    self.pending_tracker.remove(site, volume_id)
                    continue
                if not required_volume_chunks(chunks):
                    continue
                try:
                    result = await self._await_operation(
                        self.async_ingest_trigger(
                            site,
                            volume_id,
                            base_dir=self.base_dir,
                            s3_client=active_s3_client,
                            weather_session=weather_session,
                            station_vcp=pending_volume.station,
                        ),
                        site=site,
                        volume_id=volume_id,
                        stage="ingest-download",
                    )
                except NexradOperationTimeout as timeout:
                    timed_out += 1
                    self._log_timeout(timeout, cycle="PENDING")
                    continue
                if result is None or not result.complete:
                    continue
                self.pending_tracker.remove(site, volume_id)
                downloaded_records.append(_completion_record_from_result(result))

        self._emit_heartbeat("pending", downloaded_records, timed_out=timed_out)
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
    parser.add_argument("--scan-interval-seconds", type=float, default=None, help="default: from nexrad.yaml")
    parser.add_argument("--completion-interval-seconds", type=float, default=None, help="default: from nexrad.yaml")
    parser.add_argument("--max-candidate-volumes-per-site", type=int, default=None, help="default: from nexrad.yaml")
    parser.add_argument("--config-dir", type=str, default=None, help="Override the config/ directory (else EDGEWARN_CONFIG_DIR or repo root)")
    return parser


def _resolve_cli_args(args):
    # Exported before anything else here: this pipeline creates parse workers
    # through a ProcessPoolExecutor, and those children receive no config in
    # their submit payload, so the environment variable is the only way
    # --config-dir reaches them. Passing config_dir= to load_config below would
    # otherwise leave the parent reading the override while its workers read the
    # repo default.
    config_loader.export_config_root(args.config_dir)

    # The `realtime` section, not `cli`: these are pipeline settings that this
    # entry point may override, and the constructor resolves the same three from
    # the same place when no argv supplies them.
    realtime_cfg = config_loader.load_config("nexrad", config_dir=args.config_dir)["realtime"]
    args.scan_interval_seconds = overlay.resolve(args.scan_interval_seconds, yaml_value=realtime_cfg["scan_interval_seconds"])
    args.completion_interval_seconds = overlay.resolve(args.completion_interval_seconds, yaml_value=realtime_cfg["completion_interval_seconds"])
    args.max_candidate_volumes_per_site = overlay.resolve(args.max_candidate_volumes_per_site, yaml_value=realtime_cfg["max_candidate_volumes_per_site"])
    return args


def main():
    args = _resolve_cli_args(_build_parser().parse_args())
    run_realtime_ingestion_pipeline(
        sites=args.site,
        base_dir=args.base_dir,
        scan_interval_seconds=args.scan_interval_seconds,
        completion_interval_seconds=args.completion_interval_seconds,
        max_candidate_volumes_per_site=args.max_candidate_volumes_per_site,
    )

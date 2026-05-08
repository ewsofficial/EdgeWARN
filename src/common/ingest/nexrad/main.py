import argparse
import asyncio
from contextlib import asynccontextmanager
import time
from pathlib import Path

import util.file as fs
from common.ingest.nexrad.config import ALLOWED_VCPS, CHUNKS_BUCKET
from common.ingest.nexrad.models import NexradIngestResult
from common.ingest.nexrad.s3_async import (
    async_get_chunk_bytes,
    async_list_recent_volume_ids,
    async_list_volume_chunks,
    get_unsigned_s3_client_async,
)
from common.ingest.nexrad.s3_chunks import (
    extract_volume_timestamp,
    format_nexrad_timestamp,
    get_chunk_bytes,
    get_unsigned_s3_client,
    list_recent_volume_ids,
    list_volume_chunks,
    parse_nexrad_timestamp,
    required_low_chunks,
)
from common.ingest.nexrad.vcp_probe import probe_volume_vcp
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from common.ingest.nexrad.writer import NexradLocalChunkStore, chunk_output_dir, local_low_chunks_complete, prune_station_scan_dirs
from util.io import IOManager

io_manager = IOManager("[NEXRAD]")


class NexradIngestService:
    def __init__(
        self,
        *,
        chunk_lister=None,
        chunk_fetcher=None,
        volume_lister=None,
        station_fetcher=None,
        volume_prober=None,
        async_chunk_lister=None,
        async_chunk_fetcher=None,
        async_volume_lister=None,
        max_site_tasks=16,
        max_chunk_downloads=32,
    ):
        self.chunk_lister = chunk_lister or list_volume_chunks
        self.chunk_fetcher = chunk_fetcher or get_chunk_bytes
        self.volume_lister = volume_lister or list_recent_volume_ids
        self.station_fetcher = station_fetcher or fetch_radar_station_vcps
        self.volume_prober = volume_prober or probe_volume_vcp
        self.async_chunk_lister = async_chunk_lister or async_list_volume_chunks
        self.async_chunk_fetcher = async_chunk_fetcher or async_get_chunk_bytes
        self.async_volume_lister = async_volume_lister or async_list_recent_volume_ids
        self.max_site_tasks = max_site_tasks
        self.max_chunk_downloads = max_chunk_downloads
        self._stream_chunk_downloads = async_chunk_fetcher is None
        self._shared_chunk_download_semaphore = None
        self.local_chunk_store = NexradLocalChunkStore()

    @staticmethod
    def _format_perf_ms(started_at: float) -> float:
        return (time.perf_counter() - started_at) * 1000

    @staticmethod
    def _volume_timestamp(volume_id: str, chunks) -> str:
        return extract_volume_timestamp(volume_id, chunks)

    def _chunk_output_dir(self, site: str, volume_id: str, chunks) -> Path:
        return self.local_chunk_store.chunk_output_dir(site, volume_id, chunks)

    def _download_chunks_to_site_dir(self, site: str, volume_id: str, chunks, *, s3_client):
        outdir = self._chunk_output_dir(site, volume_id, chunks)
        outdir.mkdir(parents=True, exist_ok=True)
        for chunk in chunks:
            filename = chunk.key.split("/")[-1]
            local_path = outdir / filename
            if local_path.exists():
                continue
            local_path.write_bytes(self.chunk_fetcher(chunk, s3_client=s3_client))
        self.local_chunk_store.prune_station_scan_dirs(site, outdir.parent.name)

    async def _download_chunks_to_site_dir_async(self, site: str, volume_id: str, chunks, *, s3_client, chunk_download_semaphore=None):
        import aiofiles

        started_at = time.perf_counter()
        outdir = self._chunk_output_dir(site, volume_id, chunks)
        outdir.mkdir(parents=True, exist_ok=True)
        semaphore = chunk_download_semaphore or asyncio.Semaphore(self.max_chunk_downloads)

        async def _download_one(chunk):
            filename = chunk.key.split("/")[-1]
            local_path = outdir / filename
            if local_path.exists():
                return

            async with semaphore:
                async with aiofiles.open(local_path, "wb") as file_obj:
                    if self._stream_chunk_downloads:
                        response = await s3_client.get_object(Bucket=CHUNKS_BUCKET, Key=chunk.key)
                        body = response["Body"]
                        async for data in body.iter_chunks():
                            await file_obj.write(data)
                        return

                    payload = await self.async_chunk_fetcher(chunk, s3_client=s3_client)
                    if isinstance(payload, (bytes, bytearray)):
                        await file_obj.write(payload)
                        return

                    body = payload["Body"] if isinstance(payload, dict) else payload
                    if hasattr(body, "iter_chunks"):
                        async for data in body.iter_chunks():
                            await file_obj.write(data)
                        return

                    await file_obj.write(await body.read())

        await asyncio.gather(*(_download_one(chunk) for chunk in chunks))
        self.local_chunk_store.prune_station_scan_dirs(site, outdir.parent.name)
        elapsed_ms = self._format_perf_ms(started_at)
        io_manager.write_perf(
            f"[VOL {str(site).upper()}/{volume_id}] chunk_download_write: {elapsed_ms:.2f}ms "
            f"(chunks={len(chunks)}, max_chunk_downloads={self.max_chunk_downloads})"
        )

    @staticmethod
    def _required_low_chunks(chunks):
        return required_low_chunks(chunks)

    def ingest_allowed_vcp_volume(
        self,
        site,
        volume_id,
        *,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        parser=None,
        writer=None,
        station_vcp=None,
    ):
        _ = (parser, writer)
        if base_dir:
            fs.initialize_filesystem(base_dir)

        s3_client = s3_client or get_unsigned_s3_client()
        if station_vcp is None:
            probe = self.volume_prober(site, volume_id, s3_client=s3_client, weather_session=weather_session)
            if not probe.accepted:
                return None
            probe_vcp = probe.vcp
            probe_site = probe.site
            probe_volume_id = probe.volume_id
        else:
            if station_vcp.vcp not in ALLOWED_VCPS:
                return None
            probe_vcp = station_vcp.vcp
            probe_site = str(site).upper()
            probe_volume_id = str(volume_id)

        chunks = self.chunk_lister(site, volume_id, s3_client=s3_client)
        needed_chunks = self._required_low_chunks(chunks)
        if not needed_chunks:
            return NexradIngestResult(
                site=probe_site,
                volume_id=probe_volume_id,
                vcp=probe_vcp,
                dynamic_scan_type=None,
                low_path=None,
                high_path=None,
                manifest_path=None,
                chunks_downloaded=0,
                complete=False,
            )

        self._download_chunks_to_site_dir(site, volume_id, needed_chunks, s3_client=s3_client)
        return NexradIngestResult(
            site=probe_site,
            volume_id=probe_volume_id,
            vcp=probe_vcp,
            dynamic_scan_type=None,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=len(needed_chunks),
            complete=True,
        )

    async def ingest_allowed_vcp_volume_async(
        self,
        site,
        volume_id,
        *,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        parser=None,
        writer=None,
        station_vcp=None,
        chunk_download_semaphore=None,
    ):
        _ = (parser, writer)
        total_started_at = time.perf_counter()
        if base_dir:
            fs.initialize_filesystem(base_dir)

        async with self._async_s3_client(s3_client) as active_s3_client:
            if station_vcp is None:
                probe = await asyncio.to_thread(
                    self.volume_prober,
                    site,
                    volume_id,
                    weather_session=weather_session,
                )
                if not probe.accepted:
                    return None
                probe_vcp = probe.vcp
                probe_site = probe.site
                probe_volume_id = probe.volume_id
            else:
                if station_vcp.vcp not in ALLOWED_VCPS:
                    return None
                probe_vcp = station_vcp.vcp
                probe_site = str(site).upper()
                probe_volume_id = str(volume_id)

            list_started_at = time.perf_counter()
            chunks = await self.async_chunk_lister(site, volume_id, s3_client=active_s3_client)
            list_elapsed_ms = self._format_perf_ms(list_started_at)
            needed_chunks = self._required_low_chunks(chunks)
            io_manager.write_perf(
                f"[VOL {probe_site}/{probe_volume_id}] chunk_list: {list_elapsed_ms:.2f}ms "
                f"(listed={len(chunks)}, needed={len(needed_chunks)})"
            )
            if not needed_chunks:
                total_elapsed_ms = self._format_perf_ms(total_started_at)
                io_manager.write_perf(
                    f"[VOL {probe_site}/{probe_volume_id}] total_async_ingest: {total_elapsed_ms:.2f}ms "
                    f"(accepted=True, complete=False, chunks_downloaded=0)"
                )
                return NexradIngestResult(
                    site=probe_site,
                    volume_id=probe_volume_id,
                    vcp=probe_vcp,
                    dynamic_scan_type=None,
                    low_path=None,
                    high_path=None,
                    manifest_path=None,
                    chunks_downloaded=0,
                    complete=False,
                )

            await self._download_chunks_to_site_dir_async(
                site,
                volume_id,
                needed_chunks,
                s3_client=active_s3_client,
                chunk_download_semaphore=chunk_download_semaphore or self._shared_chunk_download_semaphore,
            )
            total_elapsed_ms = self._format_perf_ms(total_started_at)
            io_manager.write_perf(
                f"[VOL {probe_site}/{probe_volume_id}] total_async_ingest: {total_elapsed_ms:.2f}ms "
                f"(accepted=True, complete=True, chunks_downloaded={len(needed_chunks)})"
            )
            return NexradIngestResult(
                site=probe_site,
                volume_id=probe_volume_id,
                vcp=probe_vcp,
                dynamic_scan_type=None,
                low_path=None,
                high_path=None,
                manifest_path=None,
                chunks_downloaded=len(needed_chunks),
                complete=True,
            )

    def ingest_latest_allowed_vcp_scans(
        self,
        sites,
        *,
        max_volumes_per_site=1,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        station_vcps=None,
    ):
        results = []
        s3_client = s3_client or get_unsigned_s3_client()
        for site in sites:
            volume_ids = self.volume_lister(site, limit=max_volumes_per_site, s3_client=s3_client)
            for volume_id in volume_ids:
                station_vcp = None if station_vcps is None else station_vcps.get(str(site).upper())
                result = self.ingest_allowed_vcp_volume(
                    site,
                    volume_id,
                    base_dir=base_dir,
                    s3_client=s3_client,
                    weather_session=weather_session,
                    station_vcp=station_vcp,
                )
                if result is not None:
                    results.append(result)
        return results

    async def ingest_latest_allowed_vcp_scans_async(
        self,
        sites,
        *,
        max_volumes_per_site=1,
        base_dir=None,
        s3_client=None,
        weather_session=None,
        station_vcps=None,
    ):
        total_started_at = time.perf_counter()
        sites = [str(site).upper() for site in sites]
        if base_dir:
            fs.initialize_filesystem(base_dir)

        if station_vcps is None:
            station_started_at = time.perf_counter()
            station_vcps = await asyncio.to_thread(self.station_fetcher, session=weather_session)
            io_manager.write_perf(
                f"[RUN] station_catalog_fetch: {self._format_perf_ms(station_started_at):.2f}ms "
                f"(stations={len(station_vcps)})"
            )

        filter_started_at = time.perf_counter()
        filtered_sites = [
            site
            for site in sites
            if site.startswith("K") and station_vcps.get(site) is not None and station_vcps[site].vcp in ALLOWED_VCPS
        ]
        io_manager.write_perf(
            f"[RUN] site_filter: {self._format_perf_ms(filter_started_at):.2f}ms "
            f"(input={len(sites)}, allowed={len(filtered_sites)})"
        )
        if not filtered_sites:
            return []

        results = []
        site_semaphore = asyncio.Semaphore(self.max_site_tasks)
        chunk_download_semaphore = asyncio.Semaphore(self.max_chunk_downloads)
        self._shared_chunk_download_semaphore = chunk_download_semaphore

        try:
            async with self._async_s3_client(s3_client) as active_s3_client:
                async def _list_site_volumes(site):
                    async with site_semaphore:
                        started_at = time.perf_counter()
                        volume_ids = await self.async_volume_lister(
                            site,
                            limit=max_volumes_per_site,
                            s3_client=active_s3_client,
                        )
                        io_manager.write_perf(
                            f"[SITE {site}] recent_volume_list: {self._format_perf_ms(started_at):.2f}ms "
                            f"(volumes={len(volume_ids)}, limit={max_volumes_per_site})"
                        )
                        return site, volume_ids

                volume_discovery_started_at = time.perf_counter()
                listed_sites = await asyncio.gather(*(_list_site_volumes(site) for site in filtered_sites), return_exceptions=True)
                io_manager.write_perf(
                    f"[RUN] site_volume_discovery: {self._format_perf_ms(volume_discovery_started_at):.2f}ms "
                    f"(sites={len(filtered_sites)}, max_site_tasks={self.max_site_tasks})"
                )

                volume_work = []
                for listed_site in listed_sites:
                    if isinstance(listed_site, Exception):
                        io_manager.write_warning(f"Skipping site after async volume-list failure: {listed_site}")
                        continue

                    site, volume_ids = listed_site
                    station_vcp = station_vcps.get(site)
                    for volume_id in volume_ids:
                        volume_work.append((site, volume_id, station_vcp))

                volume_ingest_started_at = time.perf_counter()
                gathered = await asyncio.gather(
                    *(
                        self.ingest_allowed_vcp_volume_async(
                            site,
                            volume_id,
                            base_dir=base_dir,
                            s3_client=active_s3_client,
                            weather_session=weather_session,
                            station_vcp=station_vcp,
                            chunk_download_semaphore=chunk_download_semaphore,
                        )
                        for site, volume_id, station_vcp in volume_work
                    ),
                    return_exceptions=True,
                )
                io_manager.write_perf(
                    f"[RUN] volume_ingest_batch: {self._format_perf_ms(volume_ingest_started_at):.2f}ms "
                    f"(volumes={len(volume_work)}, shared_chunk_limit={self.max_chunk_downloads})"
                )
        finally:
            self._shared_chunk_download_semaphore = None

        for (site, volume_id, _station_vcp), result in zip(volume_work, gathered):
            if isinstance(result, Exception):
                io_manager.write_warning(f"Skipping {site}/{volume_id} after async ingest failure: {result}")
                continue
            if result is not None:
                results.append(result)

        complete_count = sum(1 for result in results if result.complete)
        downloaded_chunks = sum(result.chunks_downloaded for result in results)
        total_elapsed_ms = self._format_perf_ms(total_started_at)
        io_manager.write_perf(
            f"[RUN] latest_allowed_vcp_scans_async_total: {total_elapsed_ms:.2f}ms "
            f"(sites={len(filtered_sites)}, volumes={len(volume_work)}, results={len(results)}, "
            f"complete={complete_count}, chunks_downloaded={downloaded_chunks}, max_site_tasks={self.max_site_tasks})"
        )

        return results

    @staticmethod
    @asynccontextmanager
    async def _async_s3_client(s3_client=None):
        if s3_client is not None:
            yield s3_client
            return

        async with get_unsigned_s3_client_async() as client:
            yield client

    def list_allowed_vcp_sites(self, *, weather_session=None, stations=None):
        if stations is None:
            stations = self.station_fetcher(session=weather_session)
        return sorted(
            site
            for site, station in stations.items()
            if station.vcp in ALLOWED_VCPS and str(site).upper().startswith("K")
        )


def ingest_allowed_vcp_volume(
    site,
    volume_id,
    *,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    parser=None,
    writer=None,
    station_vcp=None,
):
    service = NexradIngestService()
    return service.ingest_allowed_vcp_volume(
        site,
        volume_id,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        parser=parser,
        writer=writer,
        station_vcp=station_vcp,
    )


def ingest_latest_allowed_vcp_scans(
    sites,
    *,
    max_volumes_per_site=1,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    station_vcps=None,
):
    service = NexradIngestService()
    return service.ingest_latest_allowed_vcp_scans(
        sites,
        max_volumes_per_site=max_volumes_per_site,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        station_vcps=station_vcps,
    )


async def ingest_allowed_vcp_volume_async(
    site,
    volume_id,
    *,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    parser=None,
    writer=None,
    station_vcp=None,
):
    service = NexradIngestService()
    return await service.ingest_allowed_vcp_volume_async(
        site,
        volume_id,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        parser=parser,
        writer=writer,
        station_vcp=station_vcp,
    )


async def ingest_latest_allowed_vcp_scans_async(
    sites,
    *,
    max_volumes_per_site=1,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    station_vcps=None,
):
    service = NexradIngestService()
    return await service.ingest_latest_allowed_vcp_scans_async(
        sites,
        max_volumes_per_site=max_volumes_per_site,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        station_vcps=station_vcps,
    )


async def ingest_latest_station_scans_async(
    sites=None,
    *,
    base_dir=None,
    s3_client=None,
    weather_session=None,
    max_candidate_volumes_per_site=3,
):
    from common.ingest.nexrad.coordinator import ingest_latest_station_scans_async as _impl

    return await _impl(
        sites,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        max_candidate_volumes_per_site=max_candidate_volumes_per_site,
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
    from common.ingest.nexrad.coordinator import poll_latest_station_scans_forever_async as _impl

    await _impl(
        sites,
        base_dir=base_dir,
        s3_client=s3_client,
        weather_session=weather_session,
        max_candidate_volumes_per_site=max_candidate_volumes_per_site,
        poll_interval_seconds=poll_interval_seconds,
    )


def list_allowed_vcp_sites(*, weather_session=None, stations=None):
    service = NexradIngestService()
    return service.list_allowed_vcp_sites(weather_session=weather_session, stations=stations)


def _build_parser():
    parser = argparse.ArgumentParser(description="Coordinate latest-scan NEXRAD Level-II chunk ingest")
    parser.add_argument("--site")
    parser.add_argument("--volume-id")
    parser.add_argument("--base-dir")
    parser.add_argument("--max-volumes-per-site", type=int, default=1)
    parser.add_argument("--max-candidate-volumes-per-site", type=int, default=3)
    return parser


def main():
    service = NexradIngestService()
    args = _build_parser().parse_args()
    if args.volume_id and not args.site:
        raise SystemExit("--site is required when --volume-id is provided")

    # If a specific site was provided, enforce K-only rule
    if args.site and not str(args.site).upper().startswith("K"):
        io_manager.write_info(f"Skipping {args.site}: only radar stations starting with 'K' are processed")
        return

    if args.volume_id:
        station_fetch_started_at = time.perf_counter()
        stations = fetch_radar_station_vcps()
        io_manager.write_perf(
            f"[CLI] station_catalog_fetch: {service._format_perf_ms(station_fetch_started_at):.2f}ms "
            f"(stations={len(stations)})"
        )
        station_vcp = stations.get(str(args.site).upper()) if stations is not None else None
        try:
            result = asyncio.run(
                service.ingest_allowed_vcp_volume_async(
                    args.site,
                    args.volume_id,
                    base_dir=args.base_dir,
                    station_vcp=station_vcp,
                )
            )
        except Exception as exc:
            io_manager.write_error(f"Async NEXRAD ingest failed for {args.site.upper()}/{args.volume_id}: {exc}")
            io_manager.write_info("Falling back to synchronous NEXRAD ingest...")
            result = service.ingest_allowed_vcp_volume(
                args.site,
                args.volume_id,
                base_dir=args.base_dir,
                station_vcp=station_vcp,
            )
        if result is None:
            io_manager.write_info(
                f"Skipped {args.site.upper()}/{args.volume_id}: station VCP is not allowed or unavailable from weather.gov"
            )
            return

        io_manager.write_info(
            f"Processed {result.site}/{result.volume_id} VCP-{result.vcp}: "
            f"chunks={result.chunks_downloaded}, complete={result.complete}, "
            f"low={result.low_path}, high={result.high_path}, manifest={result.manifest_path}"
        )
        if result.chunks_downloaded == 0:
            io_manager.write_warning(
                f"No chunk objects were found for {result.site}/{result.volume_id} in the chunks bucket."
            )
        return

    try:
        results = asyncio.run(
            ingest_latest_station_scans_async(
                [args.site] if args.site else None,
                base_dir=args.base_dir,
                max_candidate_volumes_per_site=args.max_candidate_volumes_per_site,
            )
        )
    except Exception as exc:
        io_manager.write_error(f"Latest-scan NEXRAD coordinator failed: {exc}")
        return
    if not results:
        if args.site:
            io_manager.write_info(
                f"No latest-scan NEXRAD action was needed for {args.site.upper()} in this pass."
            )
        else:
            io_manager.write_info("No latest-scan NEXRAD action was needed for any allowed-VCP radar site in this pass.")
        return

    for result in results:
        io_manager.write_info(
            f"Site {result.site}: action={result.action}, latest_scan={result.latest_scan_time}, "
            f"volume_id={result.volume_id}, vcp={result.vcp}, chunks_downloaded={result.chunks_downloaded}"
        )


if __name__ == "__main__":
    main()

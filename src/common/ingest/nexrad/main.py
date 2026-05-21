import argparse
import asyncio
import time

from common.ingest.nexrad.config import ALLOWED_VCPS, format_perf_ms
from common.ingest.nexrad.service import NexradIngestService as _BaseNexradIngestService
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
    required_volume_chunks,
)
from common.ingest.nexrad.vcp_probe import probe_volume_vcp
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from common.ingest.nexrad.writer import chunk_output_dir, local_volume_file_complete, prune_station_scan_dirs
from util.io import IOManager

io_manager = IOManager("[NEXRAD]", include_timestamps=True)


def _service_defaults():
    return {
        "chunk_lister": list_volume_chunks,
        "chunk_fetcher": get_chunk_bytes,
        "volume_lister": list_recent_volume_ids,
        "station_fetcher": fetch_radar_station_vcps,
        "volume_prober": probe_volume_vcp,
        "async_chunk_lister": async_list_volume_chunks,
        "async_chunk_fetcher": async_get_chunk_bytes,
        "async_volume_lister": async_list_recent_volume_ids,
    }


class NexradIngestService(_BaseNexradIngestService):
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
        defaults = _service_defaults()
        super().__init__(
            chunk_lister=chunk_lister or defaults["chunk_lister"],
            chunk_fetcher=chunk_fetcher or defaults["chunk_fetcher"],
            volume_lister=volume_lister or defaults["volume_lister"],
            station_fetcher=station_fetcher or defaults["station_fetcher"],
            volume_prober=volume_prober or defaults["volume_prober"],
            async_chunk_lister=async_chunk_lister or defaults["async_chunk_lister"],
            async_chunk_fetcher=async_chunk_fetcher or defaults["async_chunk_fetcher"],
            async_volume_lister=async_volume_lister or defaults["async_volume_lister"],
            max_site_tasks=max_site_tasks,
            max_chunk_downloads=max_chunk_downloads,
        )


def _new_service() -> NexradIngestService:
    return NexradIngestService()


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
    service = _new_service()
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
    service = _new_service()
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
    service = _new_service()
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
    service = _new_service()
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
    service = _new_service()
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
    service = _new_service()
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
            f"[CLI] station_catalog_fetch: {format_perf_ms(station_fetch_started_at):.2f}ms "
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

    downloaded_sites = getattr(results, "downloaded_sites", ())
    io_manager.write_info(f"Downloaded sites: {list(downloaded_sites)}")


if __name__ == "__main__":
    main()

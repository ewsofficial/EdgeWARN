import argparse
import re
from pathlib import Path

import util.file as fs
from common.ingest.nexrad.config import ALLOWED_VCPS, LOW_CHECKPOINT_HINT
from common.ingest.nexrad.models import NexradIngestResult
from common.ingest.nexrad.s3_chunks import get_chunk_bytes, get_unsigned_s3_client, list_recent_volume_ids, list_volume_chunks
from common.ingest.nexrad.vcp_probe import probe_volume_vcp
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from util.io import IOManager

io_manager = IOManager("[NEXRAD]")


_TIMESTAMP_RE = re.compile(r"(?P<stamp>[0-9]{8}-[0-9]{6})")
_VOLUME_ID_TS_RE = re.compile(r"(?P<date>[0-9]{8})[_-](?P<time>[0-9]{6})")


class NexradIngestService:
    def __init__(
        self,
        *,
        chunk_lister=None,
        chunk_fetcher=None,
        volume_lister=None,
        station_fetcher=None,
        volume_prober=None,
    ):
        self.chunk_lister = chunk_lister or list_volume_chunks
        self.chunk_fetcher = chunk_fetcher or get_chunk_bytes
        self.volume_lister = volume_lister or list_recent_volume_ids
        self.station_fetcher = station_fetcher or fetch_radar_station_vcps
        self.volume_prober = volume_prober or probe_volume_vcp

    @staticmethod
    def _volume_timestamp(volume_id: str, chunks) -> str:
        # Only inspect the first chunk's filename for a timestamp
        if chunks:
            first = chunks[0]
            filename = first.key.rsplit("/", 1)[-1]
            match = _TIMESTAMP_RE.search(filename)
            if match:
                return match.group("stamp")

        # Fall back to parsing the volume_id for a timestamp
        match = _VOLUME_ID_TS_RE.search(volume_id)
        if match:
            return f"{match.group('date')}-{match.group('time')}"
        return volume_id

    def _chunk_output_dir(self, site: str, volume_id: str, chunks) -> Path:
        timestamp = self._volume_timestamp(volume_id, chunks)
        return fs.NEXRAD_LEVEL2_DIR / site.upper() / timestamp / "chunks"

    def _download_chunks_to_site_dir(self, site: str, volume_id: str, chunks, *, s3_client):
        outdir = self._chunk_output_dir(site, volume_id, chunks)
        outdir.mkdir(parents=True, exist_ok=True)
        for chunk in chunks:
            filename = chunk.key.split("/")[-1]
            local_path = outdir / filename
            if local_path.exists():
                continue
            local_path.write_bytes(self.chunk_fetcher(chunk, s3_client=s3_client))

    @staticmethod
    def _required_low_chunks(chunks):
        needed = [chunk for chunk in chunks if chunk.chunk_number <= LOW_CHECKPOINT_HINT]
        if len(needed) < LOW_CHECKPOINT_HINT:
            return []
        return needed

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


def list_allowed_vcp_sites(*, weather_session=None, stations=None):
    service = NexradIngestService()
    return service.list_allowed_vcp_sites(weather_session=weather_session, stations=stations)


def _build_parser():
    parser = argparse.ArgumentParser(description="Ingest VCP-gated NEXRAD Level-II chunks")
    parser.add_argument("--site")
    parser.add_argument("--volume-id")
    parser.add_argument("--base-dir")
    parser.add_argument("--max-volumes-per-site", type=int, default=1)
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

    stations = None if args.volume_id else fetch_radar_station_vcps()

    if args.volume_id:
        station_vcp = stations.get(str(args.site).upper()) if stations is not None else None
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

    sites = [args.site] if args.site else service.list_allowed_vcp_sites(stations=stations)
    if not sites:
        io_manager.write_info("No radar sites with allowed VCPs were available from weather.gov in this pass.")
        return

    results = service.ingest_latest_allowed_vcp_scans(
        sites,
        max_volumes_per_site=args.max_volumes_per_site,
        base_dir=args.base_dir,
        station_vcps=stations,
    )
    if not results:
        if args.site:
            io_manager.write_info(
                f"No accepted VCP volumes were ingested for {args.site.upper()} in this pass."
            )
        else:
            io_manager.write_info("No accepted VCP volumes were ingested for any allowed-VCP radar site in this pass.")
        return

    for result in results:
        io_manager.write_info(
            f"Processed {result.site}/{result.volume_id} VCP-{result.vcp}: "
            f"chunks={result.chunks_downloaded}, complete={result.complete}, "
            f"low={result.low_path}, high={result.high_path}, manifest={result.manifest_path}"
        )


if __name__ == "__main__":
    main()

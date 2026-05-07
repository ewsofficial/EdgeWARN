import argparse
import re
from pathlib import Path

import util.file as fs
from common.ingest.nexrad.config import LOW_CHECKPOINT_HINT
from common.ingest.nexrad.models import NexradIngestResult
from common.ingest.nexrad.s3_chunks import get_chunk_bytes, get_unsigned_s3_client, list_recent_volume_ids, list_volume_chunks
from common.ingest.nexrad.vcp_probe import probe_volume_vcp
from util.io import IOManager

io_manager = IOManager("[NEXRAD]")


_TIMESTAMP_RE = re.compile(r"(?P<stamp>[0-9]{8}-[0-9]{6})")
_VOLUME_ID_TS_RE = re.compile(r"(?P<date>[0-9]{8})[_-](?P<time>[0-9]{6})")


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


def _chunk_output_dir(site: str, volume_id: str, chunks) -> Path:
    timestamp = _volume_timestamp(volume_id, chunks)
    return fs.NEXRAD_LEVEL2_DIR / site.upper() / timestamp / "chunks"


def _download_chunks_to_site_dir(site: str, volume_id: str, chunks, *, s3_client):
    outdir = _chunk_output_dir(site, volume_id, chunks)
    outdir.mkdir(parents=True, exist_ok=True)
    for chunk in chunks:
        filename = chunk.key.split("/")[-1]
        local_path = outdir / filename
        if local_path.exists():
            continue
        local_path.write_bytes(get_chunk_bytes(chunk, s3_client=s3_client))


def _required_low_chunks(chunks):
    needed = [chunk for chunk in chunks if chunk.chunk_number <= LOW_CHECKPOINT_HINT]
    if len(needed) < LOW_CHECKPOINT_HINT:
        return []
    return needed


def ingest_allowed_vcp_volume(site, volume_id, *, base_dir=None, s3_client=None, weather_session=None, parser=None, writer=None):
    _ = (parser, writer)
    if base_dir:
        fs.initialize_filesystem(base_dir)

    s3_client = s3_client or get_unsigned_s3_client()
    probe = probe_volume_vcp(site, volume_id, s3_client=s3_client, weather_session=weather_session)
    if not probe.accepted:
        return None

    chunks = list_volume_chunks(site, volume_id, s3_client=s3_client)
    needed_chunks = _required_low_chunks(chunks)
    if not needed_chunks:
        return NexradIngestResult(
            site=probe.site,
            volume_id=probe.volume_id,
            vcp=probe.vcp,
            dynamic_scan_type=None,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=0,
            complete=False,
        )

    _download_chunks_to_site_dir(site, volume_id, needed_chunks, s3_client=s3_client)
    return NexradIngestResult(
        site=probe.site,
        volume_id=probe.volume_id,
        vcp=probe.vcp,
        dynamic_scan_type=None,
        low_path=None,
        high_path=None,
        manifest_path=None,
        chunks_downloaded=len(needed_chunks),
        complete=True,
    )


def ingest_latest_allowed_vcp_scans(sites, *, max_volumes_per_site=1, base_dir=None, s3_client=None, weather_session=None):
    results = []
    s3_client = s3_client or get_unsigned_s3_client()
    for site in sites:
        volume_ids = list_recent_volume_ids(site, limit=max_volumes_per_site, s3_client=s3_client)
        for volume_id in volume_ids:
            result = ingest_allowed_vcp_volume(
                site,
                volume_id,
                base_dir=base_dir,
                s3_client=s3_client,
                weather_session=weather_session,
            )
            if result is not None:
                results.append(result)
    return results


def _build_parser():
    parser = argparse.ArgumentParser(description="Ingest VCP-gated NEXRAD Level-II chunks")
    parser.add_argument("--site", required=True)
    parser.add_argument("--volume-id")
    parser.add_argument("--base-dir")
    parser.add_argument("--max-volumes-per-site", type=int, default=1)
    return parser


def main():
    args = _build_parser().parse_args()
    if args.volume_id:
        result = ingest_allowed_vcp_volume(args.site, args.volume_id, base_dir=args.base_dir)
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

    results = ingest_latest_allowed_vcp_scans(
        [args.site],
        max_volumes_per_site=args.max_volumes_per_site,
        base_dir=args.base_dir,
    )
    if not results:
        io_manager.write_info(
            f"No accepted VCP volumes were ingested for {args.site.upper()} in this pass."
        )
        return

    for result in results:
        io_manager.write_info(
            f"Processed {result.site}/{result.volume_id} VCP-{result.vcp}: "
            f"chunks={result.chunks_downloaded}, complete={result.complete}, "
            f"low={result.low_path}, high={result.high_path}, manifest={result.manifest_path}"
        )


if __name__ == "__main__":
    main()

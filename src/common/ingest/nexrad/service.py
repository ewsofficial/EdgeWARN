import asyncio
from contextlib import asynccontextmanager
import time
from pathlib import Path
import shutil

import util.file as fs
from common.ingest.nexrad.config import ALLOWED_VCPS, CHUNKS_BUCKET, format_perf_ms
from common.ingest.nexrad.models import (
    ElevationArtifact,
    NexradIngestResult,
    ScanStreamState,
    WorkerParseResult,
)
from common.ingest.nexrad.parser import normalize_chunk_payload
from common.ingest.nexrad.s3_async import (
    async_get_chunk_bytes,
    async_list_recent_volume_ids,
    async_list_volume_chunks,
    get_unsigned_s3_client_async,
)
from common.ingest.nexrad.s3_chunks import (
    extract_volume_timestamp,
    get_chunk_bytes,
    get_unsigned_s3_client,
    list_recent_volume_ids,
    list_volume_chunks,
    required_low_chunks,
)
from common.ingest.nexrad.stream import (
    MAX_MAGIC_OVERLAP,
    VolumeState,
    detect_next_volume_offset,
    split_at_boundary,
)
from common.ingest.nexrad.vcp_probe import probe_volume_vcp
from common.ingest.nexrad.volume_builder import parse_level2_volume_file
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from common.ingest.nexrad.worker_pool import get_nexrad_pool
from common.ingest.nexrad.writer import (
    NexradLocalChunkStore,
    NexradElevationStore,
    runtime_scan_path,
    local_scan_elevations_complete,
)
from EWMRS.render.nexrad import serialize_nexrad_render_intermediate
from util.io import IOManager

io_manager = IOManager("[NEXRAD]", include_timestamps=True)

OPERATIONAL_ELEVATIONS = frozenset({"0.5", "0.9"})


def _artifact_group_key(artifact: ElevationArtifact) -> str:
    return f"{artifact.elevation}:{','.join(artifact.member_group_names)}"


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
        self.elevation_store = NexradElevationStore()

    @staticmethod
    def _volume_timestamp(volume_id: str, chunks) -> str:
        return extract_volume_timestamp(volume_id, chunks)

    def _chunk_output_dir(self, site: str, volume_id: str, chunks) -> Path:
        return self.local_chunk_store.chunk_output_dir(site, volume_id, chunks)

    def _volume_output_path(self, site: str, volume_id: str, chunks) -> Path:
        return self.local_chunk_store.volume_output_path(site, volume_id, chunks)

    def _prepare_render_manifest(self, site: str, volume_id: str, chunks) -> tuple[Path, Path | None, str | None]:
        volume_path = self._volume_output_path(site, volume_id, chunks)
        if not volume_path.exists():
            return volume_path, None, None
        scan_timestamp = self._volume_timestamp(volume_id, chunks)
        try:
            parsed_volume = parse_level2_volume_file(volume_path)
            manifest_path = serialize_nexrad_render_intermediate(
                str(site).upper(),
                volume_id,
                scan_timestamp,
                volume_path,
                parsed_volume,
            )
            return volume_path, manifest_path, scan_timestamp
        except Exception as exc:
            io_manager.write_warning(
                f"[VOL {str(site).upper()}/{volume_id}] NEXRAD render intermediate generation skipped: {exc}"
            )
            return volume_path, None, scan_timestamp

    @staticmethod
    def _remove_chunk_dir(outdir: Path):
        if outdir.exists():
            shutil.rmtree(outdir, ignore_errors=True)

    def _required_low_chunks(self, chunks):
        return required_low_chunks(chunks)

    def _download_chunks_to_site_dir(self, site: str, volume_id: str, chunks, *, s3_client):
        outdir = self._chunk_output_dir(site, volume_id, chunks)
        volume_path = self._volume_output_path(site, volume_id, chunks)
        if self.local_chunk_store.local_low_chunks_complete(site, volume_id, chunks):
            self._remove_chunk_dir(outdir)
            return

        self._remove_chunk_dir(outdir)
        volume_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = volume_path.with_suffix(f"{volume_path.suffix}.part")
        if temp_path.exists():
            temp_path.unlink()

        with temp_path.open("wb") as volume_file:
            for chunk in chunks:
                volume_file.write(self.chunk_fetcher(chunk, s3_client=s3_client))

        temp_path.replace(volume_path)
        self.local_chunk_store.prune_station_scan_dirs(site, outdir.parent.name)

    async def _download_chunks_to_site_dir_async(self, site: str, volume_id: str, chunks, *, s3_client, chunk_download_semaphore=None):
        import aiofiles

        started_at = time.perf_counter()
        outdir = self._chunk_output_dir(site, volume_id, chunks)
        volume_path = self._volume_output_path(site, volume_id, chunks)
        if self.local_chunk_store.local_low_chunks_complete(site, volume_id, chunks):
            self._remove_chunk_dir(outdir)
            return

        self._remove_chunk_dir(outdir)
        volume_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = volume_path.with_suffix(f"{volume_path.suffix}.part")
        if temp_path.exists():
            temp_path.unlink()
        semaphore = chunk_download_semaphore or asyncio.Semaphore(self.max_chunk_downloads)

        async with aiofiles.open(temp_path, "wb") as file_obj:
            for chunk in chunks:
                async with semaphore:
                    if self._stream_chunk_downloads:
                        response = await s3_client.get_object(Bucket=CHUNKS_BUCKET, Key=chunk.key)
                        body = response["Body"]
                        async for data in body.iter_chunks():
                            await file_obj.write(data)
                        if hasattr(body, "close"):
                            maybe_close = body.close()
                            if asyncio.iscoroutine(maybe_close):
                                await maybe_close
                    else:
                        payload = await self.async_chunk_fetcher(chunk, s3_client=s3_client)
                        if isinstance(payload, (bytes, bytearray)):
                            await file_obj.write(payload)
                        else:
                            body = payload["Body"] if isinstance(payload, dict) else payload
                            if hasattr(body, "iter_chunks"):
                                async for data in body.iter_chunks():
                                    await file_obj.write(data)
                                if hasattr(body, "close"):
                                    maybe_close = body.close()
                                    if asyncio.iscoroutine(maybe_close):
                                        await maybe_close
                            else:
                                data = await body.read()
                                await file_obj.write(data)

        temp_path.replace(volume_path)
        elapsed_ms = format_perf_ms(started_at)
        io_manager.write_perf(
            f"[VOL {str(site).upper()}/{volume_id}] async_chunk_download: {elapsed_ms:.2f}ms "
            f"(chunks={len(chunks)})"
        )

    def _stream_ingest_volume(
        self,
        site: str,
        volume_id: str,
        chunks,
        *,
        s3_client,
        base_dir=None,
    ):
        """Overlap-safe stream ingest: fetches chunks, detects boundaries, runs worker parse."""
        if base_dir:
            fs.initialize_filesystem(base_dir)

        scan_timestamp = self._volume_timestamp(volume_id, chunks)
        site_upper = str(site).upper()
        sorted_chunks = sorted(chunks, key=lambda c: (c.chunk_number, c.chunk_type))

        runtime_path = runtime_scan_path(site_upper, volume_id)
        current_state = ScanStreamState(
            index=0,
            volume_id=volume_id,
            scan_timestamp=scan_timestamp,
            file_path=str(runtime_path),
        )

        previous_tail = b""
        stream_has_started = False
        seen_elevation_keys: set[str] = set()
        first_elevation_timestamp: str | None = None

        with open(runtime_path, "wb") as scan_file:
            for chunk in sorted_chunks:
                payload = self.chunk_fetcher(chunk, s3_client=s3_client)
                if not payload:
                    continue

                found_boundary, boundary_offset = detect_next_volume_offset(
                    previous_tail, payload, stream_has_started,
                )

                if found_boundary and boundary_offset > 0:
                    before, after = split_at_boundary(payload, boundary_offset)
                    if before:
                        normalized_before = normalize_chunk_payload(
                            before,
                            first_chunk_of_volume=current_state.bytes_written == 0,
                        )
                        scan_file.write(normalized_before)
                        current_state.bytes_written += len(normalized_before)

                    scan_file.flush()

                    current_state.finalized = True
                    first_elevation_timestamp = self._run_worker_parse(
                        current_state,
                        site_upper,
                        volume_id,
                        scan_timestamp,
                        seen_elevation_keys,
                        first_elevation_timestamp,
                        base_dir=base_dir,
                    )
                    current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0

                    current_state = ScanStreamState(
                        index=current_state.index + 1,
                        volume_id=volume_id,
                        scan_timestamp=scan_timestamp,
                        file_path=str(runtime_path),
                    )
                    scan_file.seek(0)
                    scan_file.truncate()

                    if after:
                        normalized_after = normalize_chunk_payload(after, first_chunk_of_volume=True)
                        scan_file.write(normalized_after)
                        current_state.bytes_written = len(normalized_after)
                        scan_file.flush()
                        first_elevation_timestamp = self._run_worker_parse(
                            current_state,
                            site_upper,
                            volume_id,
                            scan_timestamp,
                            seen_elevation_keys,
                            first_elevation_timestamp,
                            base_dir=base_dir,
                        )
                        current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0

                    stream_has_started = True
                    previous_tail = after[-MAX_MAGIC_OVERLAP:] if len(after) >= MAX_MAGIC_OVERLAP else after
                    continue

                normalized_payload = normalize_chunk_payload(
                    payload,
                    first_chunk_of_volume=current_state.bytes_written == 0,
                )
                scan_file.write(normalized_payload)
                current_state.bytes_written += len(normalized_payload)
                scan_file.flush()
                first_elevation_timestamp = self._run_worker_parse(
                    current_state,
                    site_upper,
                    volume_id,
                    scan_timestamp,
                    seen_elevation_keys,
                    first_elevation_timestamp,
                    base_dir=base_dir,
                )
                current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0
                stream_has_started = True

                tail_candidate = payload[-MAX_MAGIC_OVERLAP:] if len(payload) >= MAX_MAGIC_OVERLAP else payload
                previous_tail = tail_candidate

            if current_state.bytes_written > 0:
                scan_file.flush()
                first_elevation_timestamp = self._run_worker_parse(
                    current_state,
                    site_upper,
                    volume_id,
                    scan_timestamp,
                    seen_elevation_keys,
                    first_elevation_timestamp,
                    base_dir=base_dir,
                )

        if runtime_path.exists():
            runtime_path.unlink(missing_ok=True)

        self.local_chunk_store.prune_station_scan_dirs(site_upper, scan_timestamp)

        required_elevations = [
            (elev, first_elevation_timestamp or scan_timestamp)
            for elev in OPERATIONAL_ELEVATIONS
        ]
        complete = local_scan_elevations_complete(site_upper, required_elevations)

        return NexradIngestResult(
            site=site_upper,
            volume_id=volume_id,
            vcp=0,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=scan_timestamp,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=len(sorted_chunks),
            complete=complete,
        )

    def _run_worker_parse(
        self,
        state: ScanStreamState,
        site: str,
        volume_id: str,
        scan_timestamp: str | None,
        seen_elevation_keys: set[str],
        first_elevation_timestamp: str | None,
        *,
        base_dir=None,
    ) -> str | None:
        """Run the worker parse/export via the shared process pool.

        Returns the first elevation timestamp seen, or the existing value.
        """
        if not state.file_path or not Path(state.file_path).exists():
            return first_elevation_timestamp

        output_root = fs.NEXRAD_LEVEL2_DIR if base_dir is None else Path(base_dir) / "data" / "NEXRAD_Level2"
        pool = get_nexrad_pool()

        try:
            future = pool.submit(
                volume_path=state.file_path,
                output_root=str(output_root),
                site=site,
                volume_id=volume_id,
                scan_timestamp=scan_timestamp,
                seen_keys=seen_elevation_keys,
                trim_buffer=True,
            )
            payload = future.result()
            result = WorkerParseResult(
                visible_sweeps=payload.visible_sweeps,
                saved_sweeps=payload.saved_sweeps,
                saved_elevations=payload.saved_elevations,
                parse_error=payload.parse_error,
                child_rss_kb=payload.child_rss_kb,
            )

            if result.parse_error:
                state.parse_errors.append(result.parse_error)
                io_manager.write_warning(
                    f"[VOL {site}/{volume_id}] worker parse error: {result.parse_error}"
                )

            for artifact in result.saved_elevations:
                seen_elevation_keys.add(_artifact_group_key(artifact))
                if first_elevation_timestamp is None and artifact.elevation_timestamp:
                    first_elevation_timestamp = artifact.elevation_timestamp

            if result.saved_sweeps:
                io_manager.write_info(
                    f"[VOL {site}/{volume_id}] worker exported {len(result.saved_sweeps)} sweeps, "
                    f"{len(result.saved_elevations)} elevations "
                    f"(visible={result.visible_sweeps}, rss_kb={result.child_rss_kb})"
                )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            message = stderr or stdout or str(exc)
            state.parse_errors.append(message)
            io_manager.write_warning(
                f"[VOL {site}/{volume_id}] worker parse failed: {message}"
            )
        except Exception as exc:
            state.parse_errors.append(str(exc))
            io_manager.write_warning(
                f"[VOL {site}/{volume_id}] worker parse failed: {exc}"
            )

        return first_elevation_timestamp

    async def _stream_ingest_volume_async(
        self,
        site: str,
        volume_id: str,
        chunks,
        *,
        s3_client,
        base_dir=None,
        chunk_download_semaphore=None,
    ):
        """Async variant: downloads chunks first, then runs stream ingest."""
        if base_dir:
            fs.initialize_filesystem(base_dir)

        scan_timestamp = self._volume_timestamp(volume_id, chunks)
        site_upper = str(site).upper()
        sorted_chunks = sorted(chunks, key=lambda c: (c.chunk_number, c.chunk_type))

        runtime_path = runtime_scan_path(site_upper, volume_id)
        runtime_path.parent.mkdir(parents=True, exist_ok=True)

        current_state = ScanStreamState(
            index=0,
            volume_id=volume_id,
            scan_timestamp=scan_timestamp,
            file_path=str(runtime_path),
        )

        previous_tail = b""
        stream_has_started = False
        seen_elevation_keys: set[str] = set()
        first_elevation_timestamp: str | None = None

        import aiofiles

        async with aiofiles.open(runtime_path, "wb") as scan_file:
            for chunk in sorted_chunks:
                if self._stream_chunk_downloads:
                    response = await s3_client.get_object(Bucket=CHUNKS_BUCKET, Key=chunk.key)
                    body = response["Body"]
                    payload_chunks = []
                    async for data in body.iter_chunks():
                        payload_chunks.append(data)
                    payload = b"".join(payload_chunks)
                    del payload_chunks
                    if hasattr(body, "close"):
                        maybe_close = body.close()
                        if asyncio.iscoroutine(maybe_close):
                            await maybe_close
                else:
                    payload = await self.async_chunk_fetcher(chunk, s3_client=s3_client)
                    if not isinstance(payload, (bytes, bytearray)):
                        body = payload["Body"] if isinstance(payload, dict) else payload
                        if hasattr(body, "iter_chunks"):
                            payload_chunks = []
                            async for data in body.iter_chunks():
                                payload_chunks.append(data)
                            payload = b"".join(payload_chunks)
                            del payload_chunks
                            if hasattr(body, "close"):
                                maybe_close = body.close()
                                if asyncio.iscoroutine(maybe_close):
                                    await maybe_close
                        else:
                            data = await body.read()
                            payload = data

                if not payload:
                    continue

                found_boundary, boundary_offset = detect_next_volume_offset(
                    previous_tail, payload, stream_has_started,
                )

                if found_boundary and boundary_offset > 0:
                    before, after = split_at_boundary(payload, boundary_offset)
                    if before:
                        normalized_before = normalize_chunk_payload(
                            before,
                            first_chunk_of_volume=current_state.bytes_written == 0,
                        )
                        await scan_file.write(normalized_before)
                        current_state.bytes_written += len(normalized_before)

                    await scan_file.flush()

                    current_state.finalized = True
                    first_elevation_timestamp = await asyncio.to_thread(
                        self._run_worker_parse,
                        current_state,
                        site_upper,
                        volume_id,
                        scan_timestamp,
                        seen_elevation_keys,
                        first_elevation_timestamp,
                        base_dir=base_dir,
                    )
                    current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0

                    current_state = ScanStreamState(
                        index=current_state.index + 1,
                        volume_id=volume_id,
                        scan_timestamp=scan_timestamp,
                        file_path=str(runtime_path),
                    )
                    await scan_file.seek(0)
                    await scan_file.truncate()

                    if after:
                        normalized_after = normalize_chunk_payload(after, first_chunk_of_volume=True)
                        await scan_file.write(normalized_after)
                        current_state.bytes_written = len(normalized_after)
                        await scan_file.flush()
                        first_elevation_timestamp = await asyncio.to_thread(
                            self._run_worker_parse,
                            current_state,
                            site_upper,
                            volume_id,
                            scan_timestamp,
                            seen_elevation_keys,
                            first_elevation_timestamp,
                            base_dir=base_dir,
                        )
                        current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0

                    stream_has_started = True
                    previous_tail = after[-MAX_MAGIC_OVERLAP:] if len(after) >= MAX_MAGIC_OVERLAP else after
                    del payload
                    continue

                normalized_payload = normalize_chunk_payload(
                    payload,
                    first_chunk_of_volume=current_state.bytes_written == 0,
                )
                await scan_file.write(normalized_payload)
                current_state.bytes_written += len(normalized_payload)
                await scan_file.flush()
                first_elevation_timestamp = await asyncio.to_thread(
                    self._run_worker_parse,
                    current_state,
                    site_upper,
                    volume_id,
                    scan_timestamp,
                    seen_elevation_keys,
                    first_elevation_timestamp,
                    base_dir=base_dir,
                )
                current_state.bytes_written = Path(current_state.file_path).stat().st_size if Path(current_state.file_path).exists() else 0
                stream_has_started = True

                tail_candidate = payload[-MAX_MAGIC_OVERLAP:] if len(payload) >= MAX_MAGIC_OVERLAP else payload
                previous_tail = tail_candidate
                del payload

            if current_state.bytes_written > 0:
                await scan_file.flush()
                first_elevation_timestamp = await asyncio.to_thread(
                    self._run_worker_parse,
                    current_state,
                    site_upper,
                    volume_id,
                    scan_timestamp,
                    seen_elevation_keys,
                    first_elevation_timestamp,
                    base_dir=base_dir,
                )

        if runtime_path.exists():
            runtime_path.unlink(missing_ok=True)

        self.local_chunk_store.prune_station_scan_dirs(site_upper, scan_timestamp)

        required_elevations = [
            (elev, first_elevation_timestamp or scan_timestamp)
            for elev in OPERATIONAL_ELEVATIONS
        ]
        complete = local_scan_elevations_complete(site_upper, required_elevations)

        return NexradIngestResult(
            site=site_upper,
            volume_id=volume_id,
            vcp=0,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=scan_timestamp,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=len(sorted_chunks),
            complete=complete,
        )

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

        if station_vcp is None:
            probe = self.volume_prober(
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

        chunks = self.chunk_lister(site, volume_id, s3_client=s3_client)
        needed_chunks = self._required_low_chunks(chunks)
        if not needed_chunks:
            return NexradIngestResult(
                site=probe_site,
                volume_id=probe_volume_id,
                vcp=probe_vcp,
                dynamic_scan_type=None,
                volume_path=None,
                scan_timestamp=None,
                low_path=None,
                high_path=None,
                manifest_path=None,
                chunks_downloaded=0,
                complete=False,
            )

        result = self._stream_ingest_volume(
            probe_site,
            probe_volume_id,
            needed_chunks,
            s3_client=s3_client,
            base_dir=base_dir,
        )
        return NexradIngestResult(
            site=result.site,
            volume_id=result.volume_id,
            vcp=probe_vcp,
            dynamic_scan_type=None,
            volume_path=None,
            scan_timestamp=result.scan_timestamp,
            low_path=None,
            high_path=None,
            manifest_path=None,
            chunks_downloaded=result.chunks_downloaded,
            complete=result.complete,
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
            list_elapsed_ms = format_perf_ms(list_started_at)
            needed_chunks = self._required_low_chunks(chunks)
            io_manager.write_perf(
                f"[VOL {probe_site}/{probe_volume_id}] chunk_list: {list_elapsed_ms:.2f}ms "
                f"(listed={len(chunks)}, needed={len(needed_chunks)})"
            )
            if not needed_chunks:
                total_elapsed_ms = format_perf_ms(total_started_at)
                io_manager.write_perf(
                    f"[VOL {probe_site}/{probe_volume_id}] total_async_ingest: {total_elapsed_ms:.2f}ms "
                    f"(accepted=True, complete=False, chunks_downloaded=0)"
                )
                return NexradIngestResult(
                    site=probe_site,
                    volume_id=probe_volume_id,
                    vcp=probe_vcp,
                    dynamic_scan_type=None,
                    volume_path=None,
                    scan_timestamp=None,
                    low_path=None,
                    high_path=None,
                    manifest_path=None,
                    chunks_downloaded=0,
                    complete=False,
                )

            result = await self._stream_ingest_volume_async(
                probe_site,
                probe_volume_id,
                needed_chunks,
                s3_client=active_s3_client,
                base_dir=base_dir,
                chunk_download_semaphore=chunk_download_semaphore or self._shared_chunk_download_semaphore,
            )
            total_elapsed_ms = format_perf_ms(total_started_at)
            io_manager.write_perf(
                f"[VOL {probe_site}/{probe_volume_id}] total_async_ingest: {total_elapsed_ms:.2f}ms "
                f"(accepted=True, complete={result.complete}, chunks_downloaded={result.chunks_downloaded})"
            )
            return NexradIngestResult(
                site=result.site,
                volume_id=result.volume_id,
                vcp=probe_vcp,
                dynamic_scan_type=None,
                volume_path=None,
                scan_timestamp=result.scan_timestamp,
                low_path=None,
                high_path=None,
                manifest_path=None,
                chunks_downloaded=result.chunks_downloaded,
                complete=result.complete,
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
                f"[RUN] station_catalog_fetch: {format_perf_ms(station_started_at):.2f}ms "
                f"(stations={len(station_vcps)})"
            )

        filter_started_at = time.perf_counter()
        filtered_sites = [
            site
            for site in sites
            if site.startswith("K") and station_vcps.get(site) is not None and station_vcps[site].vcp in ALLOWED_VCPS
        ]
        io_manager.write_perf(
            f"[RUN] site_filter: {format_perf_ms(filter_started_at):.2f}ms "
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
                            f"[SITE {site}] recent_volume_list: {format_perf_ms(started_at):.2f}ms "
                            f"(volumes={len(volume_ids)}, limit={max_volumes_per_site})"
                        )
                        return site, volume_ids

                volume_discovery_started_at = time.perf_counter()
                listed_sites = await asyncio.gather(*(_list_site_volumes(site) for site in filtered_sites), return_exceptions=True)
                io_manager.write_perf(
                    f"[RUN] site_volume_discovery: {format_perf_ms(volume_discovery_started_at):.2f}ms "
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
                    f"[RUN] volume_ingest_batch: {format_perf_ms(volume_ingest_started_at):.2f}ms "
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
        total_elapsed_ms = format_perf_ms(total_started_at)
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

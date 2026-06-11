"""In-process NEXRAD worker pool with deferred imports.

Uses a ProcessPoolExecutor whose workers defer heavy imports (numpy, xarray,
scipy, pandas, dask, netcdf4, botocore) until parse_and_export() actually
needs them. This reduces the baseline RSS of idle workers from ~180-190 MB
to ~120-150 MB while still benefiting from CoW for active workers.

Usage:
    from common.ingest.nexrad.worker_pool import get_nexrad_pool

    pool = get_nexrad_pool(max_workers=4)
    future = pool.submit(volume_path, output_root, site, volume_id,
                         scan_timestamp, seen_keys, trim_buffer)
    result = future.result()
"""

from __future__ import annotations

import ctypes
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, Future
from pathlib import Path
from typing import Any

from common.ingest.nexrad.models import ElevationArtifact, WorkerParseResult


_POOL: ProcessPoolExecutor | None = None
_POOL_SIZE: int = 0
_VOLUME_COUNT: int = 0


def _initialize_worker_name() -> None:
    process = multiprocessing.current_process()
    name = f"NEXRAD-Parser-{process.pid}"
    process.name = name
    try:
        libc = ctypes.CDLL(None)
        pr_set_name = 15
        os_name = f"NXParse-{process.pid}"
        encoded = os_name.encode("utf-8")[:15]
        libc.prctl(pr_set_name, ctypes.c_char_p(encoded), 0, 0, 0)
    except Exception:
        pass


def _worker_parse(
    volume_path: str,
    output_root: str,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    download_started_at: str | None,
    seen_keys: dict[str, str | None],
    trim_buffer: bool,
) -> dict[str, Any]:
    """Run parse_and_export inside a pool worker.

    Returns a plain dict (serializable) rather than a dataclass so the
    result can cross the process boundary without pickle issues.
    """
    from common.ingest.nexrad.worker import parse_and_export

    result = parse_and_export(
        volume_path=volume_path,
        output_root=output_root,
        site=site,
        volume_id=volume_id,
        scan_timestamp=scan_timestamp,
        download_started_at=download_started_at,
        seen_elevation_keys=seen_keys,
        trim_buffer=trim_buffer,
    )

    return {
        "visible_sweeps": result.visible_sweeps,
        "saved_sweep_count": result.saved_sweep_count,
        "saved_elevations": [
            {
                "scan_timestamp": a.scan_timestamp,
                "download_started_at": a.download_started_at,
                "elevation": a.elevation,
                "elevation_timestamp": a.elevation_timestamp,
                "member_group_names": a.member_group_names,
            }
            for a in result.saved_elevations
        ],
        "parse_error": result.parse_error,
        "child_rss_kb": result.child_rss_kb,
        "buffer_trimmed": result.buffer_trimmed,
        "runtime_size": result.runtime_size,
    }


def _dict_to_result(payload: dict[str, Any]) -> WorkerParseResult:
    return WorkerParseResult(
        visible_sweeps=payload.get("visible_sweeps", 0),
        saved_sweep_count=int(payload.get("saved_sweep_count", 0) or 0),
        saved_elevations=[
            ElevationArtifact(
                site="",
                volume_id="",
                volume_timestamp=None,
                scan_timestamp=a.get("scan_timestamp"),
                elevation=a["elevation"],
                elevation_timestamp=a.get("elevation_timestamp"),
                first_sweep_index=0,
                last_sweep_index=0,
                first_sweep_timestamp=None,
                last_sweep_timestamp=None,
                member_group_names=list(a.get("member_group_names") or []),
                member_sweeps=[],
                waveforms_present=set(),
                supplemental=False,
                download_started_at=a.get("download_started_at"),
                netcdf_path=None,
                ar2v_path=None,
            )
            for a in payload.get("saved_elevations") or []
        ],
        parse_error=payload.get("parse_error"),
        child_rss_kb=payload.get("child_rss_kb"),
        buffer_trimmed=bool(payload.get("buffer_trimmed", False)),
        runtime_size=payload.get("runtime_size"),
    )


class NexradWorkerPool:
    """Thin wrapper around ProcessPoolExecutor for NEXRAD parse work."""

    def __init__(self, max_workers: int = 4):
        self._executor = ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_initialize_worker_name,
        )
        self._max_workers = max_workers

    def submit(
        self,
        volume_path: str | Path,
        output_root: str | Path,
        site: str,
        volume_id: str,
        scan_timestamp: str | None,
        download_started_at: str | None,
        seen_keys: dict[str, str | None],
        trim_buffer: bool = False,
    ) -> Future[WorkerParseResult]:
        future = self._executor.submit(
            _worker_parse,
            str(volume_path),
            str(output_root),
            str(site).upper(),
            str(volume_id),
            scan_timestamp,
            download_started_at,
            seen_keys,
            trim_buffer,
        )

        wrapped: Future[WorkerParseResult] = Future()

        def _callback(f: Future) -> None:
            try:
                wrapped.set_result(_dict_to_result(f.result()))
            except Exception as exc:
                wrapped.set_exception(exc)

        future.add_done_callback(_callback)
        return wrapped

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    @property
    def max_workers(self) -> int:
        return self._max_workers


def get_nexrad_pool(max_workers: int | None = None) -> NexradWorkerPool:
    """Return a singleton pool, creating one if needed."""
    global _POOL, _POOL_SIZE, _VOLUME_COUNT

    target = max_workers or int(os.environ.get("NEXRAD_WORKER_POOL_SIZE", "4"))

    if _POOL is None or _POOL_SIZE != target:
        if _POOL is not None:
            _POOL.shutdown(wait=True)
        _POOL = NexradWorkerPool(max_workers=target)
        _POOL_SIZE = target
        _VOLUME_COUNT = 0

    return _POOL


def record_volume_and_maybe_recycle(max_workers: int | None = None) -> None:
    """Recycle long-lived workers after a bounded number of completed volumes.

    This caps allocator fragmentation and import/cache buildup inside pool
    workers during long realtime runs. The threshold is configurable via
    ``NEXRAD_WORKER_RECYCLE_INTERVAL`` and defaults to 24 completed volumes.
    Set the value to ``0`` to disable recycling.
    """
    global _POOL, _POOL_SIZE, _VOLUME_COUNT

    recycle_interval = int(os.environ.get("NEXRAD_WORKER_RECYCLE_INTERVAL", "24"))
    if recycle_interval <= 0:
        return

    _VOLUME_COUNT += 1
    if _POOL is None or _VOLUME_COUNT < recycle_interval:
        return

    _POOL.shutdown(wait=True)
    _POOL = None
    _POOL_SIZE = 0
    _VOLUME_COUNT = 0


def shutdown_nexrad_pool(wait: bool = True) -> None:
    global _POOL, _POOL_SIZE, _VOLUME_COUNT
    if _POOL is not None:
        _POOL.shutdown(wait=wait)
        _POOL = None
        _POOL_SIZE = 0
    _VOLUME_COUNT = 0

"""In-process NEXRAD worker pool with deferred imports.

Uses a ProcessPoolExecutor whose workers defer heavy imports (numpy, xarray,
scipy, pandas, dask, netcdf4, botocore) until parse_and_export() actually
needs them. This reduces the baseline RSS of idle workers from ~180-190 MB
to ~120-150 MB while still benefiting from CoW for active workers.

Usage:
    from common.ingest.nexrad.worker_pool import get_nexrad_pool, submit_parse

    pool = get_nexrad_pool(max_workers=4)
    future = pool.submit_parse(volume_path, output_root, site, volume_id,
                               scan_timestamp, seen_keys, trim_buffer, parse_offset)
    result = future.result()
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import util.file as fs
from common.ingest.nexrad.models import ElevationArtifact, WorkerParseResult


_POOL: ProcessPoolExecutor | None = None
_POOL_SIZE: int = 0
_VOLUME_COUNT: int = 0


def _worker_parse(
    volume_path: str,
    output_root: str,
    site: str,
    volume_id: str,
    scan_timestamp: str | None,
    seen_keys: dict[str, str | None],
    trim_buffer: bool,
    parse_offset: int,
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
        seen_elevation_keys=seen_keys,
        trim_buffer=trim_buffer,
        parse_offset=parse_offset,
    )

    return {
        "visible_sweeps": result.visible_sweeps,
        "saved_sweep_count": result.saved_sweep_count,
        "saved_elevations": [
            {
                "scan_timestamp": a.scan_timestamp,
                "elevation": a.elevation,
                "elevation_timestamp": a.elevation_timestamp,
                "member_group_names": a.member_group_names,
            }
            for a in result.saved_elevations
        ],
        "parse_error": result.parse_error,
        "child_rss_kb": result.child_rss_kb,
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
                netcdf_path=None,
                ar2v_path=None,
            )
            for a in payload.get("saved_elevations") or []
        ],
        parse_error=payload.get("parse_error"),
        child_rss_kb=payload.get("child_rss_kb"),
    )


class NexradWorkerPool:
    """Thin wrapper around ProcessPoolExecutor for NEXRAD parse work."""

    def __init__(self, max_workers: int = 4):
        self._executor = ProcessPoolExecutor(
            max_workers=max_workers,
        )
        self._max_workers = max_workers

    def submit(
        self,
        volume_path: str | Path,
        output_root: str | Path,
        site: str,
        volume_id: str,
        scan_timestamp: str | None,
        seen_keys: dict[str, str | None],
        trim_buffer: bool = False,
        parse_offset: int = 0,
    ) -> Future[WorkerParseResult]:
        future = self._executor.submit(
            _worker_parse,
            str(volume_path),
            str(output_root),
            str(site).upper(),
            str(volume_id),
            scan_timestamp,
            seen_keys,
            trim_buffer,
            parse_offset,
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
    global _POOL, _POOL_SIZE

    target = max_workers or int(os.environ.get("NEXRAD_WORKER_POOL_SIZE", "4"))

    if _POOL is None or _POOL_SIZE != target:
        if _POOL is not None:
            _POOL.shutdown(wait=True)
        _POOL = NexradWorkerPool(max_workers=target)
        _POOL_SIZE = target

    return _POOL


def record_volume_and_maybe_recycle(max_workers: int | None = None) -> None:
    """No-op placeholder. Worker recycling was removed because forking new
    workers from a grown parent process produces children with higher RSS
    baselines than keeping the existing workers and relying on malloc_trim.
    """
    pass


def shutdown_nexrad_pool(wait: bool = True) -> None:
    global _POOL, _POOL_SIZE
    if _POOL is not None:
        _POOL.shutdown(wait=wait)
        _POOL = None
        _POOL_SIZE = 0

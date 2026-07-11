"""Shared staged-ingest coordination for tandem EdgeWARN and EWMRS execution."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import common.ingest.mrms.main as mrms_ingest
from common.ingest.mrms.downloader import (
    DownloadBatchResult,
    download_all_goes_files,
    download_all_goes_files_async,
)
from common.ingest.synoptic.main import download_rap, download_rap_async


LogFunc = Callable[[str], None]
StateCallback = Callable[["CycleState"], None]


@dataclass
class CycleState:
    """Tracks staged readiness for a single shared ingest cycle."""

    timestamp: datetime
    detection_inputs_ready: bool = False
    ewmrs_mrms_inputs_ready: bool = False
    ewmrs_goes_inputs_ready: bool = False
    mrms_integration_inputs_ready: bool = False
    rap_inputs_ready: bool = False
    edgewarn_integration_inputs_ready: bool = False
    edgewarn_generated_file: str | None = None
    errors: dict[str, str] = field(default_factory=dict)


async def _safe_ingest(
    task_name: str,
    log: LogFunc,
    async_func,
    sync_fallback,
    *args,
    require_result: bool = False,
):
    try:
        result = await async_func(*args)
        if require_result and not result:
            raise RuntimeError(f"{task_name} ingestion did not return a staged file path")
        log(f"INFO: Async {task_name} ingestion successful")
        return result
    except Exception as exc:
        log(f"WARN: Async {task_name} ingestion failed: {exc}. Falling back to sync.")
        try:
            # Fallback is exceptional and phase-local; run it synchronously so
            # a cycle cannot retain an executor thread during teardown.
            result = sync_fallback(*args)
            if inspect.isawaitable(result):
                result = await result
            if require_result and not result:
                raise RuntimeError(f"{task_name} sync fallback did not return a staged file path")
            log(f"INFO: Sync fallback for {task_name} successful")
            return result
        except Exception as fallback_exc:
            log(f"ERROR: Both async and sync ingestion failed for {task_name}: {fallback_exc}")
            return None


async def _run_rap_uint16_conversion(rap_path, dt: datetime, log: LogFunc) -> bool:
    try:
        from EWMRS.pipeline import run_rap_uint16_pipeline

        # The worker phases have already been released before this derived
        # artifact starts, so keeping it in this coordinator avoids retaining
        # a default-executor thread across a cycle shutdown.
        results = run_rap_uint16_pipeline(rap_path, dt)
        successful_layers = sum(1 for path in results.values() if path is not None)
        log(f"INFO: EWMRS RAP Uint16Array conversion completed: {successful_layers}/{len(results)} layers succeeded")
        return True
    except Exception as exc:
        log(f"WARN: EWMRS RAP Uint16Array conversion failed: {exc}")
        return False


async def run_tandem_ingest_cycle(
    dt: datetime,
    log: LogFunc,
    *,
    max_entries: int = 10,
    include_goes: bool = True,
    include_ewmrs: bool = True,
    on_detection_ready: Optional[StateCallback] = None,
    on_ewmrs_mrms_ready: Optional[StateCallback] = None,
    on_ewmrs_goes_ready: Optional[StateCallback] = None,
    on_edgewarn_integration_ready: Optional[StateCallback] = None,
    on_base_integration_ready: Optional[StateCallback] = None,
) -> CycleState:
    """Run staged shared ingest and emit readiness transitions.

    The ordering intentionally preserves the low-latency EdgeWARN fast path:
    detection files become available first, then EWMRS-ready MRMS data, then
    EdgeWARN integration readiness once RAP and GOES finish.
    """

    state = CycleState(timestamp=dt)
    log(f"INFO: Starting shared ingest cycle for timestamp {dt}")

    detection_task = asyncio.create_task(
        _safe_ingest(
            "MRMS Detection",
            log,
            mrms_ingest.download_detection_files_async,
            mrms_ingest.download_detection_files,
            dt,
            max_entries,
        )
    )
    mrms_integration_task = asyncio.create_task(
        _safe_ingest(
            "MRMS Integration",
            log,
            mrms_ingest.download_integration_files_async,
            mrms_ingest.download_integration_files,
            dt,
            max_entries,
        )
    )
    goes_task = None
    if include_goes:
        goes_task = asyncio.create_task(
            _safe_ingest(
                "GOES",
                log,
                download_all_goes_files_async,
                download_all_goes_files,
                dt,
                max_entries,
                3,
            )
        )
    rap_task = asyncio.create_task(
        _safe_ingest(
            "RAP",
            log,
            download_rap_async,
            download_rap,
            dt,
            require_result=True,
        )
    )

    detection_result = await detection_task
    detection_ok = _batch_succeeded(detection_result)
    state.detection_inputs_ready = detection_ok
    if not detection_ok:
        state.errors["detection_ingest"] = "Detection inputs unavailable"
    if on_detection_ready is not None:
        on_detection_ready(state)

    integration_result = await mrms_integration_task
    mrms_integration_ok = _batch_succeeded(integration_result)
    state.mrms_integration_inputs_ready = mrms_integration_ok
    if not mrms_integration_ok:
        state.errors["mrms_integration_ingest"] = "MRMS integration inputs unavailable"

    state.ewmrs_mrms_inputs_ready = include_ewmrs and detection_ok and mrms_integration_ok
    if include_ewmrs and not state.ewmrs_mrms_inputs_ready:
        state.errors.setdefault(
            "ewmrs_ingest",
            "EWMRS render inputs unavailable from staged MRMS ingest",
        )
    if on_ewmrs_mrms_ready is not None:
        on_ewmrs_mrms_ready(state)

    # RAP is a source input; publish integration's non-GLM prerequisites before
    # the optional Uint16 conversion below.
    rap_path = await rap_task
    rap_ok = bool(rap_path)
    state.rap_inputs_ready = rap_ok

    if not rap_ok:
        state.errors["rap_ingest"] = "RAP inputs unavailable"
    state.edgewarn_integration_inputs_ready = detection_ok and mrms_integration_ok and rap_ok
    if on_base_integration_ready is not None:
        on_base_integration_ready(state)

    if goes_task is not None:
        # GOES wrappers historically return ``None`` on success, whereas a
        # false value is the explicit failure signal from ``_safe_ingest``.
        goes_ok = (await goes_task) is not False
    else:
        goes_ok = False
        log("INFO: GOES ingest is decoupled from this cycle; integration readiness does not wait for GOES availability")

    if not goes_ok:
        state.errors["goes_ingest"] = "GOES inputs unavailable"

    # This derived EWMRS artifact intentionally starts only after the raw-RAP
    # transition was published above; it never delays a worker release.
    if rap_ok and include_ewmrs and not await _run_rap_uint16_conversion(rap_path, dt, log):
        state.errors["ewmrs_rap_uint16"] = "EWMRS RAP Uint16Array conversion failed"

    state.ewmrs_goes_inputs_ready = include_ewmrs and state.ewmrs_mrms_inputs_ready and goes_ok
    if include_ewmrs and not state.ewmrs_goes_inputs_ready:
        state.errors.setdefault(
            "ewmrs_goes_ingest",
            "EWMRS GOES inputs unavailable",
        )
    if on_ewmrs_goes_ready is not None:
        on_ewmrs_goes_ready(state)

    state.edgewarn_integration_inputs_ready = (
        state.edgewarn_integration_inputs_ready and goes_ok
    )
    if not state.edgewarn_integration_inputs_ready:
        state.errors.setdefault(
            "edgewarn_integration_ingest",
            "EdgeWARN integration inputs unavailable",
        )
    if on_edgewarn_integration_ready is not None:
        on_edgewarn_integration_ready(state)

    log(f"INFO: Shared ingest cycle finished for timestamp {dt}")
    return state


def _batch_succeeded(result) -> bool:
    """A phase batch is ready only when every requested MRMS product staged.

    ``None`` remains accepted for legacy/custom ingest implementations that
    predate batch results; production wrappers return ``DownloadBatchResult``.
    """
    if isinstance(result, DownloadBatchResult):
        return bool(result.attempted) and not result.failed and set(result.downloaded) == set(result.attempted)
    return result is None or bool(result)

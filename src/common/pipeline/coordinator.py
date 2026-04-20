"""Shared staged-ingest coordination for tandem EdgeWARN and EWMRS execution."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import common.ingest.mrms.main as mrms_ingest
from common.ingest.mrms.downloader import (
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
    edgewarn_integration_inputs_ready: bool = False
    edgewarn_generated_file: str | None = None
    errors: dict[str, str] = field(default_factory=dict)


async def _safe_ingest(
    task_name: str,
    log: LogFunc,
    async_func,
    sync_fallback,
    *args,
):
    try:
        await async_func(*args)
        log(f"INFO: Async {task_name} ingestion successful")
        return True
    except Exception as exc:
        log(f"WARN: Async {task_name} ingestion failed: {exc}. Falling back to sync.")
        try:
            await asyncio.to_thread(sync_fallback, *args)
            log(f"INFO: Sync fallback for {task_name} successful")
            return True
        except Exception as fallback_exc:
            log(f"ERROR: Both async and sync ingestion failed for {task_name}: {fallback_exc}")
            return False


async def run_tandem_ingest_cycle(
    dt: datetime,
    log: LogFunc,
    *,
    max_entries: int = 10,
    include_goes: bool = True,
    on_detection_ready: Optional[StateCallback] = None,
    on_ewmrs_mrms_ready: Optional[StateCallback] = None,
    on_ewmrs_goes_ready: Optional[StateCallback] = None,
    on_edgewarn_integration_ready: Optional[StateCallback] = None,
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
            mrms_ingest.download_all_files,
            dt,
            max_entries,
        )
    )
    mrms_integration_task = asyncio.create_task(
        _safe_ingest(
            "MRMS Integration",
            log,
            mrms_ingest.download_integration_files_async,
            mrms_ingest.download_all_files,
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
        )
    )

    detection_ok = await detection_task
    state.detection_inputs_ready = detection_ok
    if not detection_ok:
        state.errors["detection_ingest"] = "Detection inputs unavailable"
    if on_detection_ready is not None:
        on_detection_ready(state)

    mrms_integration_ok = await mrms_integration_task
    if not mrms_integration_ok:
        state.errors["mrms_integration_ingest"] = "MRMS integration inputs unavailable"

    state.ewmrs_mrms_inputs_ready = detection_ok and mrms_integration_ok
    if not state.ewmrs_mrms_inputs_ready:
        state.errors.setdefault(
            "ewmrs_ingest",
            "EWMRS render inputs unavailable from staged MRMS ingest",
        )
    if on_ewmrs_mrms_ready is not None:
        on_ewmrs_mrms_ready(state)

    if goes_task is not None:
        goes_ok, rap_ok = await asyncio.gather(goes_task, rap_task)
    else:
        goes_ok = False
        rap_ok = await rap_task
        log("INFO: GOES ingest is decoupled from this cycle; integration readiness does not wait for GOES")

    if not goes_ok:
        state.errors["goes_ingest"] = "GOES inputs unavailable"
    if not rap_ok:
        state.errors["rap_ingest"] = "RAP inputs unavailable"

    state.ewmrs_goes_inputs_ready = state.ewmrs_mrms_inputs_ready and goes_ok
    if not state.ewmrs_goes_inputs_ready:
        state.errors.setdefault(
            "ewmrs_goes_ingest",
            "EWMRS GOES inputs unavailable",
        )
    if on_ewmrs_goes_ready is not None:
        on_ewmrs_goes_ready(state)

    state.edgewarn_integration_inputs_ready = (
        detection_ok and mrms_integration_ok and goes_ok and rap_ok
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

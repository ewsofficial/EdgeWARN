"""EWMRS/accessory service supervision functions (decomposition Phase 1).

Registers the loops that will become the EWMRS service's own children once
``run_ewmrs.py`` lands: METAR, NWS, and WPC continuous ingest, plus GOES ABI
ingest and rendering. Extracted from the former monolithic ``run.py`` so the
future entry point can reuse this registration verbatim while ``run.py``
remains a temporary adapter.

NEXRAD is deliberately absent: it is supervised by
``util.runtime.nexrad_service``, never here.
"""

from util.runtime.background import goes_loop, goes_render_loop, metar_loop, nws_loop, wpc_loop


def register_ewmrs_accessories(
    supervisor,
    *,
    mrms_core_only,
    metar_enabled,
    nws_enabled,
    goes_ingest_enabled,
    goes_render_enabled,
    goes_cycle_active,
    goes_render_active,
    goes_pause_ingest_during_render,
    goes_poll_seconds,
    goes_render_task_queue,
    goes_render_log_queue,
):
    """Add every non-NEXRAD accessory loop to *supervisor*.

    ``goes_ingest_enabled`` gates ABI ingest plus the scan-time GLM collection
    the primary integration consumes; ``goes_render_enabled`` additionally
    requires EWMRS, because GOES rendering only exists when EWMRS does.
    """
    supervisor.add(
        "METAR", metar_loop,
        enabled=metar_enabled,
        daemon=True,
    )
    supervisor.add(
        "NWS", nws_loop,
        enabled=nws_enabled,
        daemon=True,
    )
    supervisor.add(
        "WPC", wpc_loop,
        enabled=not mrms_core_only,
        daemon=True,
    )
    supervisor.add(
        "GOES", goes_loop,
        enabled=goes_ingest_enabled,
        args=(goes_cycle_active, goes_render_active, goes_pause_ingest_during_render, goes_poll_seconds),
        daemon=True,
        cleanup_event=goes_cycle_active,
    )
    supervisor.add(
        "GOES Render", goes_render_loop,
        enabled=bool(goes_render_enabled),
        args=(goes_render_task_queue, goes_render_log_queue, goes_render_active),
        daemon=True,
        cleanup_event=goes_render_active,
    )

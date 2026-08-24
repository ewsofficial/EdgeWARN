"""NEXRAD service supervision functions (decomposition Phase 1).

Owns the complete NEXRAD lifecycle registration: ingest (non-daemonic, with
its heartbeat-staleness restart policy) and GUI rendering. Extracted from the
former monolithic ``run.py`` so the future ``run_nexrad.py`` entry point can
reuse this supervision verbatim.

The render loop currently delegates into ``EWMRS.pipeline``; moving that
implementation out of the EWMRS package is Phase 3 work. The supervision
boundary already lives here.
"""

from common.ingest.nexrad.config import (
    heartbeat_stale_seconds,
    heartbeat_startup_grace_seconds,
)
from util.runtime.background import nexrad_ingest_loop, nexrad_render_loop


def register_nexrad_supervision(
    supervisor,
    *,
    base_dir,
    nexrad_log_queue,
    nexrad_heartbeat_path,
    enabled=True,
):
    """Add NEXRAD ingest and render children to *supervisor*."""
    supervisor.add(
        "NEXRAD Render", nexrad_render_loop,
        enabled=enabled,
        args=(base_dir,),
        daemon=True,
    )
    supervisor.add(
        "NEXRAD Ingest", nexrad_ingest_loop,
        enabled=enabled,
        args=(nexrad_log_queue, base_dir, nexrad_heartbeat_path),
        daemon=False,
        heartbeat_path=nexrad_heartbeat_path,
        # Called at registration rather than bound at import: these resolve
        # through nexrad.yaml, which needs the exported config root that only
        # exists after argument parsing.
        heartbeat_stale_seconds=heartbeat_stale_seconds(),
        heartbeat_startup_grace_seconds=heartbeat_startup_grace_seconds(),
    )

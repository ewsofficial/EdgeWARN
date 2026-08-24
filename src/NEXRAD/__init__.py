"""NEXRAD service package (decomposition Phase 3).

Owns the complete NEXRAD lifecycle: Level-II ingest supervision, GUI
rendering, manifests/indexes, retention, and cleanup. Neither the primary
EdgeWARN service nor EWMRS may import, launch, render, or clean NEXRAD work.
"""

from NEXRAD.gui_pipeline import (
    cleanup_old_nexrad_gui_files,
    render_pending_nexrad_gui_files,
    run_nexrad_render_loop,
)

__all__ = [
    "cleanup_old_nexrad_gui_files",
    "render_pending_nexrad_gui_files",
    "run_nexrad_render_loop",
]

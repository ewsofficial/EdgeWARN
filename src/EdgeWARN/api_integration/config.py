"""API index settings read from ``config/alerts.yaml``'s ``api_index`` section.

Accessors rather than module constants so the catalog is read per call: a
``--config-dir`` may be resolved after this module is imported, and a
module-level read would have frozen the repo default at import time.
"""

from common.config.loader import load_config

_CONFIG_NAME = "alerts"


def _api_index():
    """The ``api_index`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["api_index"]


def remove_old_cells_realtime() -> bool:
    """Whether the realtime pipeline expires inactive cells from the index."""
    return _api_index()["remove_old_cells"]["realtime"]


def remove_old_cells_historical() -> bool:
    """The historical pipeline's answer to the same question.

    Split from :func:`remove_old_cells_realtime` because the two pipelines
    disagree, and a replay must not delete cells the realtime run still indexes.
    """
    return _api_index()["remove_old_cells"]["historical"]


def stormcell_resync_every_updates() -> int:
    """Updates between full directory resyncs, which reconcile deletions."""
    return _api_index()["resync_every_updates"]


def inactive_cell_max_age_minutes() -> int:
    """Age above which a tracked cell is expired by ``cleanup_inactive_cells``.

    A separate owner from ``alerts.cleanup_max_age_minutes``, which prunes alert
    files. Same number today; changing one must not change the other.
    """
    return _api_index()["inactive_cell_max_age_minutes"]

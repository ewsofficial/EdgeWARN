"""Scheduler settings read from ``config/alerts.yaml``'s ``scheduler`` section.

Accessors rather than module constants so the catalog is read per call. The
scheduler is constructed inside processes spawned with no argv, which resolve the
config root from ``EDGEWARN_CONFIG_DIR`` after this module is imported; a
module-level read would have frozen the repo default at import time.
"""

from common.config.loader import load_config

_CONFIG_NAME = "alerts"


def _scheduler():
    """The ``scheduler`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["scheduler"]


def mrms_update_checker_max_entries() -> int:
    """``MRMSUpdateChecker`` listing width, used only by ``has_update``.

    Deliberately separate from :func:`modifier_lookup_max_entries`: the live
    scheduler path does not read this, so widening it does not widen the real
    check.
    """
    return _scheduler()["mrms_update_checker_max_entries"]


def modifier_lookup_max_entries() -> int:
    """Listing width for ``_get_modifier_times``, the path the scheduler runs."""
    return _scheduler()["modifier_lookup_max_entries"]


def s3_lookback_hours() -> int:
    """``StartAfter`` fallback reach when no ``last_processed`` cursor is known.

    With a known cursor the key is built from it instead and this is unused.
    """
    return _scheduler()["s3_lookback_hours"]


def slow_check_log_threshold_ms() -> int:
    """Elapsed time above which a modifier check emits a ``[PERF]`` line."""
    return _scheduler()["slow_check_log_threshold_ms"]

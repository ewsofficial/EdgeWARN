"""Historical replay settings read from ``config/historical.yaml``.

Only the keys with no CLI flag live here. ``lat``, ``lon`` and ``output`` are
resolved in ``util/io.py`` instead, because those three go through the
CLI > env > YAML overlay and a flag must be able to outrank the catalog.

Accessors rather than module constants so the catalog is read per call: a
``--config-dir`` may be resolved after this module is imported, and a
module-level read would have frozen the repo default at import time.
"""

from common.config.loader import load_config

_CONFIG_NAME = "historical"


def _historical():
    """The ``historical`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["historical"]


def historical_step_minutes() -> int:
    """Cursor advance per replay iteration, on every branch of the scan loop."""
    return _historical()["step_minutes"]


def historical_throttle_seconds() -> float:
    """Pause after an iteration that reached the pipeline.

    The early-`continue` branches skip it, so a long run of already-processed
    timestamps is not throttled at all.
    """
    return _historical()["throttle_seconds"]


def historical_cleanup_max_files() -> int:
    """Files retained per ingest data directory during historical cleanup.

    ``filesystem.yaml``'s age limit still applies on top of this: a directory is
    trimmed to this many files AND anything older than that budget is dropped.
    """
    return _historical()["cleanup_max_files"]

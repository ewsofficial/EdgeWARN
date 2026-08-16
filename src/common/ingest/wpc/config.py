"""WPC surface-analysis settings read from ``config/wpc.yaml``.

Accessors rather than module constants so the catalog is read per call: the WPC
ingest runs inside a process spawned with no argv, which resolves the config root
from ``EDGEWARN_CONFIG_DIR`` after this module is imported, and a module-level
read would have frozen the repo default at import time.
"""

from common.config.loader import load_config

_CONFIG_NAME = "wpc"


def _wpc():
    """The ``wpc`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["wpc"]


def coded_sfc_base_url() -> str:
    """Root of the WPC coded surface archive; the date directory is appended."""
    return _wpc()["coded_sfc_base_url"]


def update_interval_hours() -> int:
    """How often WPC publishes. The sole owner of the publish schedule.

    Distinct from :func:`previous_analysis_lookback_hours` despite the equal
    value: this describes the upstream product, that one is our own choice of how
    far back to backfill.
    """
    return _wpc()["update_interval_hours"]


def valid_hours() -> tuple[int, ...]:
    """The UTC hours WPC publishes at, in ascending order.

    Derived from :func:`update_interval_hours` rather than enumerated in the
    catalog. The two used to be separate keys that nothing coupled, so changing
    the interval left a stale list behind and the downloader would request hours
    WPC never publishes -- failing into its fallback on every single run.

    Order is load-bearing: the downloader steps backwards through this to pick a
    fallback analysis, and wraps to the previous day off the last element. That
    wrap is also why the schema restricts the interval to a divisor of 24: a
    non-dividing interval would leave a short final gap across midnight.
    """
    return tuple(range(0, 24, update_interval_hours()))


def http_timeout_seconds() -> float:
    """Per-request timeout for both the primary and the fallback download."""
    return _wpc()["http_timeout_seconds"]


def verify_tls() -> bool:
    """Whether the WPC connection verifies certificates.

    Pinned to ``true`` by ``wpc.schema.json``. It is read rather than assumed so
    that a schema which ever allowed ``false`` would fail loudly instead of
    silently downgrading the transport.
    """
    return _wpc()["verify_tls"]


def date_format() -> str:
    """``strftime`` format for the remote date directory and the local filename."""
    return _wpc()["date_format"]


def remote_filename_pattern() -> str:
    """Remote basename, formatted with ``hour``."""
    return _wpc()["remote_filename_pattern"]


def output_filename_pattern() -> str:
    """Local timestamped basename, formatted with ``date`` and ``hour``."""
    return _wpc()["output_filename_pattern"]


def latest_filename() -> str:
    """Name of the always-overwritten copy, which ``cleanup_glob`` must not match."""
    return _wpc()["latest_filename"]


def cleanup_glob() -> str:
    """Glob the retention sweep deletes from; pairs with the output pattern."""
    return _wpc()["cleanup_glob"]


def cleanup_max_age_minutes() -> int:
    """Age budget for the timestamped copies.

    WPC owns this rather than inheriting ``filesystem.yaml``'s default, because a
    3-hourly product needs a window wider than an hour to keep anything at all.
    """
    return _wpc()["cleanup_max_age_minutes"]


def previous_analysis_lookback_hours() -> float:
    """How far back the ingest backfills a second analysis on each run."""
    return _wpc()["previous_analysis_lookback_hours"]


def fallback_geojson_color() -> str:
    """Color for a parsed feature code absent from :func:`feature_types`."""
    return _wpc()["fallback_geojson_color"]


def feature_types():
    """Code -> ``{name, color}`` for GeoJSON styling."""
    return _wpc()["feature_types"]

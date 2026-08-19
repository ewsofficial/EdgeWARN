"""METAR ingest settings read from ``config/metar.yaml``.

Accessors rather than module constants so the catalog is read per call: METAR
runs inside a process spawned with no argv, which resolves the config root from
``EDGEWARN_CONFIG_DIR`` after this module is imported, and a module-level read
would have frozen the repo default at import time.

``metar.py`` is a flat module rather than a package, so the accessors live beside
it instead of in a ``config`` submodule.
"""

import ssl

from common.config.loader import load_config
from util.io import IOManager

_CONFIG_NAME = "metar"

_io = IOManager("[METAR Ingest]")


def _metar():
    """The ``metar`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["metar"]


def station_db_url() -> str:
    """Station coordinate database, fetched once and then cached on disk."""
    return _metar()["station_db_url"]


def station_cache_file() -> str:
    """Basename of the on-disk station cache, relative to ``fs.DATA_DIR``."""
    return _metar()["station_cache_file"]


def observation_url_pattern() -> str:
    """Cycle-file URL, formatted with ``hour`` as a two-digit UTC hour."""
    return _metar()["observation_url_pattern"]


def station_timeout_seconds() -> float:
    """Timeout for the station database download.

    Deliberately longer than :func:`observation_timeout_seconds`: it is a single
    large JSON document, not one of several small hourly text files.
    """
    return _metar()["station_timeout_seconds"]


def observation_timeout_seconds() -> float:
    """Timeout for one hourly cycle-file fetch, sync or async."""
    return _metar()["observation_timeout_seconds"]


def accept_encoding() -> str:
    """``Accept-Encoding`` for the station download.

    Narrower than the client default on purpose: ``br`` is omitted because
    brotli responses failed to decode.
    """
    return _metar()["accept_encoding"]


def verify_tls() -> bool:
    """Whether METAR requests verify certificates.

    Unlike ``wpc.verify_tls`` this is a real switch rather than a schema
    constant, so every path that builds transport security goes through
    :func:`ssl_context` or :func:`aiohttp_ssl` and warns while it is false.
    """
    return _metar()["verify_tls"]


def _warn_if_unverified() -> None:
    if verify_tls():
        return
    _io.write_warning(
        "metar.verify_tls is false: METAR accepts any TLS certificate, so a "
        "network attacker can alter observations undetected. Set it to true in "
        "config/metar.yaml once a run confirms the certificates validate."
    )


def ssl_context() -> ssl.SSLContext:
    """Context for the two ``urllib`` request paths."""
    _warn_if_unverified()
    context = ssl.create_default_context()
    if not verify_tls():
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def aiohttp_ssl():
    """``ssl`` argument for ``aiohttp.TCPConnector``.

    ``False`` rather than a permissive context, because that is aiohttp's own
    spelling for skipping verification.
    """
    _warn_if_unverified()
    if not verify_tls():
        return False
    return ssl.create_default_context()


def lookback_hours() -> int:
    """How many hourly cycles each run ingests, counting the current hour."""
    return _metar()["lookback_hours"]


def cleanup_max_age_minutes() -> int:
    """Age budget for saved METAR snapshots.

    Equal to ``filesystem.yaml``'s generic default today, but declared here so
    that retuning the generic sweep does not silently retune METAR. The file
    count cap is not restated: METAR takes the generic one.
    """
    return _metar()["cleanup_max_age_minutes"]


def coordinate_decimals() -> int:
    """Rounding applied to station latitude and longitude when caching."""
    return _metar()["coordinate_decimals"]


def pressure_decimals() -> int:
    """Rounding applied to the altimeter setting after conversion to inHg."""
    return _metar()["pressure_decimals"]


def conus_bounds():
    """Latitude/longitude box observations must fall inside to be kept."""
    return _metar()["conus_bounds"]

"""RAP ingest settings read from ``config/synoptic_rap.yaml``.

Accessors rather than module constants so the catalog is read per call. A
``--config-dir`` may be resolved after this module is imported -- spawned
accessories receive no argv and re-resolve the root themselves -- and a
module-level read would have frozen the repo default at import time.
"""

import os

from common.config.loader import load_config

_CONFIG_NAME = "synoptic_rap"

RAP_MAX_AGE_ENV = "EDGEWARN_RAP_MAX_AGE_MINUTES"


def _rap():
    """The ``rap`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["rap"]


def rap_bucket() -> str:
    return _rap()["bucket"]


def rap_file_pattern() -> str:
    """Remote object name, formatted with ``hour``."""
    return _rap()["file_pattern"]


def rap_dir_pattern() -> str:
    """Remote parent directory, formatted with ``date``."""
    return _rap()["dir_pattern"]


def rap_local_file_pattern() -> str:
    """Local name, formatted with ``date`` and ``hour``.

    Not derivable from :func:`rap_file_pattern`: it is uppercased and carries the
    date, which the remote name does not.
    """
    return _rap()["local_file_pattern"]


def rap_date_format() -> str:
    """``strftime`` format for the ``date`` field of both patterns."""
    return _rap()["date_format"]


def rap_filename_regex() -> str:
    """Parses :func:`rap_local_file_pattern` back into a timestamp."""
    return _rap()["filename_regex"]


def rap_lookback_step_hours() -> int:
    return _rap()["lookback_step_hours"]


def rap_max_files() -> int:
    return _rap()["max_files"]


def get_rap_max_age_minutes() -> int:
    """Return the configured maximum RAP analysis age.

    The environment variable outranks the catalog, per the project's
    CLI > env > YAML precedence. Kept as a bespoke read rather than routed
    through ``common.config.overlay`` because this is the one site that rejects a
    malformed value outright instead of coercing it; ``overlay._coerce`` would
    turn "abc" into an uncaught ``ValueError`` from ``int()`` and "-1" into a
    silently accepted negative budget.
    """
    raw_value = os.environ.get(RAP_MAX_AGE_ENV)
    if raw_value is None:
        return _rap()["max_age_minutes"]

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{RAP_MAX_AGE_ENV} must be a non-negative integer, got {raw_value!r}"
        ) from exc

    if value < 0:
        raise ValueError(
            f"{RAP_MAX_AGE_ENV} must be a non-negative integer, got {raw_value!r}"
        )
    return value

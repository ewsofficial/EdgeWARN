"""Generic CLI > environment > YAML precedence resolution.

Works uniformly for booleans too: ``argparse.BooleanOptionalAction`` with
``default=None`` produces ``True``/``False``/``None`` (unset), so the same
``is not None`` check used for other types applies.
"""

from __future__ import annotations

import os
from typing import Any, Iterable

_TRUE_STRINGS = {"1", "true", "yes", "on"}

# Which layer supplied each key, keyed by dotted catalog path. Write-only from
# :func:`resolve`, which never reads it, so a stale or missing entry cannot
# affect resolution. Labels are ``"cli"``, ``"env:<VAR>"``, or ``"yaml"`` -- the
# layer and, for the environment, the variable that won; never the value, so a
# key holding a secret cannot leak into a diagnostic.
#
# Accessors are called per read, some inside poll loops, so this is last-write-
# wins on a key rather than an append log. Each process records its own view:
# children spawned by the supervisor get no argv and re-resolve independently.
_origins: dict[str, str] = {}


def _coerce(raw: str, reference: Any) -> Any:
    if isinstance(reference, bool):
        # An unrecognized value reads false rather than falling through as a
        # (truthy) string. This is the pre-existing semantics of the boolean
        # environment variables being routed through here: the opt-in set is
        # tested after strip().lower(), so "2" and "enabled" mean off.
        return raw.strip().lower() in _TRUE_STRINGS
    if isinstance(reference, int):
        return int(raw)
    if isinstance(reference, float):
        return float(raw)
    return raw


def resolve(
    cli_value: Any,
    *,
    env_names: Iterable[str] = (),
    yaml_value: Any = None,
    key: str | None = None,
) -> Any:
    """Return the highest-precedence value among CLI, environment, and YAML.

    ``cli_value is None`` is treated as "the CLI flag was not supplied" for
    both non-boolean flags (``default=None``) and boolean flags using
    ``argparse.BooleanOptionalAction`` (also ``default=None``).

    ``key`` is the dotted catalog path this value came from. When supplied, the
    winning layer is recorded for :func:`overrides`; a value resolved without it
    is simply absent from that report.
    """
    if cli_value is not None:
        if key is not None:
            _origins[key] = "cli"
        return cli_value

    for env_name in env_names:
        raw = os.environ.get(env_name)
        if raw is not None:
            # Coerce before recording: an unparseable override raises, and a
            # layer that never produced a value must not be named the winner.
            coerced = _coerce(raw, yaml_value)
            if key is not None:
                _origins[key] = f"env:{env_name}"
            return coerced

    if key is not None:
        _origins[key] = "yaml"
    return yaml_value


def overrides() -> dict[str, str]:
    """Keys this process resolved from somewhere other than YAML, path -> layer."""
    return {key: layer for key, layer in _origins.items() if layer != "yaml"}


def reset_origins() -> None:
    """Forget every recorded origin. Intended for tests."""
    _origins.clear()

"""Generic CLI > environment > YAML precedence resolution.

Works uniformly for booleans too: ``argparse.BooleanOptionalAction`` with
``default=None`` produces ``True``/``False``/``None`` (unset), so the same
``is not None`` check used for other types applies.
"""

from __future__ import annotations

import os
from typing import Any, Iterable

_TRUE_STRINGS = {"1", "true", "yes", "on"}
_FALSE_STRINGS = {"0", "false", "no", "off"}


def _coerce(raw: str, reference: Any) -> Any:
    if isinstance(reference, bool):
        lowered = raw.strip().lower()
        if lowered in _TRUE_STRINGS:
            return True
        if lowered in _FALSE_STRINGS:
            return False
        return raw
    if isinstance(reference, int):
        return int(raw)
    if isinstance(reference, float):
        return float(raw)
    return raw


def resolve(cli_value: Any, *, env_names: Iterable[str] = (), yaml_value: Any = None) -> Any:
    """Return the highest-precedence value among CLI, environment, and YAML.

    ``cli_value is None`` is treated as "the CLI flag was not supplied" for
    both non-boolean flags (``default=None``) and boolean flags using
    ``argparse.BooleanOptionalAction`` (also ``default=None``).
    """
    if cli_value is not None:
        return cli_value

    for env_name in env_names:
        raw = os.environ.get(env_name)
        if raw is not None:
            return _coerce(raw, yaml_value)

    return yaml_value

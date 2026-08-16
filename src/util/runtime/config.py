"""Typed access to ``config/runtime.yaml`` for the scheduler and its children.

Accessory loops run in processes spawned with no argv, so they cannot receive a
``--config-dir`` and instead resolve the config root from
``EDGEWARN_CONFIG_DIR`` -- which ``util/io.py`` exports from the parent's flag.
That is why nothing here takes a ``config_dir`` argument: inside a child there
is no CLI value left to thread.

Memoized because these values are read from inside poll loops and per-process
shutdown paths, and ``load_config`` re-resolves the config root on every call.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import util.file as fs
from common.config.loader import ConfigError, load_config

_CONFIG_NAME = "runtime"


@lru_cache(maxsize=None)
def section(name: str) -> Any:
    """Frozen view of one top-level section of ``runtime.yaml``."""
    return load_config(_CONFIG_NAME)[name]


def reset_cache() -> None:
    """Clear memoized sections. Intended for tests, alongside loader.reset_cache."""
    section.cache_clear()


def resolve_file(spec: Any, dotted_path: str) -> Path:
    """Resolve a ``{dir: <util.file attribute>, name: <relative path>}`` spec.

    ``getattr`` runs per call so a path rebound by ``initialize_filesystem`` is
    picked up, and a bad attribute name surfaces as a ``ConfigError`` naming the
    offending key instead of a bare ``AttributeError``.
    """
    attribute = spec["dir"]
    try:
        base = getattr(fs, attribute)
    except AttributeError:
        raise ConfigError(
            f"{_CONFIG_NAME}.yaml",
            f"{dotted_path}.dir: {attribute}",
            "not an attribute of util.file",
        ) from None
    return Path(base) / spec["name"]

"""Lineage settings read from ``config/lineage.yaml``.

Every consumer here takes ``None`` to mean "the caller did not supply this" and
resolves the YAML value only then, matching the sentinel convention in
``common.config.overlay``. That keeps a caller-supplied value winning while the
YAML remains the single owner of the default.

``section()`` is read per call rather than at import so a ``--config-dir``
resolved after this module is imported is still honored -- spawned accessories
receive no argv and re-resolve the root themselves. It is memoized because
``bounds_overlap`` resolves its default from inside a per-cell loop.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Any, Optional

from common.config import loader as config_loader

_CONFIG_NAME = "lineage"


@lru_cache(maxsize=None)
def section(name: str, config_dir: Optional[str] = None) -> Any:
    """Frozen view of one top-level section of ``lineage.yaml``."""
    return config_loader.load_config(_CONFIG_NAME, config_dir=config_dir)[name]


def reset_cache() -> None:
    """Clear memoized sections. Intended for tests, alongside loader.reset_cache."""
    section.cache_clear()


def tracked_overlap_ratio() -> float:
    """Minimum overlap ratio the storm-cell tracker applies to merge/splits."""
    return section("lineage")["tracked_overlap_ratio"]


def bounds_prefilter_buffer_deg() -> float:
    """Slack on the bounding-box pre-filter that runs before any area overlap."""
    return section("lineage")["bounds_prefilter_buffer_deg"]


def buffer_settings() -> Any:
    """The hysteresis-buffer block."""
    return section("lineage")["buffer"]

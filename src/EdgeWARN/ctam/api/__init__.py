"""Private, loopback-only CTAM internal API v1."""

from typing import Any

__all__ = ["CTAMReadService", "LoopbackCTAMServer"]

_LAZY_EXPORTS = {"CTAMReadService": ".service", "LoopbackCTAMServer": ".server"}


def __getattr__(name: str) -> Any:
    """Resolve exports lazily so ``EdgeWARN.ctam.transaction`` can import
    ``api.models`` without the eager ``server``/``service`` import cycle."""
    try:
        module = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    import importlib

    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value
    return value

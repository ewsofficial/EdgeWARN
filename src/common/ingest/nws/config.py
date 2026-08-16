"""Accessors for ``nws.yaml``.

Every function reads the catalog per call. None of them memoize and none of them
are evaluated at import: ``src/run.py`` imports this package well before
``get_args()`` publishes the config root, so a value bound at module scope would
freeze the repo default and put ``--config-dir`` out of reach.
"""

from common.config.loader import load_config

_CONFIG_NAME = "nws"


def _section(name: str):
    return load_config(_CONFIG_NAME)[name]


def registry_ttl_hours() -> float:
    """How long an alert survives after it stops appearing upstream."""
    return _section("nws")["registry_ttl_hours"]


def dropped_events() -> frozenset[str]:
    """Event names discarded on ingest.

    One-way: a dropped alert is never written, so nothing downstream can recover
    it. Returned as a frozenset because the only use is a membership test.
    """
    return frozenset(_section("nws")["dropped_events"])


def active_alerts_url() -> str:
    """The NWS active-alerts feed, shared by the sync and async downloads."""
    return _section("nws")["active_alerts_url"]


def tornado_upgrade_event() -> str:
    """The one event whose name is re-derived from the alert description."""
    return _section("nws")["tornado_upgrade"]["event"]


def tornado_upgrade_rules() -> tuple[tuple[str, str], ...]:
    """``(phrase, name)`` pairs in priority order -- the first match wins."""
    return tuple(
        (rule["description_contains"], rule["name"])
        for rule in _section("nws")["tornado_upgrade"]["rules"]
    )

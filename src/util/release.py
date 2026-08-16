from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

from common.config.loader import load_config


_PACKAGE_JSON = Path(__file__).resolve().parents[2] / "package.json"


@lru_cache(maxsize=1)
def get_release_version() -> str:
    try:
        with _PACKAGE_JSON.open("r", encoding="utf-8") as handle:
            package_json = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return "unknown"

    version = package_json.get("version")
    if not version:
        return "unknown"

    return str(version)


def format_user_agent(*, config_dir: str | os.PathLike[str] | None = None) -> str:
    """The one outbound User-Agent, from ``runtime.yaml``'s ``identity`` block.

    Not memoized: ``load_config`` already is, and callers may be running
    against a different ``config_dir``.
    """
    identity = load_config("runtime", config_dir=config_dir)["identity"]
    # The schema pins both placeholders and forbids any other brace, so this
    # cannot raise on an operator-supplied template.
    return identity["user_agent_template"].format(
        version=get_release_version(),
        contact=identity["contact"],
    )

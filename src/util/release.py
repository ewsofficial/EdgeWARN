from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


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

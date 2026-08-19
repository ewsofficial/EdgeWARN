"""YAML-backed RAP Uint16 catalog accessors.

The product registry lives in the ``rap_uint16`` section of
``config/ewmrs_pipeline.yaml``.  This module only expands its declarative
pressure-level templates and resolves output paths.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from common.config.loader import load_config
import util.file as fs

_CONFIG_NAME = "ewmrs_pipeline"


def _catalog():
    return load_config(_CONFIG_NAME)["rap_uint16"]


def _copy(value: Any) -> Any:
    """Turn the loader's immutable mappings and tuples into caller-owned data."""
    if isinstance(value, dict) or hasattr(value, "items"):
        return {key: _copy(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_copy(item) for item in value]
    return value


def uint16_nodata() -> int:
    return _catalog()["uint16"]["nodata"]


def uint16_valid_max() -> int:
    return _catalog()["uint16"]["valid_max"]


def rap_uint16_max_timestamps() -> int:
    return _catalog()["max_timestamps"]


def rap_uint16_timestamp_format() -> str:
    return _catalog()["timestamp_format"]


def rap_uint16_force() -> bool:
    return _catalog()["force"]


def _format(value: Any, values: dict[str, Any]) -> Any:
    if isinstance(value, str):
        if value.startswith("{") and value.endswith("}") and value[1:-1] in values:
            return _copy(values[value[1:-1]])
        return value.format(**values)
    if isinstance(value, dict) or hasattr(value, "items"):
        return {key: _format(item, values) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_format(item, values) for item in value]
    return value


def _template_layers(template: dict[str, Any]) -> list[dict[str, Any]]:
    layers = []
    for values in template["values"]:
        for layer in template["layers"]:
            layers.append(_format(layer, values))
    return layers


def get_rap_uint16_layers() -> list[dict[str, Any]]:
    """Return YAML-configured RAP layers with absolute output directories."""
    catalog = _catalog()
    layers = [_copy(layer) for layer in catalog["layers"]]
    for template in catalog["templates"]:
        layers.extend(_template_layers(template))
    return [{**layer, "outdir": fs.GUI_RAP_DIR / layer["outdir"]} for layer in layers]

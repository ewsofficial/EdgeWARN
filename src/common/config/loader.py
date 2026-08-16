"""Load, validate, and freeze the YAML files under ``config/``.

Importable with only the standard library plus ``yaml`` -- no ``util.file``,
no domain modules -- so it can run before the rest of the application
(including the filesystem layer) is initialized.

Schema validation is a small hand-rolled walker (see ``_walk``) rather than a
full JSON Schema implementation. It supports exactly the keywords used by
``config/schema/*.schema.json`` today: ``type``, ``properties``,
``required``, ``additionalProperties``, ``items``, ``minItems``,
``maxItems``, ``uniqueItems``, ``minimum``, ``maximum``, ``exclusiveMinimum``,
``exclusiveMaximum``, ``const``, ``enum``, and ``pattern``. Any other keyword
in a schema is a startup error (``_UNSUPPORTED_KEYWORDS`` guard) rather than
a silently-unenforced constraint, so a schema author who reaches for
``oneOf``/``$ref``/``format`` finds out immediately instead of shipping a
schema that looks stricter than it is.

Precedence for locating the config root, highest first:
1. An explicit ``config_dir`` argument (typically sourced from a ``--config-dir``
   CLI flag).
2. The ``EDGEWARN_CONFIG_DIR`` environment variable.
3. A ``config/`` directory found by walking up from this file's location.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml

CONFIG_NAMES: tuple[str, ...] = (
    "runtime",
    "historical",
    "filesystem",
    "detection",
    "lineage",
    "integration",
    "scheduler",
    "api_index",
    "mrms_goes",
    "nexrad",
    "synoptic_rap",
    "wpc",
    "metar",
    "nws",
    "ewmrs_render",
    "ewmrs_rap_uint16",
    "ewmrs_pipeline",
    "api",
    "kalman",
)

_ENV_CONFIG_DIR = "EDGEWARN_CONFIG_DIR"

_config_cache: dict[tuple[str, str], Any] = {}
_provenance_cache: dict[tuple[str, str], dict[str, Any]] = {}


class ConfigError(Exception):
    """Raised for a missing config file, missing key, or schema violation."""

    def __init__(self, filename: str, dotted_path: str | None, message: str):
        self.filename = filename
        self.dotted_path = dotted_path
        self.message = message
        super().__init__(str(self))

    def __str__(self) -> str:
        if self.dotted_path:
            return f"{self.filename}: {self.dotted_path}: {self.message}"
        return f"{self.filename}: {self.message}"


def _find_config_root_by_walking_up() -> Path:
    here = Path(__file__).resolve()
    for candidate_dir in here.parents:
        config_dir = candidate_dir / "config"
        if (config_dir / "runtime.yaml").is_file():
            return config_dir
    raise ConfigError(
        "config/",
        None,
        f"could not locate a config/ directory containing runtime.yaml by "
        f"walking up from {here}",
    )


def config_root(cli_dir: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the config root directory using CLI > env > repo-root precedence."""
    if cli_dir is not None:
        resolved = Path(cli_dir).resolve()
        if not (resolved / "runtime.yaml").is_file():
            raise ConfigError(
                str(resolved),
                None,
                "--config-dir does not contain runtime.yaml",
            )
        return resolved

    env_dir = os.environ.get(_ENV_CONFIG_DIR)
    if env_dir is not None:
        resolved = Path(env_dir).resolve()
        if not (resolved / "runtime.yaml").is_file():
            raise ConfigError(
                str(resolved),
                None,
                f"{_ENV_CONFIG_DIR} does not contain runtime.yaml",
            )
        return resolved

    return _find_config_root_by_walking_up()


def repo_root(cli_dir: str | os.PathLike[str] | None = None) -> Path:
    """The repository root, derived as the parent of the config root."""
    return config_root(cli_dir).parent


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def _dotted_path(path_parts: list[Any]) -> str:
    parts: list[str] = []
    for part in path_parts:
        if isinstance(part, int):
            parts[-1] = f"{parts[-1]}[{part}]"
        else:
            parts.append(str(part))
    return ".".join(parts)


_KNOWN_SCHEMA_KEYWORDS = frozenset({
    "$schema", "title", "description",
    "type", "properties", "required", "additionalProperties",
    "items", "minItems", "maxItems", "uniqueItems",
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum",
    "const", "enum", "pattern",
})


def _check_supported_keywords(schema_path: Path, node: Any, path: list[Any]) -> None:
    """Reject any schema keyword the hand-rolled walker doesn't implement.

    Without this, an author reaching for e.g. ``oneOf`` or ``$ref`` would get
    a schema that silently enforces nothing for that constraint instead of a
    startup error.
    """
    if not isinstance(node, dict):
        return
    unknown = sorted(set(node) - _KNOWN_SCHEMA_KEYWORDS)
    if unknown:
        raise ConfigError(
            str(schema_path),
            _dotted_path(path) or None,
            f"unsupported schema keyword(s) {unknown}",
        )
    for prop_name, prop_schema in node.get("properties", {}).items():
        _check_supported_keywords(schema_path, prop_schema, path + [prop_name])
    additional = node.get("additionalProperties")
    if isinstance(additional, dict):
        _check_supported_keywords(schema_path, additional, path + ["additionalProperties"])
    items = node.get("items")
    if isinstance(items, dict):
        _check_supported_keywords(schema_path, items, path + ["items"])


def _type_matches(value: Any, type_name: str) -> bool:
    if type_name == "object":
        return isinstance(value, dict)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "null":
        return value is None
    raise ConfigError("<schema>", None, f"unsupported schema type {type_name!r}")


def _has_duplicates(items: list[Any]) -> bool:
    seen: list[Any] = []
    for item in items:
        if item in seen:
            return True
        seen.append(item)
    return False


def _walk(schema: dict[str, Any], value: Any, path: list[Any], errors: list[tuple[list[Any], str]]) -> None:
    type_spec = schema.get("type")
    if type_spec is not None:
        type_names = [type_spec] if isinstance(type_spec, str) else list(type_spec)
        if not any(_type_matches(value, t) for t in type_names):
            errors.append((path, f"{value!r} is not of type {' or '.join(type_names)}"))
            return

    if "const" in schema and value != schema["const"]:
        errors.append((path, f"must equal {schema['const']!r}"))
    if "enum" in schema and value not in schema["enum"]:
        errors.append((path, f"must be one of {schema['enum']!r}"))

    if isinstance(value, dict):
        for key in schema.get("required", ()):
            if key not in value:
                errors.append((path + [key], "is a required property"))
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, sub_value in value.items():
            if key in properties:
                _walk(properties[key], sub_value, path + [key], errors)
            elif additional is False:
                errors.append((path + [key], "additional properties are not allowed"))
            elif isinstance(additional, dict):
                _walk(additional, sub_value, path + [key], errors)

    elif isinstance(value, list):
        min_items = schema.get("minItems")
        if min_items is not None and len(value) < min_items:
            errors.append((path, f"must have at least {min_items} item(s)"))
        max_items = schema.get("maxItems")
        if max_items is not None and len(value) > max_items:
            errors.append((path, f"must have at most {max_items} item(s)"))
        if schema.get("uniqueItems") and _has_duplicates(value):
            errors.append((path, "items must be unique"))
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, item in enumerate(value):
                _walk(item_schema, item, path + [index], errors)

    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        bound = schema.get("minimum")
        if bound is not None and value < bound:
            errors.append((path, f"must be >= {bound}"))
        bound = schema.get("maximum")
        if bound is not None and value > bound:
            errors.append((path, f"must be <= {bound}"))
        bound = schema.get("exclusiveMinimum")
        if bound is not None and value <= bound:
            errors.append((path, f"must be > {bound}"))
        bound = schema.get("exclusiveMaximum")
        if bound is not None and value >= bound:
            errors.append((path, f"must be < {bound}"))

    elif isinstance(value, str) and "pattern" in schema and re.search(schema["pattern"], value) is None:
        errors.append((path, f"does not match pattern {schema['pattern']!r}"))


def _validate(name: str, document: dict[str, Any], schema_path: Path) -> None:
    if not schema_path.is_file():
        raise ConfigError(str(schema_path), None, "schema file not found")

    with schema_path.open("r", encoding="utf-8") as fh:
        schema = json.load(fh)

    _check_supported_keywords(schema_path, schema, [])

    errors: list[tuple[list[Any], str]] = []
    _walk(schema, document, [], errors)
    if errors:
        first_path, first_message = min(errors, key=lambda e: (len(e[0]), [str(p) for p in e[0]]))
        raise ConfigError(f"{name}.yaml", _dotted_path(first_path) or None, first_message)


def reset_cache() -> None:
    """Clear memoized configs and provenance. Intended for tests."""
    _config_cache.clear()
    _provenance_cache.clear()


def load_config(name: str, *, config_dir: str | os.PathLike[str] | None = None) -> Any:
    """Load, schema-validate, and freeze ``config/<name>.yaml``.

    Memoized per resolved config root and name, so repeated calls (including
    across module re-execution under multiprocessing) are cheap and return
    the identical frozen object.
    """
    root = config_root(config_dir)
    cache_key = (str(root), name)
    if cache_key in _config_cache:
        return _config_cache[cache_key]

    yaml_path = root / f"{name}.yaml"
    if not yaml_path.is_file():
        raise ConfigError(f"{name}.yaml", None, f"file not found at {yaml_path}")

    with yaml_path.open("r", encoding="utf-8") as fh:
        document = yaml.safe_load(fh)

    if not isinstance(document, dict):
        raise ConfigError(f"{name}.yaml", None, "top-level document must be a mapping")

    schema_path = root / "schema" / f"{name}.schema.json"
    _validate(name, document, schema_path)

    frozen = _freeze(document)
    _config_cache[cache_key] = frozen
    _provenance_cache[cache_key] = {
        "path": str(yaml_path),
        "schema_version": document.get("schema_version"),
    }
    return frozen


def get_provenance(name: str, *, config_dir: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Sanitized provenance (path + schema_version, no secrets) for a loaded config."""
    root = config_root(config_dir)
    cache_key = (str(root), name)
    if cache_key not in _provenance_cache:
        load_config(name, config_dir=config_dir)
    return dict(_provenance_cache[cache_key])

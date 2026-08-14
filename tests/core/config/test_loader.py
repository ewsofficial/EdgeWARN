"""Unit tests for the hand-rolled schema walker in ``common.config.loader``.

There is no upstream test suite backing this validator (unlike a library),
so each keyword it implements gets its own regression test here.
"""

from __future__ import annotations

import json

import pytest
import yaml

from common.config import loader as config_loader


def _config_dir(tmp_path):
    (tmp_path / "runtime.yaml").write_text("schema_version: 1\n", encoding="utf-8")
    (tmp_path / "schema").mkdir()
    return tmp_path


def _write(config_dir, name, document, schema):
    (config_dir / f"{name}.yaml").write_text(yaml.safe_dump(document), encoding="utf-8")
    (config_dir / "schema" / f"{name}.schema.json").write_text(json.dumps(schema), encoding="utf-8")


@pytest.fixture
def config_dir(tmp_path):
    yield _config_dir(tmp_path)
    config_loader.reset_cache()


def test_valid_document_passes(config_dir):
    schema = {
        "type": "object",
        "required": ["a"],
        "additionalProperties": False,
        "properties": {"a": {"type": "integer"}},
    }
    _write(config_dir, "sample", {"a": 1}, schema)

    loaded = config_loader.load_config("sample", config_dir=config_dir)

    assert loaded["a"] == 1


def test_missing_required_property_raises(config_dir):
    schema = {
        "type": "object",
        "required": ["a"],
        "additionalProperties": False,
        "properties": {"a": {"type": "integer"}},
    }
    _write(config_dir, "sample", {}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert excinfo.value.dotted_path == "a"
    assert "required" in excinfo.value.message


def test_unexpected_property_rejected(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"a": {"type": "integer"}},
    }
    _write(config_dir, "sample", {"a": 1, "typo": 2}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert excinfo.value.dotted_path == "typo"


def test_type_mismatch_raises(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"a": {"type": "integer"}},
    }
    _write(config_dir, "sample", {"a": "not-an-int"}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert excinfo.value.dotted_path == "a"
    assert "not of type" in excinfo.value.message


def test_boolean_is_not_accepted_as_integer(config_dir):
    schema = {"type": "object", "additionalProperties": False, "properties": {"a": {"type": "integer"}}}
    _write(config_dir, "sample", {"a": True}, schema)

    with pytest.raises(config_loader.ConfigError):
        config_loader.load_config("sample", config_dir=config_dir)


def test_additional_properties_as_schema_validates_map_values(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "env_overrides": {"type": "object", "additionalProperties": {"type": "string"}},
        },
    }
    _write(config_dir, "sample", {"env_overrides": {"A": "1", "B": 2}}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert excinfo.value.dotted_path == "env_overrides.B"


def test_array_min_max_items_and_unique_items(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "bounds": {
                "type": "array",
                "items": {"type": "number"},
                "minItems": 2,
                "maxItems": 2,
                "uniqueItems": True,
            }
        },
    }
    _write(config_dir, "sample", {"bounds": [1, 1]}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert "unique" in excinfo.value.message


def test_array_length_bounds_enforced(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"bounds": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4}},
    }
    _write(config_dir, "sample", {"bounds": [1, 2]}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert "at least 4" in excinfo.value.message


def test_numeric_bounds_inclusive_and_exclusive(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "count": {"type": "integer", "minimum": 0, "maximum": 10},
            "ratio": {"type": "number", "exclusiveMinimum": 0},
        },
    }
    _write(config_dir, "sample", {"count": 11, "ratio": 0}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert excinfo.value.dotted_path in ("count", "ratio")


def test_pattern_mismatch_raises(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {"color": {"type": "string", "pattern": "^#[0-9A-Fa-f]{6}$"}},
    }
    _write(config_dir, "sample", {"color": "not-a-hex-color"}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert "pattern" in excinfo.value.message


def test_const_and_enum_enforced(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "schema_version": {"const": 1},
            "level": {"enum": ["low", "medium", "high"]},
        },
    }
    _write(config_dir, "sample", {"schema_version": 2, "level": "extreme"}, schema)

    with pytest.raises(config_loader.ConfigError):
        config_loader.load_config("sample", config_dir=config_dir)


def test_unsupported_schema_keyword_is_a_startup_error(config_dir):
    schema = {
        "type": "object",
        "properties": {"a": {"oneOf": [{"type": "integer"}, {"type": "string"}]}},
    }
    _write(config_dir, "sample", {"a": 1}, schema)

    with pytest.raises(config_loader.ConfigError) as excinfo:
        config_loader.load_config("sample", config_dir=config_dir)

    assert "unsupported schema keyword" in excinfo.value.message
    assert excinfo.value.filename.endswith("sample.schema.json")


def test_loaded_config_is_recursively_frozen(config_dir):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "nested": {
                "type": "object",
                "additionalProperties": False,
                "properties": {"items": {"type": "array", "items": {"type": "integer"}}},
            }
        },
    }
    _write(config_dir, "sample", {"nested": {"items": [1, 2, 3]}}, schema)

    loaded = config_loader.load_config("sample", config_dir=config_dir)

    with pytest.raises(TypeError):
        loaded["nested"]["items"] = ()
    assert isinstance(loaded["nested"]["items"], tuple)

"""Precedence resolution and the origin registry it writes for diagnostics.

The registry exists so a startup summary can say where an effective value came
from; ``get_provenance`` is file-level and cannot. These tests pin that the
recording is faithful and that it stays invisible to resolution itself.
"""

from __future__ import annotations

import pytest

from common.config import overlay


@pytest.fixture(autouse=True)
def clean_origins():
    overlay.reset_origins()
    yield
    overlay.reset_origins()


def test_env_override_is_reported_as_env_sourced(monkeypatch):
    monkeypatch.setenv("EDGEWARN_TEST_ATTEMPTS", "9")

    resolved = overlay.resolve(
        None,
        env_names=("EDGEWARN_TEST_ATTEMPTS",),
        yaml_value=3,
        key="cycle.retry.max_attempts",
    )

    assert resolved == 9
    assert overlay.overrides() == {
        "cycle.retry.max_attempts": "env:EDGEWARN_TEST_ATTEMPTS"
    }


def test_the_winning_variable_is_named_not_the_first_candidate(monkeypatch):
    monkeypatch.delenv("EDGEWARN_TEST_PRIMARY", raising=False)
    monkeypatch.setenv("EDGEWARN_TEST_FALLBACK", "4")

    overlay.resolve(
        None,
        env_names=("EDGEWARN_TEST_PRIMARY", "EDGEWARN_TEST_FALLBACK"),
        yaml_value=1,
        key="sample.key",
    )

    assert overlay.overrides() == {"sample.key": "env:EDGEWARN_TEST_FALLBACK"}


def test_cli_override_outranks_env_and_is_reported_as_cli(monkeypatch):
    monkeypatch.setenv("EDGEWARN_TEST_ATTEMPTS", "9")

    resolved = overlay.resolve(
        7,
        env_names=("EDGEWARN_TEST_ATTEMPTS",),
        yaml_value=3,
        key="cycle.retry.max_attempts",
    )

    assert resolved == 7
    assert overlay.overrides() == {"cycle.retry.max_attempts": "cli"}


def test_a_yaml_value_is_recorded_but_not_reported_as_an_override():
    resolved = overlay.resolve(None, yaml_value=3, key="cycle.retry.max_attempts")

    assert resolved == 3
    assert overlay.overrides() == {}


def test_the_registry_holds_no_values(monkeypatch):
    """Labels name the layer, never the value, so a secret cannot leak."""
    monkeypatch.setenv("EDGEWARN_TEST_TOKEN", "s3cret")

    overlay.resolve(
        None, env_names=("EDGEWARN_TEST_TOKEN",), yaml_value="", key="api.security.token"
    )

    assert overlay.overrides() == {"api.security.token": "env:EDGEWARN_TEST_TOKEN"}


def test_an_unparseable_override_records_no_winner(monkeypatch):
    """The env layer raised instead of producing a value, so it did not win."""
    monkeypatch.setenv("EDGEWARN_TEST_ATTEMPTS", "not-a-number")

    with pytest.raises(ValueError):
        overlay.resolve(
            None, env_names=("EDGEWARN_TEST_ATTEMPTS",), yaml_value=3, key="sample.key"
        )

    assert overlay.overrides() == {}


def test_an_unnamed_key_resolves_without_being_recorded(monkeypatch):
    monkeypatch.setenv("EDGEWARN_TEST_ATTEMPTS", "9")

    resolved = overlay.resolve(None, env_names=("EDGEWARN_TEST_ATTEMPTS",), yaml_value=3)

    assert resolved == 9
    assert overlay.overrides() == {}


def test_the_last_read_wins_for_a_repeated_key(monkeypatch):
    """Accessors resolve per call, some inside poll loops, so this must not grow."""
    monkeypatch.setenv("EDGEWARN_TEST_ATTEMPTS", "9")
    overlay.resolve(
        None, env_names=("EDGEWARN_TEST_ATTEMPTS",), yaml_value=3, key="sample.key"
    )

    monkeypatch.delenv("EDGEWARN_TEST_ATTEMPTS")
    overlay.resolve(
        None, env_names=("EDGEWARN_TEST_ATTEMPTS",), yaml_value=3, key="sample.key"
    )

    assert overlay.overrides() == {}

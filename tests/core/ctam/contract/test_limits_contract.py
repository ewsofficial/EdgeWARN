"""Ties the numbers in the limits document to the numbers in the schemas.

``docs/ctam/internal-api-limits.md`` is the single place a limit is decided, but
several of those decisions are also expressed as machine-readable constraints --
a ``maxItems``, a pattern quantifier, a parameter default, an error example. Two
copies of a number drift, and the drift is silent because each artifact is
internally consistent.

So the table is parsed and compared. A row rename fails
``test_expected_limit_rows_are_present`` rather than quietly removing a tie, and
each tie names the mechanism the schema uses to express its limit, because those
mechanisms are indirect: a maximum length becomes a regex quantifier one smaller
than the limit, since the first character is matched separately.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.ctam

REPO_ROOT = Path(__file__).resolve().parents[4]
LIMITS_PATH = REPO_ROOT / "docs" / "ctam" / "internal-api-limits.md"
SCHEMA_DIR = REPO_ROOT / "docs" / "ctam" / "schema"
OPENAPI_PATH = REPO_ROOT / "docs" / "ctam" / "openapi" / "ctam-internal-v1.json"

# Only the rows something else depends on. The document holds more limits than
# this; a row absent here is enforced by host code the tests cannot reach yet.
TIED_ROWS = (
    "Maximum request body size",
    "Maximum operations per PATCH request",
    "Maximum module ID length",
    "Default history read window",
    "Maximum history read window",
)


def limits() -> dict[str, int]:
    """Row label to integer value, for rows whose value is a bare integer.

    ``Terminate-to-kill escalation`` is "5 then 1" and is skipped rather than
    parsed, because a two-stage value has no single number to tie anything to.
    """
    table: dict[str, int] = {}
    for line in LIMITS_PATH.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 2 or not cells[1].isdigit():
            continue
        label = cells[0]
        assert label not in table, f"duplicate limit row {label!r}"
        table[label] = int(cells[1])
    return table


def load_schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text(encoding="utf-8"))


def test_expected_limit_rows_are_present():
    """Guards every tie below: a renamed row would otherwise skip its check."""
    missing = [row for row in TIED_ROWS if row not in limits()]
    assert not missing, f"limit rows renamed or removed: {missing}"


def test_patch_operation_count_matches_the_schema_max_items():
    assert load_schema("patch-request.schema.json")["properties"]["operations"]["maxItems"] == (
        limits()["Maximum operations per PATCH request"]
    )


@pytest.mark.parametrize(
    "name",
    ("response-envelope.schema.json", "transaction.schema.json", "requirements-evaluation.schema.json"),
)
def test_module_id_pattern_bounds_match_the_documented_length(name):
    """The quantifier is one less than the limit: the first character is separate.

    ``^[a-z0-9][a-z0-9_-]{0,127}\\Z`` admits 1 to 128 characters. Asserting the
    quantifier alone would not catch a leading class being dropped, so the
    boundary is exercised against the pattern itself.
    """
    documented = limits()["Maximum module ID length"]
    schema = load_schema(name)
    patterns = {
        node["module_id"]["pattern"]
        for node in _module_id_holders(schema)
    }
    assert patterns, f"{name} declares no module_id pattern"
    for pattern in patterns:
        assert re.search(pattern, "a" * documented), f"{pattern!r} rejects a legal {documented}-character id"
        assert not re.search(pattern, "a" * (documented + 1)), f"{pattern!r} admits {documented + 1} characters"


def _module_id_holders(node):
    if isinstance(node, dict):
        if isinstance(node.get("module_id"), dict) and "pattern" in node["module_id"]:
            yield node
        for value in node.values():
            yield from _module_id_holders(value)
    elif isinstance(node, list):
        for value in node:
            yield from _module_id_holders(value)


def test_history_window_default_matches_the_openapi_parameter():
    doc = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    parameter = doc["components"]["parameters"]["historyLimit"]
    assert parameter["schema"]["default"] == limits()["Default history read window"]


def test_history_window_maximum_is_deliberately_not_declared():
    """A declared ``maximum`` would say "reject"; the document says "clamp".

    Pinning the absence keeps a future author from adding the bound as an
    apparent improvement and silently changing the contract from clamping an
    over-large request to failing it.
    """
    doc = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    parameter = doc["components"]["parameters"]["historyLimit"]
    assert "maximum" not in parameter["schema"]
    assert "clamped" in parameter["description"]
    assert limits()["Maximum history read window"] > limits()["Default history read window"]


def test_request_too_large_example_reports_the_documented_body_limit():
    """The example is what a module author reads to size their requests."""
    doc = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    example = doc["components"]["responses"]["RequestTooLarge"]["content"]["application/json"]["example"]
    limit_values = {error["limit"] for error in example["errors"] if "limit" in error}
    assert limit_values == {limits()["Maximum request body size"]}


def test_every_limit_row_states_where_it_is_enforced_and_what_happens():
    """A limit with no enforcement point is a number nobody will implement."""
    rows = 0
    for line in LIMITS_PATH.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != 5 or cells[0] in {"Limit", "---"} or set(cells[0]) == {"-"}:
            continue
        label, value, unit, enforced_at, on_excess = cells
        rows += 1
        assert value, f"{label}: no value"
        assert unit, f"{label}: no unit"
        assert enforced_at in {"Discovery", "Runtime", "Runner"}, f"{label}: {enforced_at!r}"
        assert on_excess, f"{label}: no behaviour on excess"
    assert rows >= 17, f"expected the full limits table, found {rows} rows"

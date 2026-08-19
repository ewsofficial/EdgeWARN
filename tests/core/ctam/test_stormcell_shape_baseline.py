"""Phase 0 characterization of the stormcell snapshot shape.

Phase 3 of ``plans/modular-ctam-internal-api-plan.md`` confines module writes to
the ``modules`` and ``properties`` containers of a cell entry via a JSON-Pointer
allowlist, and makes everything else unreachable. That allowlist is only correct
if the field inventory it is written against is correct, so this module freezes
the inventory and asserts the specific claims the plan makes about it.

The inventory is read out of ``save.py``'s dict literals with ``ast`` rather than
by constructing a cell at runtime, because building one requires radar arrays and
the full detection stack. The declaration is also closer to the real question:
"which keys does detection promise to emit".
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.core.ctam.baseline import assert_baseline, requires

pytestmark = pytest.mark.ctam

REPO_ROOT = Path(__file__).resolve().parents[3]
SAVE_PY = REPO_ROOT / "src" / "EdgeWARN" / "process" / "detect" / "tools" / "save.py"

# Fields the plan's "Patch and ownership rules" table lists as unreachable by any
# module patch. Kept literal so a plan edit and a code change cannot both drift
# unnoticed.
PLAN_IMMUTABLE_FIELDS = (
    "id",
    "centroid",
    "bbox",
    "hail_core",
    "max_refl",
    "num_gates",
    "event_type",
    "parent_ids",
    "split_from",
    "geometry",
)


def cell_literal_key_sets():
    """Key sets of every dict literal in ``save.py`` that builds a cell entry.

    Identified by the presence of ``num_gates``, which only appears in cell
    literals.
    """
    tree = ast.parse(SAVE_PY.read_text(encoding="utf-8"))
    key_sets = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = [k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)]
        if "num_gates" in keys:
            key_sets.append(tuple(keys))
    return key_sets


def test_cell_literal_sites_found():
    """Guards the ``ast`` reader itself: silently finding nothing would pass."""
    assert len(cell_literal_key_sets()) == 3


def test_cell_entry_field_inventory_baseline():
    assert_baseline("stormcell_entry_field_inventory", cell_literal_key_sets())


def test_all_cell_literals_agree():
    """The three construction sites must emit identical keys.

    A module allowlist derived from one site would be wrong for the others.
    """
    key_sets = cell_literal_key_sets()
    assert len({frozenset(keys) for keys in key_sets}) == 1


def test_geometry_is_not_a_cell_field():
    """Plan correction: cell entries have no ``geometry`` key.

    The plan lists ``/geometry`` among the immutable paths a patch must not
    reach. Nothing in ``src/EdgeWARN/process`` ever assigns it -- geometry is
    carried implicitly by ``centroid``, ``bbox``, and ``hail_core``. Listing a
    nonexistent field in the allowlist is harmless but misleading, and a reader
    could reasonably infer cells are GeoJSON features. They are not.
    """
    for keys in cell_literal_key_sets():
        assert "geometry" not in keys


def test_plan_immutable_fields_exist_except_geometry():
    """Every other field the plan calls immutable is really emitted at birth."""
    declared = set(cell_literal_key_sets()[0])
    expected = set(PLAN_IMMUTABLE_FIELDS) - {"geometry"}
    assert expected <= declared


def test_properties_container_seeded_with_morphology_only():
    """``properties`` is pre-populated shared state, not an empty namespace.

    The plan's ownership rule for ``properties`` depends on this: a module must
    not overwrite a key detection already wrote. ``morphology`` is the key
    present from birth.
    """
    tree = ast.parse(SAVE_PY.read_text(encoding="utf-8"))
    seeded = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        pairs = {
            k.value: v
            for k, v in zip(node.keys, node.values)
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        }
        if "num_gates" not in pairs:
            continue
        properties = pairs["properties"]
        assert isinstance(properties, ast.Dict), "properties should be an inline dict literal"
        seeded.append(
            tuple(
                k.value for k in properties.keys
                if isinstance(k, ast.Constant) and isinstance(k.value, str)
            )
        )
    assert seeded == [("morphology",), ("morphology",), ("morphology",)]


def test_modules_container_is_not_created_by_detection():
    """``modules`` is created by CTAM, not detection.

    ``ctam/engine.py`` seeds it. This is why ``run.py``'s grid attachment can
    raise ``KeyError`` when no cell modules are registered.
    """
    for keys in cell_literal_key_sets():
        assert "modules" not in keys


# ----------------------------------------------------------------------
# Snapshot envelope
# ----------------------------------------------------------------------

def test_snapshot_envelope_shape():
    """The snapshot is a flat dict, not a GeoJSON ``FeatureCollection``."""
    requires("xarray", "shapely")
    from EdgeWARN.process.detect.tools.save import CellDataSaver
    from util.release import get_release_version

    # Called off the class with a ``None`` self: the builder is pure, and
    # constructing a real CellDataSaver would require radar inputs.
    structure = CellDataSaver.create_json_structure(None, "20260805-120000", [{"id": 1}])

    assert list(structure) == ["source", "product", "version", "latest_timestamp", "features"]
    assert "type" not in structure
    assert structure["source"] == "Edgemont Weather Service"
    assert structure["product"] == "EdgeWARN Storm Cells"
    assert structure["latest_timestamp"] == "20260805-120000"
    assert structure["features"] == [{"id": 1}]
    # Snapshotted with the release version tokenized: a version bump is not a
    # CTAM behavior change, but a key rename is.
    assert structure["version"] == get_release_version()
    assert_baseline(
        "stormcell_snapshot_envelope",
        {**structure, "version": "<release-version>"},
    )


# ----------------------------------------------------------------------
# Grid module attachment
# ----------------------------------------------------------------------

def grid_module(name="Mesocyclone", attach=True):
    class DummyGridModule:
        def __init__(self):
            self.name = name

        def run(self):
            return {
                "features": {"type": "FeatureCollection", "features": []},
                "metadata": {"detection_count": 1},
                "timestamp": "2026-08-05T12:00:00+00:00",
                "attach_to_stormcells": attach,
            }

        def alerts(self, features):
            return None

    return DummyGridModule()


def run_with_grid_only(cells, module):
    from EdgeWARN.ctam.run import run_ctam

    with patch("EdgeWARN.ctam.run.CellModuleRegistry.get_all", return_value={}):
        with patch(
            "EdgeWARN.ctam.run.GridModuleRegistry.get_all",
            return_value={module.name: module},
        ):
            return run_ctam(cells)


def test_attachable_grid_output_lands_on_first_cell():
    requires("xarray", "shapely")
    cells = [{"id": 1, "modules": {}}, {"id": 2, "modules": {}}]
    result = run_with_grid_only(cells, grid_module())

    assert "_grid_outputs" in result[0]["modules"]
    assert "_grid_outputs" not in result[1]["modules"]
    assert list(result[0]["modules"]["_grid_outputs"]) == ["Mesocyclone"]


def test_grid_output_with_no_cells_creates_synthetic_entry():
    """An empty cell list is *replaced* by a single entry with no identity.

    The synthetic entry has no ``id``, ``timestamp``, or ``properties``, so it is
    skipped by history (``history.py`` requires both) and by the API index. The
    plan calls ``_grid_outputs`` a legacy compatibility concern; this is the
    shape any adapter has to keep working or deliberately drop.
    """
    requires("xarray", "shapely")
    result = run_with_grid_only([], grid_module())

    assert len(result) == 1
    assert list(result[0]) == ["modules"]
    assert list(result[0]["modules"]) == ["_grid_outputs"]
    assert_baseline(
        "stormcell_grid_only_synthetic_entry",
        {"top_level_keys": list(result[0]), "module_keys": list(result[0]["modules"])},
    )


def test_modules_container_is_created_even_with_a_grid_only_registry():
    """``run.py`` seeds ``modules`` on every cell before grid attachment.

    The per-cell loop calls ``initialize_modules`` unconditionally
    (``run.py:82-84``), with an empty ``module_names`` list when no cell modules
    are registered -- and ``initialize_modules`` still runs
    ``setdefault("modules", {})``. So ``cells[0]["modules"]`` in the attachment
    branch is always safe, even though detection never emits the container.
    Phase 4 must keep seeding it before any external module runs.
    """
    requires("xarray", "shapely")
    result = run_with_grid_only([{"id": 1}], grid_module())

    assert "modules" in result[0]
    assert list(result[0]["modules"]) == ["_grid_outputs"]


def test_no_modules_registered_returns_cells_untouched():
    """With both registries empty, cells are returned without a container."""
    requires("xarray", "shapely")
    from EdgeWARN.ctam.run import run_ctam

    with patch("EdgeWARN.ctam.run.CellModuleRegistry.get_all", return_value={}):
        with patch("EdgeWARN.ctam.run.GridModuleRegistry.get_all", return_value={}):
            result = run_ctam([{"id": 1}])

    assert result == [{"id": 1}]

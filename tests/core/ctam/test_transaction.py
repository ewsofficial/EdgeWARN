"""Phase 3 ownership and revision tests for the transport-neutral mutation core."""
from __future__ import annotations

from pathlib import Path

import pytest

from EdgeWARN.ctam.api.models import APIError
from EdgeWARN.ctam.manifest import ModuleManifest, ModuleWrite
from EdgeWARN.ctam.transaction import CTAMTransactionService, validate_patch_path
from tests.core.ctam.contract.test_pointer_allowlist import ALLOWED, HOST, TABLE


def manifest(tmp_path: Path) -> ModuleManifest:
    return ModuleManifest("cellstats", "CellStats", "1.0.0", "1", True, False, "stormcells", (), 10, (), (), (
        ModuleWrite("stormcells.current", "/features/*/modules/CellStats"),
        ModuleWrite("stormcells.current", "/features/*/properties/cellstats_severity"),
        ModuleWrite("cells.history", "/*/modules/CellStats"),
    ), tmp_path, tmp_path / "module.toml")


def service(tmp_path):
    return CTAMTransactionService(cells=[{"id": "7", "properties": {"morphology": "cluster"}, "geometry": [1]}], manifests={"cellstats": manifest(tmp_path)})


@pytest.mark.parametrize("pointer,verdict,_why", TABLE)
def test_shared_pointer_gate_enforces_contract_table(tmp_path, pointer, verdict, _why):
    if verdict == ALLOWED:
        assert validate_patch_path(manifest(tmp_path), pointer)
    elif verdict == HOST:
        with pytest.raises(APIError): validate_patch_path(manifest(tmp_path), pointer)


def test_commit_is_revisioned_idempotent_and_preserves_core_fields(tmp_path):
    transactions = service(tmp_path)
    result = transactions.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/modules/CellStats", "value": {"score": 4}}])
    assert result["staged_operations"] == 1
    committed = transactions.commit("cellstats", idempotency_key="same-request")
    assert committed["cell_revisions"] == {"7": 1}
    assert transactions.commit("cellstats", idempotency_key="same-request") == committed
    assert transactions.cells["7"]["modules"]["CellStats"] == {"score": 4}
    assert transactions.cells["7"]["geometry"] == [1]
    with pytest.raises(APIError) as error:
        transactions.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/modules/CellStats/next", "value": 1}])
    assert error.value.code == "transaction_sealed"


def test_invalid_or_host_owned_values_never_change_working_set(tmp_path):
    transactions = service(tmp_path)
    before = transactions.cells["7"].copy()
    with pytest.raises(APIError):
        transactions.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/properties/morphology", "value": "bad"}])
    with pytest.raises(APIError):
        transactions.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/modules/CellStats", "value": float("nan")}])
    assert transactions.cells["7"] == before


def test_stale_revision_is_rejected_before_staging(tmp_path):
    transactions = service(tmp_path)
    transactions.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/modules/CellStats", "value": {}}])
    transactions.commit("cellstats")
    second = CTAMTransactionService(cells=[transactions.cells["7"]], manifests={"cellstats": manifest(tmp_path)})
    second.cell_revisions["7"] = 1
    with pytest.raises(APIError) as error:
        second.stage_cell("cellstats", "7", revision=0, operations=[{"op": "add", "path": "/modules/CellStats/x", "value": 1}])
    assert error.value.code == "stale_revision"

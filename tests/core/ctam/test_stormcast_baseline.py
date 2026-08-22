"""Phase 0 golden fixtures for StormCast's observable output.

Freezes the payload StormCast writes to ``storm_entry["modules"]["StormCast"]``
and the alert it emits, across the success, skipped, and error paths. Phase 5 of
``plans/modular-ctam-internal-api-plan.md`` moves StormCast onto a host-service
boundary; these snapshots are what distinguishes that migration from a change in
forecast or alert content.

The alert tests also lock the Phase 6 compatibility decision: StormCast
publishes ``tstm_wind: "false"``.
"""

from __future__ import annotations

import json

import pytest

import util.file as fs
from tests.core.ctam.baseline import assert_baseline, requires

pytestmark = pytest.mark.ctam

requires("shapely", "xarray")

from EdgeWARN.alerts import AlertManager  # noqa: E402
from EdgeWARN.ctam.modules.StormCast import StormCastModule  # noqa: E402
from EdgeWARN.ctam.modules.StormCast.core import StormCastEngine  # noqa: E402

CURRENT_TS = "2026-08-05T12:00:00+00:00"
CELL_ID = 146904


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    """Point cell history and alert storage at ``tmp_path``.

    StormCast reads ``fs.CELL_DIR`` directly and ``AlertManager`` globs
    ``fs.EDGEWARN_ALERTS_IDS_DIR``; both resolve the attribute at call time, so
    setting them here fully isolates the test from the developer's real
    ``data/`` tree. Without this the alert-suppression path would depend on
    whatever alerts happen to be on the machine.
    """
    cell_dir = tmp_path / "cells"
    ids_dir = tmp_path / "alerts" / "ids"
    ts_dir = tmp_path / "alerts" / "timestamps"
    for directory in (cell_dir, ids_dir, ts_dir):
        directory.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(fs, "CELL_DIR", cell_dir)
    monkeypatch.setattr(fs, "EDGEWARN_ALERTS_IDS_DIR", ids_dir)
    monkeypatch.setattr(fs, "EDGEWARN_ALERTS_TS_DIR", ts_dir)
    return cell_dir


def make_cell(**overrides):
    """A realistic active cell with everything StormCast's success path needs.

    Longitude is in the 0-360 form detection writes, and ``bbox`` is a ring of
    ``[lat, lon]`` points rather than a bounding tuple, matching
    ``EdgeWARN.process.detect.tools.save``.
    """
    cell = {
        "id": CELL_ID,
        "timestamp": CURRENT_TS,
        "num_gates": 412,
        "centroid": [35.25, 262.75],
        "bbox": [
            [35.10, 262.60],
            [35.10, 262.90],
            [35.40, 262.90],
            [35.40, 262.60],
            [35.10, 262.60],
        ],
        "hail_core": [],
        "max_refl": 58.5,
        "event_type": "ACTIVE",
        "parent_ids": [],
        "split_from": None,
        "dx": 4200.0,
        "dy": 1500.0,
        "dt": 300.0,
        "properties": {
            "x": 0.0,
            "y": 0.0,
            "p100EchoTop30": 11.5,
            "EchoTop50": 9.25,
            "wind_field": {
                "u850": 12.0,
                "v850": 3.0,
                "u700": 15.0,
                "v700": 5.0,
                "u500": 22.0,
                "v500": 8.0,
                "u250": 35.0,
                "v250": 12.0,
            },
        },
        "modules": {},
    }
    cell.update(overrides)
    return cell


def write_history(cell_dir, entries):
    """Write ``data/cells/<id>.json`` -- a bare JSON array, no wrapper."""
    (cell_dir / f"{CELL_ID}.json").write_text(json.dumps(entries), encoding="utf-8")


def run_module(cell):
    StormCastModule().run(cell)
    return cell["modules"]["StormCast"]


# ----------------------------------------------------------------------
# Success, skipped, and error payloads
# ----------------------------------------------------------------------

def test_success_payload_dx_dy_fallback():
    """No history on disk: the single-frame dx/dy fallback produces a forecast."""
    result = run_module(make_cell())

    assert result["status"] == "success"
    assert set(result) == {
        "u",
        "v",
        "forecast_cones",
        "forecast_polygons",
        "polygon_0_30m",
        "status",
        "can_generate_alerts",
        "tracking_duration_min",
        "alert_blockers",
    }
    # A one-point track has no measurable duration; alert eligibility comes from
    # the polygon existing, not from the track being long.
    assert result["tracking_duration_min"] == 0.0
    assert result["can_generate_alerts"] is True
    assert_baseline("stormcast_success_dx_dy_fallback", result)


def test_success_payload_with_history(isolated_runtime):
    """Three history entries spanning ten minutes drive the multi-point path."""
    write_history(
        isolated_runtime,
        [
            {
                "id": CELL_ID,
                "timestamp": "2026-08-05T11:50:00+00:00",
                "centroid": [35.05, 262.45],
                "properties": {"p100EchoTop30": 9.0, "EchoTop50": 7.5},
            },
            {
                "id": CELL_ID,
                "timestamp": "2026-08-05T11:55:00+00:00",
                "centroid": [35.15, 262.60],
                "properties": {"p100EchoTop30": 10.5, "EchoTop50": 8.5},
            },
        ],
    )
    result = run_module(make_cell())

    assert result["status"] == "success"
    # 11:50 -> 12:00 inclusive of the synthetic current point.
    assert result["tracking_duration_min"] == 10.0
    assert_baseline("stormcast_success_with_history", result)


def test_history_read_ignores_entries_without_centroid(isolated_runtime):
    """A history entry lacking a centroid is dropped, not fatal."""
    write_history(
        isolated_runtime,
        [
            {"id": CELL_ID, "timestamp": "2026-08-05T11:50:00+00:00"},
            {
                "id": CELL_ID,
                "timestamp": "2026-08-05T11:55:00+00:00",
                "centroid": [35.15, 262.60],
                "properties": {},
            },
        ],
    )
    result = run_module(make_cell())

    assert result["status"] == "success"
    # Only 11:55 survived, so the track starts there rather than at 11:50.
    assert result["tracking_duration_min"] == 5.0
    assert_baseline("stormcast_success_history_missing_centroid", result)


def test_history_read_discards_future_points(isolated_runtime):
    """History newer than the current cycle is discarded, not extrapolated from."""
    write_history(
        isolated_runtime,
        [
            {
                "id": CELL_ID,
                "timestamp": "2026-08-05T11:55:00+00:00",
                "centroid": [35.15, 262.60],
                "properties": {},
            },
            {
                "id": CELL_ID,
                "timestamp": "2026-08-05T12:05:00+00:00",
                "centroid": [35.40, 263.10],
                "properties": {},
            },
        ],
    )
    result = run_module(make_cell())

    assert result["status"] == "success"
    assert result["tracking_duration_min"] == 5.0
    assert_baseline("stormcast_success_history_future_discarded", result)


@pytest.mark.parametrize(
    "motion",
    [
        pytest.param({"dx": None}, id="dx_none"),
        pytest.param({"dy": None}, id="dy_none"),
        pytest.param({"dt": None}, id="dt_none"),
        pytest.param({"dt": 0}, id="dt_zero"),
    ],
)
def test_skipped_without_motion(motion):
    result = run_module(make_cell(**motion))

    assert result == {
        "status": "skipped",
        "reason": "Insufficient motion data (missing dx, dy, or dt)",
    }


def test_skipped_without_motion_payload():
    assert_baseline("stormcast_skipped_no_motion", run_module(make_cell(dt=0)))


def test_skipped_without_winds():
    cell = make_cell()
    cell["properties"]["wind_field"] = {}
    result = run_module(cell)

    assert result == {
        "status": "skipped",
        "reason": "No wind data found (checked environment and properties)",
    }
    assert_baseline("stormcast_skipped_no_winds", result)


def test_skipped_when_wind_field_has_unpaired_component():
    """A level with ``u`` but no ``v`` contributes nothing."""
    cell = make_cell()
    cell["properties"]["wind_field"] = {"u850": 12.0}
    result = run_module(cell)

    assert result["status"] == "skipped"
    assert result["reason"] == "No wind data found (checked environment and properties)"


def test_error_payload(monkeypatch):
    """Any exception inside the forecast block becomes a two-key error payload."""

    def boom(self):
        raise ValueError("Environment profile not set for StormCast")

    monkeypatch.setattr(StormCastEngine, "generate_forecast", boom)
    result = run_module(make_cell())

    assert result == {
        "status": "error",
        "error": "Environment profile not set for StormCast",
    }
    assert_baseline("stormcast_error", result)


def test_skipped_and_error_payloads_omit_forecast_keys(monkeypatch):
    """The keys ``alerts()`` guards on are absent, not falsy, on the sad paths."""
    absent = {
        "u",
        "v",
        "forecast_cones",
        "forecast_polygons",
        "polygon_0_30m",
        "can_generate_alerts",
        "tracking_duration_min",
        "alert_blockers",
    }
    skipped = run_module(make_cell(dt=0))
    assert absent.isdisjoint(skipped)

    monkeypatch.setattr(
        StormCastEngine, "generate_forecast", lambda self: (_ for _ in ()).throw(ValueError("x"))
    )
    errored = run_module(make_cell())
    assert absent.isdisjoint(errored)


# ----------------------------------------------------------------------
# Alert payloads
# ----------------------------------------------------------------------

def emit_alert(cell):
    module = StormCastModule()
    module.run(cell)
    return module.alerts(cell)


def seed_prior_alert(ids_dir, alert):
    """Place a prior alert on disk without going through ``AlertManager.publish``.

    ``publish`` routes through ``util.atomic.atomic_write_json``, which fails on
    Windows: ``atomic_output_path`` calls ``os.fsync`` on a read-only descriptor
    (``src/util/atomic.py:36``), and Windows returns ``EBADF`` where Linux
    permits it. Eight tests in ``tests/core/alerts`` already fail on Windows for
    this reason. The cadence rule under test here is StormCast's, and
    ``AlertManager.load_all`` reads with a plain ``json.load``, so writing the
    file directly tests the intended behavior on both platforms.
    """
    safe_id = alert.id.replace(":", "_").replace("/", "_") + ".json"
    (ids_dir / safe_id).write_text(json.dumps(alert.to_dict()), encoding="utf-8")


def test_alert_payload_uses_release_baseline():
    """StormCast's alert payload no longer depends on another module."""
    cell = make_cell()
    alerts = emit_alert(cell)

    assert alerts is not None and len(alerts) == 1
    alert = alerts[0]
    assert cell["modules"]["StormCast"]["alert_outcome"] == "published"
    assert alert.threats == {"tstm_wind": "false"}


def test_tstm_wind_is_false_by_release_policy():
    alerts = emit_alert(make_cell())
    assert alerts is not None
    assert alerts[0].threats == {"tstm_wind": "false"}


def test_tstm_wind_is_the_only_threat():
    """Guards the Phase 6 decision: removing it empties ``threats`` entirely."""
    alerts = emit_alert(make_cell())
    assert list(alerts[0].threats) == ["tstm_wind"]


def test_alert_identity_and_window():
    """Alert ID format, the int ``cell_id``, and the 30-minute expiry window."""
    alerts = emit_alert(make_cell())
    alert = alerts[0]

    assert alert.alert_type == "TSTM"
    assert alert.source == "StormCast"
    assert alert.severity == "warning"
    # Declared as ``str`` on AlertPayload but an int in practice, and
    # ``AlertManager.load_all`` compares it to the JSON-decoded value.
    assert alert.cell_id == CELL_ID
    assert isinstance(alert.cell_id, int)
    assert alert.id == f"id:TSTM:StormCast:{CELL_ID}:2026.08.05.12.00.00"
    assert (alert.expiry_time - alert.effective_time).total_seconds() == 1800


def test_alert_geometry_rounded_to_four_places():
    serialized = emit_alert(make_cell())[0].to_dict()
    for lat, lon in serialized["geometry"]:
        assert round(lat, 4) == lat
        assert round(lon, 4) == lon


def test_alert_suppressed_by_refresh_spacing(isolated_runtime):
    """A prior alert inside the 15-minute window suppresses the replacement."""
    first = emit_alert(make_cell())
    seed_prior_alert(fs.EDGEWARN_ALERTS_IDS_DIR, first[0])
    assert AlertManager.load("StormCast", CELL_ID) is not None

    later = make_cell(timestamp="2026-08-05T12:05:00+00:00")
    assert emit_alert(later) is None

    result = later["modules"]["StormCast"]
    assert result["alert_outcome"] == "suppressed_refresh_spacing"
    assert result["next_alert_eligible_minutes"] == 10.0
    assert_baseline("stormcast_alert_outcome_suppressed", result["alert_outcome"])


def test_alert_emitted_after_refresh_window(isolated_runtime):
    first = emit_alert(make_cell())
    seed_prior_alert(fs.EDGEWARN_ALERTS_IDS_DIR, first[0])

    later = make_cell(timestamp="2026-08-05T12:15:00+00:00")
    assert emit_alert(later) is not None
    assert later["modules"]["StormCast"]["alert_outcome"] == "published"


def test_alert_outcome_not_eligible_on_skipped_cell():
    """A skipped cell gets no ``alert_outcome`` key at all."""
    cell = make_cell(dt=0)
    module = StormCastModule()
    module.run(cell)

    assert module.alerts(cell) is None
    assert "alert_outcome" not in cell["modules"]["StormCast"]


def test_alert_outcome_not_eligible_when_ineligible():
    """A successful cell that cannot generate alerts is marked, not silent."""
    cell = make_cell()
    module = StormCastModule()
    module.run(cell)
    cell["modules"]["StormCast"]["can_generate_alerts"] = False

    assert module.alerts(cell) is None
    assert cell["modules"]["StormCast"]["alert_outcome"] == "not_eligible"


def test_alert_outcome_eligible_missing_polygon():
    cell = make_cell()
    module = StormCastModule()
    module.run(cell)
    cell["modules"]["StormCast"]["polygon_0_30m"] = []

    assert module.alerts(cell) is None
    assert cell["modules"]["StormCast"]["alert_outcome"] == "eligible_missing_polygon"

from unittest.mock import patch


def test_ctam_runner_skips_non_attachable_grid_output():
    from EdgeWARN.ctam.run import run_ctam

    class DummyGridModule:
        name = "Mesocyclone"

        def run(self):
            return {
                "features": {"type": "FeatureCollection", "features": []},
                "metadata": {"detection_count": 1},
                "timestamp": "2024-01-01T00:00:00+00:00",
                "attach_to_stormcells": False,
            }

        def alerts(self, features):
            return None

    with patch("EdgeWARN.ctam.run.CellModuleRegistry.get_all", return_value={}):
        with patch("EdgeWARN.ctam.run.GridModuleRegistry.get_all", return_value={"Mesocyclone": DummyGridModule()}):
            cells = [{"id": 1, "modules": {}}]
            result = run_ctam(cells, timestamp="20240101-000000")

    assert "_grid_outputs" not in result[0]["modules"]


def test_ctam_runner_emits_stormcast_alert_summaries(capsys):
    from EdgeWARN.ctam.run import run_ctam

    class DummyStormCastAdapter:
        name = "StormCast"

        def __init__(self, service):
            self.service = service

        def run(self, cell):
            cell.setdefault("modules", {})
            cell["modules"]["StormCast"] = {
                "status": "success",
                "can_generate_alerts": False,
                "alert_blockers": ["missing_current_polygon", "forecast_polygon_unavailable"],
                "alert_outcome": "not_eligible",
            }

        def alerts(self, cell): return []
        def publish_alerts(self, alerts): return 0

    with patch("EdgeWARN.ctam.run.AlertManager.cleanup_expired", return_value=0):
        with patch("EdgeWARN.ctam.builtins.BuiltinStormCastAdapter", DummyStormCastAdapter):
            with patch("EdgeWARN.ctam.run.CellModuleRegistry.get_all", return_value={}):
                with patch("EdgeWARN.ctam.run.GridModuleRegistry.get_all", return_value={}):
                    cells = [{"id": 1, "modules": {}, "properties": {}}]
                    run_ctam(cells)

    captured = capsys.readouterr().out
    assert "StormCast summary: status[success=1] can_generate_alerts[true=0, false=1, none=0]" in captured
    assert "StormCast alert outcomes: not_eligible=1" in captured
    assert "StormCast alert blockers: forecast_polygon_unavailable=1, missing_current_polygon=1" in captured

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

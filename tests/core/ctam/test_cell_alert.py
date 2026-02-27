import pytest
import numpy as np
from shapely.geometry import Polygon, Point
from EdgeWARN.core.ctam.modules.CellAlert import CellAlertModule
from EdgeWARN.core.ctam.modules.Footprint import FootprintModule

class TestCellAlert:
    
    @pytest.fixture
    def footprint_module(self):
        return FootprintModule()
        
    @pytest.fixture
    def alert_module(self):
        return CellAlertModule()
        
    def test_alert_generation(self, footprint_module, alert_module):
        # 1. Create a mock storm cell
        cell = {
            "id": "Test_Cell_1",
            "bbox": [
                [35.0, 260.0], [35.1, 260.0], [35.1, 260.1], [35.0, 260.1], [35.0, 260.0]
            ],
            "modules": {
                "StormCast": {
                    "forecast_cones": [
                        {
                            "lead_time": 600,
                            "center": [35.2, 260.2],
                            "radius": 1000 # 1km
                        },
                        {
                            "lead_time": 1800,
                            "center": [35.4, 260.4],
                            "radius": 3000 # 3km
                        }
                    ]
                }
            }
        }
        
        # 2. Run FootprintModule
        footprint_module.run(cell)
        assert "polygon_obj" in cell
        assert cell["properties"]["polygon"] is not None
        
        # 3. Run CellAlertModule (scan_count will be 0 as history is empty)
        alert_module.run(cell)
        
        assert "CellAlert" in cell["modules"]
        alert_data = cell["modules"]["CellAlert"]
        assert "alert_polygon" in alert_data
        
        alert_poly = Polygon(alert_data["alert_polygon"])
        assert alert_poly.area > Polygon([(260.0, 35.0), (260.1, 35.0), (260.1, 35.1), (260.0, 35.1)]).area
        
        # 4. Verify it covers the forecast points
        # Forecast 1: (35.2, 260.2) -> (260.2, 35.2) in lon/lat
        assert alert_poly.contains(Point(260.2, 35.2))
        # Forecast 2: (35.4, 260.4) -> (260.4, 35.4)
        assert alert_poly.contains(Point(260.4, 35.4))

    def test_frequency_logic(self, footprint_module, alert_module, tmp_path, monkeypatch):
        # Mock CELL_DIR to use tmp_path
        import util.file as fs
        monkeypatch.setattr(fs, "CELL_DIR", tmp_path)
        
        cell_id = "Freq_Cell"
        cell = {
            "id": cell_id,
            "bbox": [[35.0, 260.0], [35.1, 260.0], [35.1, 260.1], [35.0, 260.1]],
            "modules": {}
        }
        
        footprint_module.run(cell)
        
        # First scan (count 0) -> Updated
        alert_module.run(cell)
        assert cell["modules"]["CellAlert"]["status"] == "updated"
        
        # Save history item
        import json
        with open(tmp_path / f"{cell_id}.json", 'w') as f:
            json.dump([cell], f, default=str)
            
        # Second scan (count 1) -> Reused
        cell["modules"].pop("CellAlert")
        alert_module.run(cell)
        assert cell["modules"]["CellAlert"]["status"] == "reused"
        
        # Add second item to history
        with open(tmp_path / f"{cell_id}.json", 'w') as f:
            json.dump([cell, cell], f, default=str)
            
        # Third scan (count 2) -> Reused
        cell["modules"].pop("CellAlert")
        alert_module.run(cell)
        assert cell["modules"]["CellAlert"]["status"] == "reused"
        
        # Add third item to history
        with open(tmp_path / f"{cell_id}.json", 'w') as f:
            json.dump([cell, cell, cell], f, default=str)
            
        # Fourth scan (count 3) -> Updated (since 3 % 3 == 0)
        cell["modules"].pop("CellAlert")
        alert_module.run(cell)
        assert cell["modules"]["CellAlert"]["status"] == "updated"

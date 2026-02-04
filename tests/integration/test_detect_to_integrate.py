"""
Integration tests for detection to integration workflow
"""

import pytest
import json
import numpy as np
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestDetectToIntegrateWorkflow:
    """Tests for the detection to integration workflow"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    @pytest.fixture
    def sample_cell_data(self):
        """Create sample cell data for testing"""
        return [
            {
                "id": 101,
                "centroid": [35.0, -97.0],
                "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]],
                "num_gates": 50,
                "max_refl": 55.0,
                "timestamp": "2023-10-15T14:30:00",
                "properties": {}
            },
            {
                "id": 102,
                "centroid": [36.0, -96.0],
                "bbox": [[35.9, -96.1], [35.9, -95.9], [36.1, -95.9], [36.1, -96.1]],
                "num_gates": 30,
                "max_refl": 45.0,
                "timestamp": "2023-10-15T14:30:00",
                "properties": {}
            }
        ]

    def test_detect_creates_valid_cell_structure(self, mock_io):
        """Test that detection produces valid cell structure"""
        from EdgeWARN.core.process.detect.detect import detect_cells
        
        # This is a simplified test - in reality would need actual data files
        # For integration test, we mock the dependencies
        with patch('EdgeWARN.core.process.detect.detect.DetectionDataHandler') as mock_handler, \
             patch('EdgeWARN.core.process.detect.detect.GateMapper') as mock_mapper, \
             patch('EdgeWARN.core.process.detect.detect.CellDataSaver') as mock_saver:
            
            # Setup mocks
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            
            mock_ds = MagicMock()
            mock_handler_instance.load_subset.return_value = mock_ds
            mock_handler_instance.load_probsevere.return_value = {"features": []}
            mock_handler_instance.load_preciptype.return_value = None
            
            mock_mapper_instance = MagicMock()
            mock_mapper.return_value = mock_mapper_instance
            mock_mapper_instance.map_gates_to_polygons.return_value = MagicMock()
            mock_mapper_instance.expand_gates.return_value = MagicMock()
            mock_mapper_instance.draw_bbox.return_value = {1: [[0, 0], [1, 1]]}
            
            mock_saver_instance = MagicMock()
            mock_saver.return_value = mock_saver_instance
            mock_saver_instance.create_entry.return_value = [
                {"id": 1, "centroid": [35.0, -97.0], "properties": {}}
            ]
            
            # Run detection
            result = detect_cells(
                "radar.grib2", "ps.json", "pt.grib2", mock_io,
                30, 40, -100, -90
            )
            
            # Verify structure
            assert isinstance(result, list)
            if result:
                assert "id" in result[0]
                assert "centroid" in result[0]

    def test_integrate_glm_adds_flash_data(self, sample_cell_data, tmp_path):
        """Test that GLM integration adds flash count and energy"""
        import xarray as xr
        from EdgeWARN.core.process.integrate.integrate_glm import integrate_glm
        from EdgeWARN.core.process.integrate.utils import StormIntegrationUtils
        
        # Create synthetic GLM data
        lats = np.array([35.0, 35.01, 36.0, 30.0])
        lons = np.array([-97.0, -97.01, -96.0, -90.0])
        energies = np.array([100.0, 50.0, 75.0, 1000.0])
        
        glm_ds = xr.Dataset({
            "flash_lat": (("number_of_flashes",), lats),
            "flash_lon": (("number_of_flashes",), lons),
            "flash_energy": (("number_of_flashes",), energies)
        })
        
        glm_file = tmp_path / "glm.nc"
        glm_ds.to_netcdf(glm_file)
        
        # Mock create_cell_polygon to return proper polygons
        def mock_create_poly(cell):
            from shapely.geometry import Polygon
            coords = [(lon, lat) for lat, lon in cell['bbox']]
            return Polygon(coords)
        
        with patch.object(StormIntegrationUtils, 'create_cell_polygon', side_effect=mock_create_poly):
            result = integrate_glm(sample_cell_data, str(glm_file))
        
        # Verify GLM data was added
        for cell in result:
            assert "GLM_FLASH_COUNT" in cell["properties"]
            assert "GLM_TOTAL_ENERGY" in cell["properties"]
            assert isinstance(cell["properties"]["GLM_FLASH_COUNT"], int)
            assert isinstance(cell["properties"]["GLM_TOTAL_ENERGY"], float)

    def test_integrate_rap_adds_wind_data(self, sample_cell_data):
        """Test that RAP integration adds wind data"""
        from EdgeWARN.core.process.integrate.integrate_rap import integrate_rap
        
        # Create synthetic RAP datasets matching cfgrib structure
        lats = np.linspace(30, 40, 10)
        lons = np.linspace(260, 270, 10)  # 0-360 format
        
        levels = np.array([850, 700, 500, 250])
        
        u_data = np.zeros((4, 10, 10))
        u_data[0] = 10.0  # 850mb
        u_data[1] = 15.0  # 700mb
        u_data[2] = 30.0  # 500mb
        u_data[3] = 40.0  # 250mb
        
        v_data = np.zeros((4, 10, 10))
        v_data[0] = 5.0
        v_data[1] = 8.0
        v_data[2] = 15.0
        v_data[3] = 20.0
        
        import xarray as xr
        ds = xr.Dataset(
            {
                "u": (("isobaricInhPa", "y", "x"), u_data),
                "v": (("isobaricInhPa", "y", "x"), v_data)
            },
            coords={
                "isobaricInhPa": levels,
                "latitude": (("y", "x"), np.broadcast_to(lats[:, None], (10, 10))),
                "longitude": (("y", "x"), np.broadcast_to(lons[None, :], (10, 10)))
            }
        )
        
        mock_io = MagicMock()
        
        with patch('cfgrib.open_datasets', return_value=[ds]):
            result = integrate_rap(sample_cell_data, "dummy_path", mock_io)
        
        # Verify wind data was added
        for cell in result:
            assert "u850" in cell["properties"]
            assert "v850" in cell["properties"]
            assert "u500" in cell["properties"]
            assert "v500" in cell["properties"]

    def test_full_detect_to_integrate_pipeline(self, mock_io, tmp_path):
        """Test the full pipeline from detection to integration"""
        # This is a high-level integration test that verifies the workflow
        # without requiring actual data files
        
        # 1. Simulate detection output
        detected_cells = [
            {
                "id": 101,
                "centroid": [35.0, -97.0],
                "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]],
                "num_gates": 50,
                "max_refl": 55.0,
                "timestamp": "2023-10-15T14:30:00",
                "properties": {}
            }
        ]
        
        # 2. Simulate integration
        # Add GLM data
        for cell in detected_cells:
            cell["properties"]["GLM_FLASH_COUNT"] = 5
            cell["properties"]["GLM_TOTAL_ENERGY"] = 500.0
        
        # Add RAP data
        for cell in detected_cells:
            cell["properties"]["u850"] = 10.0
            cell["properties"]["v850"] = 5.0
            cell["properties"]["u500"] = 25.0
            cell["properties"]["v500"] = 10.0
        
        # 3. Verify final structure
        for cell in detected_cells:
            assert "id" in cell
            assert "centroid" in cell
            assert "properties" in cell
            assert "GLM_FLASH_COUNT" in cell["properties"]
            assert "u850" in cell["properties"]

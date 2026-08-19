"""
Integration tests for ingest to detect workflow
"""

import pytest
import json
import numpy as np
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import xarray as xr

from EdgeWARN.process.detect.config import DetectionConfig


class TestIngestToDetectWorkflow:
    """Tests for ingest to detection workflow"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    @pytest.fixture
    def tmp_path(self):
        """Create a temporary path for testing"""
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as tmp:
            yield Path(tmp)

    def test_mrms_ingest_to_detect(self, mock_io, tmp_path):
        """Test MRMS data ingestion to detection"""
        # Create synthetic MRMS radar data
        lats = np.linspace(30, 40, 10)
        lons = np.linspace(260, 270, 10)
        lats_grid, lons_grid = np.meshgrid(lats, lons, indexing='ij')
        
        # Create reflectivity data with a storm cell
        refl_data = np.zeros((10, 10))
        refl_data[4:6, 4:6] = 50.0  # Storm cell
        refl_data[3:7, 3:7] = 40.0  # Surrounding
        
        # radar_ds created using global xr import
        radar_ds = xr.Dataset({
            'unknown': (('latitude', 'longitude'), refl_data)
        }, coords={
            'latitude': lats,
            'longitude': lons
        })
        
        radar_file = tmp_path / "radar.grib2"
        radar_ds.to_netcdf(radar_file)
        
        # Create ProbSevere data
        ps_data = {
            "features": [
                {
                    "properties": {"ID": 1},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[265, 35], [265, 36], [266, 36], [266, 35], [265, 35]]]
                    }
                }
            ]
        }
        
        ps_file = tmp_path / "probsevere.json"
        ps_file.write_text(json.dumps(ps_data))
        
        # Mock detection
        with patch('EdgeWARN.process.detect.detect.DetectionDataHandler') as mock_handler, \
             patch('EdgeWARN.process.detect.detect.GateMapper') as mock_mapper, \
             patch('EdgeWARN.process.detect.detect.CellDataSaver') as mock_saver:
            
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            
            mock_ds = MagicMock()
            mock_handler_instance.load_subset.return_value = mock_ds
            mock_handler_instance.load_probsevere.return_value = ps_data
            
            mock_mapper_instance = MagicMock()
            mock_mapper.return_value = mock_mapper_instance
            mock_mapper_instance.map_gates_to_polygons.return_value = MagicMock()
            mock_mapper_instance.expand_gates.return_value = MagicMock()
            mock_mapper_instance.draw_bbox.return_value = {1: [[4, 4], [4, 6], [6, 6], [4, 6]]}
            
            mock_saver_instance = MagicMock()
            mock_saver.return_value = mock_saver_instance
            mock_saver_instance.create_entry.return_value = [
                {"id": 1, "centroid": [35.5, 265.5], "num_gates": 4, "max_refl": 50.0}
            ]
            
            # Run detection
            from EdgeWARN.process.detect.detect import detect_cells
            result = detect_cells(
                str(radar_file), str(ps_file), None, mock_io,
                30, 40, 260, 270,
                detection_config=DetectionConfig.from_yaml(),
            )
            
            # Verify detection found the storm cell
            assert len(result) == 1
            assert result[0]["id"] == 1
            assert result[0]["max_refl"] == 50.0

    def test_nws_ingest_to_detect(self, mock_io, tmp_path):
        """Test NWS data ingestion to detection"""
        # Create synthetic NWS alert data
        nws_data = {
            "features": [
                {
                    "properties": {
                        "event": "Severe Thunderstorm Warning",
                        "geocode": {"SAME": ["048121"]}
                    },
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[265, 35], [265, 36], [266, 36], [266, 35], [265, 35]]]
                    }
                }
            ]
        }
        
        nws_file = tmp_path / "nws_alerts.json"
        nws_file.write_text(json.dumps(nws_data))
        
        # Mock detection
        with patch('EdgeWARN.process.detect.detect.DetectionDataHandler') as mock_handler, \
             patch('EdgeWARN.process.detect.detect.GateMapper') as mock_mapper, \
             patch('EdgeWARN.process.detect.detect.CellDataSaver') as mock_saver:
            
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            
            mock_ds = MagicMock()
            mock_handler_instance.load_subset.return_value = mock_ds
            mock_handler_instance.load_probsevere.return_value = nws_data
            
            mock_mapper_instance = MagicMock()
            mock_mapper.return_value = mock_mapper_instance
            mock_mapper_instance.map_gates_to_polygons.return_value = MagicMock()
            mock_mapper_instance.expand_gates.return_value = MagicMock()
            mock_mapper_instance.draw_bbox.return_value = {1: [[4, 4], [4, 6], [6, 6], [4, 6]]}
            
            mock_saver_instance = MagicMock()
            mock_saver.return_value = mock_saver_instance
            mock_saver_instance.create_entry.return_value = [
                {"id": 1, "centroid": [35.5, 265.5], "num_gates": 4, "max_refl": 50.0}
            ]
            
            # Create a dummy radar file for the test
            radar_file = tmp_path / "radar.grib2"
            xr.Dataset().to_netcdf(radar_file)

            # Run detection
            from EdgeWARN.process.detect.detect import detect_cells
            result = detect_cells(
                str(radar_file), str(nws_file), None, mock_io,
                30, 40, 260, 270,
                detection_config=DetectionConfig.from_yaml(),
            )
            
            # Verify detection found the storm cell
            assert len(result) == 1
            assert result[0]["id"] == 1



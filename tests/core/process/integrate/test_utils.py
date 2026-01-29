"""
Tests for integration utilities module
"""

import pytest
import numpy as np
from unittest.mock import MagicMock, patch
from EdgeWARN.core.process.integrate.utils import (
    RAPFileHandler,
    StormIntegrationUtils
)


class TestRAPFileHandler:
    """Tests for RAPFileHandler class"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    def test_initialization(self, mock_io):
        """Test handler initialization"""
        handler = RAPFileHandler(mock_io)
        
        assert handler.io == mock_io

    def test_get_isobaric_dataset_success(self, mock_io):
        """Test successful dataset retrieval"""
        handler = RAPFileHandler(mock_io)
        
        # Mock cfgrib.open_datasets
        mock_ds = MagicMock()
        mock_ds.data_vars = {'u': MagicMock(), 'v': MagicMock()}
        mock_ds.coords = {'isobaricInhPa': MagicMock()}
        mock_ds.isobaricInhPa.values = np.array([850, 700, 500, 250])
        
        with patch('cfgrib.open_datasets', return_value=[mock_ds]):
            result = handler.get_isobaric_dataset("dummy_path")
            
            assert result == mock_ds

    def test_get_isobaric_dataset_no_u_v(self, mock_io):
        """Test handling when u/v not found"""
        handler = RAPFileHandler(mock_io)
        
        # Mock dataset without u/v
        mock_ds = MagicMock()
        mock_ds.data_vars = {'other_var': MagicMock()}
        mock_ds.coords = {'isobaricInhPa': MagicMock()}
        
        with patch('cfgrib.open_datasets', return_value=[mock_ds]):
            result = handler.get_isobaric_dataset("dummy_path")
            
            assert result is None
            mock_io.write_error.assert_called_once()

    def test_get_isobaric_dataset_no_levels(self, mock_io):
        """Test handling when isobaricInhPa not found"""
        handler = RAPFileHandler(mock_io)
        
        # Mock dataset without isobaricInhPa
        mock_ds = MagicMock()
        mock_ds.data_vars = {'u': MagicMock(), 'v': MagicMock()}
        mock_ds.coords = {'other_coord': MagicMock()}
        
        with patch('cfgrib.open_datasets', return_value=[mock_ds]):
            result = handler.get_isobaric_dataset("dummy_path")
            
            assert result is None
            mock_io.write_error.assert_called_once()

    def test_get_isobaric_dataset_fallback(self, mock_io):
        """Test fallback to general approach"""
        handler = RAPFileHandler(mock_io)
        
        # Mock filtered approach failure
        mock_ds_filtered = MagicMock()
        mock_ds_filtered.data_vars = {'other': MagicMock()}
        mock_ds_filtered.coords = {'isobaricInhPa': MagicMock()}
        
        # Mock general approach success
        mock_ds_general = MagicMock()
        mock_ds_general.data_vars = {'u': MagicMock(), 'v': MagicMock()}
        mock_ds_general.coords = {'isobaricInhPa': MagicMock()}
        mock_ds_general.isobaricInhPa.values = np.array([850, 700, 500, 250])
        
        with patch('cfgrib.open_datasets', return_value=[mock_ds_filtered, mock_ds_general]):
            result = handler.get_isobaric_dataset("dummy_path")
            
            assert result == mock_ds_general


class TestStormIntegrationUtils:
    """Tests for StormIntegrationUtils"""

    def test_create_cell_polygon_from_bbox(self):
        """Test creating polygon from bbox"""
        cell = {
            "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]]
        }
        
        poly = StormIntegrationUtils.create_cell_polygon(cell)
        
        # Should return a Shapely Polygon
        assert poly is not None
        assert hasattr(poly, 'exterior')

    def test_create_cell_polygon_from_properties(self):
        """Test creating polygon from properties"""
        cell = {
            "properties": {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]]]
                }
            }
        }
        }
        
        poly = StormIntegrationUtils.create_cell_polygon(cell)
        
        assert poly is not None

    def test_create_cell_polygon_missing_geometry(self):
        """Test handling when geometry is missing"""
        cell = {
            "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]]
        }
        
        poly = StormIntegrationUtils.create_cell_polygon(cell)
        
        # Should return None
        assert poly is None

    def test_extract_timestamp_from_cell(self):
        """Test extracting timestamp from cell"""
        cell = {
            "timestamp": "2023-10-15T14:30:00",
            "properties": {
                "timestamp": "2023-10-15T14:30:00"
            }
        }
        
        ts = StormIntegrationUtils.extract_timestamp(cell)
        
        assert ts == "2023-10-15T14:30:00"

    def test_extract_timestamp_from_properties(self):
        """Test extracting timestamp from properties"""
        cell = {
            "properties": {
                "timestamp": "2023-10-15T14:30:00"
            }
        }
        
        ts = StormIntegrationUtils.extract_timestamp(cell)
        
        assert ts == "2023-10-15T14:30:00"

    def test_extract_timestamp_missing(self):
        """Test handling when timestamp is missing"""
        cell = {
            "id": 101,
            "properties": {}
        }
        
        ts = StormIntegrationUtils.extract_timestamp(cell)
        
        assert ts is None

    def test_extract_centroid_from_cell(self):
        """Test extracting centroid from cell"""
        cell = {
            "centroid": [35.0, -97.0]
        }
        
        centroid = StormIntegrationUtils.extract_centroid(cell)
        
        assert centroid == [35.0, -97.0]

    def test_extract_centroid_from_properties(self):
        """Test extracting centroid from properties"""
        cell = {
            "properties": {
                "centroid": [35.0, -97.0]
            }
        }
        
        centroid = StormIntegrationUtils.extract_centroid(cell)
        
        assert centroid == [35.0, -97.0]

    def test_extract_centroid_missing(self):
        """Test handling when centroid is missing"""
        cell = {
            "id": 101,
            "properties": {}
        }
        
        centroid = StormIntegrationUtils.extract_centroid(cell)
        
        assert centroid is None

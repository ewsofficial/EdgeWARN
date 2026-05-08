"""
Tests for file handler utility module
"""

import pytest
import json
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from util.handler import extract_timestamp, FileHandler


class TestExtractTimestamp:
    """Tests for extract_timestamp function"""

    def test_mrms_format_with_underscore(self):
        """Test MRMS format: YYYYMMDD_HHMMSS"""
        result = extract_timestamp("MRMS_Reflectivity_20231015_143000.grib2")
        
        assert result.year == 2023
        assert result.month == 10
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0

    def test_mrms_format_with_dash(self):
        """Test MRMS format: YYYYMMDD-HHMMSS"""
        result = extract_timestamp("stormcells_20231015-143000.json")
        
        assert result.year == 2023
        assert result.month == 10
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 0

    def test_goes_format(self):
        """Test GOES format: sYYYYDDDHHMMSST"""
        # s20232881430001 = 2023, day 288 (Oct 15), 14:30:00.1
        result = extract_timestamp("OR_GLM-L2-LCFA_G16_s20232881430000_e20232881430000.nc")
        
        assert result.year == 2023
        # Day 288 of 2023 is October 15
        assert result.month == 10
        assert result.day == 15
        assert result.hour == 14
        assert result.minute == 30

    def test_unknown_format_returns_none(self):
        """Test that unknown formats return None"""
        result = extract_timestamp("random_file.txt")
        assert result is None

    def test_round_to_minute(self):
        """Test round_to_minute parameter"""
        result = extract_timestamp(
            "MRMS_Reflectivity_20231015_143059.grib2",
            round_to_minute=True
        )
        
        assert result.second == 0
        assert result.microsecond == 0

    def test_use_timezone_utc(self):
        """Test use_timezone_utc parameter"""
        result = extract_timestamp(
            "MRMS_Reflectivity_20231015_143000.grib2",
            use_timezone_utc=True
        )
        
        assert result.tzinfo == timezone.utc

    def test_direct_timestamp_string_with_dash_uses_utc(self):
        result = extract_timestamp("20260507-150000", use_timezone_utc=True)

        assert result == datetime(2026, 5, 7, 15, 0, 0, tzinfo=timezone.utc)

    def test_direct_timestamp_string_with_underscore_uses_utc(self):
        result = extract_timestamp("20260507_150000", use_timezone_utc=True)

        assert result == datetime(2026, 5, 7, 15, 0, 0, tzinfo=timezone.utc)

    def test_iso_timestamp_string_z_uses_utc(self):
        result = extract_timestamp("2026-05-07T15:00:00Z", use_timezone_utc=True)

        assert result == datetime(2026, 5, 7, 15, 0, 0, tzinfo=timezone.utc)

    def test_iso_timestamp_string_with_offset_normalizes_to_utc(self):
        result = extract_timestamp("2026-05-07T11:00:00-04:00", use_timezone_utc=True)

        assert result == datetime(2026, 5, 7, 15, 0, 0, tzinfo=timezone.utc)

    def test_isoformat_output(self):
        """Test isoformat parameter"""
        result = extract_timestamp(
            "MRMS_Reflectivity_20231015_143000.grib2",
            isoformat=True
        )
        
        assert isinstance(result, str)
        assert "2023-10-15T14:30:00" in result


class TestFileHandler:
    """Tests for FileHandler class"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    @pytest.fixture
    def handler(self, mock_io):
        """Create a FileHandler instance"""
        return FileHandler(mock_io)

    def test_load_dataset_none_path(self, handler, mock_io):
        """Test load_dataset with None path"""
        result = handler.load_dataset(None)
        
        assert result is None
        mock_io.write_warning.assert_called_once()

    def test_load_dataset_json_file(self, handler, tmp_path):
        """Test loading JSON file"""
        json_file = tmp_path / "test.json"
        test_data = {"id": 1, "name": "test"}
        json_file.write_text(json.dumps(test_data))
        
        result = handler.load_dataset(str(json_file))
        
        assert result == test_data

    def test_load_dataset_json_file_error(self, handler, mock_io, tmp_path):
        """Test loading invalid JSON file"""
        json_file = tmp_path / "invalid.json"
        json_file.write_text("{invalid json}")
        
        result = handler.load_dataset(str(json_file))
        
        assert result is None
        mock_io.write_error.assert_called_once()

    @patch('util.grib_loader.load_grib_fast')
    def test_load_dataset_grib2_file(self, mock_load_grib, handler, tmp_path):
        """Test loading GRIB2 file"""
        grib_file = tmp_path / "test.grib2"
        grib_file.touch()
        
        mock_ds = MagicMock()
        mock_load_grib.return_value = mock_ds
        
        result = handler.load_dataset(str(grib_file))
        
        assert result is not None
        mock_load_grib.assert_called_once()

    @patch('xarray.open_dataset')
    def test_load_dataset_netcdf_file(self, mock_xr_open, handler, tmp_path):
        """Test loading NetCDF file"""
        nc_file = tmp_path / "test.nc"
        nc_file.touch()
        
        mock_ds = MagicMock()
        mock_xr_open.return_value = mock_ds
        
        result = handler.load_dataset(str(nc_file))
        
        assert result is not None
        mock_xr_open.assert_called_once()

    def test_load_dataset_unsupported_extension(self, handler, mock_io, tmp_path):
        """Test loading file with unsupported extension"""
        txt_file = tmp_path / "test.txt"
        txt_file.touch()
        
        result = handler.load_dataset(str(txt_file))
        
        assert result is None

    @patch('xarray.open_dataset')
    def test_load_dataset_with_subsetting(self, mock_xr_open, handler, tmp_path):
        """Test load_dataset with lat/lon limits"""
        nc_file = tmp_path / "test.nc"
        nc_file.touch()
        
        mock_ds = MagicMock()
        mock_ds.latitude.values = np.array([20, 30, 40, 50])
        mock_ds.longitude.values = np.array([200, 210, 220, 230, 240])
        mock_xr_open.return_value = mock_ds
        
        result = handler.load_dataset(
            str(nc_file),
            lat_limits=(25, 45),
            lon_limits=(210, 230)
        )
        
        assert result is not None

    def test_subset_dataset(self, handler):
        """Test subset_dataset method"""
        # Create a mock dataset
        mock_ds = MagicMock()
        mock_ds.latitude.values = np.array([20, 30, 40, 50, 60])
        mock_ds.longitude.values = np.array([200, 210, 220, 230, 240, 250])
        mock_ds.dims = {'latitude': 5, 'longitude': 6}
        
        # Mock the isel and sel methods
        mock_subset = MagicMock()
        mock_ds.isel.return_value = mock_subset
        
        result = handler.subset_dataset(mock_ds, (30, 50), (210, 230))
        
        assert result is not None

    def test_subset_dataset_1d_coords(self, handler):
        """Test subset_dataset with 1D coordinates"""
        mock_ds = MagicMock()
        mock_ds.latitude.values = np.array([20, 30, 40, 50])
        mock_ds.longitude.values = np.array([200, 210, 220, 230])
        mock_ds.dims = {'latitude': 4, 'longitude': 4}
        
        mock_subset = MagicMock()
        mock_ds.isel.return_value = mock_subset
        
        result = handler.subset_dataset(mock_ds, (25, 45), (205, 225))
        
        assert result is not None

    def test_subset_dataset_2d_coords(self, handler):
        """Test subset_dataset with 2D coordinates"""
        mock_ds = MagicMock()
        # 2D coordinate arrays
        mock_ds.latitude.values = np.array([[20, 21], [30, 31]])
        mock_ds.longitude.values = np.array([[200, 201], [210, 211]])
        mock_ds.dims = {'x': 2, 'y': 2}
        
        mock_subset = MagicMock()
        mock_ds.isel.return_value = mock_subset
        
        result = handler.subset_dataset(mock_ds, (25, 35), (205, 215))
        
        assert result is not None

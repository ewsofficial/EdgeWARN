"""
Tests for NWS ingest main module
"""

import pytest
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from EdgeWARN.core.ingest.nws.main import download_alerts


class TestDownloadAlerts:
    """Tests for download_alerts function"""

    @pytest.fixture
    def mock_io(self):
        """Mock the module-level io_manager"""
        with patch('EdgeWARN.core.ingest.nws.main.io_manager') as mock:
            yield mock

    @pytest.fixture
    def empty_response(self):
        """Return a valid empty GeoJSON response"""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "type": "FeatureCollection",
            "features": []
        }).encode('utf-8')
        mock_resp.__enter__.return_value = mock_resp
        return mock_resp

    def test_download_creates_output_directory(self, mock_io, empty_response, tmp_path):
        """Test that output directory is created"""
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', return_value=empty_response):
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            assert tmp_path.exists()

    def test_download_creates_correct_filename(self, mock_io, empty_response, tmp_path):
        """Test that correct filename is created"""
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', return_value=empty_response):
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should create file with format: alerts_active_YYYYMMDD-HHMM00.json
            expected_file = tmp_path / "alerts_active_20231015-143000.json"
            assert expected_file.exists()

    def test_download_filters_dropped_events(self, mock_io, tmp_path):
        """Test that events in DROPPED_EVENTS are excluded"""
        # Mock urllib response with mixed events
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "features": [
                {
                    "properties": {
                        "event": "Severe Thunderstorm Warning",
                        "geocode": {"SAME": ["048121"]}
                    },
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]}
                },
                {
                    "properties": {
                        "event": "Administrative Message",  # Should be dropped
                        "geocode": {"SAME": ["048121"]}
                    },
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]}
                }
            ]
        }).encode('utf-8')
        mock_response.__enter__.return_value = mock_response
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', return_value=mock_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Read output file
            output_file = tmp_path / "alerts_active_20231015-143000.json"
            with open(output_file) as f:
                data = json.load(f)
            
            # Should only include Severe Thunderstorm Warning
            assert len(data["features"]) == 1
            events = [f["properties"]["event"] for f in data["features"]]
            assert "Severe Thunderstorm Warning" in events
            assert "Administrative Message" not in events

    def test_download_applies_geomapper(self, mock_io, tmp_path):
        """Test that GeoMapper is applied to alerts"""
        # Mock urllib response with alert that has geocode
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "features": [
                {
                    "properties": {
                        "event": "Severe Thunderstorm Warning",
                        "geocode": {"SAME": ["048121"]},
                        "references": "should be removed"
                    },
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]}
                }
            ]
        }).encode('utf-8')
        mock_response.__enter__.return_value = mock_response
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', return_value=mock_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Read output file
            output_file = tmp_path / "alerts_active_20231015-143000.json"
            with open(output_file) as f:
                data = json.load(f)
            
            # References should be removed
            assert "references" not in data["features"][0]["properties"]

    def test_download_handles_network_error(self, mock_io, tmp_path):
        """Test handling of network errors"""
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', side_effect=Exception("Network error")):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should log error
            mock_io.write_error.assert_called_once()

    def test_download_cleans_old_files(self, mock_io, empty_response, tmp_path):
        """Test that old files are cleaned up"""
        # Create an old file
        old_file = tmp_path / "alerts_active_20231015-120000.json"
        old_file.write_text('{"old": "data"}')
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('EdgeWARN.core.ingest.nws.main.fs.clean_files_by_age') as mock_clean, \
             patch('urllib.request.urlopen', return_value=empty_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should call clean_files_by_age
            mock_clean.assert_called_once()

    def test_download_with_custom_base_dir(self, mock_io, empty_response, tmp_path):
        """Test download with custom base directory"""
        custom_dir = tmp_path / "custom_nws"
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', custom_dir), \
             patch('urllib.request.urlopen', return_value=empty_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # File should be in custom directory
            expected_file = custom_dir / "alerts_active_20231015-143000.json"
            assert expected_file.exists()

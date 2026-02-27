"""
Tests for NWS ingest main module
"""

import pytest
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from EdgeWARN.core.ingest.nws.main import download_alerts
from EdgeWARN.core.ingest.nws.registry import reset_registry

class TestDownloadAlerts:
    """Tests for download_alerts function"""

    @pytest.fixture
    def mock_io(self):
        """Mock the module-level io_manager"""
        with patch('EdgeWARN.core.ingest.nws.main.io_manager') as mock:
            yield mock

    @pytest.fixture(autouse=True)
    def reset_registry_fixture(self):
        """Reset the singleton registry instance before each test"""
        reset_registry()

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
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', tmp_path / "registry.json"), \
             patch('urllib.request.urlopen', return_value=empty_response):
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            assert tmp_path.exists()

    def test_download_creates_correct_filename(self, mock_io, empty_response, tmp_path):
        """Test that correct registry file is saved"""
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', tmp_path / "registry.json"), \
             patch('urllib.request.urlopen', return_value=empty_response):
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should create registry file
            expected_file = tmp_path / "registry.json"
            assert expected_file.exists()

    def test_download_filters_dropped_events(self, mock_io, tmp_path):
        """Test that events in DROPPED_EVENTS are excluded"""
        # Mock urllib response with mixed events
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "features": [
                {
                    "properties": {
                        "id": "1",
                        "event": "Severe Thunderstorm Warning",
                        "geocode": {"SAME": ["048121"]}
                    },
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]}
                },
                {
                    "properties": {
                        "id": "2",
                        "event": "Administrative Message",  # Should be dropped
                        "geocode": {"SAME": ["048121"]}
                    },
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]}
                }
            ]
        }).encode('utf-8')
        mock_response.__enter__.return_value = mock_response
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', tmp_path / "registry.json"), \
             patch('urllib.request.urlopen', return_value=mock_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30, tzinfo=timezone.utc))
            
            # Read output file
            output_file = tmp_path / "registry.json"
            with open(output_file) as f:
                data = json.load(f)
            
            # Should only include Severe Thunderstorm Warning
            alerts = data.get("alerts", {})
            assert len(alerts) == 1
            events = [alerts[k]["feature"]["properties"]["event"] for k in alerts]
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
                        "id": "1",
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
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', tmp_path / "registry.json"), \
             patch('urllib.request.urlopen', return_value=mock_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30, tzinfo=timezone.utc))
            
            # Read output file
            output_file = tmp_path / "registry.json"
            with open(output_file) as f:
                data = json.load(f)
            
            alerts = data.get("alerts", {})
            assert len(alerts) == 1
            feature = list(alerts.values())[0]["feature"]
            assert "references" not in feature["properties"]

    def test_download_handles_network_error(self, mock_io, tmp_path):
        """Test handling of network errors"""
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('urllib.request.urlopen', side_effect=Exception("Network error")):
            
            with pytest.raises(Exception):
                download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should log error
            mock_io.write_error.assert_called_once()

    def test_download_cleans_old_files(self, mock_io, empty_response, tmp_path):
        """Test that old registry items are cleaned up instead of old files."""
        # Note: clean_files_by_age logic has been replaced by registry cleanup
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', tmp_path), \
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', tmp_path / "registry.json"), \
             patch('EdgeWARN.core.ingest.nws.main._get_registry') as mock_get_registry, \
             patch('urllib.request.urlopen', return_value=empty_response):
            
            mock_registry = MagicMock()
            mock_registry.cleanup_expired.return_value = 0
            mock_get_registry.return_value = mock_registry
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            # Should call cleanup_expired
            mock_registry.cleanup_expired.assert_called_once()

    def test_download_with_custom_base_dir(self, mock_io, empty_response, tmp_path):
        """Test download respects base registry paths"""
        custom_dir = tmp_path / "custom_nws"
        custom_reg = custom_dir / "reg.json"
        
        with patch('EdgeWARN.core.ingest.nws.main.fs.MRMS_NWS_DIR', custom_dir), \
             patch('EdgeWARN.core.ingest.nws.main.fs.NWS_REGISTRY_PATH', custom_reg), \
             patch('urllib.request.urlopen', return_value=empty_response):
            
            download_alerts(datetime(2023, 10, 15, 14, 30))
            
            assert custom_reg.exists()

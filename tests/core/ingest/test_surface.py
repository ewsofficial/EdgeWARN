import pytest
from unittest.mock import MagicMock, call, patch
import json
from datetime import datetime, timezone
import requests
from EdgeWARN.core.ingest import surface

# Sample Data
SAMPLE_LAYERS = {
    "layers": [
        {"id": 0, "name": "Layer 1", "type": "Feature Layer"},
        {"id": 1, "name": "Layer 2", "type": "Group Layer"}, # Should be skipped
    ]
}

SAMPLE_FEATURES = {
    "features": [
        {"type": "Feature", "properties": {"idp_filedate": 1678886400000, "some_prop": "value"}} # ~2023-03-15
    ]
}

def test_fetch_layer_metadata_success(mocker):
    """Test successful layer metadata fetching."""
    mock_get = mocker.patch("requests.get")
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_LAYERS
    mock_resp.status_code = 200
    mock_get.return_value = mock_resp

    layers = surface.fetch_layer_metadata()
    assert len(layers) == 2
    assert layers[0]["name"] == "Layer 1"
    mock_get.assert_called_once_with(f"{surface.NFC_URL}/layers?f=json", timeout=10)

def test_fetch_layer_metadata_failure(mocker, mock_io_manager):
    """Test failure during metadata fetching."""
    surface.io_manager = mock_io_manager # inject mock logger
    mock_get = mocker.patch("requests.get", side_effect=requests.exceptions.RequestException("Error"))
    
    layers = surface.fetch_layer_metadata()
    assert layers == []
    mock_io_manager.write_error.assert_called()

def test_fetch_features_for_layer_success(mocker):
    """Test successful feature fetching for a layer."""
    mock_get = mocker.patch("requests.get")
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_FEATURES
    mock_resp.status_code = 200
    mock_get.return_value = mock_resp

    layer = {"id": 10, "name": "Test Layer", "type": "Feature Layer"}
    features = surface.fetch_features_for_layer(layer)
    
    assert len(features) == 1
    # Check enrichment
    assert features[0]["properties"]["layer_name"] == "Test Layer"
    assert features[0]["properties"]["layer_id"] == 10

def test_fetch_features_skip_group_layer():
    """Test that group layers are skipped."""
    layer = {"id": 11, "name": "Group", "type": "Group Layer"}
    features = surface.fetch_features_for_layer(layer)
    assert features == []

def test_fetch_features_for_layer_failure(mocker, mock_io_manager):
    """Test failure during feature fetching."""
    surface.io_manager = mock_io_manager
    mock_get = mocker.patch("requests.get", side_effect=Exception("Fetch Error"))
    
    layer = {"id": 10, "name": "Test Layer", "type": "Feature Layer"}
    features = surface.fetch_features_for_layer(layer)
    
    assert features == []
    mock_io_manager.write_warning.assert_called()

def test_ingest_surface_features_full_flow(mocker, mock_fs, mock_io_manager):
    """
    Test the full ingestion flow:
    1. Fetch metadata
    2. Parallel fetch features
    3. Calculate timestamp
    4. Save file
    """
    surface.io_manager = mock_io_manager
    
    # Patch fs.SURFACE_DIR to use our temp path
    mocker.patch("EdgeWARN.core.ingest.surface.fs.SURFACE_DIR", mock_fs / "surface")
    
    # Mock Metadata
    mocker.patch("EdgeWARN.core.ingest.surface.fetch_layer_metadata", return_value=[
        {"id": 1, "name": "L1", "type": "Feature Layer"}
    ])
    
    # Mock Features
    mocker.patch("EdgeWARN.core.ingest.surface.fetch_features_for_layer", return_value=[
        {"type": "Feature", "properties": {"idp_filedate": 1700000000000}} # 2023-11-14...
    ])
    
    # Run Ingest
    surface.ingest_surface_features()
    
    # Verify file creation
    expected_ts = datetime.fromtimestamp(1700000000, tz=timezone.utc).strftime("%Y%m%d-%H%M00")
    expected_file = mock_fs / "surface" / f"surface_features_{expected_ts}.json"
    
    assert expected_file.exists()
    
    with open(expected_file) as f:
        saved_data = json.load(f)
        assert saved_data["type"] == "FeatureCollection"
        assert len(saved_data["features"]) == 1
        assert saved_data["features"][0]["properties"]["idp_filedate"] == 1700000000000
    
    mock_io_manager.write_info.assert_called()

def test_ingest_surface_features_no_layers(mocker, mock_io_manager):
    """Test ingestion aborts when no layers found."""
    surface.io_manager = mock_io_manager
    mocker.patch("EdgeWARN.core.ingest.surface.fetch_layer_metadata", return_value=[])
    
    surface.ingest_surface_features()
    
    mock_io_manager.write_error.assert_called_with("No layers found. Aborting.")

def test_ingest_surface_features_write_failure(mocker, mock_fs, mock_io_manager):
    """Test error handling during file write."""
    surface.io_manager = mock_io_manager
    mocker.patch("EdgeWARN.core.ingest.surface.fs.SURFACE_DIR", mock_fs / "surface")
    
    mocker.patch("EdgeWARN.core.ingest.surface.fetch_layer_metadata", return_value=[
        {"id": 1, "name": "L1", "type": "Feature Layer"}
    ])
    mocker.patch("EdgeWARN.core.ingest.surface.fetch_features_for_layer", return_value=[
        {"type": "Feature", "properties": {}}
    ])
    
    # Mock open to raise exception
    mock_open = mocker.mock_open()
    mock_open.side_effect = IOError("Disk full")
    
    # We have to patch 'open' within the surface module namespace or builtins
    with patch("builtins.open", mock_open):
        surface.ingest_surface_features()
        
    mock_io_manager.write_error.assert_called()
    assert "Failed to save surface features" in mock_io_manager.write_error.call_args[0][0]

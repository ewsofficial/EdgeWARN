import pytest
from unittest.mock import MagicMock
from EdgeWARN.ctam import engine

def test_initialize_modules():
    """Test that module namespaces are correctly initialized."""
    entry = {"id": 123}
    modules = ["modA", "modB"]
    
    engine.initialize_modules(entry, modules)
    
    assert "modules" in entry
    assert "modA" in entry["modules"]
    assert "modB" in entry["modules"]
    assert entry["modules"]["modA"] == {}

def test_initialize_modules_existing():
    """Test consistent initialization if modules key already exists."""
    entry = {"modules": {"modA": {"prev": 1}}}
    modules = ["modA", "modB"]
    
    engine.initialize_modules(entry, modules)
    
    # Should preserve existing data
    assert entry["modules"]["modA"]["prev"] == 1
    # Should add new module
    assert "modB" in entry["modules"]

def test_run_modules_snapshot(mocker):
    """Test running modules on a snapshot (GeoJSON-like dict)."""
    # Mock AnalysisModules
    modA = MagicMock()
    modB = MagicMock()
    
    modules_map = {
        "modA": modA,
        "modB": modB
    }
    
    data = {"features": [{"id": 1}, {"id": 2}]}
    
    engine.run_modules(data, modules_map, ["modA", "modB"])
    
    # Verify modA called for both entries
    assert modA.run.call_count == 2
    # Verify modB called for both entries
    assert modB.run.call_count == 2
    
    # Verify initialization happened
    assert "modules" in data["features"][0]
    assert "modA" in data["features"][0]["modules"]

def test_run_modules_history():
    """Test running modules on a history list."""
    modA = MagicMock()
    modules_map = {"modA": modA}
    
    data = [{"id": 1}, {"id": 1, "t": 2}]
    
    engine.run_modules(data, modules_map, ["modA"])
    
    assert modA.run.call_count == 2

def test_run_modules_error_handling():
    """Test that module errors are caught and logged to the entry."""
    modErr = MagicMock()
    modErr.run.side_effect = Exception("Analysis Failed")
    
    modules_map = {"modErr": modErr}
    data = [{"id": 1}]
    
    engine.run_modules(data, modules_map, ["modErr"])
    
    entry = data[0]
    assert "error" in entry["modules"]["modErr"]
    assert entry["modules"]["modErr"]["error"] == "Analysis Failed"

def test_run_modules_invalid_input():
    """Test graceful handling of invalid input data."""
    # Pass something that isn't a dict with features or a list
    engine.run_modules("invalid string", {}, [])
    # Should just return without error

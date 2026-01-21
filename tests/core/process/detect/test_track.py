import pytest
from unittest.mock import MagicMock
from EdgeWARN.core.process.detect.track import StormCellTracker

@pytest.fixture
def tracker(mock_io_manager):
    """Create a StormCellTracker with mocked dependencies."""
    return StormCellTracker(None, None, mock_io_manager)

# === Tests for update_cells ===

def test_update_cells_updates_existing(tracker):
    """Test that existing cells are updated with new data."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]},
        {"id": 102, "num_gates": 30, "centroid": [36.0, -96.0], "max_refl": 45, "bbox": [5,6,7,8]}
    ]
    
    updated_data = [
        {"id": 101, "num_gates": 60, "centroid": [35.1, -97.1], "max_refl": 60, "bbox": [10,20,30,40]},
        {"id": 102, "num_gates": 35, "centroid": [36.1, -96.1], "max_refl": 50, "bbox": [50,60,70,80]}
    ]
    
    result = tracker.update_cells(entries, updated_data, timestamp="2023-10-15T12:00:00")
    
    assert len(result) == 2
    
    cell_101 = next(c for c in result if c["id"] == 101)
    assert cell_101["num_gates"] == 60
    assert cell_101["max_refl"] == 60
    assert cell_101["timestamp"] == "2023-10-15T12:00:00"

def test_update_cells_removes_missing(tracker):
    """Test that cells not in updated_data are removed."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]},
        {"id": 102, "num_gates": 30, "centroid": [36.0, -96.0], "max_refl": 45, "bbox": [5,6,7,8]},
        {"id": 103, "num_gates": 20, "centroid": [37.0, -95.0], "max_refl": 40, "bbox": [9,10,11,12]}
    ]
    
    updated_data = [
        {"id": 101, "num_gates": 55, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]}
        # 102 and 103 are missing
    ]
    
    result = tracker.update_cells(entries, updated_data)
    
    assert len(result) == 1
    assert result[0]["id"] == 101

def test_update_cells_adds_new(tracker):
    """Test that new cells from updated_data are appended."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]}
    ]
    
    updated_data = [
        {"id": 101, "num_gates": 55, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]},
        {"id": 104, "num_gates": 40, "centroid": [38.0, -94.0], "max_refl": 50, "bbox": [13,14,15,16]}  # New cell
    ]
    
    result = tracker.update_cells(entries, updated_data, timestamp="2023-10-15T12:00:00")
    
    assert len(result) == 2
    
    new_cell = next(c for c in result if c["id"] == 104)
    assert new_cell["num_gates"] == 40
    assert new_cell["timestamp"] == "2023-10-15T12:00:00"

def test_update_cells_preserves_storm_history(tracker):
    """Test that storm_history is not overwritten."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4], "storm_history": [{"t": 1, "val": 10}]}
    ]
    
    updated_data = [
        {"id": 101, "num_gates": 60, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]}
    ]
    
    result = tracker.update_cells(entries, updated_data)
    
    assert "storm_history" in result[0]
    assert result[0]["storm_history"] == [{"t": 1, "val": 10}]

def test_update_cells_empty_entries(tracker):
    """Test updating with empty entries list."""
    entries = []
    
    updated_data = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]}
    ]
    
    result = tracker.update_cells(entries, updated_data)
    
    assert len(result) == 1
    assert result[0]["id"] == 101

def test_update_cells_empty_updated_data(tracker):
    """Test updating with empty updated_data (all cells removed)."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]},
        {"id": 102, "num_gates": 30, "centroid": [36.0, -96.0], "max_refl": 45, "bbox": [5,6,7,8]}
    ]
    
    result = tracker.update_cells(entries, [])
    
    assert len(result) == 0

def test_update_cells_no_timestamp(tracker):
    """Test updating without providing a timestamp."""
    entries = [
        {"id": 101, "num_gates": 50, "centroid": [35.0, -97.0], "max_refl": 55, "bbox": [1,2,3,4]}
    ]
    
    updated_data = [
        {"id": 101, "num_gates": 60, "centroid": [35.1, -97.1], "max_refl": 60, "bbox": [10,20,30,40]}
    ]
    
    result = tracker.update_cells(entries, updated_data, timestamp=None)
    
    # Should not add timestamp key
    assert "timestamp" not in result[0] or result[0].get("timestamp") is None

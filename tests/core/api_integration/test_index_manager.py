import pytest
import json
from unittest.mock import patch
from EdgeWARN.core.api_integration.index_manager import APIIndexManager

@pytest.fixture
def index_manager(mock_io_manager, mock_fs):
    """Fixture for APIIndexManager pointing to mock fs."""
    # We must patch the fs constants in index_manager to point to our temp paths
    with patch("EdgeWARN.core.api_integration.index_manager.fs.STORMCELL_DIR", mock_fs / "stormcell"), \
         patch("EdgeWARN.core.api_integration.index_manager.fs.CELL_DIR", mock_fs / "cell"):
        
        manager = APIIndexManager(mock_io_manager)
        yield manager

def test_initialize_stormcell_index(index_manager, mock_fs):
    """Test creation of stormcell index from existing files."""
    storm_dir = mock_fs / "stormcell"
    # Create sample files
    (storm_dir / "stormcells_20230101-120000.json").touch()
    (storm_dir / "stormcells_20230101-120500.json").touch()
    (storm_dir / "ignore_me.txt").touch()
    
    index_manager.initialize_indexes()
    
    index_path = storm_dir / "stormcell_index.json"
    assert index_path.exists()
    
    with open(index_path) as f:
        data = json.load(f)
        assert "timestamps" in data
        assert len(data["timestamps"]) == 2
        assert "20230101-120000" in data["timestamps"]
        assert "20230101-120500" in data["timestamps"]

def test_initialize_cell_index(index_manager, mock_fs):
    """Test creation of cell index from existing files."""
    cell_dir = mock_fs / "cell"
    # Create sample cell files
    (cell_dir / "101.json").touch()
    (cell_dir / "102.json").touch()
    (cell_dir / "not_a_number.json").touch()
    
    index_manager.initialize_indexes()
    
    index_path = cell_dir / "cell_index.json"
    assert index_path.exists()
    
    with open(index_path) as f:
        data = json.load(f)
        assert "cellIds" in data
        assert len(data["cellIds"]) == 2
        assert 101 in data["cellIds"]
        assert 102 in data["cellIds"]

def test_cleanup_inactive_cells(index_manager, mocker):
    """Test that cleanup calls the fs utility and updates indexes."""
    mock_clean = mocker.patch("EdgeWARN.core.api_integration.index_manager.fs.clean_files_by_age")
    
    # Spy on _initialize_cell_index to ensure it's called after cleanup
    spy_init = mocker.spy(index_manager, "_initialize_cell_index")
    
    index_manager.cleanup_inactive_cells()
    
    mock_clean.assert_called_once()
    spy_init.assert_called_once()

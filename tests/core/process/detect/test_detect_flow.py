import pytest
from unittest.mock import MagicMock, patch
from EdgeWARN.core.process.detect.detect import detect_cells

@pytest.fixture
def mock_dependencies():
    handler = MagicMock()
    mapper = MagicMock()
    saver = MagicMock()
    io_manager = MagicMock()
    
    # Mock Handler returns
    handler.load_subset.return_value = MagicMock() # radar_ds
    handler.load_probsevere.return_value = MagicMock() # ps_ds
    handler.load_preciptype.return_value = MagicMock() # preciptype_ds
    
    # Mock Mapper returns
    mapper.map_gates_to_polygons.return_value = MagicMock()
    mapper.expand_gates.return_value = MagicMock()
    mapper.draw_bbox.return_value = {}
    
    # Mock Saver returns
    saver.create_entry.return_value = []
    
    return handler, mapper, saver, io_manager

def test_detect_cells_flow(mock_dependencies):
    """Test the main detection flow."""
    handler, mapper, saver, io_manager = mock_dependencies
    
    with patch("EdgeWARN.core.process.detect.detect.DetectionDataHandler", return_value=handler), \
         patch("EdgeWARN.core.process.detect.detect.GateMapper", return_value=mapper), \
         patch("EdgeWARN.core.process.detect.detect.CellDataSaver", return_value=saver):
         
        entries = detect_cells(
            "radar.grib2", "ps.json", "pt.grib2", io_manager,
            30, 40, -100, -90
        )
        
    assert entries == []
    
    # Verify calls
    handler.load_subset.assert_called_once()
    handler.load_probsevere.assert_called_once()
    
    mapper.map_gates_to_polygons.assert_called_once()
    mapper.expand_gates.assert_called_once()
    mapper.draw_bbox.assert_called_once()
    
    # Check that Preciptype is loaded LATE (after mapping)
    # Python mocks don't easily track order between different objects without a manager,
    # but we can check it was called.
    handler.load_preciptype.assert_called_once()
    
    saver.create_entry.assert_called_once()

def test_detect_cells_load_fail(mock_dependencies):
    """Test failure to load radar data."""
    handler, mapper, saver, io_manager = mock_dependencies
    handler.load_subset.return_value = None # Fail
    
    with patch("EdgeWARN.core.process.detect.detect.DetectionDataHandler", return_value=handler):
         entries = detect_cells(
            "radar.grib2", "ps.json", "pt.grib2", io_manager,
            30, 40, -100, -90
        )
    
    assert entries == []
    io_manager.write_error.assert_called()
    mapper.map_gates_to_polygons.assert_not_called()

def test_detect_cells_preciptype_fail(mock_dependencies):
    """Test handling of precip type load failure."""
    handler, mapper, saver, io_manager = mock_dependencies
    handler.load_preciptype.return_value = None # Fail
    
    with patch("EdgeWARN.core.process.detect.detect.DetectionDataHandler", return_value=handler), \
         patch("EdgeWARN.core.process.detect.detect.GateMapper", return_value=mapper), \
         patch("EdgeWARN.core.process.detect.detect.CellDataSaver", return_value=saver):
         
        entries = detect_cells(
            "radar.grib2", "ps.json", "pt.grib2", io_manager,
            30, 40, -100, -90
        )
        
    # Should still proceed to save, but warn
    io_manager.write_warning.assert_called()
    saver.create_entry.assert_called_once()

def test_detect_return_probsevere(mock_dependencies):
    """Test return_probsevere flag."""
    handler, mapper, saver, io_manager = mock_dependencies
    ps_ds_mock = MagicMock()
    handler.load_probsevere.return_value = ps_ds_mock
    
    with patch("EdgeWARN.core.process.detect.detect.DetectionDataHandler", return_value=handler), \
         patch("EdgeWARN.core.process.detect.detect.GateMapper", return_value=mapper), \
         patch("EdgeWARN.core.process.detect.detect.CellDataSaver", return_value=saver):
         
        entries, ps_ds = detect_cells(
            "radar.grib2", "ps.json", "pt.grib2", io_manager,
            30, 40, -100, -90, return_probsevere=True
        )
        
    assert ps_ds == ps_ds_mock

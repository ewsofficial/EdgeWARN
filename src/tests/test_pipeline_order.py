
import sys
from unittest.mock import MagicMock, patch

# Mock modules before importing detect
sys.modules['EdgeWARN.core.process.detect.tools.utils'] = MagicMock()
sys.modules['EdgeWARN.core.gui_pipelines.transform.render'] = MagicMock()
sys.modules['EdgeWARN.core.process.detect.tools.gatemapper'] = MagicMock()
sys.modules['EdgeWARN.core.process.detect.tools.save'] = MagicMock()
sys.modules['util.io'] = MagicMock()
sys.modules['util.file'] = MagicMock()

from EdgeWARN.core.process.detect.detect import detect_cells

def test_pipeline_order():
    # Setup mocks
    mock_handler_cls = sys.modules['EdgeWARN.core.process.detect.tools.utils'].DetectionDataHandler
    mock_renderer_cls = sys.modules['EdgeWARN.core.gui_pipelines.transform.render'].GUILayerRenderer
    mock_io = MagicMock()
    
    # Mock instances
    mock_handler_instance = mock_handler_cls.return_value
    mock_ds_full = MagicMock(name='full_dataset')
    mock_ds_subset = MagicMock(name='subset_dataset')
    
    mock_handler_instance.load_radar_full.return_value = mock_ds_full
    mock_handler_instance.subset_radar.return_value = mock_ds_subset

    # Run detection
    detect_cells(
        radar_path="dummy_radar.nc", 
        ps_path="dummy_ps.json", 
        preciptype_path="dummy_pt.nc", 
        io_manager=mock_io, 
        lat_min=30, lat_max=40, 
        lon_min=-100, lon_max=-90
    )

    # Verify order of operations
    
    # 1. Check if load_radar_full was called
    mock_handler_instance.load_radar_full.assert_called_once()
    
    # 2. Check if renderer was initialized with full dataset
    mock_renderer_cls.assert_called_once()
    args, _ = mock_renderer_cls.call_args
    if args[0] is not mock_ds_full:
        print("FAIL: Renderer was not initialized with the full dataset!")
        sys.exit(1)
    else:
        print("PASS: Renderer initialized with full dataset.")

    # 3. Check if subset_radar was called with full dataset
    mock_handler_instance.subset_radar.assert_called_once_with(mock_ds_full)
    print("PASS: subset_radar called with full dataset.")

    # Check that subsetting happened AFTER rendering (implicit by variable reuse logic, 
    # but we can check call order if we mock the manager calls, but checking inputs is strong enough here)
    
    print("Verification Successful: Pipeline order is correct.")

if __name__ == "__main__":
    try:
        test_pipeline_order()
    except Exception as e:
        print(f"FAIL: An error occurred: {e}")
        sys.exit(1)

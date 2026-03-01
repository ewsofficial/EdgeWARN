
import pytest
import numpy as np
import xarray as xr
from unittest.mock import MagicMock
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper

class MockIOManager:
    def write_debug(self, msg):
        pass
    def write_warning(self, msg):
        pass

def test_connectivity_constraint():
    """
    Test that watershed expansion respects connectivity constraints.
    It should NOT jump across low-reflectivity gaps.
    """
    # 1. Setup Grid 20x20
    lats = np.arange(20)
    lons = np.arange(20)
    
    # 2. Create Radar Data (High Reflectivity Mask)
    # Refl threshold is 40.
    refl_data = np.zeros((20, 20))
    
    # Region 1: Connected to Seed (Polygon A)
    # Rectangle from (5,5) to (10,10)
    refl_data[5:11, 5:11] = 50 
    
    # Region 2: Disconnected Blob
    # Rectangle at (15,15) to (17,17)
    refl_data[15:18, 15:18] = 50
    
    radar_ds = xr.Dataset(
        {'unknown': (('latitude', 'longitude'), refl_data),
         'latitude': lats,
         'longitude': lons}
    )
    
    # 3. Create Mapped Dataset (Polygons)
    # Polygon A covers a subset of Region 1: (5,5) to (7,7)
    polygon_grid = np.zeros((20, 20), dtype=np.int32)
    polygon_grid[5:8, 5:8] = 1 # ID 1
    
    mapped_ds = xr.Dataset(
        {'PolygonID': (('latitude', 'longitude'), polygon_grid),
         'latitude': lats,
         'longitude': lons}
    )
    
    # 4. Initialize GateMapper
    mapper = GateMapper(radar_ds, ps_ds=None, io_manager=MockIOManager(), refl_threshold=40.0)
    
    # 5. Run Expand Gates
    expanded_ds = mapper.expand_gates(mapped_ds)
    final_grid = expanded_ds['PolygonID'].values
    
    # 6. Verify Results
    
    # Check Region 1 (Connected) - Should be ID 1
    # Specifically check a point outside the original polygon but connected
    assert final_grid[9, 9] == 1, "Connected high-reflectivity pixel OUTSIDE polygon should be captured"
    
    # Check that expansion FILLED the connected blob
    assert final_grid[10, 10] == 1, "Extreme edge of connected blob should be captured"
    
    # Check Region 2 (Disconnected) - Should be 0
    # Even though we allowed expansion, it cannot jump the gap.
    assert final_grid[16, 16] == 0, "Disconnected high-reflectivity pixel should NOT be assigned an ID"
    
    # Check Background - Should match 0
    assert final_grid[0, 0] == 0

def test_merger_split():
    """
    Test that two nearby cells split reasonably (watershed behavior).
    """
    lats = np.arange(20)
    lons = np.arange(20)
    refl_data = np.zeros((20, 20))
    refl_data[5:15, 5:15] = 50 # Large block
    
    radar_ds = xr.Dataset(
        {'unknown': (('latitude', 'longitude'), refl_data),
         'latitude': lats,
         'longitude': lons}
    )
    
    polygon_grid = np.zeros((20, 20), dtype=np.int32)
    polygon_grid[6, 6] = 1 # Seed A top-left
    polygon_grid[13, 13] = 2 # Seed B bottom-right
    
    mapped_ds = xr.Dataset(
        {'PolygonID': (('latitude', 'longitude'), polygon_grid),
         'latitude': lats,
         'longitude': lons}
    )
    
    mapper = GateMapper(radar_ds, ps_ds=None, io_manager=MockIOManager(), refl_threshold=40.0)
    expanded_ds = mapper.expand_gates(mapped_ds)
    final_grid = expanded_ds['PolygonID'].values
    
    # Both IDs should be present
    assert 1 in final_grid
    assert 2 in final_grid
    
    # Check that they cover the block
    assert np.all(final_grid[5:15, 5:15] > 0)

def test_dynamic_thresholding():
    """
    Test that dynamic thresholding applies correctly to strong vs weak cells.
    Strong cell (>= 45 max refl) drops to 40.
    Weak cell (< 45 max refl) drops to 37.5.
    """
    lats = np.arange(20)
    lons = np.arange(20)
    
    # Baseline mask is 37.5
    refl_data = np.zeros((20, 20))
    
    # Cell 1: Strong cell. Max refl = 50. Should threshold at max(40, 50-10) = 40.
    refl_data[2:8, 2:8] = 42 # Within expanded area
    refl_data[4:6, 4:6] = 50 # Core
    refl_data[2:8, 8] = 39   # Just below its allowed threshold, should NOT expand here
    
    # Cell 2: Weak cell. Max refl = 44. Should threshold at max(37.5, 44-10) = 37.5.
    refl_data[12:18, 12:18] = 38 # Within expanded area
    refl_data[14:16, 14:16] = 44 # Core
    refl_data[12:18, 18] = 37    # Below 37.5 baseline entirely

    radar_ds = xr.Dataset(
        {'unknown': (('latitude', 'longitude'), refl_data),
         'latitude': lats,
         'longitude': lons}
    )
    
    polygon_grid = np.zeros((20, 20), dtype=np.int32)
    polygon_grid[5, 5] = 1 # ID 1 (Strong)
    polygon_grid[15, 15] = 2 # ID 2 (Weak)
    
    mapped_ds = xr.Dataset(
        {'PolygonID': (('latitude', 'longitude'), polygon_grid),
         'latitude': lats,
         'longitude': lons}
    )
    
    mapper = GateMapper(radar_ds, ps_ds=None, io_manager=MockIOManager(), refl_threshold=37.5, drop_offset=10.0)
    expanded_ds = mapper.expand_gates(mapped_ds)
    final_grid = expanded_ds['PolygonID'].values
    
    # Verify Strong Cell (Thresh = 40)
    assert final_grid[4, 4] == 1, "Core should be included"
    assert final_grid[2, 2] == 1, "Area >= 40 should be included"
    assert final_grid[5, 8] == 0, "Area < 40 should NOT be included for strong cell"
    
    # Verify Weak Cell (Thresh = 37.5)
    assert final_grid[15, 15] == 2, "Core should be included"
    assert final_grid[13, 13] == 2, "Area >= 37.5 should be included"
    assert final_grid[15, 18] == 0, "Area < 37.5 should NOT be included for weak cell"

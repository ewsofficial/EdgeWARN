
import pytest
import numpy as np
import xarray as xr
import shapely.geometry as sg
from unittest.mock import MagicMock
from EdgeWARN.core.process.integrate.integrate import StormCellIntegrator

@pytest.fixture
def mock_io_manager():
    return MagicMock()

@pytest.fixture
def integrator(mock_io_manager):
    return StormCellIntegrator(mock_io_manager)

@pytest.fixture
def synthetic_dataset(tmp_path):
    """Create a synthetic netcdf file for testing integration logic"""
    # 0.01 degree grid, covering 30-40N, -100 to -90W
    lat = np.linspace(30.0, 31.0, 101) # 0.01 spacing
    lon = np.linspace(-96.0, -95.0, 101)
    
    # Create simple pattern: Value = lat index + lon index (gradient)
    # shape (101, 101)
    data = np.zeros((101, 101))
    
    # Place a "hotspot" value of 100 at center, with 50 surrounding to pull down mean
    data[49:52, 49:52] = 50.0
    data[50, 50] = 100.0
    
    # Gradient background
    for i in range(101):
        for j in range(101):
            if data[i, j] == 0:
                data[i, j] = (i + j) / 10.0 # mostly 0-20
    
    ds = xr.Dataset(
        data_vars=dict(
            test_var=(["latitude", "longitude"], data)
        ),
        coords=dict(
            latitude=(["latitude"], lat),
            longitude=(["longitude"], lon),
        ),
        attrs=dict(description="Synthetic Test Data")
    )
    
    path = tmp_path / "synthetic_test.nc"
    ds.to_netcdf(path)
    return str(path)

def test_integrate_multi_stats(integrator, synthetic_dataset):
    """Test integrate_multi_stats with various statistical configs"""
    
    # Defined hotspot at idx (50, 50) -> lat=30.5, lon=-95.5
    # create_cell_polygon expects 'bbox' list of [lat, lon] tuples
    
    # Box expanded to STRICTLY contain 3x3 grid points (49, 50, 51)
    # Grid points are at .49, .50, .51
    # Box should be .485 to .515
    bbox = [
        [30.485, -95.515],
        [30.485, -95.485],
        [30.515, -95.485],
        [30.515, -95.515],
        [30.485, -95.515]
    ]
    
    cell = {
        "id": "test_cell_1",
        "bbox": bbox,
        "centroid": [30.5, -95.5],
        "properties": {}
    }
    
    cells = [cell]
    
    stats_config = [
        {"key": "p100Test", "method": "max"},
        {"key": "p90Test", "method": "percentile", "percentile": 90},
        {"key": "MeanTest", "method": "mean"}
    ]
    
    # Run integration
    result = integrator.integrate_multi_stats(synthetic_dataset, cells, stats_config)
    
    props = result[0]["properties"]
    
    # Max should be 100.0 (the hotspot pixel)
    assert props["p100Test"] == 100.0
    
    # Mean should be significantly lower as it includes neighbors
    # Neighbors are ~10.0
    assert props["MeanTest"] < 100.0
    assert props["MeanTest"] > 0.0
    
    # p90 should be close to max if the hotspot dominates, or lower if many pixels
    assert props["p90Test"] <= 100.0
    assert props["p90Test"] > 0.0

def test_integrate_empty_intersection(integrator, synthetic_dataset):
    """Test integration where cell is outside dataset bounds"""
    # Create cell far away (e.g., lat 40, lon -80)
    bbox = [
        [40.0, -80.0],
        [40.0, -79.9],
        [40.1, -79.9],
        [40.1, -80.0]
    ]
    
    cell = {
        "id": "test_cell_outside",
        "bbox": bbox,
        "centroid": [40.05, -79.95],
        "properties": {}
    }
    
    stats_config = [{"key": "p100Zero", "method": "max"}]
    
    result = integrator.integrate_multi_stats(synthetic_dataset, [cell], stats_config)
    
    # Should get 0
    assert result[0]["properties"]["p100Zero"] == 0

def test_integrate_error_handling(integrator):
    """Test handling of invalid file path"""
    cell = {
        "id": "cell_1",
        "bbox": [[0,0], [0,1], [1,1], [1,0]],
        "properties": {}
    }
    
    stats_config = [{"key": "p100Err", "method": "max"}]
    
    # Invalid path
    result = integrator.integrate_multi_stats("/invalid/path/test.nc", [cell], stats_config)
    
    assert "p100Err" not in result[0]["properties"]

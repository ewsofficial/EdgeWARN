
import pytest
import numpy as np
import xarray as xr
import shapely.geometry as sg
from unittest.mock import MagicMock
from EdgeWARN.process.integrate.integrate import StormCellIntegrator

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


@pytest.fixture
def synthetic_dataset_360(tmp_path):
    """Create a synthetic netcdf file with 0-360 longitude coordinates."""
    lat = np.linspace(30.0, 31.0, 101)
    lon = np.linspace(264.0, 265.0, 101)

    data = np.zeros((101, 101))
    data[50, 50] = 100.0

    ds = xr.Dataset(
        data_vars=dict(
            test_var=(["latitude", "longitude"], data)
        ),
        coords=dict(
            latitude=(["latitude"], lat),
            longitude=(["longitude"], lon),
        ),
        attrs=dict(description="Synthetic 0-360 Longitude Test Data")
    )

    path = tmp_path / "synthetic_test_360.nc"
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture
def synthetic_dataset_2d_coords(tmp_path):
    """Create a synthetic netcdf file with 2D latitude/longitude coordinates."""
    lat_1d = np.linspace(30.0, 31.0, 101)
    lon_1d = np.linspace(-96.0, -95.0, 101)
    lon_2d, lat_2d = np.meshgrid(lon_1d, lat_1d)

    data = np.zeros((101, 101))
    data[50, 50] = 100.0

    ds = xr.Dataset(
        data_vars=dict(
            test_var=(["y", "x"], data)
        ),
        coords=dict(
            latitude=(["y", "x"], lat_2d),
            longitude=(["y", "x"], lon_2d),
        ),
        attrs=dict(description="Synthetic 2D Coordinate Test Data")
    )

    path = tmp_path / "synthetic_test_2d.nc"
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


def test_integrate_multi_stats_with_360_longitudes(integrator, synthetic_dataset_360):
    """Integration should handle datasets using 0-360 longitude coordinates."""
    cell = {
        "id": "test_cell_360",
        "bbox": [
            [30.495, 264.495],
            [30.495, 264.505],
            [30.505, 264.505],
            [30.505, 264.495],
            [30.495, 264.495],
        ],
        "centroid": [30.5, 264.5],
        "properties": {}
    }

    result = integrator.integrate_multi_stats(
        synthetic_dataset_360,
        [cell],
        [{"key": "p100Test", "method": "max"}]
    )

    assert result[0]["properties"]["p100Test"] == 100.0


def test_integrate_multi_stats_with_2d_coords(integrator, synthetic_dataset_2d_coords):
    """Integration should handle datasets with 2D latitude/longitude coordinates."""
    cell = {
        "id": "test_cell_2d",
        "bbox": [
            [30.495, -95.505],
            [30.495, -95.495],
            [30.505, -95.495],
            [30.505, -95.505],
            [30.495, -95.505],
        ],
        "centroid": [30.5, -95.5],
        "properties": {}
    }

    result = integrator.integrate_multi_stats(
        synthetic_dataset_2d_coords,
        [cell],
        [{"key": "p100Test", "method": "max"}]
    )

    assert result[0]["properties"]["p100Test"] == 100.0

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


@pytest.fixture
def synthetic_azshear_dataset_pair(tmp_path):
    lat = np.linspace(30.0, 31.0, 101)
    lon = np.linspace(-96.0, -95.0, 101)

    low = np.zeros((101, 101))
    mid = np.zeros((101, 101))

    # Positioned just east of the storm polygon so buffered AzShear extraction
    # sees the signature while the raw storm polygon does not.
    low[49:52, 53:56] = 8.6
    low[50, 54] = 10.8
    mid[49:52, 53:56] = 6.4
    mid[50, 54] = 8.1

    low_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], low)),
        coords=dict(latitude=( ["latitude"], lat), longitude=( ["longitude"], lon)),
    )
    mid_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], mid)),
        coords=dict(latitude=( ["latitude"], lat), longitude=( ["longitude"], lon)),
    )

    low_path = tmp_path / "azshear_low.nc"
    mid_path = tmp_path / "azshear_mid.nc"
    low_ds.to_netcdf(low_path)
    mid_ds.to_netcdf(mid_path)
    return str(low_path), str(mid_path)


def test_integrate_azshear_features_uses_buffer_without_affecting_generic_stats(integrator, synthetic_azshear_dataset_pair, synthetic_dataset):
    low_path, mid_path = synthetic_azshear_dataset_pair
    cell = {
        "id": "test_cell_buffered_azshear",
        "bbox": [
            [30.49, -95.53],
            [30.49, -95.49],
            [30.51, -95.49],
            [30.51, -95.53],
            [30.49, -95.53],
        ],
        "centroid": [30.5, -95.51],
        "properties": {"ExistingField": 123.0},
    }

    stats_result = integrator.integrate_multi_stats(
        synthetic_dataset,
        [cell],
        [{"key": "p100Test", "method": "max"}],
    )
    assert stats_result[0]["properties"]["ExistingField"] == 123.0

    result = integrator.integrate_azshear_features(low_path, mid_path, stats_result)
    props = result[0]["properties"]
    azshear = props["azshear"]

    assert props["ExistingField"] == 123.0
    assert props["p100Test"] >= 0.0
    assert azshear["buffer_km"] == 5.0
    assert azshear["low"]["peak_value"] == 10.8
    assert azshear["mid"]["peak_value"] == 8.1
    assert azshear["low"]["area_km2"] > 0.0
    assert azshear["mid"]["area_km2"] > 0.0
    assert azshear["low"]["weighted_centroid_lat"] is not None
    assert azshear["low"]["weighted_centroid_lon"] is not None
    assert azshear["mid"]["weighted_centroid_lat"] is not None
    assert azshear["mid"]["weighted_centroid_lon"] is not None
    assert azshear["alignment"]["paired"] is True
    assert azshear["alignment"]["is_vertically_aligned"] is True
    assert azshear["alignment"]["centroid_distance_km"] is not None
    assert azshear["alignment"]["overlap_area_km2"] > 0.0
    assert azshear["alignment"]["overlap_ratio"] > 0.0
    assert azshear["alignment"]["low_overlap_fraction"] > 0.0
    assert azshear["alignment"]["mid_overlap_fraction"] > 0.0


def test_integrate_azshear_features_handles_missing_signal(integrator, tmp_path):
    lat = np.linspace(30.0, 31.0, 101)
    lon = np.linspace(-96.0, -95.0, 101)
    zeros = xr.Dataset(
        data_vars=dict(unknown=( ["latitude", "longitude"], np.zeros((101, 101)))),
        coords=dict(latitude=( ["latitude"], lat), longitude=( ["longitude"], lon)),
    )
    zero_path = tmp_path / "azshear_zero.nc"
    zeros.to_netcdf(zero_path)

    cell = {
        "id": "test_cell_no_azshear",
        "bbox": [
            [30.49, -95.53],
            [30.49, -95.49],
            [30.51, -95.49],
            [30.51, -95.53],
            [30.49, -95.53],
        ],
        "centroid": [30.5, -95.51],
        "properties": {},
    }

    result = integrator.integrate_azshear_features(str(zero_path), str(zero_path), [cell])
    azshear = result[0]["properties"]["azshear"]

    assert azshear["low"] is None
    assert azshear["mid"] is None
    assert azshear["alignment"]["paired"] is False
    assert azshear["alignment"]["centroid_distance_km"] is None
    assert azshear["alignment"]["overlap_area_km2"] is None


def test_integrate_azshear_features_applies_updated_thresholds(integrator, tmp_path):
    lat = np.linspace(30.0, 31.0, 101)
    lon = np.linspace(-96.0, -95.0, 101)

    low = np.zeros((101, 101))
    mid = np.zeros((101, 101))
    low[49:52, 53:56] = 7.9
    mid[49:52, 53:56] = 5.9

    low_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], low)),
        coords=dict(latitude=(["latitude"], lat), longitude=(["longitude"], lon)),
    )
    mid_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], mid)),
        coords=dict(latitude=(["latitude"], lat), longitude=(["longitude"], lon)),
    )

    low_path = tmp_path / "azshear_low_threshold.nc"
    mid_path = tmp_path / "azshear_mid_threshold.nc"
    low_ds.to_netcdf(low_path)
    mid_ds.to_netcdf(mid_path)

    cell = {
        "id": "test_cell_threshold_gate",
        "bbox": [
            [30.49, -95.53],
            [30.49, -95.49],
            [30.51, -95.49],
            [30.51, -95.53],
            [30.49, -95.53],
        ],
        "centroid": [30.5, -95.51],
        "properties": {},
    }

    result = integrator.integrate_azshear_features(str(low_path), str(mid_path), [cell])
    azshear = result[0]["properties"]["azshear"]

    assert azshear["low"] is None
    assert azshear["mid"] is None
    assert azshear["low_candidate_count"] == 0
    assert azshear["mid_candidate_count"] == 0


def test_integrate_azshear_features_rejects_distant_midlevel_pairing(integrator, tmp_path):
    lat = np.linspace(30.0, 31.0, 101)
    lon = np.linspace(-96.0, -95.0, 101)

    low = np.zeros((101, 101))
    mid = np.zeros((101, 101))

    low[50:53, 50:53] = 8.8
    low[51, 51] = 9.4

    mid[50:53, 52:55] = 6.4
    mid[51, 53] = 7.4

    mid[72:75, 72:75] = 8.5
    mid[73, 73] = 10.2

    low_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], low)),
        coords=dict(latitude=(["latitude"], lat), longitude=(["longitude"], lon)),
    )
    mid_ds = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], mid)),
        coords=dict(latitude=(["latitude"], lat), longitude=(["longitude"], lon)),
    )

    low_path = tmp_path / "azshear_low_pair.nc"
    mid_path = tmp_path / "azshear_mid_pair.nc"
    low_ds.to_netcdf(low_path)
    mid_ds.to_netcdf(mid_path)

    cell = {
        "id": "test_cell_pairing_gate",
        "bbox": [
            [30.47, -95.53],
            [30.47, -95.43],
            [30.57, -95.43],
            [30.57, -95.53],
            [30.47, -95.53],
        ],
        "centroid": [30.52, -95.48],
        "properties": {},
    }

    result = integrator.integrate_azshear_features(str(low_path), str(mid_path), [cell])
    azshear = result[0]["properties"]["azshear"]

    assert azshear["low"]["peak_value"] == 9.4
    assert azshear["mid"]["peak_value"] == 7.4
    assert azshear["alignment"]["paired"] is True
    assert azshear["alignment"]["vertical_centroid_sep_km"] < 12.0
    assert azshear["alignment"]["centroid_distance_km"] < 12.0
    assert azshear["alignment"]["overlap_area_km2"] > 0.0


def test_integrate_azshear_features_uses_independent_default_alignment_objects(integrator, tmp_path):
    lat = np.linspace(30.0, 31.0, 11)
    lon = np.linspace(-96.0, -95.0, 11)
    zeros = xr.Dataset(
        data_vars=dict(unknown=(["latitude", "longitude"], np.zeros((11, 11)))),
        coords=dict(latitude=(["latitude"], lat), longitude=(["longitude"], lon)),
    )
    zero_path = tmp_path / "azshear_zero_shared.nc"
    zeros.to_netcdf(zero_path)

    cells = [
        {
            "id": "cell_one",
            "bbox": [[30.4, -95.6], [30.4, -95.5], [30.5, -95.5], [30.5, -95.6], [30.4, -95.6]],
            "centroid": [30.45, -95.55],
            "properties": {},
        },
        {
            "id": "cell_two",
            "bbox": [[30.6, -95.4], [30.6, -95.3], [30.7, -95.3], [30.7, -95.4], [30.6, -95.4]],
            "centroid": [30.65, -95.35],
            "properties": {},
        },
    ]

    result = integrator.integrate_azshear_features(str(zero_path), str(zero_path), cells)

    assert result[0]["properties"]["azshear"]["alignment"] is not result[1]["properties"]["azshear"]["alignment"]

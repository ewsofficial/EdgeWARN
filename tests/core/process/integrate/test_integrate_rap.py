import pytest
import numpy as np
import xarray as xr
from unittest.mock import MagicMock, patch
from EdgeWARN.core.process.integrate.integrate_rap import integrate_rap


@pytest.fixture
def mock_io_manager():
    io = MagicMock()
    io.write_debug = MagicMock()
    io.write_warning = MagicMock()
    io.write_error = MagicMock()
    return io


@pytest.fixture
def mock_datasets():
    """
    Create mock datasets matching cfgrib.open_datasets output structure.
    The find_dataset_for_product function checks:
    1. level_type in ds.coords (e.g., 'isobaricInhPa', 'heightAboveGround')
    2. var_name in ds.data_vars (e.g., 'u', 'v', 't2m')
    """
    # Grid: 10x10 covering CONUS subset
    lats = np.linspace(30, 40, 10)
    lons = np.linspace(260, 270, 10)  # 0-360 format (will be converted)
    
    levels = np.array([850, 700, 500, 250])
    
    # U-wind: varies by level
    u_data = np.zeros((4, 10, 10))
    u_data[0, :, :] = 10.0  # 850
    u_data[1, :, :] = 15.0  # 700
    u_data[2, :, :] = 25.0  # 500
    u_data[3, :, :] = 40.0  # 250
    
    # V-wind
    v_data = np.zeros((4, 10, 10))
    v_data[:] = 5.0
    
    # Isobaric dataset (u, v at multiple levels)
    # Key: 'isobaricInhPa' must be a coordinate
    ds_isobaric = xr.Dataset(
        {
            "u": (("isobaricInhPa", "y", "x"), u_data),
            "v": (("isobaricInhPa", "y", "x"), v_data)
        },
        coords={
            "isobaricInhPa": levels,
            "latitude": (("y", "x"), np.broadcast_to(lats[:, None], (10, 10))),
            "longitude": (("y", "x"), np.broadcast_to(lons[None, :], (10, 10)))
        }
    )
    
    # Surface 2m dataset
    # Key: 'heightAboveGround' must be a scalar coordinate with value 2
    temp_data = np.full((10, 10), 300.0)  # 300 K = 26.85 C
    dewpoint_data = np.full((10, 10), 290.0)  # 290 K = 16.85 C
    
    ds_surface = xr.Dataset(
        {
            "t2m": (("y", "x"), temp_data),
            "d2m": (("y", "x"), dewpoint_data)
        },
        coords={
            "heightAboveGround": 2,
            "latitude": (("y", "x"), np.broadcast_to(lats[:, None], (10, 10))),
            "longitude": (("y", "x"), np.broadcast_to(lons[None, :], (10, 10)))
        }
    )
    
    # Freezing level dataset
    # Key: 'isothermZero' must be a coordinate
    fl_data = np.full((10, 10), 3500.0)  # 3500m
    
    ds_freezing = xr.Dataset(
        {
            "gh": (("y", "x"), fl_data)
        },
        coords={
            "isothermZero": 0,
            "latitude": (("y", "x"), np.broadcast_to(lats[:, None], (10, 10))),
            "longitude": (("y", "x"), np.broadcast_to(lons[None, :], (10, 10)))
        }
    )
    
    return [ds_isobaric, ds_surface, ds_freezing]


@pytest.fixture
def storm_cells():
    # Cell at (35.0, 265.0) -> middle of grid
    return [
        {"id": 1, "centroid": [35.0, 265.0], "properties": {}}
    ]


def test_integrate_rap_basic(mock_io_manager, storm_cells):
    """Test basic RAP integration with isobaric winds."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_instance = MockExtractor.return_value
        mock_instance.extract_batch.return_value = {
            "wind_field.u850": {1: 10.0}, "wind_field.v850": {1: 5.0},
            "wind_field.u700": {1: 15.0}, "wind_field.v700": {1: 5.0},
            "wind_field.u500": {1: 25.0}, "wind_field.v500": {1: 5.0},
            "wind_field.u250": {1: 40.0}, "wind_field.v250": {1: 5.0}
        }
        results = integrate_rap(storm_cells, "dummy_path.grib2", mock_io_manager)
        
    cell = results[0]
    props = cell['properties']
    
    # Check isobaric winds (nested in wind_field)
    wind = props['wind_field']
    assert wind['u850'] == 10.0
    assert wind['v850'] == 5.0
    assert wind['u700'] == 15.0
    assert wind['u500'] == 25.0
    assert wind['u250'] == 40.0


def test_integrate_rap_derived_fields(mock_io_manager, storm_cells):
    """Test derived field calculation (dewpoint_depression)."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_instance = MockExtractor.return_value
        mock_instance.extract_batch.return_value = {
            "temp_2m": {1: 300.0},
            "dewpoint_2m": {1: 290.0},
            "freezing_level_m": {1: 3500.0}
        }
        results = integrate_rap(storm_cells, "dummy_path.grib2", mock_io_manager)
        
    cell = results[0]
    props = cell['properties']
    
    # temp_2m and dewpoint_2m should be in Celsius
    assert props.get('temp_2m') == pytest.approx(26.85, abs=0.1)
    assert props.get('dewpoint_2m') == pytest.approx(16.85, abs=0.1)
    
    # dewpoint_depression = temp_2m - dewpoint_2m = 10.0
    assert props.get('dewpoint_depression') == pytest.approx(10.0, abs=0.1)
    
    # freezing_level_height = freezing_level_m / 1000 = 3.5
    assert props.get('freezing_level_height') == pytest.approx(3.5, abs=0.1)


def test_integrate_rap_no_file(mock_io_manager, storm_cells):
    """Test no file path returns unchanged cells."""
    results = integrate_rap(storm_cells, None, mock_io_manager)
    assert results == storm_cells
    mock_io_manager.write_warning.assert_called()


def test_integrate_rap_load_fail(mock_io_manager, storm_cells):
    """Test dataset load failure."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_instance = MockExtractor.return_value
        mock_instance.extract_batch.side_effect = Exception("Load failed")
        results = integrate_rap(storm_cells, "bad_path.grib2", mock_io_manager)
        
    assert results == storm_cells
    mock_io_manager.write_error.assert_called()


def test_integrate_rap_empty_datasets(mock_io_manager, storm_cells):
    """Test empty datasets list."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_instance = MockExtractor.return_value
        mock_instance.extract_batch.return_value = {}
        results = integrate_rap(storm_cells, "empty.grib2", mock_io_manager)
        
    # Should return unchanged
    assert results == storm_cells


def test_safe_eval_rejects_unsafe_formula(mock_io_manager, storm_cells):
    """Ensure unsupported expressions are rejected and set to None."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.get_rap_products") as mock_products, \
         patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_products.return_value = {
            "products": [],
            "derived": [{"formula": "__import__('os').system('echo hi')", "key": "unsafe"}]
        }
        MockExtractor.return_value.extract_batch.return_value = {}

        results = integrate_rap(storm_cells, "dummy_path.grib2", mock_io_manager)

    assert results[0]["properties"]["unsafe"] is None


def test_safe_eval_handles_missing_input_value(mock_io_manager, storm_cells):
    """Ensure missing variables in formulas do not crash integration."""
    with patch("EdgeWARN.core.process.integrate.integrate_rap.get_rap_products") as mock_products, \
         patch("EdgeWARN.core.process.integrate.integrate_rap.RAPPointExtractor") as MockExtractor:
        mock_products.return_value = {
            "products": [],
            "derived": [{"formula": "temp_2m - dewpoint_2m", "key": "dewpoint_depression"}]
        }
        MockExtractor.return_value.extract_batch.return_value = {}

        results = integrate_rap(storm_cells, "dummy_path.grib2", mock_io_manager)

    assert results[0]["properties"]["dewpoint_depression"] is None

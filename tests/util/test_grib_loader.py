from unittest.mock import mock_open, patch

import numpy as np

from util.grib_loader import RAPPointExtractor


def test_extract_batch_supports_variable_aliases():
    extractor = RAPPointExtractor("dummy.grib2")
    products = [
        {
            "filter": {"typeOfLevel": "isobaricInhPa"},
            "var": "v",
            "var_aliases": ["v", "VGRD"],
            "levels": [850],
            "key_template": "wind_field.v{level}",
        }
    ]
    cell_coords = {101: (35.0, -97.0)}

    messages = ["msg-vgrd", None]

    def mock_codes_get_string(gid, key):
        if key == "shortName":
            return "VGRD"
        if key == "typeOfLevel":
            return "isobaricInhPa"
        raise KeyError(key)

    def mock_codes_get_long(gid, key):
        if key == "level":
            return 850
        raise KeyError(key)

    def mock_codes_get_double_array(gid, key):
        if key == "latitudes":
            return np.array([35.0])
        if key == "longitudes":
            return np.array([263.0])
        if key == "values":
            return np.array([7.5])
        raise KeyError(key)

    with patch("builtins.open", mock_open(read_data=b"grib")), \
         patch("util.grib_loader.eccodes.codes_grib_new_from_file", side_effect=messages), \
         patch("util.grib_loader.eccodes.codes_get_string", side_effect=mock_codes_get_string), \
         patch("util.grib_loader.eccodes.codes_get_long", side_effect=mock_codes_get_long), \
         patch("util.grib_loader.eccodes.codes_get_double_array", side_effect=mock_codes_get_double_array), \
         patch("util.grib_loader.eccodes.codes_release"):
        results = extractor.extract_batch(products, cell_coords)

    assert results == {"wind_field.v850": {101: 7.5}}


def test_extract_batch_reenables_multifield_support():
    extractor = RAPPointExtractor("dummy.grib2")

    with patch("util.grib_loader.eccodes.codes_grib_multi_support_on") as mock_multi_on:
        with patch.object(extractor, "_get_product_vars", return_value=[]):
            with patch("builtins.open", mock_open(read_data=b"grib")), \
                 patch("util.grib_loader.eccodes.codes_grib_new_from_file", return_value=None):
                extractor.extract_batch([], {})

    mock_multi_on.assert_called_once()

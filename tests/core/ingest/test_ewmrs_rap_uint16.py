import json
from datetime import datetime, timezone
from unittest.mock import patch

import numpy as np

from EWMRS.rap.config import UINT16_NODATA, get_rap_uint16_layers
from EWMRS.rap.uint16_pipeline import run_rap_uint16_pipeline, scale_to_uint16


def test_default_config_defines_rap_uint16_layers():
    layers = get_rap_uint16_layers()

    assert any(layer["name"] == "RAP_Temperature_2m" for layer in layers)
    assert all("outdir" in layer for layer in layers)
    assert all("scale" in layer for layer in layers)


def test_scale_to_uint16_clips_and_reserves_nodata():
    values = np.array([[-10.0, 0.0, 50.0, 100.0, 150.0, np.nan]])

    encoded = scale_to_uint16(values, {"min": 0.0, "max": 100.0})

    assert encoded.dtype == np.dtype("<u2")
    assert encoded.tolist() == [[0, 0, 32767, 65534, 65534, UINT16_NODATA]]


def test_rap_uint16_pipeline_writes_entire_grid_array_and_metadata(tmp_path):
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")
    layer = {
        "name": "RAP_TestLayer",
        "short_names": ["2t"],
        "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
        "units": "K",
        "scale": {"min": 180.0, "max": 330.0},
        "outdir": tmp_path / "gui" / "RAP" / "RAP_TestLayer",
    }

    def mock_codes_get_string(gid, key):
        if key == "shortName":
            return "2t"
        if key == "typeOfLevel":
            return "heightAboveGround"
        raise KeyError(key)

    def mock_codes_get_long(gid, key):
        values = {"level": 2, "Ni": 3, "Nj": 2}
        if key in values:
            return values[key]
        raise KeyError(key)

    def mock_codes_get_double_array(gid, key):
        if key == "values":
            return np.array([180.0, 210.0, 240.0, 270.0, 330.0, np.nan])
        raise KeyError(key)

    with patch("EWMRS.rap.uint16_pipeline.eccodes.codes_grib_multi_support_on"), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_grib_new_from_file", side_effect=["msg", None]), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_get_string", side_effect=mock_codes_get_string), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_get_long", side_effect=mock_codes_get_long), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_get_double_array", side_effect=mock_codes_get_double_array), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_get_double", side_effect=KeyError("missingValue")), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_release"):
        timings = {}
        results = run_rap_uint16_pipeline(
            rap_file,
            dt=datetime(2026, 4, 27, 13, 0, tzinfo=timezone.utc),
            layers=[layer],
            timings=timings,
        )

    data_path = layer["outdir"] / "20260427-130000" / "data.u16"
    metadata_path = layer["outdir"] / "20260427-130000" / "metadata.json"
    index_path = layer["outdir"] / "index.json"

    assert results == {"RAP_TestLayer": data_path}
    assert data_path.is_file()
    written_values = np.fromfile(data_path, dtype="<u2")
    assert written_values.size == 6
    assert written_values.tolist() == [0, 13107, 26214, 39320, 65534, UINT16_NODATA]

    metadata = json.loads(metadata_path.read_text())
    assert metadata["shape"] == [2, 3]
    assert metadata["grid"] == {"ni": 3, "nj": 2, "point_count": 6}
    assert metadata["dtype"] == "uint16"
    assert metadata["byte_order"] == "little_endian"
    assert metadata["grib"] == {"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2}

    index = json.loads(index_path.read_text())
    assert index["timestamps"] == ["20260427-130000"]
    assert index["missing_value"] == UINT16_NODATA
    assert metadata["conversion_time_seconds"] >= 0.0
    assert timings["RAP_TestLayer"]["status"] == "converted"
    assert timings["RAP_TestLayer"]["point_count"] == 6


def test_rap_uint16_pipeline_reports_missing_layer_without_failing(tmp_path):
    rap_file = tmp_path / "RAP.20260427-13z.awp130pgrbf00.grib2"
    rap_file.write_bytes(b"grib")
    layer = {
        "name": "RAP_MissingLayer",
        "short_names": ["2t"],
        "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
        "scale": {"min": 180.0, "max": 330.0},
        "outdir": tmp_path / "gui" / "RAP" / "RAP_MissingLayer",
    }

    with patch("EWMRS.rap.uint16_pipeline.eccodes.codes_grib_multi_support_on"), \
         patch("EWMRS.rap.uint16_pipeline.eccodes.codes_grib_new_from_file", return_value=None):
        timings = {}
        results = run_rap_uint16_pipeline(rap_file, layers=[layer], timings=timings)

    assert results == {"RAP_MissingLayer": None}
    assert not (layer["outdir"] / "index.json").exists()
    assert timings["RAP_MissingLayer"] == {
        "status": "missing",
        "seconds": None,
        "output_path": None,
    }

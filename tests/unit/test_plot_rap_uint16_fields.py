import importlib.util
import sys
from pathlib import Path

import numpy as np


def _load_plot_script():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "plot_rap_uint16_fields.py"
    spec = importlib.util.spec_from_file_location("plot_rap_uint16_fields", script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_decode_uint16_field_reconstructs_scaled_values(tmp_path):
    module = _load_plot_script()
    data_path = tmp_path / "data.u16"
    np.array([0, 32767, 65534, 65535], dtype="<u2").tofile(data_path)
    metadata = {
        "shape": [2, 2],
        "scale": {"min": 0.0, "max": 100.0},
        "missing_value": 65535,
    }

    decoded = module.decode_uint16_field(data_path, metadata)

    assert decoded.shape == (2, 2)
    assert np.isclose(decoded[0, 0], 0.0)
    assert np.isclose(decoded[0, 1], 50.0, atol=0.01)
    assert np.isclose(decoded[1, 0], 100.0)
    assert np.isnan(decoded[1, 1])


def test_wind_pairing_groups_u_and_v_components():
    module = _load_plot_script()
    u_field = module.RapField(
        layer="RAP_UWind_925mb",
        timestamp="20260427-152800",
        data_path=Path("u.u16"),
        metadata_path=Path("u.json"),
        metadata={"grib": {"shortName": "u", "typeOfLevel": "isobaricInhPa", "level": 925}},
    )
    v_field = module.RapField(
        layer="RAP_VWind_925mb",
        timestamp="20260427-152800",
        data_path=Path("v.u16"),
        metadata_path=Path("v.json"),
        metadata={"grib": {"shortName": "v", "typeOfLevel": "isobaricInhPa", "level": 925}},
    )

    assert module.wind_component(u_field) == "u"
    assert module.wind_component(v_field) == "v"
    assert module.wind_pair_key(u_field) == module.wind_pair_key(v_field)
    assert module.wind_speed_name("isobaricInhPa", 925) == "RAP_WindSpeed_925mb"


def test_wind_vector_colormap_name_uses_component_metadata():
    module = _load_plot_script()
    u_field = module.RapField(
        layer="RAP_UWind_700mb",
        timestamp="20260427-152800",
        data_path=Path("u.u16"),
        metadata_path=Path("u.json"),
        metadata={"colormap_key": "RAP_Wind_HL", "grib": {"shortName": "u", "typeOfLevel": "isobaricInhPa", "level": 700}},
    )
    v_field = module.RapField(
        layer="RAP_VWind_700mb",
        timestamp="20260427-152800",
        data_path=Path("v.u16"),
        metadata_path=Path("v.json"),
        metadata={"colormap_key": "RAP_Wind_HL", "grib": {"shortName": "v", "typeOfLevel": "isobaricInhPa", "level": 700}},
    )

    assert module.wind_vector_colormap_name(u_field, v_field) == "RAP_Wind_HL"

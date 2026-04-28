import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

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


def test_should_plot_field_only_when_colormap_is_supported():
    module = _load_plot_script()
    supported = module.RapField(
        layer="RAP_Temperature_2m",
        timestamp="20260427-152800",
        data_path=Path("supported.u16"),
        metadata_path=Path("supported.json"),
        metadata={"grib": {"shortName": "2t", "typeOfLevel": "heightAboveGround", "level": 2}},
    )
    unsupported = module.RapField(
        layer="RAP_CategoricalRain_Surface",
        timestamp="20260427-152800",
        data_path=Path("unsupported.u16"),
        metadata_path=Path("unsupported.json"),
        metadata={"grib": {"shortName": "crain", "typeOfLevel": "surface", "level": 0}},
    )

    assert module.should_plot_field(supported) is True
    assert module.should_plot_field(unsupported) is False


def test_load_project_colormap_range_reads_fixed_bounds():
    module = _load_plot_script()

    assert module.load_project_colormap_range("RAP_Temperature") == (180.0, 330.0)


def test_plot_all_fields_skips_scalar_layers_without_colormaps(tmp_path, monkeypatch):
    module = _load_plot_script()
    plotted = []

    supported_data = tmp_path / "supported.u16"
    unsupported_data = tmp_path / "unsupported.u16"
    np.array([0, 65534, 32767, 1000], dtype="<u2").tofile(supported_data)
    np.array([0, 1, 1, 0], dtype="<u2").tofile(unsupported_data)

    metadata = {
        "shape": [2, 2],
        "scale": {"min": 0.0, "max": 100.0},
        "missing_value": 65535,
        "units": "K",
        "grib": {"typeOfLevel": "heightAboveGround", "level": 2},
    }
    supported = module.RapField(
        layer="RAP_Temperature_2m",
        timestamp="20260427-152800",
        data_path=supported_data,
        metadata_path=tmp_path / "supported.json",
        metadata={**metadata, "grib": {**metadata["grib"], "shortName": "2t"}},
    )
    unsupported = module.RapField(
        layer="RAP_CategoricalRain_Surface",
        timestamp="20260427-152800",
        data_path=unsupported_data,
        metadata_path=tmp_path / "unsupported.json",
        metadata={
            **metadata,
            "units": "1",
            "grib": {"shortName": "crain", "typeOfLevel": "surface", "level": 0},
        },
    )

    monkeypatch.setattr(module, "load_project_colormap", lambda name: SimpleNamespace(name=name))
    monkeypatch.setattr(module, "load_project_colormap_range", lambda name: (180.0, 330.0))

    def fake_plot_field(data, title, units, output_path, *, cmap, vmin, vmax):
        plotted.append((title, units, output_path.name, getattr(cmap, "name", cmap), vmin, vmax))

    monkeypatch.setattr(module, "plot_field", fake_plot_field)

    written = module.plot_all_fields([supported, unsupported], tmp_path / "plots")

    assert len(written) == 1
    assert plotted == [
        ("RAP_Temperature_2m 20260427-152800", "K", "RAP_Temperature_2m.png", "RAP_Temperature", 180.0, 330.0)
    ]

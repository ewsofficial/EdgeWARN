import json

import netCDF4
import numpy as np
import xarray as xr

from common.ingest.nexrad.writer import _dataset_encoding, _write_grouped_netcdf, serialize_nexrad_render_intermediate


class _Group:
    def __init__(self, dataset):
        self._dataset = dataset

    def to_dataset(self):
        return self._dataset


class _Tree:
    def __init__(self, dataset):
        self._dataset = dataset

    def __getitem__(self, key):
        assert key == "/sweep_00"
        return _Group(self._dataset)


class _ParsedTree:
    def __init__(self, dataset):
        self._dataset = dataset

    def __getitem__(self, key):
        assert key == "/sweep_00"
        return _Group(self._dataset)


def test_write_grouped_netcdf_sanitizes_boolean_attrs(tmp_path):
    dataset = xr.Dataset(
        {"DBZH": (("azimuth",), [1.0, 2.0]), "CCORH": (("azimuth",), [0.0, 0.0])},
        coords={"azimuth": [0, 1]},
        attrs={"sails_cut": True, "meta": {"mode": "test"}},
    )
    dataset["DBZH"].attrs["has_mask"] = False

    path = tmp_path / "test.nc"
    _write_grouped_netcdf(path, {"complete": True}, _Tree(dataset), ["/sweep_00"])

    reopened = xr.open_dataset(path, group="sweep_00")
    try:
        assert reopened.attrs == {}
        assert reopened["DBZH"].attrs == {}
        assert "CCORH" in reopened.data_vars
    finally:
        reopened.close()


def test_dataset_encoding_preserves_packed_storage_and_fill_value():
    data = xr.DataArray(np.array([[1.0, np.nan]], dtype=np.float64), dims=("azimuth", "range"))
    data.encoding = {"dtype": np.dtype("uint8"), "scale_factor": 0.5, "add_offset": -33.0, "_FillValue": None}
    dataset = xr.Dataset({"DBZH": data})

    encoding = _dataset_encoding(dataset)

    assert encoding["DBZH"]["dtype"] == np.dtype("uint8")
    assert encoding["DBZH"]["scale_factor"] == 0.5
    assert encoding["DBZH"]["add_offset"] == -33.0
    assert encoding["DBZH"]["_FillValue"] == 255
    assert "zlib" not in encoding["DBZH"]


def test_write_grouped_netcdf_uses_packed_compressed_encoding(tmp_path):
    data = xr.DataArray(np.array([[1.0, np.nan]], dtype=np.float64), dims=("azimuth", "range"))
    data.encoding = {"dtype": np.dtype("uint8"), "scale_factor": 0.5, "add_offset": -33.0, "_FillValue": None}
    dataset = xr.Dataset({"DBZH": data}, attrs={"sails_cut": False})

    path = tmp_path / "packed.nc"
    _write_grouped_netcdf(path, {}, _Tree(dataset), ["/sweep_00"])

    handle = netCDF4.Dataset(path)
    try:
        variable = handle.groups["sweep_00"].variables["DBZH"]
        assert variable.dtype == np.dtype("uint8")
        assert variable.getncattr("_FillValue") == 255
    finally:
        handle.close()


def test_write_grouped_netcdf_preserves_all_variables(tmp_path):
    dataset = xr.Dataset(
        {
            "DBZH": (("azimuth",), [1.0, 2.0]),
            "VRADH": (("azimuth",), [0.1, 0.2]),
            "WRADH": (("azimuth",), [0.3, 0.4]),
            "RHOHV": (("azimuth",), [0.9, 0.95]),
            "noise": (("azimuth",), [5.0, 6.0]),
        },
        coords={"azimuth": [0, 1]},
    )
    path = tmp_path / "important.nc"
    _write_grouped_netcdf(path, {}, _Tree(dataset), ["/sweep_00"])

    reopened = xr.open_dataset(path, group="sweep_00")
    try:
        assert {"DBZH", "VRADH", "WRADH", "RHOHV", "noise"}.issubset(set(reopened.data_vars))
    finally:
        reopened.close()


def test_serialize_nexrad_render_intermediate_writes_float16_triples(tmp_path):
    dataset = xr.Dataset(
        {
            "DBZH": (("azimuth", "range"), np.array([[1.5, np.nan], [3.5, 4.5]], dtype=np.float32)),
            "VRADH": (("azimuth", "range"), np.array([[10.0, 11.0], [12.0, 13.0]], dtype=np.float32)),
            "noise": (("azimuth", "range"), np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float32)),
        },
        coords={
            "azimuth": np.array([0.0, 90.0], dtype=np.float32),
            "range": np.array([1000.0, 2000.0], dtype=np.float32),
        },
    )
    parsed = type(
        "ParsedVolumeFixture",
        (),
        {
            "scan_name": "VCP-212",
            "dynamic_scan_type": "standard",
            "sweeps": [type("SweepFixture", (), {"group_name": "/sweep_00", "fixed_angle": 0.5})()],
            "datatree": _ParsedTree(dataset),
        },
    )()
    volume_path = tmp_path / "KTLH_20260507-150000_999.ar2v"
    volume_path.parent.mkdir(parents=True, exist_ok=True)
    volume_path.write_bytes(b"volume")

    manifest_path = serialize_nexrad_render_intermediate("KTLH", "999", "20260507-150000", volume_path, parsed)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert [layer["variable_name"] for layer in manifest["layers"]] == ["DBZH", "VRADH"]

    payload = np.load(manifest["layers"][0]["payload_path"])
    assert payload.dtype == np.float16
    assert payload.tolist() == [[0.0, 1000.0, 1.5], [90.0, 1000.0, 3.5], [90.0, 2000.0, 4.5]]

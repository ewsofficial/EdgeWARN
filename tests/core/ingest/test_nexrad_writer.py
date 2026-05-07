import netCDF4
import numpy as np
import xarray as xr

from common.ingest.nexrad.writer import _dataset_encoding, _write_grouped_netcdf


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
        assert "CCORH" not in reopened.data_vars
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
    assert encoding["DBZH"]["zlib"] is True


def test_write_grouped_netcdf_uses_packed_compressed_encoding(tmp_path):
    data = xr.DataArray(np.array([[1.0, np.nan]], dtype=np.float64), dims=("azimuth", "range"))
    data.encoding = {"dtype": np.dtype("uint8"), "scale_factor": 0.5, "add_offset": -33.0, "_FillValue": None}
    dataset = xr.Dataset({"DBZH": data}, attrs={"sails_cut": False})

    path = tmp_path / "packed.nc"
    _write_grouped_netcdf(path, {}, _Tree(dataset), ["/sweep_00"])

    handle = netCDF4.Dataset(path)
    try:
        variable = handle.groups["sweep_00"].variables["DBZH"]
        filters = variable.filters()
        assert variable.dtype == np.dtype("uint8")
        assert filters["zlib"] is True
        assert variable.getncattr("_FillValue") == 255
    finally:
        handle.close()


def test_write_grouped_netcdf_keeps_only_important_variables(tmp_path):
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
        assert set(reopened.data_vars) == {"DBZH", "VRADH", "WRADH", "RHOHV"}
        assert "noise" not in reopened.data_vars
    finally:
        reopened.close()

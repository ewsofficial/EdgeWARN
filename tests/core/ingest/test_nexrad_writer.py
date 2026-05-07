import xarray as xr

from common.ingest.nexrad.writer import _write_grouped_netcdf


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
        {"DBZH": (("azimuth",), [1.0, 2.0])},
        coords={"azimuth": [0, 1]},
        attrs={"sails_cut": True, "meta": {"mode": "test"}},
    )
    dataset["DBZH"].attrs["has_mask"] = False

    path = tmp_path / "test.nc"
    _write_grouped_netcdf(path, {"complete": True}, _Tree(dataset), ["/sweep_00"])

    reopened = xr.open_dataset(path, group="sweep_00")
    try:
        assert reopened.attrs["sails_cut"] == 1
        assert reopened.attrs["meta"] == '{"mode": "test"}'
        assert reopened["DBZH"].attrs["has_mask"] == 0
    finally:
        reopened.close()

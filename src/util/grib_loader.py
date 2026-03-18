"""
Fast GRIB2 loaders using eccodes bindings directly.
Bypasses cfgrib indexing overhead for MRMS and RAP GRIB files.
"""

import eccodes
import numpy as np
import xarray as xr

eccodes.codes_grib_multi_support_on()


def load_grib_fast(filepath: str) -> xr.Dataset:
    try:
        with open(filepath, "rb") as f:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                raise ValueError("No GRIB message found in file")

            try:
                ni = eccodes.codes_get_long(gid, "Ni")
                nj = eccodes.codes_get_long(gid, "Nj")
                lat0 = eccodes.codes_get_double(gid, "latitudeOfFirstGridPointInDegrees")
                lon0 = eccodes.codes_get_double(gid, "longitudeOfFirstGridPointInDegrees")
                latN = eccodes.codes_get_double(gid, "latitudeOfLastGridPointInDegrees")
                lonN = eccodes.codes_get_double(gid, "longitudeOfLastGridPointInDegrees")
                lats = np.linspace(lat0, latN, nj, dtype=np.float32)
                lons = np.linspace(lon0, lonN, ni, dtype=np.float32)
                vals = eccodes.codes_get_double_array(gid, "values")
                vals = vals.reshape(nj, ni).astype(np.float32)

                try:
                    name = eccodes.codes_get_string(gid, "shortName")
                    if not name or name == "unknown":
                        name = "unknown"
                except Exception:
                    name = "unknown"

                da = xr.DataArray(vals, coords={"latitude": lats, "longitude": lons}, dims=("latitude", "longitude"), name=name)
                return da.to_dataset()
            finally:
                eccodes.codes_release(gid)
    except Exception as e:
        raise RuntimeError(f"Fast GRIB load failed for {filepath}: {e}") from e


class RAPPointExtractor:
    def __init__(self, filepath: str):
        self.filepath = filepath

    @staticmethod
    def _get_product_vars(product: dict) -> list[str]:
        """Return all supported GRIB short-name aliases for a product."""
        var_aliases = product.get("var_aliases")
        if var_aliases is not None:
            return list(var_aliases)

        var = product["var"]
        if isinstance(var, (list, tuple, set)):
            return list(var)
        return [var]

    def extract(self, products: list, cell_coords: dict) -> dict:
        eccodes.codes_grib_multi_support_on()
        wanted = {}
        for product in products:
            short_names = self._get_product_vars(product)
            type_of_level = product["filter"]["typeOfLevel"]
            filter_level = product["filter"].get("level")
            if "levels" in product:
                for level in product["levels"]:
                    output_key = product["key_template"].format(level=level)
                    for short_name in short_names:
                        wanted[(short_name, type_of_level, level)] = output_key
            else:
                for short_name in short_names:
                    wanted[(short_name, type_of_level, filter_level)] = product["key"]

        cell_ids = list(cell_coords.keys())
        lats = np.array([cell_coords[cid][0] for cid in cell_ids])
        lons = np.array([cell_coords[cid][1] for cid in cell_ids])
        lons_360 = np.where(lons < 0, lons + 360, lons)
        results = {}
        matched_keys = set()

        with open(self.filepath, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                try:
                    try:
                        msg_short_name = eccodes.codes_get_string(gid, "shortName")
                        msg_type_of_level = eccodes.codes_get_string(gid, "typeOfLevel")
                    except Exception:
                        continue

                    try:
                        msg_level = eccodes.codes_get_long(gid, "level")
                    except Exception:
                        msg_level = None

                    output_key = wanted.get((msg_short_name, msg_type_of_level, msg_level))
                    if output_key is None:
                        output_key = wanted.get((msg_short_name, msg_type_of_level, None))
                    if output_key is None or output_key in matched_keys:
                        continue

                    cell_values = {}
                    for i, cid in enumerate(cell_ids):
                        try:
                            nearest = eccodes.codes_grib_find_nearest(gid, lats[i], lons_360[i])
                            cell_values[cid] = nearest[0].value if nearest and len(nearest) > 0 else None
                        except Exception:
                            cell_values[cid] = None

                    results[output_key] = cell_values
                    matched_keys.add(output_key)
                    if len(matched_keys) == len(wanted):
                        break
                finally:
                    eccodes.codes_release(gid)

        return results

    def extract_batch(self, products: list, cell_coords: dict) -> dict:
        from scipy.spatial import cKDTree

        eccodes.codes_grib_multi_support_on()
        wanted = {}
        for product in products:
            short_names = self._get_product_vars(product)
            type_of_level = product["filter"]["typeOfLevel"]
            filter_level = product["filter"].get("level")
            if "levels" in product:
                for level in product["levels"]:
                    output_key = product["key_template"].format(level=level)
                    for short_name in short_names:
                        wanted[(short_name, type_of_level, level)] = output_key
            else:
                for short_name in short_names:
                    wanted[(short_name, type_of_level, filter_level)] = product["key"]

        cell_ids = list(cell_coords.keys())
        cell_lats = np.array([cell_coords[cid][0] for cid in cell_ids])
        cell_lons = np.array([cell_coords[cid][1] for cid in cell_ids])
        cell_lons_360 = np.where(cell_lons < 0, cell_lons + 360, cell_lons)
        cell_points = np.column_stack((cell_lats, cell_lons_360))
        results = {}
        matched_keys = set()
        tree = None
        nearest_indices = None

        with open(self.filepath, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                try:
                    try:
                        msg_short_name = eccodes.codes_get_string(gid, "shortName")
                        msg_type_of_level = eccodes.codes_get_string(gid, "typeOfLevel")
                    except Exception:
                        continue

                    try:
                        msg_level = eccodes.codes_get_long(gid, "level")
                    except Exception:
                        msg_level = None

                    output_key = wanted.get((msg_short_name, msg_type_of_level, msg_level))
                    if output_key is None:
                        output_key = wanted.get((msg_short_name, msg_type_of_level, None))
                    if output_key is None or output_key in matched_keys:
                        continue

                    if tree is None:
                        try:
                            grid_lats = eccodes.codes_get_double_array(gid, "latitudes")
                            grid_lons = eccodes.codes_get_double_array(gid, "longitudes")
                        except Exception:
                            ni = eccodes.codes_get_long(gid, "Ni")
                            nj = eccodes.codes_get_long(gid, "Nj")
                            lat0 = eccodes.codes_get_double(gid, "latitudeOfFirstGridPointInDegrees")
                            lon0 = eccodes.codes_get_double(gid, "longitudeOfFirstGridPointInDegrees")
                            latN = eccodes.codes_get_double(gid, "latitudeOfLastGridPointInDegrees")
                            lonN = eccodes.codes_get_double(gid, "longitudeOfLastGridPointInDegrees")
                            lats_1d = np.linspace(lat0, latN, nj)
                            lons_1d = np.linspace(lon0, lonN, ni)
                            grid_lons, grid_lats = np.meshgrid(lons_1d, lats_1d)
                            grid_lats = grid_lats.ravel()
                            grid_lons = grid_lons.ravel()

                        grid_lons = np.where(grid_lons < 0, grid_lons + 360, grid_lons)
                        tree = cKDTree(np.column_stack((grid_lats, grid_lons)))
                        _, nearest_indices = tree.query(cell_points)

                    vals = eccodes.codes_get_double_array(gid, "values")
                    results[output_key] = {cid: float(vals[idx]) if idx is not None else None for cid, idx in zip(cell_ids, nearest_indices)}
                    matched_keys.add(output_key)
                    if len(matched_keys) == len(wanted):
                        break
                finally:
                    eccodes.codes_release(gid)

        return results

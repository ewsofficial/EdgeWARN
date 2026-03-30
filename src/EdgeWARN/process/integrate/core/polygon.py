from shapely.geometry import Polygon

import numpy as np


def uses_360_longitude(lon_vals):
    finite_lons = np.asarray(lon_vals)
    finite_lons = finite_lons[np.isfinite(finite_lons)]
    if finite_lons.size == 0:
        return False
    return float(np.min(finite_lons)) >= 0.0 and float(np.max(finite_lons)) > 180.0


def polygon_for_dataset(poly, lon_vals):
    if poly is None or not uses_360_longitude(lon_vals):
        return poly

    return Polygon([
        (lon + 360.0 if lon < 0.0 else lon, lat)
        for lon, lat in poly.exterior.coords
    ])

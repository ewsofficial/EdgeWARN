from shapely.geometry import Polygon

import numpy as np


def uses_360_longitude(lon_vals):
    finite_lons = np.asarray(lon_vals)
    finite_lons = finite_lons[np.isfinite(finite_lons)]
    if finite_lons.size == 0:
        return False
    return float(np.min(finite_lons)) >= 0.0 and float(np.max(finite_lons)) > 180.0


def polygon_for_longitude_mode(poly, use_360_longitude):
    if poly is None or not use_360_longitude:
        return poly

    return Polygon([
        (lon + 360.0 if lon < 0.0 else lon, lat)
        for lon, lat in poly.exterior.coords
    ])


def polygon_for_dataset(poly, lon_vals):
    return polygon_for_longitude_mode(poly, uses_360_longitude(lon_vals))

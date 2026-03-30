import numpy as np


def axis_slice_indices(coord_vals, min_val, max_val):
    """Return start/end indices for monotonic 1D coordinate bounds."""
    if coord_vals[0] < coord_vals[-1]:
        start_idx = np.searchsorted(coord_vals, min_val)
        end_idx = np.searchsorted(coord_vals, max_val, side="right")
    else:
        reversed_vals = coord_vals[::-1]
        coord_len = len(coord_vals)
        end_idx = coord_len - np.searchsorted(reversed_vals, min_val)
        start_idx = coord_len - np.searchsorted(reversed_vals, max_val, side="right")

    return start_idx, end_idx


def extract_spatial_subset(ds, var, is_grib, var_values, lat_name, lon_name, lat_vals, lon_vals, poly):
    minx, miny, maxx, maxy = poly.bounds

    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        lat_start_idx, lat_end_idx = axis_slice_indices(lat_vals, miny, maxy)
        lon_start_idx, lon_end_idx = axis_slice_indices(lon_vals, minx, maxx)

        lat_start_idx = max(0, min(lat_start_idx, len(lat_vals)))
        lat_end_idx = max(0, min(lat_end_idx, len(lat_vals)))
        lon_start_idx = max(0, min(lon_start_idx, len(lon_vals)))
        lon_end_idx = max(0, min(lon_end_idx, len(lon_vals)))

        lat_subset = lat_vals[lat_start_idx:lat_end_idx]
        lon_subset = lon_vals[lon_start_idx:lon_end_idx]
        if lat_subset.size == 0 or lon_subset.size == 0:
            return None, None, None

        if is_grib:
            sub_var = var_values[lat_start_idx:lat_end_idx, lon_start_idx:lon_end_idx]
        else:
            lat_dim = ds[lat_name].dims[0]
            lon_dim = ds[lon_name].dims[0]
            sub_var = var.isel(
                {lat_dim: slice(lat_start_idx, lat_end_idx), lon_dim: slice(lon_start_idx, lon_end_idx)}
            )
            extra_dims = {
                dim: 0
                for dim, size in sub_var.sizes.items()
                if dim not in (lat_dim, lon_dim)
            }
            for dim, size in sub_var.sizes.items():
                if dim not in (lat_dim, lon_dim) and size != 1:
                    raise ValueError(f"Non-spatial dimension {dim} has size {size}")
            if extra_dims:
                sub_var = sub_var.isel(extra_dims, drop=True)
            if sub_var.dims != (lat_dim, lon_dim):
                sub_var = sub_var.transpose(lat_dim, lon_dim)
            sub_var = sub_var.compute().values

        sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)
        return np.asarray(sub_var), sub_lat, sub_lon

    if lat_vals.ndim == 2 and lon_vals.ndim == 2:
        finite_mask = np.isfinite(lat_vals) & np.isfinite(lon_vals)
        bbox_mask = (
            finite_mask
            & (lon_vals >= minx)
            & (lon_vals <= maxx)
            & (lat_vals >= miny)
            & (lat_vals <= maxy)
        )
        if not np.any(bbox_mask):
            return None, None, None

        row_indices, col_indices = np.where(bbox_mask)
        row_slice = slice(int(row_indices.min()), int(row_indices.max()) + 1)
        col_slice = slice(int(col_indices.min()), int(col_indices.max()) + 1)

        if is_grib:
            sub_var = var_values[row_slice, col_slice]
        else:
            spatial_dims = ds[lat_name].dims
            if len(spatial_dims) != 2:
                raise ValueError(f"Unsupported coordinate dimensions for {lat_name}: {spatial_dims}")
            sub_var = var.isel(
                {spatial_dims[0]: row_slice, spatial_dims[1]: col_slice}
            )
            extra_dims = {
                dim: 0
                for dim, size in sub_var.sizes.items()
                if dim not in spatial_dims
            }
            for dim, size in sub_var.sizes.items():
                if dim not in spatial_dims and size != 1:
                    raise ValueError(f"Non-spatial dimension {dim} has size {size}")
            if extra_dims:
                sub_var = sub_var.isel(extra_dims, drop=True)
            if sub_var.dims != spatial_dims:
                sub_var = sub_var.transpose(*spatial_dims)
            sub_var = sub_var.compute().values

        sub_var = np.asarray(sub_var)
        sub_lat = lat_vals[row_slice, col_slice]
        sub_lon = lon_vals[row_slice, col_slice]
        if sub_var.ndim != 2 or sub_var.shape != sub_lat.shape:
            raise ValueError(
                f"Spatial subset shape mismatch: data={sub_var.shape}, lat={sub_lat.shape}, lon={sub_lon.shape}"
            )
        return sub_var, sub_lat, sub_lon

    raise ValueError(
        f"Unsupported coordinate layout: lat.ndim={lat_vals.ndim}, lon.ndim={lon_vals.ndim}"
    )

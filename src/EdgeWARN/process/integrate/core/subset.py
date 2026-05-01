import numpy as np
import shapely.vectorized as sv


def axis_slice_indices(coord_vals, min_val, max_val, interior=False):
    """Return start/end indices for monotonic 1D coordinate bounds."""
    if coord_vals[0] < coord_vals[-1]:
        start_side = "right" if interior else "left"
        end_side = "left" if interior else "right"
        start_idx = np.searchsorted(coord_vals, min_val, side=start_side)
        end_idx = np.searchsorted(coord_vals, max_val, side=end_side)
    else:
        reversed_vals = coord_vals[::-1]
        coord_len = len(coord_vals)
        if interior:
            end_idx = coord_len - np.searchsorted(reversed_vals, min_val, side="right")
            start_idx = coord_len - np.searchsorted(reversed_vals, max_val, side="left")
        else:
            end_idx = coord_len - np.searchsorted(reversed_vals, min_val, side="left")
            start_idx = coord_len - np.searchsorted(reversed_vals, max_val, side="right")

    return start_idx, end_idx


def build_spatial_lookup(ds, lat_name, lon_name, lat_vals, lon_vals, poly, axis_aligned_rectangle=False):
    minx, miny, maxx, maxy = poly.bounds

    if lat_vals.ndim == 1 and lon_vals.ndim == 1:
        lat_start_idx, lat_end_idx = axis_slice_indices(lat_vals, miny, maxy, interior=axis_aligned_rectangle)
        lon_start_idx, lon_end_idx = axis_slice_indices(lon_vals, minx, maxx, interior=axis_aligned_rectangle)

        lat_start_idx = max(0, min(lat_start_idx, len(lat_vals)))
        lat_end_idx = max(0, min(lat_end_idx, len(lat_vals)))
        lon_start_idx = max(0, min(lon_start_idx, len(lon_vals)))
        lon_end_idx = max(0, min(lon_end_idx, len(lon_vals)))

        lat_subset = lat_vals[lat_start_idx:lat_end_idx]
        lon_subset = lon_vals[lon_start_idx:lon_end_idx]
        if lat_subset.size == 0 or lon_subset.size == 0:
            return {"empty": True}

        lookup = {
            "empty": False,
            "layout": "1d",
            "lat_slice": slice(lat_start_idx, lat_end_idx),
            "lon_slice": slice(lon_start_idx, lon_end_idx),
            "lat_dim": ds[lat_name].dims[0],
            "lon_dim": ds[lon_name].dims[0],
            "inside_mask": None,
        }

        if not axis_aligned_rectangle:
            sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)
            lookup["inside_mask"] = sv.contains(poly, sub_lon, sub_lat)

        return lookup

    if lat_vals.ndim == 2 and lon_vals.ndim == 2:
        finite_mask = np.isfinite(lat_vals) & np.isfinite(lon_vals)
        lon_lower_op = np.greater if axis_aligned_rectangle else np.greater_equal
        lon_upper_op = np.less if axis_aligned_rectangle else np.less_equal
        lat_lower_op = np.greater if axis_aligned_rectangle else np.greater_equal
        lat_upper_op = np.less if axis_aligned_rectangle else np.less_equal
        bbox_mask = (
            finite_mask
            & lon_lower_op(lon_vals, minx)
            & lon_upper_op(lon_vals, maxx)
            & lat_lower_op(lat_vals, miny)
            & lat_upper_op(lat_vals, maxy)
        )
        if not np.any(bbox_mask):
            return {"empty": True}

        row_indices, col_indices = np.where(bbox_mask)
        row_slice = slice(int(row_indices.min()), int(row_indices.max()) + 1)
        col_slice = slice(int(col_indices.min()), int(col_indices.max()) + 1)

        lookup = {
            "empty": False,
            "layout": "2d",
            "row_slice": row_slice,
            "col_slice": col_slice,
            "spatial_dims": ds[lat_name].dims,
            "inside_mask": None,
        }

        if not axis_aligned_rectangle:
            sub_lat = lat_vals[row_slice, col_slice]
            sub_lon = lon_vals[row_slice, col_slice]
            lookup["inside_mask"] = sv.contains(poly, sub_lon, sub_lat)

        return lookup

    raise ValueError(
        f"Unsupported coordinate layout: lat.ndim={lat_vals.ndim}, lon.ndim={lon_vals.ndim}"
    )


def extract_spatial_subset(ds, var, is_grib, var_values, spatial_lookup):
    if spatial_lookup.get("empty"):
        return None, None

    if spatial_lookup["layout"] == "1d":
        lat_slice = spatial_lookup["lat_slice"]
        lon_slice = spatial_lookup["lon_slice"]

        if is_grib:
            sub_var = var_values[lat_slice, lon_slice]
        else:
            lat_dim = spatial_lookup["lat_dim"]
            lon_dim = spatial_lookup["lon_dim"]
            sub_var = var.isel({lat_dim: lat_slice, lon_dim: lon_slice})
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

        return np.asarray(sub_var), spatial_lookup["inside_mask"]

    if spatial_lookup["layout"] == "2d":
        row_slice = spatial_lookup["row_slice"]
        col_slice = spatial_lookup["col_slice"]

        if is_grib:
            sub_var = var_values[row_slice, col_slice]
        else:
            spatial_dims = spatial_lookup["spatial_dims"]
            if len(spatial_dims) != 2:
                raise ValueError(f"Unsupported coordinate dimensions: {spatial_dims}")
            sub_var = var.isel({spatial_dims[0]: row_slice, spatial_dims[1]: col_slice})
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
        inside_mask = spatial_lookup["inside_mask"]
        if inside_mask is not None and (sub_var.ndim != 2 or sub_var.shape != inside_mask.shape):
            raise ValueError(
                f"Spatial subset shape mismatch: data={sub_var.shape}, mask={inside_mask.shape}"
            )
        return sub_var, inside_mask

    raise ValueError(f"Unsupported lookup layout: {spatial_lookup['layout']}")

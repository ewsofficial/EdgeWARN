"""
Config-driven RAP integration.
Optimized: Uses filter_by_keys to load only necessary level types.
"""
import numpy as np
import xarray as xr
from .config import get_rap_products

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}

# Define the specific level types we need (only 3 instead of 39)
_REQUIRED_LEVEL_TYPES = [
    {"typeOfLevel": "isobaricInhPa"},  # Winds at pressure levels
    {"typeOfLevel": "heightAboveGround", "level": 2},  # T2m, D2m
    {"typeOfLevel": "isothermZero"},  # Freezing level
]


def integrate_rap(storm_cells, rap_file_path, io_manager):
    """
    Integrate RAP data into storm cells.
    Optimized: Only loads 3 required level types instead of all 39 datasets.
    """
    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided")
        return storm_cells

    config = get_rap_products()
    products = config.get("products", [])
    derived = config.get("derived", [])

    # Load ONLY needed datasets using filter_by_keys (3 calls vs scanning 39)
    datasets = {}
    lat_vals = None
    lon_vals = None
    
    for filter_keys in _REQUIRED_LEVEL_TYPES:
        try:
            ds = xr.open_dataset(
                rap_file_path,
                engine="cfgrib",
                backend_kwargs={"filter_by_keys": filter_keys}
            )
            ds.load()  # Eager load for fast access
            
            # Create key for lookup
            level_type = filter_keys["typeOfLevel"]
            level = filter_keys.get("level")
            key = (level_type, level) if level else (level_type,)
            datasets[key] = ds
            
            # Extract lat/lon from first successful load
            if lat_vals is None and 'latitude' in ds.coords:
                lat_vals = ds.latitude.values
                lon_vals = ds.longitude.values
                if lon_vals.max() > 180:
                    lon_vals = np.where(lon_vals > 180, lon_vals - 360, lon_vals)
                    
            io_manager.write_debug(f"Loaded RAP dataset: {key}")
        except Exception as e:
            io_manager.write_debug(f"Could not load RAP with filter {filter_keys}: {e}")

    if lat_vals is None:
        io_manager.write_error("No lat/lon in RAP datasets")
        return storm_cells

    io_manager.write_debug(f"Loaded {len(datasets)} RAP datasets (filtered)")

    # Pre-compute cell indices ONCE
    cell_indices = _precompute_cell_indices(storm_cells, lat_vals, lon_vals)

    # Find matching dataset for a product
    def find_dataset_for_product(product):
        filter_keys = product["filter"]
        level_type = filter_keys.get("typeOfLevel")
        level = filter_keys.get("level")
        
        # Try exact match first
        key = (level_type, level) if level else (level_type,)
        if key in datasets:
            return datasets[key]
        
        # Try without level (for multi-level datasets like isobaric)
        key = (level_type,)
        return datasets.get(key)

    # Process each product
    for product in products:
        var_name = product["var"]
        ds = find_dataset_for_product(product)
        if ds is None or var_name not in ds.data_vars:
            continue

        transform_fn = TRANSFORMS.get(product.get("transform"), lambda x: x)

        if "levels" in product:
            levels = product["levels"]
            key_template = product["key_template"]
            level_coord = product["filter"]["typeOfLevel"]

            for level in levels:
                try:
                    data = ds[var_name].sel({level_coord: level}).values
                    key = key_template.format(level=level)
                    _apply_to_cells_fast(storm_cells, cell_indices, data, key, transform_fn)
                except Exception:
                    pass
        else:
            key = product["key"]
            try:
                data = ds[var_name].values
                _apply_to_cells_fast(storm_cells, cell_indices, data, key, transform_fn)
            except Exception:
                pass

    # Calculate derived fields
    for d in derived:
        _calculate_derived(storm_cells, d["formula"], d["key"])

    # Cleanup
    for ds in datasets.values():
        try:
            ds.close()
        except Exception:
            pass

    return storm_cells


def _precompute_cell_indices(storm_cells, lat_vals, lon_vals):
    """Pre-compute grid indices for all cells once."""
    indices = {}
    for cell in storm_cells:
        cell_id = cell.get("id")
        centroid = cell.get("centroid", [0, 0])
        lat, lon = centroid[0], centroid[1]
        if lon > 180:
            lon -= 360
        try:
            dist_sq = (lat_vals - lat) ** 2 + (lon_vals - lon) ** 2
            indices[cell_id] = np.unravel_index(np.argmin(dist_sq), dist_sq.shape)
        except Exception:
            indices[cell_id] = None
    return indices


def _apply_to_cells_fast(storm_cells, cell_indices, data, key, transform_fn):
    """Extract value using pre-computed indices."""
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
        idx = cell_indices.get(cell.get("id"))
        if idx is None:
            cell["properties"][key] = None
        else:
            try:
                cell["properties"][key] = round(transform_fn(float(data[idx])), 2)
            except Exception:
                cell["properties"][key] = None


def _calculate_derived(storm_cells, formula, key):
    """Calculate derived field from existing properties."""
    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            props[key] = round(eval(formula, {"__builtins__": {}}, props), 2)
        except Exception:
            props[key] = None

"""
Config-driven RAP integration.
Optimized: Single cfgrib.open_datasets call, then select needed datasets from list.
Optimized: Grid-aware indexing for O(1) cell-to-grid lookups.
"""
import numpy as np
import cfgrib
from .config import get_rap_products
from .grid_index import GridIndex

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}


def integrate_rap(storm_cells, rap_file_path, io_manager):
    """
    Integrate RAP data into storm cells.
    Uses single cfgrib.open_datasets call for efficiency.
    """
    if not storm_cells:
        return storm_cells

    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided")
        return storm_cells

    config = get_rap_products()
    products = config.get("products", [])
    derived = config.get("derived", [])

    # Load ALL datasets in ONE call (efficient: single scan of GRIB file)
    try:
        all_datasets = cfgrib.open_datasets(rap_file_path)
        io_manager.write_debug(f"Loaded {len(all_datasets)} datasets from RAP")
    except Exception as e:
        io_manager.write_error(f"Failed to load RAP file: {e}")
        return storm_cells

    # Extract lat/lon from first available dataset
    lat_vals = None
    lon_vals = None
    for ds in all_datasets:
        if 'latitude' in ds.coords:
            lat_vals = ds.latitude.values
            lon_vals = ds.longitude.values
            if lon_vals.max() > 180:
                lon_vals = np.where(lon_vals > 180, lon_vals - 360, lon_vals)
            break

    if lat_vals is None:
        io_manager.write_error("No lat/lon in RAP datasets")
        return storm_cells

    # Pre-compute cell indices ONCE
    cell_indices = _precompute_cell_indices(storm_cells, lat_vals, lon_vals)

    # Find matching datasets for each product
    def find_dataset_for_product(product):
        """Find the dataset that matches the product's filter and has the variable."""
        filter_keys = product["filter"]
        var_name = product["var"]
        level_type = filter_keys.get("typeOfLevel")
        level = filter_keys.get("level")

        for ds in all_datasets:
            if level_type not in ds.coords:
                continue
            if var_name not in ds.data_vars:
                continue
            
            coord_val = ds[level_type].values
            if level is not None:
                # Check level match
                if coord_val.ndim == 0:
                    if float(coord_val) != float(level):
                        continue
                else:
                    if float(level) not in coord_val:
                        continue
            return ds
        return None

    # Process each product
    for product in products:
        var_name = product["var"]
        ds = find_dataset_for_product(product)
        if ds is None:
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

    # Cleanup all datasets
    for ds in all_datasets:
        try:
            ds.close()
        except Exception:
            pass

    return storm_cells


def _precompute_cell_indices(storm_cells, lat_vals, lon_vals):
    """
    Pre-compute grid indices for all cells using optimized grid indexing.
    
    Uses GridIndex factory to automatically select the optimal indexing
    strategy (regular grid O(1) or k-d tree O(log N)) based on grid type.
    
    Args:
        storm_cells: List of storm cell dictionaries with 'id' and 'centroid'
        lat_vals: 2D array of latitudes from RAP grid
        lon_vals: 2D array of longitudes from RAP grid
    
    Returns:
        Dictionary mapping cell_id -> (lat_idx, lon_idx) or None
    """
    # Create appropriate indexer based on grid type (auto-detected)
    indexer = GridIndex.create(lat_vals, lon_vals)
    
    indices = {}
    for cell in storm_cells:
        cell_id = cell.get("id")
        centroid = cell.get("centroid", [0, 0])
        lat, lon = centroid[0], centroid[1]
        
        try:
            indices[cell_id] = indexer.query(lat, lon)
        except Exception:
            indices[cell_id] = None
    
    return indices


def _set_nested(root, key, value):
    """Set value in a nested dictionary using dot notation."""
    parts = key.split(".")
    curr = root
    for part in parts[:-1]:
        if part not in curr:
            curr[part] = {}
        elif not isinstance(curr[part], dict):
            # Overwrite if it exists but isn't a dict (shouldn't happen with clean data)
            curr[part] = {}
        curr = curr[part]
    curr[parts[-1]] = value


def _apply_to_cells_fast(storm_cells, cell_indices, data, key, transform_fn):
    """Extract value using pre-computed indices."""
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
        idx = cell_indices.get(cell.get("id"))
        
        value = None
        if idx is not None:
            try:
                value = round(transform_fn(float(data[idx])), 2)
            except Exception:
                value = None
        
        _set_nested(cell["properties"], key, value)


def _calculate_derived(storm_cells, formula, key):
    """Calculate derived field from existing properties."""
    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            props[key] = round(eval(formula, {"__builtins__": {}}, props), 2)
        except Exception:
            props[key] = None

"""
Config-driven RAP integration.
Loads RAP file once and extracts all configured products.
"""
import numpy as np
import cfgrib
from .config import get_rap_products

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}


def integrate_rap(storm_cells, rap_file_path, io_manager):
    """
    Integrate RAP data into storm cells using config-driven approach.
    Loads file once, extracts all products, calculates derived fields.
    """
    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided")
        return storm_cells

    config = get_rap_products()
    products = config.get("products", [])
    derived = config.get("derived", [])

    # Load ALL datasets once (no filter)
    try:
        all_datasets = cfgrib.open_datasets(rap_file_path)
        io_manager.write_debug(f"Loaded {len(all_datasets)} datasets from RAP file")
    except Exception as e:
        io_manager.write_error(f"Failed to load RAP file: {e}")
        return storm_cells

    # Index datasets by their level type coordinate
    dataset_index = {}
    for ds in all_datasets:
        coords = list(ds.coords.keys())
        for coord in coords:
            if coord not in ['time', 'step', 'latitude', 'longitude', 'valid_time', 'x', 'y']:
                # This is a level coordinate (e.g., isobaricInhPa, heightAboveGround)
                key = (coord, ds[coord].values.item() if ds[coord].values.ndim == 0 else tuple(ds[coord].values.tolist()))
                if key not in dataset_index:
                    dataset_index[key] = []
                dataset_index[key].append(ds)

    # Extract lat/lon from first dataset
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
        io_manager.write_error("Could not find lat/lon coordinates in RAP file")
        return storm_cells

    def find_dataset(filter_keys, var_name):
        """Find dataset matching the filter criteria AND containing the variable."""
        level_type = filter_keys.get("typeOfLevel")
        level = filter_keys.get("level")
        
        for ds in all_datasets:
            if level_type in ds.coords:
                # Check if variable exists in this dataset
                if var_name not in ds.data_vars:
                    continue
                    
                coord_val = ds[level_type].values
                # Check if level matches (if specified)
                if level is not None:
                    if coord_val.ndim == 0:
                        if float(coord_val) == float(level):
                            return ds
                    else:
                        if float(level) in coord_val:
                            return ds
                else:
                    # No specific level required, just return first match with var
                    return ds
        return None

    # Process each product
    for product in products:
        var_name = product["var"]
        ds = find_dataset(product["filter"], var_name)
        if ds is None:
            io_manager.write_warning(f"No dataset found for filter: {product['filter']} with var '{var_name}'")
            continue
        if var_name not in ds.data_vars:
            io_manager.write_warning(f"Variable '{var_name}' not found in dataset for filter {product['filter']}")
            continue

        transform_fn = TRANSFORMS.get(product.get("transform"), lambda x: x)

        # Handle multi-level products (winds)
        if "levels" in product:
            levels = product["levels"]
            key_template = product["key_template"]
            level_coord = product["filter"]["typeOfLevel"]

            for level in levels:
                try:
                    data = ds[var_name].sel({level_coord: level}).values
                    key = key_template.format(level=level)
                    _apply_to_cells(storm_cells, lat_vals, lon_vals, data, key, transform_fn, io_manager)
                except Exception as e:
                    io_manager.write_debug(f"Could not extract {var_name} at {level}: {e}")
        else:
            # Single-level product
            key = product["key"]
            data = ds[var_name].values
            _apply_to_cells(storm_cells, lat_vals, lon_vals, data, key, transform_fn, io_manager)

    # Calculate derived fields
    for d in derived:
        formula = d["formula"]
        key = d["key"]
        _calculate_derived(storm_cells, formula, key)

    # Cleanup
    for ds in all_datasets:
        if hasattr(ds, "close"):
            try:
                ds.close()
            except Exception:
                pass

    return storm_cells


def _apply_to_cells(storm_cells, lat_vals, lon_vals, data, key, transform_fn, io_manager):
    """Extract value at each storm cell centroid and store in properties."""
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}

        centroid = cell.get("centroid", [0, 0])
        centroid_lat, centroid_lon = centroid[0], centroid[1]

        # Normalize longitude
        if centroid_lon > 180:
            centroid_lon -= 360

        try:
            dist_sq = (lat_vals - centroid_lat) ** 2 + (lon_vals - centroid_lon) ** 2
            min_idx = np.unravel_index(np.argmin(dist_sq), dist_sq.shape)
            val = float(data[min_idx])
            cell["properties"][key] = round(transform_fn(val), 2)
        except Exception as e:
            io_manager.write_debug(f"Error extracting {key} for cell {cell.get('id')}: {e}")
            cell["properties"][key] = None


def _calculate_derived(storm_cells, formula, key):
    """Calculate derived field from existing properties."""
    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            result = eval(formula, {"__builtins__": {}}, props)
            props[key] = round(result, 2)
        except Exception:
            props[key] = None

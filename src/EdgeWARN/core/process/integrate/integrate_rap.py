"""
Config-driven RAP integration.
Optimized: Zero-grid-memory extraction via eccodes RAPPointExtractor.
"""
from .config import get_rap_products
from util.grib_loader import RAPPointExtractor

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}


def integrate_rap(storm_cells, rap_file_path, io_manager):
    """
    Integrate RAP data into storm cells.
    Uses RAPPointExtractor for efficient point-based data extraction.
    """
    if not storm_cells:
        return storm_cells

    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided")
        return storm_cells

    config = get_rap_products()
    products = config.get("products", [])
    derived = config.get("derived", [])

    # Prepare cell coordinates
    cell_coords = {}
    for cell in storm_cells:
        cid = cell.get("id")
        centroid = cell.get("centroid", [0, 0])
        cell_coords[cid] = (centroid[0], centroid[1])

    # Run batch extraction
    try:
        print(">>> [DEBUG] Before RAPPointExtractor initialization", flush=True)
        extractor = RAPPointExtractor(rap_file_path)
        print(">>> [DEBUG] After RAPPointExtractor initialization, before extract_batch", flush=True)
        extracted_data = extractor.extract_batch(products, cell_coords)
        print(">>> [DEBUG] After extract_batch", flush=True)
        io_manager.write_debug(f"Extracted {len(extracted_data)} keys from RAP")
    except Exception as e:
        io_manager.write_error(f"Failed to extract RAP data: {e}")
        return storm_cells

    # Apply extracted data to cells
    for product in products:
        transform_fn = TRANSFORMS.get(product.get("transform"), lambda x: x)
        
        if "levels" in product:
            levels = product["levels"]
            key_template = product["key_template"]
            
            for level in levels:
                key = key_template.format(level=level)
                if key in extracted_data:
                    _apply_to_cells(storm_cells, extracted_data[key], key, transform_fn)
        else:
            key = product["key"]
            if key in extracted_data:
                _apply_to_cells(storm_cells, extracted_data[key], key, transform_fn)
                
    # Calculate derived fields
    for d in derived:
        _calculate_derived(storm_cells, d["formula"], d["key"])

    return storm_cells


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


def _apply_to_cells(storm_cells, cell_values, key, transform_fn):
    """Apply extracted values to cells."""
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
            
        cid = cell.get("id")
        val = cell_values.get(cid)
        
        final_val = None
        if val is not None:
            try:
                final_val = round(transform_fn(float(val)), 2)
            except Exception:
                final_val = None
                
        _set_nested(cell["properties"], key, final_val)


def _calculate_derived(storm_cells, formula, key):
    """Calculate derived field from existing properties."""
    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            props[key] = round(eval(formula, {"__builtins__": {}}, props), 2)
        except Exception:
            props[key] = None

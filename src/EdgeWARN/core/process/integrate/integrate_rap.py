import xarray as xr
import numpy as np
import shapely.vectorized as sv
from .utils import StormIntegrationUtils, RAPFileHandler

# Suppress cfgrib/xarray compatibility warnings
xr.set_options(use_new_combine_kwarg_defaults=True)

def integrate_rap_winds(storm_cells, rap_file_path, io_manager):
    """
    Integrates RAP wind components (U and V) for 850, 700, 500, and 250mb levels.
    Finds the maximum value within each storm cell's boundary.
    """
    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided for integration")
        return storm_cells

    handler = RAPFileHandler(io_manager)
    ds = handler.get_isobaric_dataset(rap_file_path)
    
    if ds is None:
        io_manager.write_warning("Could not load RAP dataset, returning cells unchanged")
        return storm_cells

    levels = [850, 700, 500, 250]
    variables = ['u', 'v']
    
    # Check available levels
    if 'isobaricInhPa' not in ds.coords:
        io_manager.write_error("No isobaricInhPa coordinate found in RAP file")
        return storm_cells
    
    available_levels = [l for l in levels if l in ds.isobaricInhPa.values]
    if not available_levels:
        io_manager.write_error(f"None of the target levels {levels} found in RAP file. Available levels: {ds.isobaricInhPa.values}")
        return storm_cells

    io_manager.write_info(f"Integrating RAP wind data for levels: {available_levels}")

    # Extract coordinates once and handle coordinate system
    lat_vals = ds.latitude.values
    lon_vals = ds.longitude.values
    
    # Handle longitude coordinate system conversion if needed
    lon_needs_conversion = lon_vals.max() > 180
    if lon_needs_conversion:
        io_manager.write_debug("Converting longitude from 0-360 to -180-180 range")
        lon_vals = np.where(lon_vals > 180, lon_vals - 360, lon_vals)
        # Also update the dataset coordinates for consistency
        ds = ds.assign_coords(longitude=lon_vals)

    # Check for available variables and their naming conventions
    available_vars = []
    for var in variables:
        if var in ds.data_vars:
            available_vars.append(var)
        else:
            # Try alternative naming conventions for RAP data
            alt_names = {
                'u': ['UGRD', 'u-component_of_wind_isobaric', 'wind_u'],
                'v': ['VGRD', 'v-component_of_wind_isobaric', 'wind_v']
            }
            for alt_name in alt_names.get(var, []):
                if alt_name in ds.data_vars:
                    available_vars.append(alt_name)
                    io_manager.write_debug(f"Found alternative variable name '{alt_name}' for '{var}'")
                    break
    
    if not available_vars:
        io_manager.write_error(f"No wind components found in RAP file. Available variables: {list(ds.data_vars.keys())}")
        return storm_cells

    # Pre-load all required level/variable arrays to avoid repetitive sel() calls
    data_cache = {}
    for level in available_levels:
        ds_level = ds.sel(isobaricInhPa=level)
        for var_name in available_vars:
            if var_name in ds_level.data_vars:
                data_cache[(var_name, level)] = ds_level[var_name].values
                # Debug: Check data range
                var_data = ds_level[var_name].values
                io_manager.write_debug(f"Variable {var_name} at {level}mb: shape={var_data.shape}, min={np.nanmin(var_data):.2f}, max={np.nanmax(var_data):.2f}, non-zero count={np.count_nonzero(var_data)}")
                io_manager.write_debug(f"  Data coords: lat shape={lat_vals.shape}, lon shape={lon_vals.shape}")

    # Process each storm cell
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
        target = cell["properties"]

        poly = StormIntegrationUtils.create_cell_polygon(cell)
        if poly is None:
            io_manager.write_warning(f"Cell {cell.get('id')} has invalid geometry, setting wind values to 0")
            # Set defaults if polygon is invalid
            for level in levels:
                for var in variables:
                    target[f"{var}{level}"] = 0
            continue

        try:
            minx, miny, maxx, maxy = poly.bounds
            
            # Create mask for bounding box to subset data (handles 2D coords)
            bbox_mask = (lat_vals >= miny) & (lat_vals <= maxy) & (lon_vals >= minx) & (lon_vals <= maxx)

            if not np.any(bbox_mask):
                io_manager.write_debug(f"No data found in bounding box for cell {cell.get('id')}, bounds: ({minx:.2f}, {miny:.2f}, {maxx:.2f}, {maxy:.2f})")
                io_manager.write_debug(f"Data bounds: lat({lat_vals.min():.2f}, {lat_vals.max():.2f}), lon({lon_vals.min():.2f}, {lon_vals.max():.2f})")
                for level in levels:
                    for var in variables:
                        target[f"{var}{level}"] = 0
                continue

            # Flatten coordinates within the bbox for spatial check
            sub_lat = lat_vals[bbox_mask]
            sub_lon = lon_vals[bbox_mask]

            # Use shapely for polygon containment check
            inside = sv.contains(poly, sub_lon, sub_lat)
            
            if not np.any(inside):
                io_manager.write_debug(f"No points found inside polygon for cell {cell.get('id')}")
                for level in levels:
                    for var in variables:
                        target[f"{var}{level}"] = 0
                continue

            # Integrate each available level and variable
            for level in available_levels:
                for var_name in available_vars:
                    if (var_name, level) in data_cache:
                        var_array = data_cache[(var_name, level)]
                        
                        # Handle different data shapes properly
                        if var_array.ndim == 2:
                            # 2D data: subset using bbox_mask directly
                            sub_var = var_array[bbox_mask]
                        elif var_array.ndim == 3:
                            # 3D data: already selected by level, treat as 2D
                            sub_var = var_array[bbox_mask]
                        else:
                            io_manager.write_warning(f"Unexpected data shape for {var_name}: {var_array.shape}")
                            sub_var = var_array.flatten()[bbox_mask] if var_array.size == bbox_mask.size else np.array([])
                        
                        masked_vals = sub_var[inside]
                        
                        if masked_vals.size == 0 or np.all(np.isnan(masked_vals)):
                            io_manager.write_debug(f"No valid data for {var_name} at {level}mb for cell {cell.get('id')}")
                            # Map back to original variable name for output
                            output_var = 'u' if var_name in ['u', 'UGRD', 'u-component_of_wind_isobaric', 'wind_u'] else 'v'
                            target[f"{output_var}{level}"] = 0
                        else:
                            # Use absolute max for wind components to capture strongest flow regardless of direction
                            max_val = np.nanmax(np.abs(masked_vals))
                            output_var = 'u' if var_name in ['u', 'UGRD', 'u-component_of_wind_isobaric', 'wind_u'] else 'v'
                            target[f"{output_var}{level}"] = float(max_val)
                            io_manager.write_debug(f"Cell {cell.get('id')}: {output_var}{level} = {max_val:.2f} (from {var_name}, {masked_vals.size} points)")

            # Set 0 for any missing levels that were requested but not in file
            missing_levels = set(levels) - set(available_levels)
            for level in missing_levels:
                for var in variables:
                    target[f"{var}{level}"] = 0
                    
            # Set 0 for any missing variables (when alternative naming was used)
            for level in available_levels:
                for var in variables:
                    if f"{var}{level}" not in target:
                        target[f"{var}{level}"] = 0

        except Exception as e:
            io_manager.write_error(f"Error integrating RAP winds for cell {cell.get('id')}: {e}")
            for level in levels:
                for var in variables:
                    target[f"{var}{level}"] = "PROCESSING_ERROR"

    # Close dataset if possible
    try:
        if hasattr(ds, 'close'):
            ds.close()
    except Exception:
        pass
        
    return storm_cells

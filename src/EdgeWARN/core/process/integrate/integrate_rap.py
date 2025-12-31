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
        ds = ds.assign_coords(longitude=(ds.longitude.dims, lon_vals))

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
                data_cache[(var_name, level)] = ds_level[var_name].values

    # Process each storm cell
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
        target = cell["properties"]

        # Normalize storm cell centroid to -180 to 180 if data was converted
        centroid_lat, centroid_lon = cell.get('centroid', [0, 0])
        if lon_needs_conversion and centroid_lon > 180:
            centroid_lon -= 360

        try:
            # Find the grid point that the centroid is closest to
            dist_sq = (lat_vals - centroid_lat)**2 + (lon_vals - centroid_lon)**2
            min_idx = np.unravel_index(np.argmin(dist_sq), dist_sq.shape)
            
            # Map values from the nearest grid point
            for level in available_levels:
                for var_name in available_vars:
                    if (var_name, level) in data_cache:
                        var_array = data_cache[(var_name, level)]
                        val = var_array[min_idx]
                        
                        # Capture absolute max of components as before
                        max_val = float(np.abs(val))
                        output_var = 'u' if var_name in ['u', 'UGRD', 'u-component_of_wind_isobaric', 'wind_u'] else 'v'
                        target[f"{output_var}{level}"] = max_val

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

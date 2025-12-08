import xarray as xr
import numpy as np
import shapely.vectorized as sv
from .utils import StormIntegrationUtils, io_manager

def integrate_glm(storm_cells, glm_file_path=None):
    """
    Integrate GOES GLM flash count and total flash energy into storm cells.
    
    Args:
        storm_cells (list): List of storm cell dictionaries.
        glm_file_path (str, optional): Path to the GLM L2 LCFA NetCDF file. 
                                       REQUIRED. If None, logs error and returns.
                                       
    Returns:
        list: Updated storm cells with GLM_FLASH_COUNT and GLM_TOTAL_ENERGY.
    """
    if glm_file_path is None:
        io_manager.write_error("GLM file path not provided to integrate_glm")
        return storm_cells

    # Load dataset
    ds = None
    try:
        ds = xr.open_dataset(glm_file_path, engine="netcdf4")
    except Exception as e:
        io_manager.write_error(f"Failed to load GLM file {glm_file_path}: {e}")
        return storm_cells

    try:
        # Check variables
        required_vars = ["flash_lat", "flash_lon", "flash_energy"]
        for v in required_vars:
            if v not in ds:
                io_manager.write_error(f"Variable '{v}' not found in GLM file")
                ds.close()
                return storm_cells

        flash_lats = ds["flash_lat"].values
        flash_lons = ds["flash_lon"].values
        # flash_energy in Joules (J) - sometimes it's femtojoules (fJ) depending on product, 
        # but usually post-processed to J or similar unit. Taking raw values as requested.
        flash_energies = ds["flash_energy"].values

        # Filter active cells
        latest_ts = max(
            (
                cell["storm_history"][-1]["timestamp"]
                for cell in storm_cells
                if cell.get("storm_history")
            ),
            default=None,
        )

        if latest_ts is None:
            ds.close()
            return storm_cells
            
        active_cells = [
            cell for cell in storm_cells 
            if cell.get("storm_history") and cell["storm_history"][-1]["timestamp"] == latest_ts
        ]

        io_manager.write_info(f"Integrating GLM data for {len(active_cells)} cells")

        for cell in active_cells:
            latest = cell["storm_history"][-1]
            poly = StormIntegrationUtils.create_cell_polygon(cell)
            
            if poly is None:
                latest["GLM_FLASH_COUNT"] = 0
                latest["GLM_TOTAL_ENERGY"] = 0.0
                continue
                
            try:
                # Optimized point-in-polygon check
                # First filter by bounding box
                minx, miny, maxx, maxy = poly.bounds
                
                # Check for flashes within bbox - this is faster than running contains on all points
                # Note: flash_lat/lon are 1D arrays of shape (number_of_flashes,)
                
                bbox_mask = (
                    (flash_lats >= miny) & (flash_lats <= maxy) &
                    (flash_lons >= minx) & (flash_lons <= maxx)
                )
                
                if not np.any(bbox_mask):
                    latest["GLM_FLASH_COUNT"] = 0
                    latest["GLM_TOTAL_ENERGY"] = 0.0
                    continue

                subset_lats = flash_lats[bbox_mask]
                subset_lons = flash_lons[bbox_mask]
                subset_energies = flash_energies[bbox_mask]

                # Precise check
                inside = sv.contains(poly, subset_lons, subset_lats)
                
                if not np.any(inside):
                     latest["GLM_FLASH_COUNT"] = 0
                     latest["GLM_TOTAL_ENERGY"] = 0.0
                else:
                    # Filter energies
                    final_energies = subset_energies[inside]
                    
                    latest["GLM_FLASH_COUNT"] = int(len(final_energies))
                    latest["GLM_TOTAL_ENERGY"] = float(np.nansum(final_energies))
                    
            except Exception as e:
                io_manager.write_error(f"Process cell {cell.get('id')} for GLM: {e}")
                latest["GLM_FLASH_COUNT"] = "PROCESSING_ERROR"
                latest["GLM_TOTAL_ENERGY"] = "PROCESSING_ERROR"

    except Exception as e:
        io_manager.write_error(f"Error during GLM integration: {e}")
    finally:
        if ds:
            ds.close()

    return storm_cells

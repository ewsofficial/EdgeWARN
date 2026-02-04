"""
Fast GRIB2 loader using eccodes bindings directly.
Bypasses cfgrib indexing overhead for simple single-field GRIB files.
"""
import eccodes
import numpy as np
import xarray as xr


def load_grib_fast(filepath: str) -> xr.Dataset:
    """
    Fast GRIB2 loader optimized for single-field MRMS GRIB files.
    
    This function reads GRIB data using low-level eccodes bindings,
    bypassing the slow cfgrib indexing that causes performance issues
    with certain MRMS file formats.
    
    Args:
        filepath: Path to GRIB2 file
        
    Returns:
        xr.Dataset containing the grid data with latitude/longitude coordinates
        
    Raises:
        RuntimeError: If loading fails
    """
    try:
        with open(filepath, 'rb') as f:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                raise ValueError("No GRIB message found in file")
            
            try:
                # Read grid dimensions
                ni = eccodes.codes_get_long(gid, "Ni")
                nj = eccodes.codes_get_long(gid, "Nj")
                
                # Read grid definition
                lat0 = eccodes.codes_get_double(gid, "latitudeOfFirstGridPointInDegrees")
                lon0 = eccodes.codes_get_double(gid, "longitudeOfFirstGridPointInDegrees")
                latN = eccodes.codes_get_double(gid, "latitudeOfLastGridPointInDegrees")
                lonN = eccodes.codes_get_double(gid, "longitudeOfLastGridPointInDegrees")
                
                # Construct coordinate arrays
                # Handle both ascending and descending latitude grids
                lats = np.linspace(lat0, latN, nj)
                lons = np.linspace(lon0, lonN, ni)
                
                # Read data values
                vals = eccodes.codes_get_double_array(gid, "values")
                vals = vals.reshape(nj, ni)
                
                # Try to get variable name, default to 'unknown' for compatibility
                try:
                    name = eccodes.codes_get_string(gid, "shortName")
                    if not name or name == "unknown":
                        name = "unknown"
                except Exception:
                    name = "unknown"

                # Construct DataArray with coordinates
                da = xr.DataArray(
                    vals, 
                    coords={"latitude": lats, "longitude": lons}, 
                    dims=("latitude", "longitude"), 
                    name=name
                )
                
                return da.to_dataset()

            finally:
                eccodes.codes_release(gid)
                
    except Exception as e:
        raise RuntimeError(f"Fast GRIB load failed for {filepath}: {e}")

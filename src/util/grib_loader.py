"""
Fast GRIB2 loaders using eccodes bindings directly.
Bypasses cfgrib indexing overhead for MRMS and RAP GRIB files.
"""
import eccodes
import numpy as np
import xarray as xr

# Enable multi-message support globally so fields like V-component wind (packed with U-component) are extracted correctly
eccodes.codes_grib_multi_support_on()


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
                
                # Read data values and aggressively downcast to float32
                vals = eccodes.codes_get_double_array(gid, "values")
                vals = vals.reshape(nj, ni).astype(np.float32)
                
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


# ---------------------------------------------------------------------------
# RAP GRIB2 Loader — Zero-grid-memory point extraction via eccodes
# ---------------------------------------------------------------------------

class RAPPointExtractor:
    """
    Memory-efficient RAP GRIB2 point extractor.
    
    Instead of loading all RAP datasets into memory via cfgrib (which causes
    a ~2-3 GB transient spike), this class iterates through GRIB messages
    one at a time using eccodes and extracts point values at cell centroids
    using `codes_grib_find_nearest`.
    
    Peak memory usage: ~0 MB (no grids are ever materialized).
    
    Usage:
        extractor = RAPPointExtractor(rap_file_path)
        results = extractor.extract(products_config, cell_coords)
        # results = { "u1000": {cell_id: value, ...}, "v1000": {cell_id: value, ...}, ... }
    """
    
    def __init__(self, filepath: str):
        """
        Args:
            filepath: Path to the RAP GRIB2 file.
        """
        self.filepath = filepath
    
    def extract(self, products: list, cell_coords: dict) -> dict:
        """
        Extract point values for all configured products at all cell locations.
        
        Scans the GRIB file once, matching messages against the products config.
        For each match, uses codes_grib_find_nearest to get values at cell
        centroids without loading the full grid.
        
        Args:
            products: List of product config dicts from get_rap_products()["products"].
                      Each has: filter (typeOfLevel, level), var (shortName),
                      and either key (single) or levels + key_template (multi-level).
            cell_coords: Dict of {cell_id: (lat, lon)} for all storm cells.
                         Longitudes should be in degrees East (0-360 or -180 to 180).
        
        Returns:
            Dict of {output_key: {cell_id: value}} for all matched products.
            output_key is either product["key"] or product["key_template"].format(level=...).
        """
        # Pre-build a lookup: (shortName, typeOfLevel, level) -> output_key
        # For multi-level products, we register one entry per level.
        wanted = {}  # (shortName, typeOfLevel, level_or_None) -> output_key
        for product in products:
            short_name = product["var"]
            type_of_level = product["filter"]["typeOfLevel"]
            filter_level = product["filter"].get("level")
            
            if "levels" in product:
                for level in product["levels"]:
                    key = product["key_template"].format(level=level)
                    wanted[(short_name, type_of_level, level)] = key
            else:
                key = product["key"]
                wanted[(short_name, type_of_level, filter_level)] = key
        
        # Prepare coordinate arrays for batch nearest-neighbor queries
        cell_ids = list(cell_coords.keys())
        lats = np.array([cell_coords[cid][0] for cid in cell_ids])
        lons = np.array([cell_coords[cid][1] for cid in cell_ids])
        
        # Normalize longitudes to 0-360 for eccodes (it uses 0-360 internally)
        lons_360 = np.where(lons < 0, lons + 360, lons)
        
        # Results accumulator
        results = {}
        matched_keys = set()
        
        # Scan GRIB messages one at a time
        with open(self.filepath, 'rb') as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break  # End of file
                
                try:
                    # Read message metadata
                    try:
                        msg_short_name = eccodes.codes_get_string(gid, "shortName")
                    except Exception:
                        eccodes.codes_release(gid)
                        continue
                    
                    try:
                        msg_type_of_level = eccodes.codes_get_string(gid, "typeOfLevel")
                    except Exception:
                        eccodes.codes_release(gid)
                        continue
                    
                    try:
                        msg_level = eccodes.codes_get_long(gid, "level")
                    except Exception:
                        msg_level = None
                    
                    # Check if this message matches any wanted product
                    # Try exact match first (shortName, typeOfLevel, level)
                    lookup_key = (msg_short_name, msg_type_of_level, msg_level)
                    output_key = wanted.get(lookup_key)
                    
                    # Also try with level=None for products that don't filter by level
                    if output_key is None:
                        lookup_key_no_level = (msg_short_name, msg_type_of_level, None)
                        output_key = wanted.get(lookup_key_no_level)
                    
                    if output_key is None or output_key in matched_keys:
                        continue  # Skip unneeded messages or already-matched keys
                    
                    # Extract values at all cell locations using find_nearest
                    cell_values = {}
                    for i, cid in enumerate(cell_ids):
                        try:
                            nearest = eccodes.codes_grib_find_nearest(
                                gid, lats[i], lons_360[i]
                            )
                            # codes_grib_find_nearest returns a list of Nearest objects
                            # Each has .value, .lat, .lon, .distance, .index
                            # Take the first (closest) point
                            if nearest and len(nearest) > 0:
                                cell_values[cid] = nearest[0].value
                            else:
                                cell_values[cid] = None
                        except Exception:
                            cell_values[cid] = None
                    
                    results[output_key] = cell_values
                    matched_keys.add(output_key)
                    
                    # Early exit if all wanted keys have been matched
                    if len(matched_keys) == len(wanted):
                        break
                        
                finally:
                    eccodes.codes_release(gid)
        
        return results
    
    def extract_batch(self, products: list, cell_coords: dict) -> dict:
        """
        Optimized batch extraction using vectorized nearest-neighbor lookups.
        
        Same interface as extract(), but processes all cells at once per message.
        Instead of calling codes_grib_find_nearest `C` times per message,
        this builds a scipy cKDTree ONCE from the grid's native lat/lon arrays,
        then queries all cells simultaneously.
        
        Peak memory: ~20 MB (one RAP grid + one cKDTree built once).
        
        Args:
            products: Same as extract().
            cell_coords: Same as extract().
        
        Returns:
            Same as extract().
        """
        import scipy.spatial
        
        # Pre-build wanted lookup
        wanted = {}
        for product in products:
            short_name = product["var"]
            type_of_level = product["filter"]["typeOfLevel"]
            filter_level = product["filter"].get("level")
            
            if "levels" in product:
                for level in product["levels"]:
                    key = product["key_template"].format(level=level)
                    wanted[(short_name, type_of_level, level)] = key
            else:
                key = product["key"]
                wanted[(short_name, type_of_level, filter_level)] = key
        
        cell_ids = list(cell_coords.keys())
        # For cKDTree, we need coordinates in Cartesian/3D or we can approximate locally.
        # Given this is CONUS RAP, 2D Euclidean distance on (lat, lon_360) is generally
        # "close enough" for finding the nearest 13km grid point.
        cell_lats = np.array([cell_coords[cid][0] for cid in cell_ids])
        cell_lons = np.array([cell_coords[cid][1] for cid in cell_ids])
        
        # Normalize longitudes to 0-360 to match standard GRIB
        cell_lons_360 = np.where(cell_lons < 0, cell_lons + 360, cell_lons)
        cell_points = np.column_stack((cell_lats, cell_lons_360))
        
        results = {}
        matched_keys = set()
        
        # Grid index cache (computed once)
        tree = None
        nearest_indices = None  # Flat indices into the 1D values array
        
        with open(self.filepath, 'rb') as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                
                try:
                    # Read metadata
                    try:
                        msg_short_name = eccodes.codes_get_string(gid, "shortName")
                        msg_type_of_level = eccodes.codes_get_string(gid, "typeOfLevel")
                    except Exception:
                        continue
                    
                    try:
                        msg_level = eccodes.codes_get_long(gid, "level")
                    except Exception:
                        msg_level = None
                    
                    # Match against wanted products
                    lookup_key = (msg_short_name, msg_type_of_level, msg_level)
                    output_key = wanted.get(lookup_key)
                    if output_key is None:
                        output_key = wanted.get(
                            (msg_short_name, msg_type_of_level, None)
                        )
                    
                    if output_key is None or output_key in matched_keys:
                        continue
                    
                    # Compute grid geometry and KDTree once from first matched message
                    if tree is None:
                        print(f">>> [DEBUG] Building KDTree for grid geometry...", flush=True)
                        try:
                            # 1D arrays of shape (Ni * Nj,)
                            grid_lats = eccodes.codes_get_double_array(gid, "latitudes")
                            grid_lons = eccodes.codes_get_double_array(gid, "longitudes")
                        except Exception as e:
                            # Fallback if latitudes/longitudes keys don't exist
                            ni = eccodes.codes_get_long(gid, "Ni")
                            nj = eccodes.codes_get_long(gid, "Nj")
                            lat0 = eccodes.codes_get_double(gid, "latitudeOfFirstGridPointInDegrees")
                            lon0 = eccodes.codes_get_double(gid, "longitudeOfFirstGridPointInDegrees")
                            latN = eccodes.codes_get_double(gid, "latitudeOfLastGridPointInDegrees")
                            lonN = eccodes.codes_get_double(gid, "longitudeOfLastGridPointInDegrees")
                            
                            lats_1d = np.linspace(lat0, latN, nj)
                            lons_1d = np.linspace(lon0, lonN, ni)
                            grid_lons, grid_lats = np.meshgrid(lons_1d, lats_1d)
                            grid_lats = grid_lats.ravel()
                            grid_lons = grid_lons.ravel()
                        
                        # Normalize grid longitudes to 0-360
                        grid_lons = np.where(grid_lons < 0, grid_lons + 360, grid_lons)
                        grid_points = np.column_stack((grid_lats, grid_lons))
                        
                        print(f">>> [DEBUG] Instantiating cKDTree on {len(grid_points)} points...", flush=True)
                        tree = scipy.spatial.cKDTree(grid_points)
                        print(f">>> [DEBUG] Querying cKDTree for {len(cell_points)} cells...", flush=True)
                        
                        # Query tree once for all cells
                        _, nearest_indices = tree.query(cell_points)
                        print(f">>> [DEBUG] Query complete.", flush=True)
                        
                        del grid_lats, grid_lons, grid_points
                    
                    # Load flat values array
                    vals = eccodes.codes_get_double_array(gid, "values")
                    
                    # Vectorized extraction at nearest points
                    extracted = vals[nearest_indices]
                    
                    cell_values = {
                        cid: float(extracted[i]) 
                        for i, cid in enumerate(cell_ids)
                    }
                    
                    results[output_key] = cell_values
                    matched_keys.add(output_key)
                    
                    # Free grid memory immediately
                    del vals, extracted
                    
                    if len(matched_keys) == len(wanted):
                        break
                        
                finally:
                    eccodes.codes_release(gid)
        
        return results

import numpy as np
from skimage import measure

class CellDataSaver:
    def __init__(self, bboxes, radar_ds, mapped_ds, expanded_ds, ps_ds, preciptype_ds):
        self.bboxes = bboxes
        self.radar_ds = radar_ds
        self.mapped_ds = mapped_ds
        self.expanded_ds = expanded_ds
        self.ps_ds = ps_ds
        self.preciptype_ds = preciptype_ds
    
    def create_json_structure(self, latest_timestamp, features):
        """
        Creates the main structure for the output data
        
        Args:
            latest_timestamp (str): Latest timestamp of the data
            features (list of dict): List of features to be saved
        """
        return {
            "source": "Edgemont Weather Service",
            "product": "EdgeWARN Storm Cells",
            "version": "2.0.0-rc1",
            "latest_timestamp": latest_timestamp,
            "features": features
        }


    def __create_hailcore_polygon(self, poly_id, slice_obj, step=5):
        """
        Creates a hail core polygon by tracing the exterior of hail-classified 
        cells (preciptype == 7) within a ProbSevere polygon, using a slice to 
        avoid full-grid scans.
        """
        if self.preciptype_ds is None:
            return []

        # Slices are passed from create_entry
        poly_subgrid = self.expanded_ds['PolygonID'].values[slice_obj]
        precip_subgrid = self.preciptype_ds['unknown'].values[slice_obj]
        
        # Create mask on subgrid
        poly_mask = poly_subgrid == poly_id
        if not np.any(poly_mask):
            return []

        hail_mask = (precip_subgrid == 6) & poly_mask
        if not np.any(hail_mask):
            return []

        # Find contours on the hail mask (local coordinates)
        contours = measure.find_contours(hail_mask.astype(float), 0.5)
        if not contours:
            return []

        # Take the largest contour
        contour = max(contours, key=lambda c: c.shape[0])

        # Sample every 'step' points
        sampled = contour[::step]

        # Get global lat/lon grids (can be optimized to only extract subgrid if needed, 
        # but indexing is fast enough if we have the full array in memory)
        
        # Optimization: Don't load full grid if not needed.
        # But here we need to map indices to coords.
        # If lat/lon are 1D (which they are for GRIB/netCDF usually), we can index directly.

        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values

        # Calculate offsets
        r_offset = slice_obj[0].start
        c_offset = slice_obj[1].start

        polygon_points = []

        # Bolt Optimization: Vectorized coordinate lookup (~10x faster)
        if lats.ndim == 1:
            r_global = (sampled[:, 0] + r_offset).astype(int)
            c_global = (sampled[:, 1] + c_offset).astype(int)

            # Safety clamp
            np.clip(r_global, 0, lats.shape[0] - 1, out=r_global)
            np.clip(c_global, 0, lons.shape[0] - 1, out=c_global)

            lat_vals = lats[r_global]
            lon_vals = lons[c_global] % 360

            # Stack and convert to list of tuples
            polygon_points = np.column_stack((lat_vals, lon_vals)).tolist()
        else:
            # Fallback for 2D coords
            r_global = (sampled[:, 0] + r_offset).astype(int)
            c_global = (sampled[:, 1] + c_offset).astype(int)

            # Safety clamp
            np.clip(r_global, 0, lats.shape[0] - 1, out=r_global)
            np.clip(c_global, 0, lats.shape[1] - 1, out=c_global)

            lat_vals = lats[r_global, c_global]
            lon_vals = lons[r_global, c_global] % 360

            polygon_points = np.column_stack((lat_vals, lon_vals)).tolist()

        return polygon_points

        return results

    def create_entry(self, vil_ds=None, et_ds=None):
        """
        Appends maximum reflectivity, num_gates, and reflectivity-weighted centroid
        to each ProbSevere cell entry using exponential weighting.
        Optimized with slice-based processing and Watershed-expanded masks.
        
        Now includes Eager Scalar Extraction for MorphoWind metrics.
        """
        from EdgeWARN.process.detect.tools.morphology import MorphologyEngine
        
        # CRITICAL: Use expanded_ds (the watershed result) for all attribute calculations
        polygon_grid = self.expanded_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values
        
        # Optimize: Avoid full meshgrid creation
        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values
        
        is_1d_coords = (lats.ndim == 1)

        results = []
        
        # Get bounding boxes slices for all polygons
        import scipy.ndimage
        max_id = np.max(polygon_grid)
        if max_id == 0:
            return []
            
        slices = scipy.ndimage.find_objects(polygon_grid, max_label=max_id)

        for poly_id, bbox in self.bboxes.items():
            if poly_id == 0:
                continue
                
            # slice index is poly_id - 1
            if poly_id > len(slices):
                continue
                
            sl = slices[poly_id - 1]
            if sl is None:
                continue

            # Extract sub-grids
            mask_slice = polygon_grid[sl] == poly_id
            
            # Pre-filter: if mask is empty (shouldn't happen if slice is valid)
            count = np.count_nonzero(mask_slice)
            if count == 0:
                continue
            
            # === Morphology Engine Call ===
            refl_slice = refl_grid[sl]
            morph_stats = MorphologyEngine.process_cell(mask_slice, refl_slice)
                
            # === Standard Reflectivity Logic ===
            refl_vals = refl_slice[mask_slice]
            
            valid_refl_mask = ~np.isnan(refl_vals)
            refl_vals = refl_vals[valid_refl_mask]
            
            if refl_vals.size > 0:
                # Optimize coordinate extraction
                if is_1d_coords:
                     combined_mask = mask_slice.copy()
                     rows, cols = np.where(combined_mask)
                     vals = refl_slice[rows, cols]
                     valid = ~np.isnan(vals)
                     valid_rows = rows[valid]
                     valid_cols = cols[valid]
                     refl_vals = vals[valid] 
                     global_rows = valid_rows + sl[0].start
                     global_cols = valid_cols + sl[1].start
                     lat_vals = lats[global_rows]
                     lon_vals = lons[global_cols]
                else:
                    # Fallback to meshgrid for 2D coords
                    lat_slice = lats[sl]
                    lon_slice = lons[sl]
                    if lat_slice.ndim == 1:
                         lat_slice, lon_slice = np.meshgrid(lat_slice, lon_slice, indexing='ij')
                    lat_vals = lat_slice[mask_slice][valid_refl_mask]
                    lon_vals = lon_slice[mask_slice][valid_refl_mask]
                
                max_refl_val = float(np.nanmax(refl_vals))
                
                # Use Log-Sum-Exp Trick for stability
                weights = np.exp(refl_vals - max_refl_val)
                sum_weights = np.sum(weights)
                
                if sum_weights > 0:
                    lat_centroid = float(np.sum(lat_vals * weights) / sum_weights)
                    lon_centroid = float(np.sum(lon_vals * weights) / sum_weights)
                    lon_centroid = lon_centroid % 360
                    centroid = (lat_centroid, lon_centroid)
                else:
                    centroid = (np.nan, np.nan)
            else:
                max_refl_val = float('nan')
                centroid = (np.nan, np.nan)

            hail_core = self.__create_hailcore_polygon(poly_id, sl)

            entry = {
                "id": int(poly_id),
                "num_gates": int(count),
                "centroid": centroid,
                "bbox": [[float(pt[0]), float(pt[1]) % 360] for pt in bbox],
                "hail_core": [[float(pt[0]), float(pt[1]) % 360] for pt in hail_core],
                "max_refl": max_refl_val,
                "event_type": "ACTIVE",
                "parent_ids": [],
                "split_from": None,
                "properties": {
                    "morphology": morph_stats # Inject MorphoWind stats
                }
            }
            results.append(entry)

        return results



        

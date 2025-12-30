import numpy as np
from skimage import measure
from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler

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
            "version": "1.0.0",
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

        hail_mask = (precip_subgrid == 7) & poly_mask
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
        lat_grid = self.radar_ds['latitude'].values
        lon_grid = self.radar_ds['longitude'].values
        
        if lat_grid.ndim == 1:
            lat_grid, lon_grid = np.meshgrid(lat_grid, lon_grid, indexing='ij')

        # Calculate offsets
        r_offset = slice_obj[0].start
        c_offset = slice_obj[1].start

        polygon_points = []
        for r_local, c_local in sampled:
            r_global = int(r_local + r_offset)
            c_global = int(c_local + c_offset)
            
            # Safety clamp
            r_global = max(0, min(r_global, lat_grid.shape[0] - 1))
            c_global = max(0, min(c_global, lat_grid.shape[1] - 1))
            
            lat = float(lat_grid[r_global, c_global])
            lon = float(lon_grid[r_global, c_global] % 360)
            polygon_points.append((lat, lon))

        return polygon_points

    def create_entry(self):
        """
        Appends maximum reflectivity, num_gates, and reflectivity-weighted centroid
        to each ProbSevere cell entry using exponential weighting.
        Optimized with slice-based processing.
        """
        polygon_grid = self.mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values
        lat_grid = self.radar_ds['latitude'].values
        lon_grid = self.radar_ds['longitude'].values

        if lat_grid.ndim == 1:
            lat_grid, lon_grid = np.meshgrid(lat_grid, lon_grid, indexing='ij')

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
                
            refl_slice = refl_grid[sl]
            lat_slice = lat_grid[sl]
            lon_slice = lon_grid[sl]
            
            # Apply mask to sub-grids
            refl_vals = refl_slice[mask_slice]
            
            valid_refl_mask = ~np.isnan(refl_vals)
            refl_vals = refl_vals[valid_refl_mask]
            
            # If no valid reflectivity, we still need centroid? 
            # Original logic: if refl_vals.size > 0 ==> weighted centroid. Else nan.
            
            if refl_vals.size > 0:
                lat_vals = lat_slice[mask_slice][valid_refl_mask]
                lon_vals = lon_slice[mask_slice][valid_refl_mask]
                
                max_refl = float(np.nanmax(refl_vals))
                weights = np.exp(refl_vals)
                sum_weights = np.sum(weights)
                
                if sum_weights > 0:
                    lat_centroid = float(np.sum(lat_vals * weights) / sum_weights)
                    lon_centroid = float(np.sum(lon_vals * weights) / sum_weights)
                    lon_centroid = lon_centroid % 360
                    centroid = (lat_centroid, lon_centroid)
                else:
                    centroid = (np.nan, np.nan)
            else:
                max_refl = float('nan')
                centroid = (np.nan, np.nan)

            hail_core = self.__create_hailcore_polygon(poly_id, sl)

            results.append({
                "id": poly_id,
                "num_gates": count,
                "centroid": centroid,
                "bbox": bbox,
                "hail_core": hail_core,
                "max_refl": max_refl,  # Fixed variable name
                "properties": {}
            })

        return results



        

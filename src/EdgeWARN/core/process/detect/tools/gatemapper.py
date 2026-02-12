from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import numpy as np
import xarray as xr
from scipy.ndimage import distance_transform_edt
from skimage import measure
import rasterio.features
from affine import Affine
import scipy.ndimage

class GateMapper:
    def __init__(self, radar_ds, ps_ds, io_manager, refl_threshold=37.5, min_seed_percentage=0.001):
        self.radar_ds = radar_ds
        self.ps_ds = ps_ds
        self.refl_threshold = refl_threshold
        self.min_seed_percentage = min_seed_percentage
        self.io_manager = io_manager

    def map_gates_to_polygons(self):
        """
        Map radar gates to ProbSevere polygons using rasterization (fastest approach),
        handling negative longitudes and avoiding Shapely deprecation warnings.
        """
        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values

        if lats.size < 2 or lons.size < 2:
            self.io_manager.write_warning("Radar dataset is too small or empty. Skipping mapping.")
            # Return empty dataset with correct structure but empty
            return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros((lats.size, lons.size), dtype=np.int32))},
                coords={'latitude': lats, 'longitude': lons}
            )

        if self.ps_ds is None or not isinstance(self.ps_ds, dict):
             # Return empty grid if ProbSevere data is missing or invalid
             polygon_grid = np.zeros((len(lats), len(lons)), dtype=np.int32)
             return xr.Dataset(
                 {'PolygonID': (('latitude', 'longitude'), polygon_grid)},
                 coords={'latitude': lats, 'longitude': lons}
             )

        raster_polygons = []
        for feature in self.ps_ds.get('features', []):
            poly_id = int(feature['properties'].get('ID', 0))
            geom = shape(feature['geometry'])

            # Convert negative longitudes to 0-360
            if geom.geom_type == 'Polygon':
                coords = [(lon + 360 if lon < 0 else lon, lat) for lon, lat in geom.exterior.coords]
                geom = Polygon(coords)

            elif geom.geom_type == 'MultiPolygon':
                new_polys = []
                for p in geom.geoms:
                    coords = [(lon + 360 if lon < 0 else lon, lat) for lon, lat in p.exterior.coords]
                    new_polys.append(Polygon(coords))
                geom = MultiPolygon(new_polys)

            raster_polygons.append((mapping(geom), poly_id))
            
        # Define grid transform
        lat_res = lats[1] - lats[0]
        lon_res = lons[1] - lons[0]
        transform = Affine.translation(lons[0] - lon_res / 2, lats[0] - lat_res / 2) * Affine.scale(lon_res, lat_res)

        # Rasterize polygons
        polygon_grid = rasterio.features.rasterize(
            raster_polygons,
            out_shape=(len(lats), len(lons)),
            transform=transform,
            fill=0,
            all_touched=True,  # or False for strict "contains" behavior
            dtype=np.int32
        )

        return xr.Dataset(
            {'PolygonID': (('latitude', 'longitude'), polygon_grid)},
            coords={'latitude': lats, 'longitude': lons}
        )

    def expand_gates(self, mapped_ds):
        """
        Constrained expansion using Watershed algorithm with optimizations.
        - Cropped processing (Speed)
        - Vectorized coverage check (Speed)
        - Float16 elevation map (Memory)
        - Connected expansion outside polygon (Functionality)
        - Discrimination Logic: Stratiform vs Convective
        """
        from skimage.segmentation import watershed
        
        # 1. Create High Reflectivity Mask
        polygon_grid = mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values
        mask = refl_grid >= self.refl_threshold
        

        # Optimization: Crop to active area
        # Find bounding box of high reflectivity
        rows_with_data = np.any(mask, axis=1)
        cols_with_data = np.any(mask, axis=0)
        
        if not np.any(rows_with_data):
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )
            
        rmin, rmax = np.where(rows_with_data)[0][[0, -1]]
        cmin, cmax = np.where(cols_with_data)[0][[0, -1]]
        
        # Add a small buffer (e.g., 2 pixels) to ensure boundaries are handled cleanly
        rmin = max(0, rmin - 2)
        rmax = min(mask.shape[0], rmax + 3)
        cmin = max(0, cmin - 2)
        cmax = min(mask.shape[1], cmax + 3)
        
        # Slice views
        sub_mask = mask[rmin:rmax, cmin:cmax]
        sub_polygon = polygon_grid[rmin:rmax, cmin:cmax]
        
        # 2. Filter IDs based on Percentage Coverage (Vectorized)
        unique_ids = np.unique(sub_polygon)
        unique_ids = unique_ids[unique_ids > 0]
        
        if len(unique_ids) == 0:
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )
            
        # Optimization: Use scipy.ndimage.sum for vectorized counting
        max_id = unique_ids.max()
        pixel_counts = scipy.ndimage.sum_labels(np.ones_like(sub_polygon), sub_polygon, index=unique_ids)
        refl_counts = scipy.ndimage.sum_labels(sub_mask, sub_polygon, index=unique_ids)
        coverage_ratios = refl_counts / pixel_counts
        
        # Initial Filtering: Trigger expansion for ANY polygon with >= min_seed_percentage coverage
        # With min_seed_percentage=0.001, this effectively triggers for "any pixel".
        valid_indices = coverage_ratios >= self.min_seed_percentage
        valid_ids = unique_ids[valid_indices]
        
        if len(valid_ids) == 0:
            return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )

        # 4. Perform Watershed Expansion
        valid_id_mask = np.zeros(max_id + 1, dtype=bool)
        valid_id_mask[valid_ids] = True
        
        # Markers are valid polygons intersecting high reflectivity
        markers = np.where(valid_id_mask[sub_polygon] & sub_mask, sub_polygon, 0)
        
        if not np.any(markers > 0):
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )

        dist = distance_transform_edt(sub_mask)
        elevation = -dist.astype(np.float16)
        sub_final = watershed(elevation, markers, mask=sub_mask)
        
        # 5. Final Size Filter: > 5 gates total in expanded cell
        final_ids = np.unique(sub_final)
        final_ids = final_ids[final_ids > 0]
        
        if len(final_ids) > 0:
             final_counts = scipy.ndimage.sum_labels(np.ones_like(sub_final), sub_final, index=final_ids)
             # Map IDs to their counts for filtering
             id_to_count = dict(zip(final_ids, final_counts))
             
             # Zero out small clusters
             rejected_ids = [fid for fid, count in id_to_count.items() if count <= 5]
             if rejected_ids:
                  self.io_manager.write_debug(f"Rejecting small expanded clusters: {rejected_ids}")
                  reject_mask = np.isin(sub_final, rejected_ids)
                  sub_final[reject_mask] = 0

        # Place result back into full grid
        final_grid = np.zeros_like(polygon_grid)
        final_grid[rmin:rmax, cmin:cmax] = sub_final
        
        return xr.Dataset(
            {'PolygonID': (('latitude', 'longitude'), final_grid.astype(np.int32))},
            coords={
                'latitude': mapped_ds['latitude'].values,
                'longitude': mapped_ds['longitude'].values
            }
        )
        
        # Watershed returns 0 where mask is False.
        
        return xr.Dataset(
            {'PolygonID': (('latitude', 'longitude'), final_grid.astype(np.int32))},
            coords={
                'latitude': mapped_ds['latitude'].values,
                'longitude': mapped_ds['longitude'].values
            }
        )

    def draw_bbox(self, expanded_ds, step=8):
        """
        Return a dictionary of polygons for each polygon ID by tracing the exterior points.
        Optimized to use find_objects for slice-based processing.

        Parameters:
            expanded_ds (xarray.Dataset): Dataset from expand_gates()
            step (int): take every N-th point along the contour

        Returns:
            dict: {polygon_id: list of (lon, lat) tuples forming the polygon}
        """
        polygon_grid = expanded_ds['PolygonID'].values
        lats = expanded_ds['latitude'].values
        lons = expanded_ds['longitude'].values

        if lats.ndim == 1:
            lats, lons = np.meshgrid(lats, lons, indexing='ij')

        # Get unique IDs and their bounding box slices
        # range(max_id + 1) to cover all possible IDs
        max_id = np.max(polygon_grid)
        if max_id == 0:
            return {}
            
        # find_objects returns a list of slices, index i corresponds to value i+1
        # so slices[0] is for ID 1, slices[1] is for ID 2, etc.
        slices = scipy.ndimage.find_objects(polygon_grid, max_label=max_id)
        
        bboxes = {}
        
        for idx, sl in enumerate(slices):
            poly_id = idx + 1
            if sl is None:
                continue
                
            # Extract sub-grid
            poly_mask = polygon_grid[sl] == poly_id
            
            # Since we sliced, we need to map back to global coordinates.
            # But measure.find_contours returns coordinates relative to the slice.
            # We can just add the slice offset!
            
            # Pad mask to ensure closed contours if touching border
            padded_mask = np.pad(poly_mask, 1, mode='constant', constant_values=0)
            
            # Find contours
            contours = measure.find_contours(padded_mask.astype(float), 0.5)
            if not contours:
                continue
                
            # Take the longest contour
            contour = max(contours, key=len)
            
            # Adaptive Downsample
            n_points = len(contour)
            if n_points < 8:
                final_step = 1
            elif n_points < 24:
                final_step = 4
            else:
                final_step = step
                
            contour = contour[::final_step]
            
            # Convert to global coordinates
            # contour points are (row, col) in padded_mask
            # -1 to correct for padding
            # +sl[0].start to correct for row slice
            # +sl[1].start to correct for col slice
            
            r_offset = sl[0].start - 1
            c_offset = sl[1].start - 1

            if len(contour) > 0:
                # Vectorized coordinate transformation
                r_global = (contour[:, 0] + r_offset).astype(int)
                c_global = (contour[:, 1] + c_offset).astype(int)
                
                # Safety clamp
                np.clip(r_global, 0, lats.shape[0] - 1, out=r_global)
                np.clip(c_global, 0, lats.shape[1] - 1, out=c_global)
                
                # Vectorized lookup and rounding
                lat_vals = np.round(lats[r_global, c_global], 3)
                lon_vals = np.round(lons[r_global, c_global], 3)
                
                # Create list of tuples
                bboxes[poly_id] = list(zip(lat_vals.tolist(), lon_vals.tolist()))
            else:
                bboxes[poly_id] = []

        return bboxes


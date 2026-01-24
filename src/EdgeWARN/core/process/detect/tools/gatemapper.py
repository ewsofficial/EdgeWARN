from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import numpy as np
import xarray as xr
from scipy.ndimage import distance_transform_edt
from skimage import measure
import rasterio.features
from affine import Affine
import scipy.ndimage

class GateMapper:
    def __init__(self, radar_ds, ps_ds, io_manager, refl_threshold=40.0, min_seed_percentage=0.40):
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
        Constrained expansion using Watershed algorithm.
        Cells expand into ADJACENT high-reflectivity areas but CANNOT jump gaps.
        Complexity: O(H*W).
        """
        from skimage.segmentation import watershed

        # 1. Create High Reflectivity Mask
        polygon_grid = mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values
        mask = refl_grid >= self.refl_threshold
        
        # 2. Filter IDs based on Percentage Coverage
        unique_ids = np.unique(polygon_grid)
        unique_ids = unique_ids[unique_ids > 0] # Ignore 0 (background)
        
        valid_ids = []
        for pid in unique_ids:
            # Create mask for this polygon
            poly_mask = (polygon_grid == pid)
            total_pixels = np.sum(poly_mask)
            
            if total_pixels == 0:
                continue
                
            # Count how many of these are high-reflectivity
            high_refl_pixels = np.sum(poly_mask & mask)
            
            coverage = high_refl_pixels / total_pixels
            
            if coverage >= self.min_seed_percentage:
                valid_ids.append(pid)
            
        # 3. Create Refined Seed Grid (MARKERS)
        # Seeds are the intersection of Valid Polygons AND High Reflectivity
        markers = np.zeros_like(polygon_grid)
        
        if valid_ids:
            valid_poly_mask = np.isin(polygon_grid, valid_ids)
            active_seeds_mask = valid_poly_mask & mask
            markers = np.where(active_seeds_mask, polygon_grid, 0)
            
        if not np.any(markers > 0):
            self.io_manager.write_debug("No valid expansion seeds found after filtering.")
            return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )

        # 4. Perform Watershed Expansion
        # We want to fill the "mask" (high reflectivity) starting from "markers" (seeds).
        # We use a distance transform as the "elevation" map to help split mergers naturally.
        # Ideally, the "deepest" parts (furthest from edge) are basins.
        
        # Calculate distance from the edge of the storm mask
        # We want high values in the center, low values at edges.
        # -distance_transform_edt gives negative values (basins) at centers of objects.
        elevation = -distance_transform_edt(mask)
        
        # Run watershed
        # mask=mask ensures we ONLY label pixels where mask is True.
        # markers=markers provides the starting labels.
        final_grid = watershed(elevation, markers, mask=mask)
        
        # Watershed returns 0 where mask is False, which is exactly what we want.
        
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
            
            coords = []
            for c in contour:
                r_global = int(c[0] + r_offset)
                c_global = int(c[1] + c_offset)
                
                # Safety clamp
                r_global = max(0, min(r_global, lats.shape[0] - 1))
                c_global = max(0, min(c_global, lats.shape[1] - 1))
                
                coords.append((
                    round(float(lats[r_global, c_global]), 3),
                    round(float(lons[r_global, c_global]), 3)
                ))
                
            bboxes[poly_id] = coords

        return bboxes


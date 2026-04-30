from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import numpy as np
import xarray as xr
from scipy.ndimage import distance_transform_edt
from skimage import measure
import rasterio.features
from affine import Affine
import scipy.ndimage
import time

class GateMapper:
    def __init__(self, radar_ds, ps_ds, io_manager, refl_threshold=37.5, min_seed_percentage=0.001, drop_offset=10.0):
        self.radar_ds = radar_ds
        self.ps_ds = ps_ds
        self.refl_threshold = refl_threshold
        self.min_seed_percentage = min_seed_percentage
        self.drop_offset = drop_offset
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
        step_timings = {}

        def _mark(step_name, start_time):
            step_timings[step_name] = time.perf_counter() - start_time
        
        # 1. Create Baseline High Reflectivity Mask
        polygon_grid = mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values
        baseline_mask = refl_grid >= min(37.5, self.refl_threshold)
        

        # Optimization: Crop to active area
        # Find bounding box of high reflectivity
        rows_with_data = np.any(baseline_mask, axis=1)
        cols_with_data = np.any(baseline_mask, axis=0)
        
        if not np.any(rows_with_data):
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )
            
        rmin, rmax = np.where(rows_with_data)[0][[0, -1]]
        cmin, cmax = np.where(cols_with_data)[0][[0, -1]]
        
        # Add a small buffer (e.g., 2 pixels) to ensure boundaries are handled cleanly
        rmin = max(0, rmin - 2)
        rmax = min(baseline_mask.shape[0], rmax + 3)
        cmin = max(0, cmin - 2)
        cmax = min(baseline_mask.shape[1], cmax + 3)
        
        # Slice views
        sub_mask = baseline_mask[rmin:rmax, cmin:cmax]
        sub_refl = refl_grid[rmin:rmax, cmin:cmax]
        sub_polygon = polygon_grid[rmin:rmax, cmin:cmax]
        
        # 2. Filter IDs based on Percentage Coverage (Vectorized)
        coverage_start = time.perf_counter()
        unique_ids = np.unique(sub_polygon)
        unique_ids = unique_ids[unique_ids > 0]
        
        if len(unique_ids) == 0:
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )
            
        max_id = int(unique_ids.max())
        pixel_counts = scipy.ndimage.sum_labels(np.ones_like(sub_polygon, dtype=np.int32), sub_polygon, index=unique_ids)
        refl_counts = scipy.ndimage.sum_labels(sub_mask.astype(np.int32), sub_polygon, index=unique_ids)
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
        _mark("coverage_filter", coverage_start)

        # 4. Perform Watershed Expansion
        threshold_start = time.perf_counter()
        valid_id_mask = np.zeros(max_id + 1, dtype=bool)
        valid_id_mask[valid_ids] = True

        dyn_thresh = np.full(max_id + 1, np.inf, dtype=np.float32)
        max_refl_by_id = np.full(max_id + 1, -np.inf, dtype=np.float32)
        finite_label_mask = (sub_polygon > 0) & np.isfinite(sub_refl)
        if np.any(finite_label_mask):
            np.maximum.at(max_refl_by_id, sub_polygon[finite_label_mask], sub_refl[finite_label_mask].astype(np.float32, copy=False))

        valid_max_refl = max_refl_by_id[valid_ids]
        min_thresh = np.where(valid_max_refl < 45.0, 37.5, 40.0).astype(np.float32, copy=False)
        capped_max_refl = np.minimum(valid_max_refl, 52.0)
        dyn_thresh[valid_ids] = np.maximum(min_thresh, capped_max_refl - self.drop_offset)

        # Preserve existing global-union behavior while avoiding a Python loop.
        min_valid_threshold = float(np.min(dyn_thresh[valid_ids]))
        composite_mask = np.isfinite(sub_refl) & (sub_refl >= min_valid_threshold)
        
        # Markers are valid polygons intersecting their own logical threshold mask
        pixel_thresholds = dyn_thresh[np.clip(sub_polygon, 0, max_id)]
        markers = np.where(valid_id_mask[sub_polygon] & np.isfinite(sub_refl) & (sub_refl >= pixel_thresholds), sub_polygon, 0)
        _mark("threshold_prep", threshold_start)

        if not np.any(markers > 0):
             return xr.Dataset(
                {'PolygonID': (('latitude', 'longitude'), np.zeros_like(polygon_grid))},
                coords={'latitude': mapped_ds['latitude'].values, 'longitude': mapped_ds['longitude'].values}
            )

        watershed_start = time.perf_counter()
        component_labels, component_count = scipy.ndimage.label(composite_mask)
        component_slices = scipy.ndimage.find_objects(component_labels, max_label=component_count)
        sub_final = np.zeros_like(sub_polygon, dtype=np.int32)
        edt_total = 0.0
        watershed_total = 0.0

        for component_idx, component_slice in enumerate(component_slices, start=1):
            if component_slice is None:
                continue

            r0 = max(0, component_slice[0].start - 1)
            r1 = min(component_labels.shape[0], component_slice[0].stop + 1)
            c0 = max(0, component_slice[1].start - 1)
            c1 = min(component_labels.shape[1], component_slice[1].stop + 1)
            local_slice = (slice(r0, r1), slice(c0, c1))

            component_region = component_labels[local_slice] == component_idx
            local_markers = markers[local_slice]

            if not np.any(local_markers[component_region] > 0):
                continue

            edt_start = time.perf_counter()
            dist = distance_transform_edt(component_region)
            edt_total += time.perf_counter() - edt_start

            local_elevation = -dist.astype(np.float16)
            watershed_run_start = time.perf_counter()
            local_result = watershed(local_elevation, local_markers, mask=component_region)
            watershed_total += time.perf_counter() - watershed_run_start

            local_final = sub_final[local_slice]
            local_final[component_region] = local_result[component_region]

        _mark("watershed_total", watershed_start)
        step_timings["edt_total"] = edt_total
        step_timings["watershed_only"] = watershed_total
        
        # Post-filtering to enforce strict per-cell thresholds against watershed competition leaks.
        post_filter_start = time.perf_counter()
        final_thresholds = dyn_thresh[np.clip(sub_final, 0, max_id)]
        invalid_mask = (sub_final > 0) & (~np.isfinite(sub_refl) | (sub_refl < final_thresholds))
        sub_final[invalid_mask] = 0
        
        # 5. Final Size Filter: > 5 gates total in expanded cell
        final_ids = np.unique(sub_final)
        final_ids = final_ids[final_ids > 0]
        
        if len(final_ids) > 0:
             final_counts = np.bincount(sub_final.ravel(), minlength=int(final_ids.max()) + 1)
             rejected_ids = np.flatnonzero((final_counts > 0) & (final_counts <= 5))
             rejected_ids = rejected_ids[rejected_ids > 0]
             if rejected_ids.size > 0:
                  self.io_manager.write_debug(f"Rejecting small expanded clusters: {rejected_ids.tolist()}")
                  reject_lookup = np.zeros(final_counts.shape[0], dtype=bool)
                  reject_lookup[rejected_ids] = True
                  sub_final[reject_lookup[sub_final]] = 0
        _mark("post_filter", post_filter_start)

        self.io_manager.write_debug(
            "Expand Gates stats: "
            f"crop_shape={sub_mask.shape}, crop_pixels={sub_mask.size}, "
            f"valid_ids={len(valid_ids)}, composite_pixels={int(np.count_nonzero(composite_mask))}, "
            f"components={component_count}, coverage={step_timings.get('coverage_filter', 0.0):.3f}s, "
            f"thresholds={step_timings.get('threshold_prep', 0.0):.3f}s, "
            f"edt={step_timings.get('edt_total', 0.0):.3f}s, "
            f"watershed={step_timings.get('watershed_only', 0.0):.3f}s, "
            f"expand_total={step_timings.get('watershed_total', 0.0):.3f}s, "
            f"post_filter={step_timings.get('post_filter', 0.0):.3f}s"
        )

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
        coords_are_1d = lats.ndim == 1 and lons.ndim == 1

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
                if coords_are_1d:
                    np.clip(r_global, 0, lats.shape[0] - 1, out=r_global)
                    np.clip(c_global, 0, lons.shape[0] - 1, out=c_global)
                    lat_vals = np.round(lats[r_global], 3)
                    lon_vals = np.round(lons[c_global], 3)
                else:
                    np.clip(r_global, 0, lats.shape[0] - 1, out=r_global)
                    np.clip(c_global, 0, lats.shape[1] - 1, out=c_global)
                    lat_vals = np.round(lats[r_global, c_global], 3)
                    lon_vals = np.round(lons[r_global, c_global], 3)
                
                # Create list of tuples
                bboxes[poly_id] = list(zip(lat_vals.tolist(), lon_vals.tolist()))
            else:
                bboxes[poly_id] = []

        return bboxes

from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import numpy as np
import xarray as xr
from scipy.ndimage import distance_transform_edt
from skimage import measure
import rasterio.features
from affine import Affine

class GateMapper:
    def __init__(self, radar_ds, ps_ds, io_manager, refl_threshold=40.0):
        self.radar_ds = radar_ds
        self.ps_ds = ps_ds
        self.refl_threshold = refl_threshold
        self.io_manager = io_manager

    def map_gates_to_polygons(self):
        """
        Map radar gates to ProbSevere polygons using rasterization (fastest approach),
        handling negative longitudes and avoiding Shapely deprecation warnings.
        """
        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values

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
        Fully vectorized expansion using distance transform.
        Cells are assigned to the nearest polygon.
        If multiple polygons are equidistant, assign to 0 (midline).
        """
        polygon_grid = mapped_ds['PolygonID'].values.copy()
        refl_grid = self.radar_ds['unknown'].values  # replace with actual variable
        mask = refl_grid >= self.refl_threshold

        unique_ids = np.unique(polygon_grid[polygon_grid > 0])
        H, W = polygon_grid.shape

        # Stack a boolean mask for each polygon
        poly_stack = np.stack([polygon_grid == pid for pid in unique_ids], axis=0)  # (n_polygons, H, W)

        # Compute distance transform for each polygon
        dist_stack = np.stack([distance_transform_edt(~layer) for layer in poly_stack], axis=0)

        # For each cell, find the minimal distance(s)
        min_dist = np.min(dist_stack, axis=0)
        closest = dist_stack == min_dist  # boolean array (n_polygons, H, W)

        # Assign unique closest polygon IDs
        counts = closest.sum(axis=0)
        assignment = np.zeros_like(polygon_grid)
        unique_cells = (counts == 1) & mask
        assignment[unique_cells] = unique_ids[np.argmax(closest[:, :, :], axis=0)][unique_cells]

        # Apply assignments
        polygon_grid[assignment > 0] = assignment[assignment > 0]

        # Return as xarray Dataset
        return xr.Dataset(
            {'PolygonID': (('latitude', 'longitude'), polygon_grid)},
            coords={
                'latitude': mapped_ds['latitude'].values,
                'longitude': mapped_ds['longitude'].values
            }
        )

    def draw_bbox(self, expanded_ds, step=8):
        """
        Return a dictionary of polygons for each polygon ID by tracing the exterior points
        and downsampling every 'step' points to reduce complexity.

        Parameters:
            expanded_ds (xarray.Dataset): Dataset from expand_gates()
            step (int): take every N-th point along the contour

        Returns:
            dict: {polygon_id: list of (lon, lat) tuples forming the polygon}
        """
        polygon_grid = expanded_ds['PolygonID'].values
        lats = expanded_ds['latitude'].values
        lons = expanded_ds['longitude'].values

        unique_ids = np.unique(polygon_grid)
        unique_ids = unique_ids[unique_ids != 0]  # skip background

        bboxes = {}

        for poly_id in unique_ids:
            mask = polygon_grid == poly_id
            if not np.any(mask):
                continue

            # Find contours at the 0.5 level (between 0 and 1)
            contours = measure.find_contours(mask.astype(float), 0.5)
            if not contours:
                continue

            # Take the longest contour (usually the exterior)
            contour = max(contours, key=len)

            # Downsample every 'step' points
            contour = contour[::step]

            # Convert from array indices to lon/lat with 3-digit rounding
            coords = [
                (round(lats[int(c[0])], 3), round(lons[int(c[1])], 3))
                for c in contour
            ]
            bboxes[poly_id] = coords

        return bboxes


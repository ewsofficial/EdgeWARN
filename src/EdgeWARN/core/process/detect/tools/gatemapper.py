from shapely.geometry import shape, mapping, Polygon, MultiPolygon
import numpy as np
import xarray as xr
from scipy.ndimage import binary_dilation
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

    def expand_gates(self, mapped_ds, max_iterations=100):
        """
        Vectorized expansion of ProbSevere polygons, preserving the rule that
        once a gate is assigned to a polygon, it cannot be claimed by another.

        Each iteration expands all polygons simultaneously into neighboring
        reflectivity-qualified gates (4-connected neighborhood).

        Parameters:
            mapped_ds (xarray.Dataset): Dataset from map_gates_to_polygons()
            max_iterations (int): Maximum iterations (safety limit)

        Returns:
            xarray.Dataset: Expanded PolygonID dataset
        """
        if self.refl_threshold is None:
            raise ValueError("self.refl_threshold must be set to expand polygons.")

        # Base data
        polygon_grid = mapped_ds['PolygonID'].values.copy()
        refl_grid = self.radar_ds['unknown'].values  # <-- replace 'unknown' with actual variable name
        mask = refl_grid >= self.refl_threshold

        # 4-connected structure for expansion
        structure = np.array([[0,1,0],
                            [1,1,1],
                            [0,1,0]], dtype=bool)

        for iteration in range(max_iterations):
            # Identify which cells belong to any polygon
            occupied = polygon_grid > 0

            # Binary dilation of the occupied mask (potential expansion front)
            expanded = binary_dilation(occupied, structure=structure)

            # Candidates: cells that are unassigned, above threshold, and adjacent to polygons
            candidates = expanded & (~occupied) & mask

            if not np.any(candidates):
                print(f"[CellDetection] Completed expansion in {iteration} iterations (vectorized, non-overwriting)")
                break

            # Find all polygon IDs to expand
            unique_ids = np.unique(polygon_grid[occupied])
            new_assignments = np.zeros_like(polygon_grid)

            # For each polygon, expand only into its own adjacent area (vectorized per ID)
            for poly_id in unique_ids:
                poly_mask = polygon_grid == poly_id
                expanded_poly = binary_dilation(poly_mask, structure=structure)
                new_pixels = expanded_poly & candidates & (polygon_grid == 0)
                new_assignments[new_pixels] = poly_id

            # Apply new assignments — once a cell is filled, it never changes
            polygon_grid[new_assignments > 0] = new_assignments[new_assignments > 0]

        else:
            print(f"[CellDetection] Reached max_iterations ({max_iterations}) without convergence")

        # Return as xarray dataset
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

            # Convert from array indices to lon/lat
            coords = [(lats[int(c[0])], lons[int(c[1])]) for c in contour]
            bboxes[poly_id] = coords

        return bboxes


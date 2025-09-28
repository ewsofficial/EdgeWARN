from shapely.geometry import Point, shape
import numpy as np
import xarray as xr
from .utils import DetectionDataHandler
from .save import CellDataSaver
from shapely.geometry import MultiPoint, MultiLineString
from shapely.ops import unary_union, polygonize
from scipy.spatial import Delaunay
from scipy.ndimage import binary_dilation, binary_closing
import math


class GateMapper:
    def __init__(self, radar_ds, ps_ds, refl_threshold=None):
        self.radar_ds = radar_ds
        self.ps_ds = ps_ds
        self.refl_threshold = refl_threshold  # ignore for now

    def map_gates_to_polygons(self):
        """
        Map radar gates to ProbSevere polygons, returning an xarray.Dataset
        with each gate storing the polygon ID covering it (0 if none).
        Longitudes are converted to 0-360 to match radar grid.
        """
        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values

        # Create meshgrid for radar gates
        lat_grid, lon_grid = np.meshgrid(lats, lons, indexing='ij')
        polygon_grid = np.zeros_like(lat_grid, dtype=int)

        # Convert ProbSevere longitudes to 0-360
        for feature in self.ps_ds.get('features', []):
            for ring in feature['geometry']['coordinates']:
                for i, (lon, lat) in enumerate(ring):
                    if lon < 0:
                        ring[i] = (lon + 360, lat)

        # Loop over each polygon in ProbSevere data
        for feature in self.ps_ds.get('features', []):
            poly_id = int(feature['properties'].get('ID', 0))
            print(f"ID:{poly_id}")
            polygon = shape(feature['geometry'])

            # Assign polygon ID to all gates inside this polygon
            for i in range(lat_grid.shape[0]):
                for j in range(lat_grid.shape[1]):
                    if polygon_grid[i, j] == 0:  # only assign if empty
                        point = Point(lon_grid[i, j], lat_grid[i, j])
                        if polygon.contains(point):
                            polygon_grid[i, j] = poly_id

        # Return as xarray.Dataset
        return xr.Dataset(
            {
                'PolygonID': (('latitude', 'longitude'), polygon_grid)
            },
            coords={
                'latitude': lats,
                'longitude': lons
            }
        )

    def expand_gates(self, mapped_ds, max_iterations=100):
        """
        Iteratively expand ProbSevere polygons one gate at a time, constrained by reflectivity threshold.
        A gate can belong to only one polygon.

        Parameters:
            mapped_ds (xarray.Dataset): Dataset from map_gates_to_polygons()
            max_iterations (int): Maximum number of iterations (safety limit)

        Returns:
            xarray.Dataset: Expanded PolygonID dataset
        """
        if self.refl_threshold is None:
            raise ValueError("self.refl_threshold must be set to expand polygons.")

        polygon_grid = mapped_ds['PolygonID'].values.copy()
        refl_grid = self.radar_ds['unknown'].values  # assume reflectivity variable is named 'reflectivity'

        # Step 1: Mask gates below threshold
        mask = refl_grid >= self.refl_threshold
        polygon_grid = polygon_grid * mask  # zero-out gates below threshold

        iteration = 0
        changed = True

        while changed and iteration < max_iterations:
            iteration += 1
            changed = False
            new_grid = polygon_grid.copy()

            for poly_id in np.unique(polygon_grid):
                if poly_id == 0:
                    continue

                # Create mask for current polygon
                poly_mask = polygon_grid == poly_id

                # Expand by one gate in each direction
                expanded = binary_dilation(poly_mask, structure=np.array([[0,1,0],[1,1,1],[0,1,0]])) & mask

                # Only assign gates not yet claimed by any polygon
                new_pixels = expanded & (polygon_grid == 0)
                if np.any(new_pixels):
                    new_grid[new_pixels] = poly_id
                    changed = True
                
        # --- After expansion loop, fill small concavities (morphological closing) ---
        new_grid = polygon_grid.copy()
        for poly_id in np.unique(polygon_grid):
            if poly_id == 0:
                continue
            poly_mask = polygon_grid == poly_id
            # Use 4-connected structure to avoid over-closing
            closed = binary_closing(poly_mask, structure=np.array([[0,1,0],
                                                                [1,1,1],
                                                                [0,1,0]]))
            new_grid[closed] = poly_id
        polygon_grid = new_grid

        return xr.Dataset(
            {
                'PolygonID': (('latitude', 'longitude'), polygon_grid)
            },
            coords={
                'latitude': mapped_ds['latitude'].values,
                'longitude': mapped_ds['longitude'].values
            }
        )
    
    def draw_bbox(self, expanded_ds, tolerance=0.01):
        """
        Return a dictionary of simplified bounding polygons for each polygon ID.
        
        Parameters:
            expanded_ds (xarray.Dataset): Dataset from expand_gates()
            tolerance (float): simplification tolerance (higher = more simplified)
        
        Returns:
            dict: {polygon_id: list of (lon, lat) tuples forming the simplified polygon}
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

            lat_idx, lon_idx = np.where(mask)
            coords = list(zip(lons[lon_idx], lats[lat_idx]))
            if len(coords) < 3:
                continue

            # Create convex hull from the gate points
            points = MultiPoint(coords)
            hull = points.convex_hull
            
            # Simplify the polygon
            simplified = hull.simplify(tolerance, preserve_topology=True)
            
            # Convert to list of points
            if simplified.geom_type == 'Polygon':
                bboxes[poly_id] = list(simplified.exterior.coords)
            else:
                # Fallback to original hull if simplification fails
                bboxes[poly_id] = list(hull.exterior.coords)

        return bboxes
    
handler = DetectionDataHandler(
    radar_path=r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250927-210640.nc",
    ps_path=r"C:\input_data\mrms_probsevere\MRMS_PROBSEVERE_20250927_210640.json",
    lat_min=35.0, lat_max=38.0,
    lon_min=283.0, lon_max=285.0
)

radar_ds = handler.load_subset()
ps_ds = handler.load_probsevere()

mapper = GateMapper(radar_ds, ps_ds, refl_threshold=37.0)
mapped_ds = mapper.map_gates_to_polygons()
expanded_ds = mapper.expand_gates(mapped_ds)
bboxes = mapper.draw_bbox(expanded_ds, tolerance=0.01)

entry = CellDataSaver(bboxes, r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250927-210640.nc", radar_ds, expanded_ds, r"C:\input_data\mrms_probsevere\MRMS_PROBSEVERE_20250927_210640.json", ps_ds)

data = entry.create_entry()
data = entry.append_storm_history(data)

print(data)

    

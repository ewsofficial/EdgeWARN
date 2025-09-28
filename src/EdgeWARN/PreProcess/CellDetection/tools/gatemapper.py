from shapely.geometry import Point, shape
import numpy as np
import xarray as xr
from .utils import DetectionDataHandler
from .save import CellDataSaver
from shapely.geometry import MultiPoint, MultiLineString
from shapely.ops import unary_union, polygonize
from scipy.spatial import Delaunay
from scipy.ndimage import binary_dilation, binary_closing
from skimage import measure
import math
import alphashape

class GateMapper:
    def __init__(self, radar_ds, ps_ds, refl_threshold=35.0):
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

        # Step 1: create mask for gates above threshold (gates eligible for expansion)
        mask = refl_grid >= self.refl_threshold

        iteration = 0
        changed = True

        while changed and iteration < max_iterations:
            iteration += 1
            changed = False
            new_grid = polygon_grid.copy()

            for poly_id in np.unique(polygon_grid):
                if poly_id == 0:
                    continue

                poly_mask = polygon_grid == poly_id

                # Expand one gate in each direction
                expanded = binary_dilation(poly_mask, structure=np.array([[0,1,0],
                                                                        [1,1,1],
                                                                        [0,1,0]]))
                # Only assign gates not yet claimed and that meet threshold
                new_pixels = expanded & (polygon_grid == 0) & mask
                if np.any(new_pixels):
                    new_grid[new_pixels] = poly_id
                    changed = True

            polygon_grid = new_grid
        """
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
        """

        print(f"DEBUG: Completed expansion after {iteration} iterations")
        return xr.Dataset(
            {
                'PolygonID': (('latitude', 'longitude'), polygon_grid)
            },
            coords={
                'latitude': mapped_ds['latitude'].values,
                'longitude': mapped_ds['longitude'].values
            }
        )
    
    def draw_bbox(self, expanded_ds, step=5):
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
            coords = [(lons[int(c[1])], lats[int(c[0])]) for c in contour]
            bboxes[poly_id] = coords

        return bboxes
    
handler = DetectionDataHandler(
    radar_path=r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250927-210640.nc",
    ps_path=r"C:\input_data\mrms_probsevere\MRMS_PROBSEVERE_20250927_210640.json",
    lat_min=35.0, lat_max=38.0,
    lon_min=283.0, lon_max=285.0
)

    

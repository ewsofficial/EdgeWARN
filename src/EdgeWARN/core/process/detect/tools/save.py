import numpy as np
<<<<<<< HEAD
=======
from skimage import measure
>>>>>>> origin/version-test/0.5.1-alpha
from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler

class CellDataSaver:
    def __init__(self, bboxes, radar_ds, mapped_ds, expanded_ds, ps_ds, preciptype_ds):
        self.bboxes = bboxes
        self.radar_ds = radar_ds
        self.mapped_ds = mapped_ds
        self.expanded_ds = expanded_ds
        self.ps_ds = ps_ds
        self.preciptype_ds = preciptype_ds

    def __create_hailcore_polygon(self, poly_id, step=5):
        """
        Creates a hail core polygon by tracing the exterior of hail-classified 
        cells (preciptype == 7) within a ProbSevere polygon.
        Returns a list of (lat, lon) points sampled every 'step' along the contour.
        """
        # Polygon mask
        polygon_grid = self.expanded_ds['PolygonID'].values
        poly_mask = polygon_grid == poly_id
        if not np.any(poly_mask):
            return []

        # Hail mask (preciptype == 7) inside polygon
        precip_grid = self.preciptype_ds['unknown'].values
        hail_mask = (precip_grid == 7) & poly_mask
        if not np.any(hail_mask):
            return []

        # Latitude and longitude grids
        lat_grid = self.radar_ds['latitude'].values
        lon_grid = self.radar_ds['longitude'].values
        if lat_grid.ndim == 1 and lon_grid.ndim == 1:
            lat_grid, lon_grid = np.meshgrid(lat_grid, lon_grid, indexing='ij')

        # Find contours on the hail mask
        contours = measure.find_contours(hail_mask.astype(float), 0.5)
        if not contours:
            return []

        # Take the largest contour (most points)
        contour = max(contours, key=lambda c: c.shape[0])

        # Sample every 'step' points
        sampled = contour[::step]

        # Convert indices to lat/lon and return as list of tuples
        polygon_points = [
            (float(lat_grid[int(r), int(c)]), float(lon_grid[int(r), int(c)] % 360))
            for r, c in sampled
        ]

        return polygon_points

    def create_entry(self):
        """
        Appends maximum reflectivity, num_gates, and reflectivity-weighted centroid
        to each ProbSevere cell entry using exponential weighting.
        Returns a list of dictionaries with properties.
        """
        # Polygon and reflectivity grids (aligned 2D arrays)
        polygon_grid = self.mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values

        # Get matching latitude and longitude grids
        lat_grid = self.radar_ds['latitude'].values
        lon_grid = self.radar_ds['longitude'].values

        # Ensure lat/lon are 2D
        if lat_grid.ndim == 1 and lon_grid.ndim == 1:
            lat_grid, lon_grid = np.meshgrid(lat_grid, lon_grid, indexing='ij')

        results = []

        for poly_id, bbox in self.bboxes.items():
            if poly_id == 0:
                continue

            # Mask gates belonging to this polygon
            mask = polygon_grid == poly_id
            if not np.any(mask):
                continue

            # Extract reflectivity values inside polygon gates
            refl_vals = refl_grid[mask]
            lat_vals = lat_grid[mask]
            lon_vals = lon_grid[mask]

            valid_mask = ~np.isnan(refl_vals)
            refl_vals = refl_vals[valid_mask]
            lat_vals = lat_vals[valid_mask]
            lon_vals = lon_vals[valid_mask]

            # Max reflectivity
            max_refl = float(np.nanmax(refl_vals)) if refl_vals.size > 0 else float('nan')

            # Exponential reflectivity weights
            if refl_vals.size > 0:
                max_refl = float(np.nanmax(refl_vals))
                weights = np.exp(refl_vals)
                lat_centroid = float(np.sum(lat_vals * weights) / np.sum(weights))
                lon_centroid = float(np.sum(lon_vals * weights) / np.sum(weights))
                lon_centroid = lon_centroid % 360  # wrap longitude to 0–360
                centroid = (lat_centroid, lon_centroid)
            else:
                max_refl = float('nan')
                centroid = (np.nan, np.nan)

            # Count number of gates
            num_gates = np.count_nonzero(mask)

            # Find hailcore
            hail_core = self.__create_hailcore_polygon(poly_id)

            results.append({
                "id": poly_id,
                "num_gates": num_gates,
                "centroid": centroid,
                "bbox": bbox,
                "hail_core": hail_core,
                "max_refl": max_refl,
                "storm_history": []
            })

        return results

    def append_storm_history(self, entries, radar_path):
        timestamp_new = DetectionDataHandler.find_timestamp(radar_path)
        for cell in entries:
            storm_history = cell['storm_history']
            # check for duplicate timestamp
            if storm_history and storm_history[-1]['timestamp'] == timestamp_new:
                continue
            # Build new storm history
            latest_storm_history = {
                "id": cell['id'],
                "timestamp": timestamp_new,
                "max_refl": cell['max_refl'],
                "num_gates": cell['num_gates'],
                "centroid": cell['centroid']
            }

            if storm_history:
                last_entry = storm_history[-1]
                if (last_entry['max_refl'] == cell['max_refl'] and
                    last_entry['num_gates'] == cell['num_gates'] and
                    last_entry['centroid'] == cell['centroid']):
                    continue

            storm_history.append(latest_storm_history)

        return entries

        

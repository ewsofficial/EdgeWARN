import numpy as np
from EdgeWARN.PreProcess.CellDetection.tools.utils import DetectionDataHandler

class CellDataSaver:
    def __init__(self, bboxes, radar_path, radar_ds, mapped_ds, ps_path, ps_ds):
        self.bboxes = bboxes
        self.radar_ds = radar_ds
        self.radar_path = radar_path
        self.mapped_ds = mapped_ds
        self.ps_ds = ps_ds
        self.ps_path = ps_path
    
    def create_entry(self):
        """
        Appends maximum reflectivity, num_gates, and reflectivity-weighted centroid
        to each ProbSevere cell entry using exponential weighting.
        Returns a list of dictionaries with properties.
        """
        # Flatten polygon and reflectivity grids
        polygon_grid = self.mapped_ds['PolygonID'].values.flatten()
        refl_grid = self.radar_ds['unknown'].values.flatten()

        # Build 2D grid coordinates
        lats = self.radar_ds['latitude'].values
        lons = self.radar_ds['longitude'].values
        lat_grid_2d, lon_grid_2d = np.meshgrid(lats, lons, indexing='ij')
        lat_grid = lat_grid_2d.flatten()
        lon_grid = lon_grid_2d.flatten()

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
                weights = np.exp(refl_vals)  # exponential weighting
                lat_centroid = float(np.sum(lat_vals * weights) / np.sum(weights))
                lon_centroid = float(np.sum(lon_vals * weights) / np.sum(weights))
                lon_centroid = lon_centroid % 360  # convert from -180:180 to 0:360
                centroid = (lat_centroid, lon_centroid)
            else:
                centroid = (np.nan, np.nan)

            # Count number of gates
            num_gates = np.count_nonzero(mask)

            results.append({
                "id": poly_id,
                "num_gates": num_gates,
                "centroid": centroid,
                "bbox": bbox,
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

        

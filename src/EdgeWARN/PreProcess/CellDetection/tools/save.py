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
        Appends maximum reflectivity, num_gates, empty storm_history to each ProbSevere cell entry
        Returns a list of dictionaries with properties.
        """
        polygon_grid = self.mapped_ds['PolygonID'].values
        refl_grid = self.radar_ds['unknown'].values  # TODO: change to actual reflectivity variable name

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

            # Get maximum reflectivity (NaN-safe)
            if refl_vals.size > 0:
                max_refl = np.nanmax(refl_vals)
            else:
                max_refl = np.nan

            # Calculate Reflectivity Weighted Centroid
            # Example inside your loop for each polygon
            mask = polygon_grid == poly_id
            refl_vals = refl_grid[mask]

            # Assuming lat_grid and lon_grid are same shape as refl_grid
            lat_grid = self.radar_ds['latitude'].values
            lon_grid = self.radar_ds['longitude'].values

            lat_vals = lat_grid[mask]
            lon_vals = lon_grid[mask]

            if refl_vals.size > 0:
                # Handle NaNs
                valid_mask = ~np.isnan(refl_vals)
                refl_vals = refl_vals[valid_mask]
                lat_vals = lat_vals[valid_mask]
                lon_vals = lon_vals[valid_mask]

                if refl_vals.size > 0:
                    # Compute weighted centroid
                    weighted_lat = np.sum(lat_vals * refl_vals) / np.sum(refl_vals)
                    weighted_lon = np.sum(lon_vals * refl_vals) / np.sum(refl_vals)

                    # Convert longitude to 0-360 if needed
                    if weighted_lon < 0:
                        weighted_lon += 360

                    centroid = (weighted_lat, weighted_lon)
                else:
                    centroid = (np.nan, np.nan)
            else:
                centroid = (np.nan, np.nan)


            # Count number of gates
            num_gates = np.count_nonzero(mask)

            results.append({
                "id": poly_id,
                "num_gates": num_gates,
                "centroid": centroid,
                "bbox": bbox,
                "max_refl": float(max_refl),
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

        

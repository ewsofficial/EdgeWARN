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

            # Get MLAT and MLON from ProbSevere data
            for feature in self.ps_ds.get("features", []):
                if int(feature["properties"].get("ID", 0)) == poly_id:
                    mlat = float(feature["properties"].get("MLAT"))
                    mlon = float(feature["properties"].get("MLON"))
                    if mlon < 0:
                        mlon += 360
                    centroid = (mlat, mlon)
                    break


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
    
    def append_storm_history(self, entries):
        """
        Appends storm history to each cell entry based on radar timestamp.
        Only adds a new entry if the timestamp does not already exist.
        Returns the updated list of dictionaries.
        """
        timestamp_new = DetectionDataHandler.find_timestamp(self.radar_path)

        for cell in entries:
            storm_history = cell.get('storm_history', [])

            # Skip if this timestamp already exists
            if any(entry['timestamp'] == timestamp_new for entry in storm_history):
                print(f"DEBUG: Skipping adding entry to {cell['id']}: timestamp already exists")
                continue

            # Append new storm history entry
            latest_storm_history = {
                "timestamp": timestamp_new,
                "max_refl": cell['max_refl'],
                "num_gates": cell['num_gates'],
                "centroid": cell['centroid'],
                "data": [],
                "analysis": []
            }
            storm_history.append(latest_storm_history)
            cell['storm_history'] = storm_history  # update in case 'storm_history' key was missing

        return entries

        

from typing import Dict, Any, Optional
from ..interface import AnalysisModule
from EdgeWARN.core.process.integrate.utils import StormIntegrationUtils

class FootprintModule(AnalysisModule):
    """
    Standardizes the storm cell's footprint polygon before analysis.
    """
    
    @property
    def name(self) -> str:
        return "Footprint"

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None) -> None:
        """
        Generate or extract the base footprint polygon for the storm cell.
        """
        # If the cell already has an 'alert_polygon' coordinate list from a previous scan, 
        # we still want the CURRENT footprint to be calculated.
        
        # Use existing utility to create a Polygon object from bbox or centroid
        poly = StormIntegrationUtils.create_cell_polygon(storm_entry)
        
        if poly is not None:
            # Store the shapely object in the entry for easy access by subsequent modules
            storm_entry["polygon_obj"] = poly
            
            # Also ensure a GeoJSON-like coordinate list is available in properties for persistence
            if "properties" not in storm_entry:
                storm_entry["properties"] = {}
                
            # Convert to coordinate list (lon, lat)
            coords = list(poly.exterior.coords)
            storm_entry["properties"]["polygon"] = coords
        else:
            storm_entry["modules"][self.name] = {
                "status": "error",
                "error": "Failed to generate polygon from bbox or centroid"
            }

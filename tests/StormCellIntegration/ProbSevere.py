import json
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from datetime import datetime
import os
import glob
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection

class ProbSevereVisualizer:
    def __init__(self):
        self.probsevere_cells = []
        self.storm_cells = []
        
    def find_latest_probsevere_file(self, directory):
        """Find the latest ProbSevere JSON file in the directory"""
        pattern = os.path.join(directory, "MRMS_PROBSEVERE_*.json")
        files = glob.glob(pattern)
        if not files:
            return None
        files.sort(key=os.path.getmtime, reverse=True)
        return files[0]
    
    def load_probsevere_data(self, filepath):
        """Load and parse ProbSevere JSON data"""
        print(f"Loading ProbSevere data from {filepath}")
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            if not data or 'features' not in data:
                print("No valid ProbSevere data found")
                return False
                
            self.probsevere_cells = self._parse_probsevere_json(data)
            print(f"Loaded {len(self.probsevere_cells)} ProbSevere cells")
            return True
            
        except Exception as e:
            print(f"Error loading ProbSevere data: {e}")
            return False
    
    def load_storm_cells(self, filepath):
        """Load storm cell data"""
        print(f"Loading storm cells from {filepath}")
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Check if it's a list of cells or a single object
            if isinstance(data, list):
                self.storm_cells = data
            elif isinstance(data, dict) and 'features' in data:
                # Handle GeoJSON format
                self.storm_cells = data['features']
            else:
                self.storm_cells = [data]
                
            print(f"Loaded {len(self.storm_cells)} storm cells")
            
            # Debug: print the structure of first storm cell
            if self.storm_cells:
                print("First storm cell keys:", list(self.storm_cells[0].keys()))
                if 'storm_history' in self.storm_cells[0]:
                    print("First storm history entry keys:", list(self.storm_cells[0]['storm_history'][0].keys()))
            
            return True
        except Exception as e:
            print(f"Error loading storm cells: {e}")
            return False
    
    def _parse_probsevere_json(self, json_data):
        """Parse ProbSevere JSON data and extract polygon coordinates"""
        storm_cells = []
        
        for feature in json_data['features']:
            if feature.get('type') != 'Feature':
                continue
                
            # Extract polygon coordinates from GeoJSON geometry
            if 'geometry' in feature and feature['geometry']:
                geometry = feature['geometry']
                if geometry.get('type') == 'Polygon' and 'coordinates' in geometry:
                    coords = geometry['coordinates']
                    if coords and len(coords) > 0 and len(coords[0]) > 0:
                        storm_cells.append(coords[0])
        
        return storm_cells
    
    def _extract_storm_polygons(self):
        """Extract polygon coordinates from storm cells"""
        storm_polygons = []
        
        for cell in self.storm_cells:
            polygon_coords = None
            
            # FIRST, check for alpha_shape at the root level (this is where your data is)
            if 'alpha_shape' in cell and cell['alpha_shape']:
                polygon_coords = cell['alpha_shape']
                # Convert longitude from 0-360 range to -180 to 180 if needed
                if polygon_coords and len(polygon_coords) > 0:
                    polygon_coords = [[lon - 360 if lon > 180 else lon, lat] for lon, lat in polygon_coords]
            
            # Then check for geometry directly in cell (GeoJSON format)
            elif 'geometry' in cell and cell['geometry']:
                geometry = cell['geometry']
                if geometry.get('type') == 'Polygon' and 'coordinates' in geometry:
                    polygon_coords = geometry['coordinates'][0]  # Get first ring
            
            # Finally, check for centroid - create a small square around it (fallback)
            elif 'centroid' in cell and len(cell['centroid']) >= 2:
                lat, lon = cell['centroid'][0], cell['centroid'][1]
                # Adjust longitude if needed (some systems use 0-360 instead of -180 to 180)
                if lon > 180:
                    lon -= 360
                d = 0.1  # degrees
                polygon_coords = [
                    [lon - d, lat - d],
                    [lon - d, lat + d],
                    [lon + d, lat + d],
                    [lon + d, lat - d]
                ]
            
            if polygon_coords is not None:
                storm_polygons.append(polygon_coords)
        
        return storm_polygons
    
    def generate_graph(self):
        """Generate and display the graph with polygons"""
        if not self.probsevere_cells and not self.storm_cells:
            print("No data to plot")
            return
        
        # Extract storm cell polygons
        storm_polygons = self._extract_storm_polygons()
        print(f"Found {len(storm_polygons)} storm cell polygons")
        
        # Determine plot bounds from all coordinates
        all_lons = []
        all_lats = []
        
        # Get bounds from ProbSevere polygons
        for polygon_coords in self.probsevere_cells:
            coords_array = np.array(polygon_coords)
            all_lons.extend(coords_array[:, 0])
            all_lats.extend(coords_array[:, 1])
        
        # Get bounds from storm cell polygons
        for polygon_coords in storm_polygons:
            coords_array = np.array(polygon_coords)
            all_lons.extend(coords_array[:, 0])
            all_lats.extend(coords_array[:, 1])
        
        if not all_lons or not all_lats:
            print("No valid coordinates found for plotting")
            return
        
        # Create figure with Cartopy projection
        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())
        
        # Add map features
        ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
        ax.add_feature(cfeature.BORDERS, linewidth=0.8)
        ax.add_feature(cfeature.STATES, linewidth=0.5, alpha=0.3)
        
        # Set plot bounds with padding
        lon_min, lon_max = min(all_lons), max(all_lons)
        lat_min, lat_max = min(all_lats), max(all_lats)
        padding = 2.0
        
        ax.set_extent([
            lon_min - padding, 
            lon_max + padding, 
            lat_min - padding, 
            lat_max + padding
        ], crs=ccrs.PlateCarree())
        
        # Plot ProbSevere polygons in BLUE
        if self.probsevere_cells:
            prob_patches = []
            
            for polygon_coords in self.probsevere_cells:
                polygon = Polygon(polygon_coords, closed=True)
                prob_patches.append(polygon)
            
            # Create patch collection for ProbSevere (BLUE)
            prob_collection = PatchCollection(prob_patches, 
                                            facecolor='blue', 
                                            edgecolor='darkblue', 
                                            linewidth=1.0,
                                            alpha=0.6,
                                            label='ProbSevere')
            ax.add_collection(prob_collection)
            print(f"Plotted {len(prob_patches)} ProbSevere polygons")
        
        # Plot storm cell polygons in RED
        if storm_polygons:
            storm_patches = []
            
            for polygon_coords in storm_polygons:
                polygon = Polygon(polygon_coords, closed=True)
                storm_patches.append(polygon)
            
            # Create patch collection for storm cells (RED)
            storm_collection = PatchCollection(storm_patches, 
                                             facecolor='red', 
                                             edgecolor='darkred', 
                                             linewidth=1.5,
                                             alpha=0.6,
                                             label='Storm Cells')
            ax.add_collection(storm_collection)
            print(f"Plotted {len(storm_patches)} storm cell polygons")
        
        # Add grid lines
        ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        
        # Add title
        ax.set_title('ProbSevere (Blue) vs Storm Cells (Red)', fontsize=12, fontweight='bold')
        
        # Add simple legend
        ax.legend(loc='upper right')
        
        # Show plot
        plt.tight_layout()
        plt.show()

def main():
    """Main function - just show the graph"""
    visualizer = ProbSevereVisualizer()
    
    # File paths
    probsevere_directory = r"C:\input_data\mrms_probsevere"
    storm_cells_file = r"stormcell_test.json"
    
    # Find and load the latest ProbSevere file
    probsevere_file = visualizer.find_latest_probsevere_file(probsevere_directory)
    if not probsevere_file:
        print(f"No ProbSevere files found in {probsevere_directory}")
        return
    
    print(f"Using ProbSevere file: {probsevere_file}")
    
    # Load data
    if not visualizer.load_probsevere_data(probsevere_file):
        return
    
    visualizer.load_storm_cells(storm_cells_file)
    
    # Generate graph
    print("Generating graph...")
    visualizer.generate_graph()

if __name__ == "__main__":
    main()
import numpy as np
import matplotlib.pyplot as plt
from ..CellIntegration.utils import StormIntegrationUtils
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.patches as mpatches
from matplotlib.patches import Polygon as MplPolygon


class Visualizer:
    def __init__():
        """
        Initializes Visualizer class
        """

    @staticmethod
    def plot_radar_and_cells(refl, lat_grid, lon_grid, cells0, cells1, matches):
        # Check if lon is decreasing, then flip arrays to make lon increasing
        if lon_grid[0, 1] < lon_grid[0, 0]:
            lon_grid = np.flip(lon_grid, axis=1)
            refl = np.flip(refl, axis=1)

        fig, ax = plt.subplots(figsize=(14, 12))

        # Plot reflectivity
        pcm = ax.pcolormesh(lon_grid, lat_grid, refl, cmap='NWSRef', shading='auto', vmin=0, vmax=80, alpha=0.7)
        fig.colorbar(pcm, ax=ax, label='Reflectivity (dBZ)', pad=0.02)

        # Plot OLD cells (black outlines only)
        for cell in cells0:
            polygon = cell.get('convex_hull') or cell.get('alpha_shape', [])
            if polygon and len(polygon) >= 3:
                hull_lon, hull_lat = zip(*polygon)
                # Black outline, no fill
                ax.plot(hull_lon, hull_lat, 'k-', linewidth=2, alpha=0.8)
                # Close the polygon if not already closed
                if hull_lon[0] != hull_lon[-1] or hull_lat[0] != hull_lat[-1]:
                    ax.plot([hull_lon[-1], hull_lon[0]], [hull_lat[-1], hull_lat[0]], 'k-', linewidth=2, alpha=0.8)
            
            # Black centroid with larger marker
            if cell == cells0[0]:
                ax.plot(cell['centroid'][1], cell['centroid'][0], 'ko', markersize=8, 
                        markeredgewidth=2, markerfacecolor='white', label='Old Cell')
            else:
                ax.plot(cell['centroid'][1], cell['centroid'][0], 'ko', markersize=8, 
                        markeredgewidth=2, markerfacecolor='white')

        # Plot NEW cells (RED OUTLINES ONLY - NO FILL)
        for cell in cells1:
            polygon = cell.get('convex_hull') or cell.get('alpha_shape', [])
            if polygon and len(polygon) >= 3:
                hull_lon, hull_lat = zip(*polygon)
                # Red outline only, no fill
                ax.plot(hull_lon, hull_lat, 'r-', linewidth=2, alpha=0.8)
                # Close the polygon if not already closed
                if hull_lon[0] != hull_lon[-1] or hull_lat[0] != hull_lat[-1]:
                    ax.plot([hull_lon[-1], hull_lon[0]], [hull_lat[-1], hull_lat[0]], 'r-', linewidth=2, alpha=0.8)
            
            # Red centroid
            if cell == cells1[0]:
                ax.plot(cell['centroid'][1], cell['centroid'][0], 'ro', markersize=8, 
                        markeredgewidth=2, markerfacecolor='red', label='New Cell')
            else:
                ax.plot(cell['centroid'][1], cell['centroid'][0], 'ro', markersize=8, 
                        markeredgewidth=2, markerfacecolor='red')

        # Plot 1: Current matches with arrows (from matching algorithm)
        for i, j, cost in matches:
            c0 = cells0[i]['centroid']
            c1 = cells1[j]['centroid']
            
            # Calculate distance
            dist = np.sqrt((c1[0]-c0[0])**2 + (c1[1]-c0[1])**2)
            
            # Draw arrow from old to new centroid (NO DISTANCE FILTER)
            ax.annotate('', xy=(c1[1], c1[0]), xytext=(c0[1], c0[0]),
                    arrowprops=dict(arrowstyle='->', color='blue', lw=2, 
                                    shrinkA=5, shrinkB=5, alpha=0.8))
            
            # Add distance/cost text near the arrow
            mid_lon = (c0[1] + c1[1]) / 2
            mid_lat = (c0[0] + c1[0]) / 2
            ax.text(mid_lon, mid_lat, f'dist: {dist:.2f}\ncost: {cost:.2f}', 
                fontsize=8, bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))

        # Plot 2: Movement history from storm data (if available)
        # This shows the actual movement stored in your JSON
        for cell in cells0:
            if 'storm_history' in cell and len(cell['storm_history']) >= 2:
                # Get the last two positions from history
                hist = cell['storm_history']
                if len(hist) >= 2:
                    prev_pos = hist[-2]['centroid']  # Previous position
                    curr_pos = hist[-1]['centroid']  # Current position
                    
                    # Draw historical movement arrow (green for history)
                    ax.annotate('', xy=(curr_pos[1], curr_pos[0]), 
                            xytext=(prev_pos[1], prev_pos[0]),
                            arrowprops=dict(arrowstyle='->', color='green', lw=2, 
                                            shrinkA=5, shrinkB=5, alpha=0.6, linestyle=':'))

        # Add legend
        from matplotlib.lines import Line2D
        legend_elements = [
            Line2D([0], [0], marker='o', color='w', markerfacecolor='black', markersize=8, label='Old Cell Centroid'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=8, label='New Cell Centroid'),
            Line2D([0], [0], color='black', lw=2, label='Old Cell Outline'),
            Line2D([0], [0], color='red', lw=2, label='New Cell Outline'),
            Line2D([0], [0], color='blue', lw=2, label='Current Match'),
            Line2D([0], [0], color='green', lw=2, linestyle=':', label='Historical Movement')
        ]
        ax.legend(handles=legend_elements, loc='upper right')

        # Set labels and title with timestamp info
        old_time = cells0[0]['storm_history'][0]['timestamp'] if cells0 and cells0[0].get('storm_history') else 'N/A'
        new_time = cells1[0]['storm_history'][0]['timestamp'] if cells1 and cells1[0].get('storm_history') else 'N/A'
        
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_title(f'Storm Cell Tracking\nOld: {old_time} → New: {new_time}\n{len(matches)} matches found')

        # Add grid for better spatial reference
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Set equal aspect ratio for proper spatial representation
        ax.set_aspect('equal', adjustable='box')

        plt.tight_layout()
        plt.show()

        # Print match statistics
        if matches:
            distances = [np.sqrt((cells0[i]['centroid'][0]-cells1[j]['centroid'][0])**2 + 
                                (cells0[i]['centroid'][1]-cells1[j]['centroid'][1])**2) 
                        for i, j, _ in matches]
            print(f"Match statistics: {len(matches)} matches")
            print(f"Average distance: {np.mean(distances):.2f} degrees")
            print(f"Max distance: {np.max(distances):.2f} degrees")
            print(f"Min distance: {np.min(distances):.2f} degrees")

    def graph_probsevere_stormcells(self, probsevere_data, storm_cells, output_path="probsevere_stormcells_map.png"):
        """
        Graph ProbSevere polygons (blue) and storm cell polygons (red) on a CONUS map.
        
        Args:
            probsevere_data: ProbSevere JSON data with features
            storm_cells: List of storm cell dictionaries
            output_path: Path to save the output image
        """
        # Create figure and map
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.LambertConformal())
        
        # Set extent for CONUS
        ax.set_extent([-125, -65, 20, 50], ccrs.Geodetic())
        
        # Add map features
        ax.add_feature(cfeature.STATES, linewidth=0.5)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
        ax.add_feature(cfeature.BORDERS, linewidth=0.8)
        ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.5)
        ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.3)
        
        # Plot ProbSevere polygons (blue)
        if probsevere_data and 'features' in probsevere_data:
            probsevere_features = probsevere_data['features']
            for feature in probsevere_features:
                try:
                    geometry = feature.get('geometry')
                    if geometry and geometry['type'] == 'Polygon':
                        # Extract coordinates
                        coords = geometry['coordinates'][0]
                        # Convert to matplotlib polygon
                        poly = MplPolygon(coords, closed=True, 
                                            edgecolor='blue', facecolor='blue', 
                                            alpha=0.3, transform=ccrs.PlateCarree())
                        ax.add_patch(poly)
                except Exception as e:
                    print(f"Error plotting ProbSevere polygon: {e}")
        
        # Plot storm cell polygons (red)
        for cell in storm_cells:
            try:
                polygon = StormIntegrationUtils.create_cell_polygon(cell)
                if polygon is not None:
                    # Convert to matplotlib polygon
                    poly = MplPolygon(polygon, closed=True, 
                                        edgecolor='red', facecolor='red', 
                                        alpha=0.3, transform=ccrs.PlateCarree())
                    ax.add_patch(poly)
                    
                    # Also plot centroid if available
                    if 'centroid' in cell and len(cell['centroid']) >= 2:
                        lat, lon = cell['centroid'][0], cell['centroid'][1]
                        ax.plot(lon, lat, 'ro', markersize=4, transform=ccrs.PlateCarree())
                        
            except Exception as e:
                print(f"Error plotting storm cell polygon: {e}")
        
        # Add legend
        probsevere_patch = mpatches.Patch(color='blue', alpha=0.3, label='ProbSevere Polygons')
        stormcell_patch = mpatches.Patch(color='red', alpha=0.3, label='Storm Cell Polygons')
        plt.legend(handles=[probsevere_patch, stormcell_patch], loc='lower right')
        
        # Add title
        plt.title('ProbSevere and Storm Cell Polygons', fontsize=14)
        
        # Save and show
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"Map saved to {output_path}")




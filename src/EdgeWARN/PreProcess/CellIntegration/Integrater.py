import xarray as xr
import util.file as fs
from util.file import StatFileHandler
from datetime import datetime
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from matplotlib.patches import Polygon as MplPolygon
import matplotlib.patches as mpatches
from .utils import StormIntegrationUtils
from .integrator import StormCellIntegrator
    
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
        
def main():
    """
    Main function to run integration of all data types with storm cells.
    """
    handler = StatFileHandler()
    
    # Load storm cells
    storm_json_path = "stormcell_test.json"
    print(f"Loading storm cells from {storm_json_path}")
    cells = handler.load_json(storm_json_path)
    if cells is None:
        print("No storm cells loaded; aborting.")
        return
    
    integrator = StormCellIntegrator()
    result_cells = cells
    
    # 1. Integrate NLDN lightning data
    print("\n" + "="*50)
    print("INTEGRATING NLDN LIGHTNING DATA")
    print("="*50)
    nldn_list = fs.latest_nldn(1)
    if nldn_list:
        nldn_path = nldn_list[-1]
        print(f"Using NLDN file: {nldn_path}")
        
        nldn_timestamp = handler.find_timestamp(nldn_path)
        if nldn_timestamp:
            try:
                nldn_ds = xr.open_dataset(nldn_path)
                result_cells = integrator.integrate_nldn(nldn_ds, result_cells, nldn_timestamp)
                print("NLDN integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate NLDN data: {e}")
        else:
            print("Could not determine timestamp from NLDN file")
    else:
        print("No NLDN files found")
    
    # 2. Integrate EchoTop data
    print("\n" + "="*50)
    print("INTEGRATING ECHOTOP DATA")
    print("="*50)
    echotop_list = fs.latest_echotop18(1)
    if echotop_list:
        echotop_path = echotop_list[-1]
        print(f"Using EchoTop file: {echotop_path}")
        
        echotop_timestamp = handler.find_timestamp(echotop_path)
        if echotop_timestamp:
            try:
                echotop_ds = xr.open_dataset(echotop_path)
                result_cells = integrator.integrate_echotop(echotop_ds, result_cells, echotop_timestamp)
                print("EchoTop integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate EchoTop data: {e}")
        else:
            print("Could not determine timestamp from EchoTop file")
    else:
        print("No EchoTop files found")
    
    # 3. Integrate ProbSevere data
    print("\n" + "="*50)
    print("INTEGRATING PROBSEVERE DATA")
    print("="*50)
    probsevere_list = fs.latest_probsevere(1)
    if probsevere_list:
        probsevere_path = probsevere_list[-1]
        print(f"Using ProbSevere file: {probsevere_path}")
        
        # Load raw ProbSevere JSON
        probsevere_data = handler.load_json(probsevere_path)
        if probsevere_data:
            # Extract timestamp from ProbSevere data
            probsevere_timestamp_str = probsevere_data.get('validTime', '').replace('_', ' ')
            try:
                probsevere_timestamp = datetime.strptime(probsevere_timestamp_str, "%Y%m%d %H%M%S UTC")
                # Pass the full JSON dict instead of just features
                result_cells = integrator.integrate_probsevere(probsevere_data, result_cells, probsevere_timestamp)
                print("ProbSevere integration completed successfully")
                
                # Create visualization of ProbSevere and storm cell polygons
                print("\nCreating visualization of ProbSevere and storm cell polygons...")
                graph_probsevere_stormcells(probsevere_data, result_cells)
                
            except ValueError:
                print(f"Could not parse timestamp from ProbSevere data: {probsevere_timestamp_str}")
        else:
            print("Failed to load ProbSevere JSON data")
    else:
        print("No ProbSevere files found")
    
    # 4. Integrate MRMS PrecipRate data
    print("\n" + "="*50)
    print("INTEGRATING MRMS PRECIPRATE DATA")
    print("="*50)
    preciprate_list = fs.latest_preciprate(1)
    if preciprate_list:
        preciprate_path = preciprate_list[-1]
        print(f"Using PrecipRate file: {preciprate_path}")
        
        preciprate_timestamp = handler.find_timestamp(preciprate_path)
        if preciprate_timestamp:
            try:
                preciprate_ds = xr.open_dataset(preciprate_path)
                result_cells = integrator.integrate_preciprate(preciprate_ds, result_cells, preciprate_timestamp)
                print("MRMS PrecipRate integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate PrecipRate data: {e}")
        else:
            print("Could not determine timestamp from PrecipRate file")
    else:
        print("No PrecipRate files found")
    
    # 5. Integrate GLM lightning data
    print("\n" + "="*50)
    print("INTEGRATING GLM LIGHTNING DATA")
    print("="*50)
    glm_list = fs.latest_glm(1)
    if glm_list:
        glm_path = glm_list[-1]
        print(f"Using GLM file: {glm_path}")
        
        glm_timestamp = handler.find_timestamp(glm_path)
        if glm_timestamp:
            try:
                glm_ds = xr.open_dataset(glm_path)
                result_cells = integrator.integrate_glm(glm_ds, result_cells, glm_timestamp)
                print("GLM integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate GLM data: {e}")
        else:
            print("Could not determine timestamp from GLM file")
    else:
        print("No GLM files found")

    # 6. Integrate MRMS VIL Density data
    print("\n" + "="*50)
    print("INTEGRATING MRMS VIL DENSITY DATA")
    print("="*50)
    vil_density_list = fs.latest_vil_density(1)
    if vil_density_list:
        vil_density_path = vil_density_list[-1]
        print(f"Using VIL Density file: {vil_density_path}")
        
        vil_density_timestamp = handler.find_timestamp(vil_density_path)
        if vil_density_timestamp:
            try:
                vil_density_ds = xr.open_dataset(vil_density_path)
                result_cells = integrator.integrate_vil_density(vil_density_ds, result_cells, vil_density_timestamp)
                print("MRMS VIL Density integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate VIL Density data: {e}")
        else:
            print("Could not determine timestamp from VIL Density file")
    else:
        print("No VIL Density files found")
    
    # Save final integrated results
    output_json = storm_json_path.replace('.json', '_fully_integrated.json')
    print(f"\nSaving fully integrated results to {output_json}")
    handler.write_json(result_cells, output_json)
    
    # Print comprehensive summary
    print("\n" + "="*60)
    print("INTEGRATION SUMMARY")
    print("="*60)
    
    # Update these variable declarations:
    cells_with_nldn = 0
    cells_with_echotop = 0
    cells_with_probsevere = 0
    cells_with_glm = 0
    cells_with_preciprate = 0
    cells_with_vil_density = 0 
    total_entries = 0

    # Count number of cells with valid data
    for cell in result_cells:
        if 'storm_history' in cell:
            for entry in cell['storm_history']:
                total_entries += 1
                if isinstance(entry.get('max_flash_rate', "N/A"), (int, float)):
                    cells_with_nldn += 1
                if isinstance(entry.get('max_echotop_height', "N/A"), (int, float)):
                    cells_with_echotop += 1
                if isinstance(entry.get('prob_severe', "N/A"), (int, float)):
                    cells_with_probsevere += 1
                if isinstance(entry.get('glm_flashrate_1m', "N/A"), (int, float)):
                    cells_with_glm += 1
                if isinstance(entry.get('max_precip_rate', "N/A"), (int, float)):
                    cells_with_preciprate += 1
                if isinstance(entry.get('max_vil_density', "N/A"), (int, float)): 
                    cells_with_vil_density += 1

    # Print Summary
    print(f"Entries with NLDN data: {cells_with_nldn}/{total_entries}")
    print(f"Entries with EchoTop data: {cells_with_echotop}/{total_entries}")
    print(f"Entries with ProbSevere data: {cells_with_probsevere}/{total_entries}")
    print(f"Entries with GLM data: {cells_with_glm}/{total_entries}")
    print(f"Entries with PrecipRate data: {cells_with_preciprate}/{total_entries}")
    print(f"Entries with VIL Density data: {cells_with_vil_density}/{total_entries}") 

if __name__ == "__main__":
    main()
import sys
from pathlib import Path
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Ensure 'src' is in the path so we can import util
# Assuming this script is located in src/
sys.path.append(str(Path(__file__).parent))

try:
    from util.file import GOES_GLM_DIR
except ImportError:
    # Fallback if running from root or elsewhere
    sys.path.append(str(Path(__file__).parent.parent))
    from src.util.file import GOES_GLM_DIR

def plot_latest_glm():
    if not GOES_GLM_DIR.exists():
        print(f"Directory not found: {GOES_GLM_DIR}")
        return

    # Find latest file
    # Exclude .idx files manually just in case, though glob("*.nc") should be safe
    glm_files = sorted([f for f in GOES_GLM_DIR.glob("*.nc") if f.is_file()])
    
    if not glm_files:
        print(f"No NetCDF (.nc) files found in {GOES_GLM_DIR}")
        return

    latest_file = glm_files[-1]
    print(f"Plotting {latest_file}")

    try:
        ds = xr.open_dataset(latest_file, engine="netcdf4")
    except Exception as e:
        print(f"Error opening file: {e}")
        return

    if 'flash_lon' not in ds or 'flash_lat' not in ds:
        print("File does not contain flash_lon or flash_lat variables.")
        ds.close()
        return

    lons = ds['flash_lon'].values
    lats = ds['flash_lat'].values
    
    # Check if longitudes are in -180 to 180 format (contain negatives)
    if np.any(lons < 0):
        print("GLM Longitude detected in -180 to 180 format. Converting to 0-360.")
        lons = (lons + 360) % 360
    
    # Check for energy variable, usually flash_energy
    energy = None
    if 'flash_energy' in ds:
        energy = ds['flash_energy'].values
    
    ds.close()

    # Create Plot
    fig = plt.figure(figsize=(15, 10))
    # Use PlateCarree for lat/lon data
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    
    # Set extent to CONUS roughly
    ax.set_extent([-125, -66, 24, 50], ccrs.PlateCarree())

    # Add Map Features
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.STATES, linestyle=':', edgecolor='gray')

    # Plot Data
    if len(lons) > 0:
        # Create a copy for plotting that maps back to -180 to 180 
        # because the map extent is defined in that range [-125, -66]
        plot_lons = lons.copy()
        plot_lons[plot_lons > 180] -= 360

        if energy is not None:
            # Use energy for color
            sc = ax.scatter(plot_lons, lats, c=energy, cmap='plasma', s=20, transform=ccrs.PlateCarree(), alpha=0.8, marker='+')
            cbar = plt.colorbar(sc, orientation='horizontal', pad=0.05, aspect=50)
            cbar.set_label('Flash Energy (J)')
        else:
            ax.scatter(plot_lons, lats, c='yellow', s=20, transform=ccrs.PlateCarree(), alpha=0.8, marker='+', label='Flashes')
            plt.legend()
        
        print(f"Plotted {len(lons)} flashes.")
    else:
        print("No flashes found in this file.")

    plt.title(f"GLM Flash Data - {latest_file.name}")
    plt.show()

if __name__ == "__main__":
    plot_latest_glm()

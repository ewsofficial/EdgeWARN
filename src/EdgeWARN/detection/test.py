import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import numpy as np

def load_mrms_slice_bbox(filepath, lat_limits=None, lon_limits=None):
    ds = xr.open_dataset(filepath)

    # --- Reflectivity ---
    if "reflectivity_combined" in ds:
        refl = ds["reflectivity_combined"]
    elif "unknown" in ds:
        refl = ds["unknown"]
    else:
        raise ValueError("No valid reflectivity data found.")

    # --- Coordinates ---
    if "x" in ds and "y" in ds:
        lat = ds["y"]
        lon = ds["x"]
        lat_dim, lon_dim = "y", "x"
    elif "latitude" in ds and "longitude" in ds:
        lat = ds["latitude"]
        lon = ds["longitude"]
        lat_dim, lon_dim = "latitude", "longitude"
    else:
        raise ValueError("No valid coordinates.")

    # Find index ranges that satisfy bounding box
    if lat_limits is not None:
        lat_mask = (lat >= lat_limits[0]) & (lat <= lat_limits[1])
        lat_inds = np.where(lat_mask)
        y_start, y_end = lat_inds[0].min(), lat_inds[0].max()+1
    else:
        y_start, y_end = 0, lat.shape[0]

    if lon_limits is not None:
        lon_mask = (lon >= lon_limits[0]) & (lon <= lon_limits[1])
        lon_inds = np.where(lon_mask)
        x_start, x_end = lon_inds[1].min(), lon_inds[1].max()+1
    else:
        x_start, x_end = 0, lon.shape[1]

    # Slice reflectivity and coords
    refl_crop = refl.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)})
    lat_crop = lat.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)})
    lon_crop = lon.isel({lat_dim: slice(y_start, y_end), lon_dim: slice(x_start, x_end)})

    return refl_crop, lat_crop, lon_crop

def plot_mrms(refl, lat, lon):
    fig, ax = plt.subplots(figsize=(12, 8), subplot_kw={'projection': ccrs.PlateCarree()})
    refl_masked = np.ma.masked_invalid(refl.values)
    pcm = ax.pcolormesh(lon.values, lat.values, refl_masked, cmap='turbo', shading='auto')

    ax.add_feature(cfeature.STATES.with_scale('50m'), edgecolor='gray')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')
    ax.add_feature(cfeature.OCEAN, facecolor='lightblue')
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)

    plt.colorbar(pcm, ax=ax, label='Reflectivity (dBZ)')
    ax.set_title("MRMS Reflectivity (Bounding Box)")
    plt.show()

# -----------------------------
# Main
# -----------------------------
filepath = Path(r"C:\input_data\MRMS_MergedReflectivityQC_3D_20250804-235241_renamed.nc")
lat_limits = (38.8, 40.1)
lon_limits = (256, 258.5)

refl_crop, lat_vals, lon_vals = load_mrms_slice_bbox(filepath, lat_limits, lon_limits)
plot_mrms(refl_crop, lat_vals, lon_vals)

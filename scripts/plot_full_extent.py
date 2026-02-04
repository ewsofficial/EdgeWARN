import json
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from pathlib import Path
import sys

# Paths
SNAPSHOT_DIR = Path("snapshot_20260204-0614")

try:
    GLM_FILE = list((SNAPSHOT_DIR / "data/GLM").glob("*.nc"))[0]
except IndexError:
    print("Error: No GLM .nc file found in snapshot.")
    sys.exit(1)

try:
    PS_FILE = list((SNAPSHOT_DIR / "data/ProbSevere").glob("*.json"))[0]
except IndexError:
    print("Error: No ProbSevere .json file found in snapshot.")
    sys.exit(1)

print(f"GLM File: {GLM_FILE}")
print(f"ProbSevere File: {PS_FILE}")

# Load GLM
print("Loading GLM data...")
try:
    ds = xr.open_dataset(GLM_FILE, engine="netcdf4")
    glm_lats = ds["flash_lat"].values
    glm_lons_raw = ds["flash_lon"].values
    
    # Normalize to -180 to 180
    glm_lons = ((glm_lons_raw + 180) % 360) - 180
    
    print(f"Loaded {len(glm_lats)} flashes.")
except Exception as e:
    print(f"Error loading GLM data: {e}")
    sys.exit(1)

# Load ProbSevere
print("Loading ProbSevere data...")
try:
    with open(PS_FILE, "r") as f:
        ps_data = json.load(f)
    features = ps_data.get("features", [])
    print(f"Loaded {len(features)} ProbSevere features.")
except Exception as e:
    print(f"Error loading ProbSevere data: {e}")
    sys.exit(1)

# Plot
print("Generating plot...")
fig, ax = plt.subplots(figsize=(16, 10))

# Plot ProbSevere Polygons
for feature in features:
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [])
    geom_type = geom.get("type", "")
    
    if geom_type == "Polygon":
        for ring in coords:
            lons, lats = zip(*ring)
            ax.plot(lons, lats, color='red', linewidth=2, alpha=0.7)
    elif geom_type == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                lons, lats = zip(*ring)
                ax.plot(lons, lats, color='red', linewidth=2, alpha=0.7)

# Plot GLM Flashes
ax.scatter(glm_lons, glm_lats, c='blue', s=5, alpha=0.5, label='GLM Flashes')

# Draw Florida Box (from previous script)
fl_xlims = (-89, -78)
fl_ylims = (22, 33)
fl_rect = patches.Rectangle((fl_xlims[0], fl_ylims[0]), 
                            fl_xlims[1]-fl_xlims[0], 
                            fl_ylims[1]-fl_ylims[0], 
                            linewidth=2, edgecolor='green', facecolor='none', label='Florida Zoom')
ax.add_patch(fl_rect)

# Draw Standard Analysis Box (approx US)
us_xlims = (-130, -60)
us_ylims = (20, 55)
us_rect = patches.Rectangle((us_xlims[0], us_ylims[0]), 
                            us_xlims[1]-us_xlims[0], 
                            us_ylims[1]-us_ylims[0], 
                            linewidth=2, edgecolor='black', linestyle='--', facecolor='none', label='Standard Analysis Area')
ax.add_patch(us_rect)


# Set limits to full data extent + padding
if len(glm_lats) > 0:
    min_lat, max_lat = min(glm_lats), max(glm_lats)
    min_lon, max_lon = min(glm_lons), max(glm_lons)
    
    # Also considering ProbSevere bounds if any
    # (Simplified: relying on GLM usually having wider coverage, but let's be safe)
    # ... ignoring specific PS bounds calculation for simplicity as GLM usually global/hemispheric
    
    pad = 5.0
    
    # Ensure standard box is visible even if data is small
    plot_min_lon = min(min_lon, us_xlims[0]) - pad
    plot_max_lon = max(max_lon, us_xlims[1]) + pad
    plot_min_lat = min(min_lat, us_ylims[0]) - pad
    plot_max_lat = max(max_lat, us_ylims[1]) + pad
    
    ax.set_xlim(plot_min_lon, plot_max_lon)
    ax.set_ylim(plot_min_lat, plot_max_lat)

ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.set_title("Full Data Extent with Reference Areas (06:14 UTC)")
ax.legend(loc='upper right')
ax.grid(True, alpha=0.3)

# Save plot
output_path = "full_extent_map.png"
fig.savefig(output_path, dpi=150)
print(f"Plot saved to {output_path}")

import json
import xarray as xr
import matplotlib.pyplot as plt
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
    if len(glm_lats) > 0:
        print(f"GLM Lat Range: {min(glm_lats):.4f} to {max(glm_lats):.4f}")
        print(f"GLM Lon Range: {min(glm_lons):.4f} to {max(glm_lons):.4f}")
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
plt.figure(figsize=(15, 12))

# Plot ProbSevere Polygons
legend_added = False
for feature in features:
    geom = feature.get("geometry", {})
    cols = feature.get("properties", {})
    coords = geom.get("coordinates", [])
    geom_type = geom.get("type", "")
    
    label = 'ProbSevere' if not legend_added else ""
    
    if geom_type == "Polygon":
        for ring in coords:
            lons, lats = zip(*ring)
            plt.plot(lons, lats, color='red', linewidth=2, label=label)
            legend_added = True
    elif geom_type == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                lons, lats = zip(*ring)
                plt.plot(lons, lats, color='red', linewidth=2, label=label)
                legend_added = True

# Plot GLM Flashes
plt.scatter(glm_lons, glm_lats, c='blue', s=5, alpha=0.6, label='GLM Flashes')

# Focus map
# Get bounds
if len(glm_lats) > 0:
    min_lat, max_lat = min(glm_lats), max(glm_lats)
    min_lon, max_lon = min(glm_lons), max(glm_lons)
    
    # Zoom into Florida with 2 deg padding
    # Florida approx: Lat 24-31, Lon -87 to -80
    # Padded (+/- 2): Lat 22-33, Lon -89 to -78
    xlims = (-89, -78)
    ylims = (22, 33)
    
    # Check counts in view
    in_view = np.sum((glm_lats >= ylims[0]) & (glm_lats <= ylims[1]) & 
                     (glm_lons >= xlims[0]) & (glm_lons <= xlims[1]))
    print(f"Flashes in view ({xlims}, {ylims}): {in_view}")
    
    plt.xlim(*xlims)
    plt.ylim(*ylims)

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("GLM Flashes vs ProbSevere Polygons (06:14 UTC) - Florida Zoom")
plt.legend()
plt.grid(True, alpha=0.3)

# Save plot
output_path = "glm_ps_verification_florida.png"
plt.savefig(output_path, dpi=150)
print(f"Plot saved to {output_path}")

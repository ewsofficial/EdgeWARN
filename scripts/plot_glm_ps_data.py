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
for feature in features:
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [])
    geom_type = geom.get("type", "")
    
    if geom_type == "Polygon":
        for ring in coords:
            lons, lats = zip(*ring)
            plt.plot(lons, lats, color='red', linewidth=2)
    elif geom_type == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                lons, lats = zip(*ring)
                plt.plot(lons, lats, color='red', linewidth=2)

# Plot GLM Flashes
plt.scatter(glm_lons, glm_lats, c='blue', s=5, alpha=0.6, label='GLM Flashes')

# Zoom to data bounds with padding
if len(glm_lats) > 0:
    min_lat, max_lat = min(glm_lats), max(glm_lats)
    min_lon, max_lon = min(glm_lons), max(glm_lons)
    pad = 2.0
    plt.xlim(min_lon - pad, max_lon + pad)
    plt.ylim(min_lat - pad, max_lat + pad)
    print(f"Zooming to data: Lat [{min_lat:.2f}, {max_lat:.2f}], Lon [{min_lon:.2f}, {max_lon:.2f}]")

plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title(f"GLM Flashes vs ProbSevere Polygons (06:14 UTC) - Data Bounds")
plt.legend()
plt.grid(True, alpha=0.3)

# Save plot
output_path = "glm_ps_verification_data_bounds.png"
plt.savefig(output_path, dpi=150)
print(f"Plot saved to {output_path}")

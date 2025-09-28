import matplotlib.pyplot as plt
from shapely.geometry import shape
from matplotlib.patches import Polygon as MplPolygon
from EdgeWARN.PreProcess.CellDetection.tools.utils import DetectionDataHandler
import json as js


import matplotlib.pyplot as plt
from shapely.geometry import shape
from matplotlib.patches import Polygon as MplPolygon

import matplotlib.pyplot as plt
from shapely.geometry import shape
from matplotlib.patches import Polygon as MplPolygon
import numpy as np
import pyart

def plot_probsevere_vs_cells_with_radar(ps_ds, cell_entries, radar_ds, refl_var='unknown', title="ProbSevere vs Mapped Cells with Radar"):
    fig, ax = plt.subplots(figsize=(10, 8))

    # --- Radar reflectivity overlay ---
    refl_grid = radar_ds[refl_var].values
    lats = radar_ds['latitude'].values
    lons = radar_ds['longitude'].values

    # imshow expects (rows, cols), so extent is [min_lon, max_lon, min_lat, max_lat]
    im = ax.imshow(refl_grid, origin='upper', 
                   extent=[lons.min(), lons.max(), lats.min(), lats.max()],
                   cmap='NWSRef', interpolation='nearest', alpha=0.5, vmin=0, vmax=75)
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Reflectivity (dBZ)')

    # --- Plot ProbSevere polygons ---
    for feature in ps_ds.get("features", []):
        poly_id = int(feature["properties"].get("ID", 0))
        geom = shape(feature["geometry"])
        if geom.geom_type == "Polygon":
            polygons = [geom]
        elif geom.geom_type == "MultiPolygon":
            polygons = list(geom.geoms)
        else:
            continue

        for poly in polygons:
            coords = [(lon+360 if lon < 0 else lon, lat) for lon, lat in poly.exterior.coords]
            lons_poly, lats_poly = zip(*coords)
            patch = MplPolygon(list(zip(lons_poly, lats_poly)), closed=True,
                               edgecolor='red', facecolor='none', linewidth=1.5, label='ProbSevere')
            ax.add_patch(patch)

            centroid_lon = poly.centroid.x
            if centroid_lon < 0:
                centroid_lon += 360
            centroid_lat = poly.centroid.y
            ax.plot(centroid_lon, centroid_lat, 'ro')  # red dot for centroid
            ax.text(centroid_lon, centroid_lat, str(poly_id), color='red', fontsize=8)

    # --- Plot your dataset cells ---
    for cell in cell_entries:
        bbox = cell.get("bbox", [])
        if not bbox or len(bbox) < 3:
            continue

        lons_cell, lats_cell = zip(*bbox)
        patch = MplPolygon(list(zip(lons_cell, lats_cell)), closed=True,
                           edgecolor='blue', facecolor='none', linewidth=1.2, label='Mapped Cell')
        ax.add_patch(patch)

        centroid_lat, centroid_lon = cell["centroid"]
        ax.plot(centroid_lon, centroid_lat, 'bo')  # blue dot for centroid
        ax.text(centroid_lon, centroid_lat, str(cell["id"]), color='blue', fontsize=8)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(title)
    ax.set_aspect('equal')
    ax.grid(True)

    # Prevent duplicate labels in legend
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys())

    plt.show()


with open('stormcell_test_ps.json', 'r') as f:
    cell_entries = js.load(f)

import util.core.file as fs
handler = DetectionDataHandler(r"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_20250927-210640.nc", fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1], 35.0, 38.0, 283.0, 285.0)
ps_ds = handler.load_probsevere()
radar_ds = handler.load_subset()

plot_probsevere_vs_cells_with_radar(ps_ds, cell_entries, radar_ds)

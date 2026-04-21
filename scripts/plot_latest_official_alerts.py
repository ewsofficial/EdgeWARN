#!/usr/bin/env python3
"""Plot latest official alerts JSON as a PNG.

Usage: python scripts/plot_latest_official_alerts.py 
Writes ./alerts_latest.png in the working directory.
"""
import json
import glob
import os
from shapely.geometry import shape, Polygon, MultiPolygon, box
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection


def find_latest_timestamp_file(dirpath):
    files = sorted(glob.glob(os.path.join(dirpath, "*.json")))
    if not files:
        raise FileNotFoundError(f"no json files in {dirpath}")
    return files[-1]


def severity_color(sev):
    sev = (sev or "").lower()
    if sev in ("extreme", "catastrophic"):
        return "#800000"  # dark red
    if sev == "severe":
        return "#ff4500"  # orange red
    if sev == "moderate":
        return "#ffbf00"  # amber
    if sev == "minor":
        return "#1f78b4"  # blue
    return "#808080"  # gray for unknown/other


def geom_to_patches(geom):
    """Return a list of Matplotlib Polygon patches for any polygonal parts.

    This handles Polygon, MultiPolygon, and GeometryCollection by iterating
    parts and extracting exterior rings. Interiors (holes) are ignored for
    simplicity.
    """
    patches = []
    if geom is None:
        return patches

    # Some shapely geometries are collections; iterate if possible
    if hasattr(geom, 'geoms'):
        for part in geom.geoms:
            patches.extend(geom_to_patches(part))
        return patches

    # Single polygon
    if isinstance(geom, Polygon):
        try:
            coords = list(geom.exterior.coords)
            patches.append(MplPolygon(coords, closed=True))
        except Exception:
            pass

    # Other geometry types (Point/Line) are skipped
    return patches


def main():
    # allow overriding input base dir via EDGEWARN_INPUT env or default
    base = os.environ.get("EDGEWARN_INPUT") or os.path.expanduser("~/EdgeWARN_input")
    timestamps_dir = os.path.join(base, "data/Alerts/official/timestamps")
    timestamps_dir = os.path.abspath(timestamps_dir)
    latest = find_latest_timestamp_file(timestamps_dir)
    print(f"Using latest file: {latest}")
    with open(latest, "r") as f:
        data = json.load(f)

    alerts = data.get("alerts", [])
    if not alerts:
        raise SystemExit("no alerts in latest file")

    fig, ax = plt.subplots(figsize=(14, 9))

    all_patches = []
    all_colors = []

    xmin = ymin = 1e9
    xmax = ymax = -1e9

    # CONUS bounding box: lon/lat roughly -125..-66.5, 24.5..49.5
    conus = box(-125.0, 24.5, -66.5, 49.5)

    for a in alerts:
        geom = a.get("geometry")
        if geom is None:
            continue
        try:
            shapely_geom = shape(geom)
        except Exception:
            continue

        # clip to CONUS
        try:
            clipped = shapely_geom.intersection(conus)
        except Exception:
            continue
        if clipped.is_empty:
            continue

        patches = geom_to_patches(clipped)
        if not patches:
            continue

        color = severity_color(a.get("severity"))
        all_patches.extend(patches)
        all_colors.extend([color] * len(patches))

        bxmin, bymin, bxmax, bymax = clipped.bounds
        xmin = min(xmin, bxmin)
        ymin = min(ymin, bymin)
        xmax = max(xmax, bxmax)
        ymax = max(ymax, bymax)

    if not all_patches:
        raise SystemExit("no plottable geometries found in latest alerts")

    pc = PatchCollection(all_patches, facecolor=all_colors, edgecolor="#222222", linewidths=0.4, alpha=0.6)
    ax.add_collection(pc)

    # set axis limits with small margin
    dx = (xmax - xmin) * 0.05 if xmax > xmin else 1
    dy = (ymax - ymin) * 0.05 if ymax > ymin else 1
    ax.set_xlim(xmin - dx, xmax + dx)
    ax.set_ylim(ymin - dy, ymax + dy)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"Official alerts — {os.path.basename(latest)}")
    ax.set_aspect("equal", adjustable="box")

    # create custom legend
    from matplotlib.lines import Line2D
    legend_elems = [
        Line2D([0], [0], marker='s', color='w', label='Severe', markerfacecolor=severity_color('Severe'), markersize=12),
        Line2D([0], [0], marker='s', color='w', label='Moderate', markerfacecolor=severity_color('Moderate'), markersize=12),
        Line2D([0], [0], marker='s', color='w', label='Minor', markerfacecolor=severity_color('Minor'), markersize=12),
        Line2D([0], [0], marker='s', color='w', label='Other/Unknown', markerfacecolor=severity_color(''), markersize=12),
    ]
    ax.legend(handles=legend_elems, loc='lower left')

    outpath = os.path.abspath("alerts_latest.png")
    fig.tight_layout()
    fig.savefig(outpath, dpi=150)
    print(outpath)


if __name__ == '__main__':
    main()

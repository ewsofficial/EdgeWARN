#!/usr/bin/env python3
"""Compare official alerts between two data sources.

Produces a 3-panel PNG: EdgeWARN_input (left), Operational (center), Differences (right).

Usage:
  EDGEWARN_INPUT=~/EdgeWARN_input EDGEWARN_OPER=~/EdgeWARN-Operational-Data python3 scripts/plot_compare_official_alerts.py

Output: alerts_comparison.png in repo root.
"""
import os
import glob
import json
from shapely.geometry import shape, Polygon, MultiPolygon, box
from shapely.ops import unary_union
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection


def find_latest(dirpath):
    files = sorted(glob.glob(os.path.join(dirpath, "*.json")))
    if not files:
        raise FileNotFoundError(f"no json files in {dirpath}")
    return files[-1]


def extract_polygons(geom):
    """Return list of shapely Polygons for polygonal parts of geom."""
    if geom is None:
        return []
    if hasattr(geom, 'geoms'):
        polys = []
        for g in geom.geoms:
            polys.extend(extract_polygons(g))
        return polys
    if isinstance(geom, Polygon):
        return [geom]
    return []


def load_alerts_polygons(json_path, conus):
    with open(json_path, 'r') as f:
        data = json.load(f)
    polys = []
    for a in data.get('alerts', []):
        g = a.get('geometry')
        if g is None:
            continue
        try:
            sg = shape(g)
        except Exception:
            continue
        try:
            clipped = sg.intersection(conus)
        except Exception:
            # sometimes geometries are invalid; try buffering to fix
            try:
                sg_fixed = sg.buffer(0)
                clipped = sg_fixed.intersection(conus)
            except Exception:
                continue
        if clipped.is_empty:
            continue
        parts = extract_polygons(clipped)
        polys.extend(parts)
    return polys


def patches_from_polygons(polys):
    patches = []
    for p in polys:
        try:
            coords = list(p.exterior.coords)
            patches.append(MplPolygon(coords, closed=True))
        except Exception:
            continue
    return patches


def plot_panel(ax, polys, facecolor, title):
    patches = patches_from_polygons(polys)
    if patches:
        pc = PatchCollection(patches, facecolor=facecolor, edgecolor='#222222', linewidths=0.3, alpha=0.7)
        ax.add_collection(pc)


def main():
    base_input = os.environ.get('EDGEWARN_INPUT') or os.path.expanduser('~/EdgeWARN_input')
    base_oper = os.environ.get('EDGEWARN_OPER') or os.path.expanduser('~/EdgeWARN-Operational-Data')

    dir_input = os.path.join(base_input, 'data/Alerts/official/timestamps')
    dir_oper = os.path.join(base_oper, 'data/Alerts/official/timestamps')

    latest_input = find_latest(dir_input)
    latest_oper = find_latest(dir_oper)

    print('Input file:', latest_input)
    print('Oper file :', latest_oper)

    # CONUS box
    conus = box(-125.0, 24.5, -66.5, 49.5)

    polys_input = load_alerts_polygons(latest_input, conus)
    polys_oper = load_alerts_polygons(latest_oper, conus)

    # unions for diff
    union_in = unary_union(polys_input) if polys_input else None
    union_op = unary_union(polys_oper) if polys_oper else None

    input_only = []
    oper_only = []
    if union_in is not None and union_op is not None:
        diff_in = union_in.difference(union_op)
        diff_op = union_op.difference(union_in)
        input_only = extract_polygons(diff_in)
        oper_only = extract_polygons(diff_op)
    elif union_in is not None:
        input_only = extract_polygons(union_in)
    elif union_op is not None:
        oper_only = extract_polygons(union_op)

    # determine common bounds
    all_polys = polys_input + polys_oper + input_only + oper_only
    if not all_polys:
        raise SystemExit('no polygons to plot after clipping to CONUS')

    xmin = min(p.bounds[0] for p in all_polys)
    ymin = min(p.bounds[1] for p in all_polys)
    xmax = max(p.bounds[2] for p in all_polys)
    ymax = max(p.bounds[3] for p in all_polys)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), constrained_layout=True)

    plot_panel(axes[0], polys_input, '#1f78b4', f'EdgeWARN_input\n{os.path.basename(latest_input)}')
    plot_panel(axes[1], polys_oper, '#e31a1c', f'Operational\n{os.path.basename(latest_oper)}')

    # Differences: input-only (blue tint) and oper-only (red tint)
    patches_in_only = patches_from_polygons(input_only)
    patches_op_only = patches_from_polygons(oper_only)
    if patches_in_only:
        axes[2].add_collection(PatchCollection(patches_in_only, facecolor='#6a3d9a', edgecolor='#222222', linewidths=0.3, alpha=0.7))
    if patches_op_only:
        axes[2].add_collection(PatchCollection(patches_op_only, facecolor='#ff7f00', edgecolor='#222222', linewidths=0.3, alpha=0.7))
    axes[2].set_title('Differences\n(input-only purple, oper-only orange)')

    for ax in axes:
        ax.set_xlim(xmin - 0.5, xmax + 0.5)
        ax.set_ylim(ymin - 0.5, ymax + 0.5)
        ax.set_aspect('equal', adjustable='box')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

    out = os.path.abspath('alerts_comparison.png')
    fig.savefig(out, dpi=150)
    print('Saved', out)


if __name__ == '__main__':
    main()

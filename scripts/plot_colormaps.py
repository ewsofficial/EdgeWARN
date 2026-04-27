#!/usr/bin/env python3
"""Plot colormaps from src/EWMRS/colormaps.json.

Generates one PNG per colormap plus a combined gallery image to
visually verify threshold ordering and color ramps.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap


def _load_colormaps(colormaps_path: Path, filter_prefix: str | None = None) -> list[dict]:
    text = colormaps_path.read_text()
    raw = json.loads(text)
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"Unexpected colormap schema in {colormaps_path}")

    all_colormaps = raw[0].get("colormaps", [])
    if filter_prefix:
        filtered_colormaps = [
            c for c in all_colormaps
            if str(c.get("name", "")).startswith(filter_prefix)
        ]
    else:
        filtered_colormaps = all_colormaps

    if not filtered_colormaps:
        raise ValueError(f"No colormaps found in {colormaps_path}" + (f" with prefix '{filter_prefix}'" if filter_prefix else ""))

    return sorted(filtered_colormaps, key=lambda c: str(c.get("name", "")))


def _build_mpl_colormap(colormap_def: dict) -> tuple[LinearSegmentedColormap, float, float]:
    value_range = colormap_def.get("range")
    if not isinstance(value_range, list) or len(value_range) != 2:
        raise ValueError(f"Colormap {colormap_def.get('name')} missing valid range")

    vmin = float(value_range[0])
    vmax = float(value_range[1])
    if vmax <= vmin:
        raise ValueError(f"Colormap {colormap_def.get('name')} has invalid range {value_range}")

    thresholds = sorted(colormap_def.get("thresholds", []), key=lambda t: float(t["value"]))
    if len(thresholds) < 2:
        raise ValueError(f"Colormap {colormap_def.get('name')} needs at least two thresholds")

    stops: list[tuple[float, tuple[float, float, float]]] = []
    for threshold in thresholds:
        value = float(threshold["value"])
        # Use "rgba" for RAP colormaps, "rgb" for others
        color_key = "rgba" if colormap_def.get('name', '').startswith("RAP_") else "rgb"
        rgba = threshold[color_key]
        if len(rgba) not in (3, 4):
            raise ValueError(f"Colormap {colormap_def.get('name')} has invalid color array {rgba}")

        pos = (value - vmin) / (vmax - vmin)
        pos = min(1.0, max(0.0, pos))
        # Use first 3 values (RGB), ignore alpha if present
        stops.append((pos, (float(rgba[0]) / 255.0, float(rgba[1]) / 255.0, float(rgba[2]) / 255.0)))

    if stops[0][0] > 0.0:
        stops.insert(0, (0.0, stops[0][1]))
    if stops[-1][0] < 1.0:
        stops.append((1.0, stops[-1][1]))

    cmap = LinearSegmentedColormap.from_list(str(colormap_def.get("name")), stops)
    return cmap, vmin, vmax


def _plot_single(colormap_def: dict, output_path: Path) -> None:
    cmap, vmin, vmax = _build_mpl_colormap(colormap_def)
    gradient = np.linspace(vmin, vmax, 1400, dtype=np.float32).reshape(1, -1)

    fig, ax = plt.subplots(figsize=(12, 1.7), constrained_layout=True)
    ax.imshow(gradient, aspect="auto", cmap=cmap, extent=[vmin, vmax, 0, 1])
    ax.set_yticks([])
    ax.set_ylabel("")
    ax.set_xlabel(colormap_def.get("units", ""))
    ax.set_title(f"{colormap_def.get('name')} ({vmin:g} to {vmax:g})")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=140)
    plt.close(fig)


def _plot_gallery(colormaps: list[dict], output_path: Path) -> None:
    count = len(colormaps)
    fig, axes = plt.subplots(count, 1, figsize=(14, max(2.0, count * 1.15)), constrained_layout=True)
    if count == 1:
        axes = [axes]

    for axis, colormap_def in zip(axes, colormaps):
        cmap, vmin, vmax = _build_mpl_colormap(colormap_def)
        gradient = np.linspace(vmin, vmax, 1400, dtype=np.float32).reshape(1, -1)
        axis.imshow(gradient, aspect="auto", cmap=cmap, extent=[vmin, vmax, 0, 1])
        axis.set_yticks([])
        axis.set_ylabel("")
        axis.set_title(str(colormap_def.get("name")), fontsize=9, loc="left")
        axis.set_xlabel(str(colormap_def.get("units", "")), fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot colormaps for visual verification")
    parser.add_argument(
        "--colormaps-file",
        default="src/EWMRS/colormaps.json",
        help="Path to colormaps.json",
    )
    parser.add_argument(
        "--filter-prefix",
        help="Only plot colormaps with names starting with this prefix (e.g., 'RAP_', 'GOES_')",
    )
    parser.add_argument(
        "--output-dir",
        default="colormap_plots",
        help="Directory where individual colormap PNGs are written",
    )
    parser.add_argument(
        "--gallery",
        default="colormaps_gallery.png",
        help="Path to combined gallery PNG",
    )
    args = parser.parse_args()

    colormaps_path = Path(args.colormaps_file).resolve()
    output_dir = Path(args.output_dir).resolve()
    gallery_path = Path(args.gallery).resolve()

    colormaps = _load_colormaps(colormaps_path, args.filter_prefix)

    for colormap_def in colormaps:
        out_name = f"{colormap_def['name']}.png"
        _plot_single(colormap_def, output_dir / out_name)

    _plot_gallery(colormaps, gallery_path)

    print(f"Plotted {len(colormaps)} colormaps")
    print(f"Individual plots: {output_dir}")
    print(f"Gallery plot: {gallery_path}")


if __name__ == "__main__":
    main()
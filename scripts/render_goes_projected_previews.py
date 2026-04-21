#!/usr/bin/env python3
"""Render EPSG:3857 GOES ABI preview PNGs using the EWMRS pipeline path."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common.ingest.mrms.config import get_abi_radc_channel_specs
from EWMRS.pipeline import WEB_MERCATOR_SHAPE, WEB_MERCATOR_TRANSFORM
from EWMRS.render.goes_transform import load_goes_abi_render_dataset, reproject_goes_abi_to_web_mercator
from EWMRS.render.render import GUILayerRenderer
from rasterio.enums import Resampling

REFLECTANCE_CHANNELS = {"C01", "C02", "C03", "C04", "C05", "C06"}
BRIGHTNESS_TEMP_LIMITS = {
    "C07": (180.0, 330.0),
    "C08": (180.0, 300.0),
    "C09": (180.0, 310.0),
    "C10": (185.0, 320.0),
    "C11": (180.0, 330.0),
    "C12": (180.0, 330.0),
    "C13": (180.0, 330.0),
    "C14": (180.0, 330.0),
    "C15": (180.0, 330.0),
    "C16": (180.0, 330.0),
}


def _latest_non_idx_file(directory: Path) -> Path | None:
    if not directory.exists():
        return None

    candidates = [p for p in directory.iterdir() if p.is_file() and p.suffix.lower() != ".idx"]
    if not candidates:
        return None

    candidates.sort(key=lambda p: p.stat().st_mtime)
    return candidates[-1]


def _layer_config_for_channel(channel_id: str) -> dict:
    if channel_id in REFLECTANCE_CHANNELS:
        colormap_key = "GOES_RGB_Raw" if channel_id in {"C01", "C02", "C03", "C04", "C05", "C06", "C07", "C11", "C12", "C16"} else f"GOES_ABI_{channel_id}_Reflectance"
        return {
            "name": f"GOES_ABI_{channel_id}_Reflectance",
            "colormap_key": colormap_key,
            "source_type": "goes_abi",
            "variable_name": "CMI",
            "fallback_variable_names": ["Rad"],
            "channel_id": channel_id,
            "value_transform": "reflectance_from_rad",
            "mask_min": {channel_id: 0.0, "default": 0.0},
            "mask_max": {channel_id: 1.2, "default": 1.2},
        }

    min_temp, max_temp = BRIGHTNESS_TEMP_LIMITS[channel_id]
    return {
        "name": f"GOES_ABI_{channel_id}_BrightnessTemp",
        "colormap_key": "GOES_IR" if channel_id in {"C13", "C14", "C15"} else f"GOES_ABI_{channel_id}_BrightnessTemp",
        "source_type": "goes_abi",
        "variable_name": "CMI",
        "fallback_variable_names": ["Rad"],
        "channel_id": channel_id,
        "value_transform": "brightness_temp_from_rad",
        "mask_min": {channel_id: min_temp, "default": min_temp},
        "mask_max": {channel_id: max_temp, "default": max_temp},
    }


def _update_single_png_index(outdir: Path, png_path: Path, timestamp: str) -> None:
    index_path = outdir / "index.json"
    data = {
        "timestamps": [timestamp],
        "png": png_path.name,
    }
    index_path.write_text(json.dumps(data, separators=(",", ":")))


def main() -> None:
    parser = argparse.ArgumentParser(description="Render projected GOES preview PNGs using EWMRS reprojection")
    parser.add_argument(
        "--base-dir",
        default=str(Path.home() / "EdgeWARN_input"),
        help="Runtime base directory containing staged ABI_RadC data",
    )
    parser.add_argument(
        "--output-dir",
        default="goes_projected_previews",
        help="Directory for projected preview PNGs",
    )
    args = parser.parse_args()

    base_dir = Path(args.base_dir).expanduser().resolve()
    output_root = Path(args.output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    rendered = []
    for spec in get_abi_radc_channel_specs():
        if not spec.channel_id:
            continue

        latest_file = _latest_non_idx_file(base_dir / "data" / "ABI_RadC" / spec.outdir.name)
        if latest_file is None:
            continue

        layer = _layer_config_for_channel(spec.channel_id)
        ds = load_goes_abi_render_dataset(latest_file, layer)
        if ds is None:
            continue

        projected = reproject_goes_abi_to_web_mercator(
            ds,
            shape=WEB_MERCATOR_SHAPE,
            transform=WEB_MERCATOR_TRANSFORM,
            resampling=Resampling.bilinear,
        )
        if projected is None:
            continue

        channel_outdir = output_root / layer["name"]
        renderer = GUILayerRenderer(
            projected,
            channel_outdir,
            layer["colormap_key"],
            layer["name"],
            "2026-04-20T12:46:00",
        )
        png_paths, timestamp = renderer.convert_to_png(tile_output=False)
        if not png_paths:
            continue

        _update_single_png_index(channel_outdir, png_paths[0], timestamp)
        rendered.append(png_paths[0])

    print(f"Rendered {len(rendered)} projected GOES previews")
    for path in rendered:
        print(path)


if __name__ == "__main__":
    main()

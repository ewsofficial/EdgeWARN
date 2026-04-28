#!/usr/bin/env python3
"""Plot EWMRS RAP Uint16Array fields and derived wind-speed products."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from EWMRS.rap.config import get_rap_uint16_layers


UINT16_VALID_MAX = 65534.0
COLORMAPS_PATH = Path(__file__).resolve().parents[1] / "src" / "EWMRS" / "colormaps.json"
_CONFIGURED_LAYER_COLORMAPS: dict[str, str] | None = None


@dataclass(frozen=True)
class RapField:
    layer: str
    timestamp: str
    data_path: Path
    metadata_path: Path
    metadata: dict


def decode_uint16_field(data_path: Path, metadata: dict) -> np.ndarray:
    """Decode one RAP data.u16 file into float values using metadata scale."""
    shape = tuple(int(value) for value in metadata["shape"])
    scale = metadata["scale"]
    missing_value = int(metadata.get("missing_value", 65535))

    raw = np.fromfile(data_path, dtype=np.dtype("<u2"))
    expected_size = int(np.prod(shape))
    if raw.size != expected_size:
        raise ValueError(f"{data_path} has {raw.size} values, expected {expected_size} for shape {shape}")

    raw = raw.reshape(shape)
    valid = raw != missing_value

    values = np.full(shape, np.nan, dtype=np.float32)
    min_value = float(scale["min"])
    max_value = float(scale["max"])
    values[valid] = min_value + (raw[valid].astype(np.float32) / UINT16_VALID_MAX) * (max_value - min_value)
    return values


def discover_fields(rap_gui_dir: Path, timestamp: str | None = None) -> list[RapField]:
    """Discover RAP Uint16 fields from <BASE_DIR>/gui/RAP."""
    if not rap_gui_dir.exists():
        raise FileNotFoundError(f"RAP GUI directory not found: {rap_gui_dir}")

    fields: list[RapField] = []
    for layer_dir in sorted(path for path in rap_gui_dir.iterdir() if path.is_dir()):
        timestamp_dirs = sorted((path for path in layer_dir.iterdir() if path.is_dir()), key=lambda path: path.name)
        if timestamp is None:
            timestamp_dirs = timestamp_dirs[-1:]
        else:
            timestamp_dirs = [path for path in timestamp_dirs if path.name == timestamp]

        for timestamp_dir in timestamp_dirs:
            metadata_path = timestamp_dir / "metadata.json"
            data_path = timestamp_dir / "data.u16"
            if not metadata_path.is_file() or not data_path.is_file():
                continue

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            fields.append(
                RapField(
                    layer=str(metadata.get("layer") or layer_dir.name),
                    timestamp=timestamp_dir.name,
                    data_path=data_path,
                    metadata_path=metadata_path,
                    metadata=metadata,
                )
            )
    return fields


def wind_component(field: RapField) -> str | None:
    """Return 'u' or 'v' when the field is a wind component."""
    short_name = str(field.metadata.get("grib", {}).get("shortName", "")).lower()
    layer = field.layer.lower()
    if short_name in {"u", "10u", "u10"} or re.search(r"(^|_)uwind_", layer):
        return "u"
    if short_name in {"v", "10v", "v10"} or re.search(r"(^|_)vwind_", layer):
        return "v"
    return None


def wind_pair_key(field: RapField) -> tuple[str, str, int | None] | None:
    """Return timestamp/type/level pairing key for wind components."""
    if wind_component(field) is None:
        return None

    grib = field.metadata.get("grib", {})
    level = grib.get("level")
    return (field.timestamp, str(grib.get("typeOfLevel", "unknown")), int(level) if level is not None else None)


def wind_speed_name(type_of_level: str, level: int | None) -> str:
    if type_of_level == "isobaricInhPa" and level is not None:
        return f"RAP_WindSpeed_{level}mb"
    if type_of_level == "heightAboveGround" and level is not None:
        return f"RAP_WindSpeed_{level}m"
    if level is not None:
        return f"RAP_WindSpeed_{type_of_level}_{level}"
    return f"RAP_WindSpeed_{type_of_level}"


def load_project_colormap(colormap_name: str) -> LinearSegmentedColormap:
    """Build a Matplotlib colormap from src/EWMRS/colormaps.json."""
    raw = json.loads(COLORMAPS_PATH.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw = [raw]

    for source in raw:
        for colormap_def in source.get("colormaps", []):
            if str(colormap_def.get("name")) != colormap_name:
                continue

            value_range = colormap_def.get("range")
            if not isinstance(value_range, list) or len(value_range) != 2:
                raise ValueError(f"Colormap {colormap_name} is missing a valid range")

            vmin = float(value_range[0])
            vmax = float(value_range[1])
            color_key = "rgba" if colormap_name.startswith("RAP_") else "rgb"
            thresholds = sorted(colormap_def.get("thresholds", []), key=lambda t: float(t["value"]))
            if len(thresholds) < 2:
                raise ValueError(f"Colormap {colormap_name} must define at least two thresholds")

            stops: list[tuple[float, tuple[float, float, float, float]]] = []
            for threshold in thresholds:
                color = threshold[color_key]
                pos = (float(threshold["value"]) - vmin) / (vmax - vmin)
                pos = min(1.0, max(0.0, pos))
                alpha = float(color[3]) / 255.0 if len(color) >= 4 else 1.0
                stops.append(
                    (
                        pos,
                        (
                            float(color[0]) / 255.0,
                            float(color[1]) / 255.0,
                            float(color[2]) / 255.0,
                            alpha,
                        ),
                    )
                )

            if stops[0][0] > 0.0:
                stops.insert(0, (0.0, stops[0][1]))
            if stops[-1][0] < 1.0:
                stops.append((1.0, stops[-1][1]))

            return LinearSegmentedColormap.from_list(colormap_name, stops)

    raise ValueError(f"Colormap {colormap_name} not found in {COLORMAPS_PATH}")


def draw_transparency_background(ax, width: int, height: int) -> None:
    pattern = np.array(
        [
            [[0.92, 0.92, 0.92, 1.0], [0.78, 0.78, 0.78, 1.0]] * 40,
            [[0.78, 0.78, 0.78, 1.0], [0.92, 0.92, 0.92, 1.0]] * 40,
        ],
        dtype=np.float32,
    )
    ax.imshow(pattern, origin="lower", aspect="auto", extent=[0, width, 0, height], interpolation="nearest", zorder=0)


def plot_field(data: np.ndarray, title: str, units: str, output_path: Path, *, cmap: str = "viridis") -> None:
    """Plot one 2D field to PNG."""
    finite = np.isfinite(data)
    if not np.any(finite):
        raise ValueError(f"Cannot plot {title}: field has no finite values")

    fig, ax = plt.subplots(figsize=(11, 8), constrained_layout=True)
    draw_transparency_background(ax, data.shape[1], data.shape[0])
    image = ax.imshow(data, origin="lower", cmap=cmap, aspect="auto", zorder=1)
    ax.set_title(title)
    ax.set_xlabel("RAP grid x")
    ax.set_ylabel("RAP grid y")
    colorbar = fig.colorbar(image, ax=ax, shrink=0.85)
    colorbar.set_label(units)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def field_colormap_name(field: RapField) -> str | None:
    colormap_key = field.metadata.get("colormap_key")
    if isinstance(colormap_key, str) and colormap_key.strip():
        return colormap_key.strip()
    return configured_colormap_name(field.layer)


def should_plot_field(field: RapField) -> bool:
    return field_colormap_name(field) is not None


def configured_colormap_name(layer_name: str) -> str | None:
    global _CONFIGURED_LAYER_COLORMAPS
    if _CONFIGURED_LAYER_COLORMAPS is None:
        _CONFIGURED_LAYER_COLORMAPS = {
            str(layer["name"]): str(layer.get("colormap_key"))
            for layer in get_rap_uint16_layers()
            if layer.get("colormap_key")
        }
    return _CONFIGURED_LAYER_COLORMAPS.get(layer_name)


def wind_vector_colormap_name(u_field: RapField, v_field: RapField) -> str:
    u_colormap = field_colormap_name(u_field)
    v_colormap = field_colormap_name(v_field)
    if u_colormap and v_colormap and u_colormap != v_colormap:
        raise ValueError(
            f"Wind component colormap mismatch: {u_field.layer} -> {u_colormap}, "
            f"{v_field.layer} -> {v_colormap}"
        )
    if u_colormap:
        return u_colormap
    if v_colormap:
        return v_colormap
    raise ValueError(f"Wind fields {u_field.layer} and {v_field.layer} are missing colormap_key metadata")


def plot_wind_vector_field(
    u_data: np.ndarray,
    v_data: np.ndarray,
    title: str,
    units: str,
    output_path: Path,
    *,
    cmap: str | LinearSegmentedColormap,
) -> None:
    """Plot wind speed with a downsampled quiver overlay."""
    speed = np.hypot(u_data, v_data)
    finite = np.isfinite(speed)
    if not np.any(finite):
        raise ValueError(f"Cannot plot {title}: wind field has no finite values")

    fig, ax = plt.subplots(figsize=(11, 8), constrained_layout=True)
    draw_transparency_background(ax, speed.shape[1], speed.shape[0])
    image = ax.imshow(speed, origin="lower", cmap=cmap, aspect="auto", zorder=1)

    step = max(1, min(speed.shape) // 40)
    y_coords, x_coords = np.mgrid[0 : speed.shape[0] : step, 0 : speed.shape[1] : step]
    u_sample = u_data[::step, ::step]
    v_sample = v_data[::step, ::step]
    valid = np.isfinite(u_sample) & np.isfinite(v_sample)

    ax.quiver(
        x_coords[valid],
        y_coords[valid],
        u_sample[valid],
        v_sample[valid],
        color="black",
        alpha=0.65,
        pivot="mid",
        scale=900,
        width=0.002,
    )

    ax.set_title(title)
    ax.set_xlabel("RAP grid x")
    ax.set_ylabel("RAP grid y")
    colorbar = fig.colorbar(image, ax=ax, shrink=0.85)
    colorbar.set_label(units)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def plot_all_fields(fields: list[RapField], output_dir: Path) -> list[Path]:
    """Plot scalar fields and derived wind-vector products with configured colormaps."""
    written: list[Path] = []
    decoded: dict[tuple[str, str], np.ndarray] = {}
    project_colormaps: dict[str, LinearSegmentedColormap] = {}
    wind_colormaps = {
        "RAP_Wind_LL": load_project_colormap("RAP_Wind_LL"),
        "RAP_Wind_HL": load_project_colormap("RAP_Wind_HL"),
    }

    for field in fields:
        data = decode_uint16_field(field.data_path, field.metadata)
        decoded[(field.layer, field.timestamp)] = data
        if wind_component(field) is not None:
            continue
        if not should_plot_field(field):
            continue

        units = str(field.metadata.get("units") or "")
        output_path = output_dir / field.timestamp / f"{field.layer}.png"
        colormap_name = field_colormap_name(field)
        if colormap_name not in project_colormaps:
            project_colormaps[colormap_name] = load_project_colormap(colormap_name)
        cmap: str | LinearSegmentedColormap = project_colormaps[colormap_name]

        plot_field(data, f"{field.layer} {field.timestamp}", units, output_path, cmap=cmap)
        written.append(output_path)

    wind_groups: dict[tuple[str, str, int | None], dict[str, RapField]] = {}
    for field in fields:
        key = wind_pair_key(field)
        component = wind_component(field)
        if key is None or component is None:
            continue
        wind_groups.setdefault(key, {})[component] = field

    for (timestamp, type_of_level, level), components in sorted(wind_groups.items()):
        if "u" not in components or "v" not in components:
            continue

        u_field = components["u"]
        v_field = components["v"]
        u_data = decoded[(u_field.layer, u_field.timestamp)]
        v_data = decoded[(v_field.layer, v_field.timestamp)]
        if u_data.shape != v_data.shape:
            raise ValueError(f"Wind component shape mismatch: {u_field.layer} {u_data.shape} vs {v_field.layer} {v_data.shape}")

        name = wind_speed_name(type_of_level, level)
        colormap_name = wind_vector_colormap_name(u_field, v_field)
        output_path = output_dir / timestamp / f"{name}.png"
        plot_wind_vector_field(
            u_data,
            v_data,
            f"{name} {timestamp}",
            "m s-1",
            output_path,
            cmap=wind_colormaps[colormap_name],
        )
        written.append(output_path)

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot RAP Uint16Array fields and derived wind-speed products")
    parser.add_argument(
        "--base-dir",
        default=str(Path.home() / "EdgeWARN_input"),
        help="Runtime base directory containing gui/RAP",
    )
    parser.add_argument(
        "--rap-dir",
        default=None,
        help="Direct path to a gui/RAP directory. Overrides --base-dir.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Timestamp folder to plot. Defaults to latest timestamp per layer.",
    )
    parser.add_argument(
        "--output-dir",
        default="rap_uint16_field_plots",
        help="Directory where plot PNGs are written",
    )
    args = parser.parse_args()

    rap_dir = Path(args.rap_dir).expanduser().resolve() if args.rap_dir else Path(args.base_dir).expanduser().resolve() / "gui" / "RAP"
    output_dir = Path(args.output_dir).expanduser().resolve()

    fields = discover_fields(rap_dir, timestamp=args.timestamp)
    if not fields:
        raise SystemExit(f"No RAP Uint16 fields found in {rap_dir}")

    written = plot_all_fields(fields, output_dir)
    print(f"Plotted {len(written)} RAP field image(s)")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()

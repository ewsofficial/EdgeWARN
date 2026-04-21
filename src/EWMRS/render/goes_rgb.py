from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
from rasterio.enums import Resampling

import util.file as fs
from common.ingest.mrms.config import get_abi_radc_channel_specs
from common.ingest.mrms.utils import extract_timestamp
from util.io import IOManager

from .goes_transform import load_goes_abi_render_dataset, reproject_goes_abi_to_web_mercator

io_manager = IOManager("[GOES RGB]")

REFLECTANCE_CHANNELS = {"C01", "C02", "C03", "C04", "C05", "C06"}
WEB_MERCATOR_RADIUS_METERS = 6378137.0
TRUE_COLOR_TERMINATOR_START_DEGREES = 80.0
TRUE_COLOR_TERMINATOR_END_DEGREES = 96.0


@dataclass(frozen=True)
class GoesRGBRecipe:
    key: str
    display_name: str
    required_channels: tuple[str, ...]


GOES_RGB_RECIPES: dict[str, GoesRGBRecipe] = {
    "true_color": GoesRGBRecipe("true_color", "True Color RGB", ("C01", "C02", "C03", "C07")),
    "airmass": GoesRGBRecipe("airmass", "Airmass RGB", ("C08", "C10", "C12", "C13")),
    "nighttime_microphysics": GoesRGBRecipe(
        "nighttime_microphysics",
        "Nighttime Microphysics RGB",
        ("C07", "C13", "C15"),
    ),
    "day_cloud_phase": GoesRGBRecipe("day_cloud_phase", "Day Cloud Phase", ("C02", "C05", "C13")),
    "simple_water_vapor": GoesRGBRecipe(
        "simple_water_vapor",
        "Simple Water Vapor RGB",
        ("C08", "C10", "C13"),
    ),
    "sandwich": GoesRGBRecipe("sandwich", "Sandwich RGB", ("C02", "C13")),
}


def _layer_config_for_channel(channel_id: str) -> dict[str, Any]:
    if channel_id in REFLECTANCE_CHANNELS:
        return {
            "source_type": "goes_abi",
            "variable_name": "CMI",
            "fallback_variable_names": ["Rad"],
            "channel_id": channel_id,
            "value_transform": "reflectance_from_rad",
            "mask_min": {channel_id: 0.0, "default": 0.0},
            "mask_max": {channel_id: 1.2, "default": 1.2},
        }

    return {
        "source_type": "goes_abi",
        "variable_name": "CMI",
        "fallback_variable_names": ["Rad"],
        "channel_id": channel_id,
        "value_transform": "brightness_temp_from_rad",
        "mask_min": {channel_id: 180.0, "default": 180.0},
        "mask_max": {channel_id: 330.0, "default": 330.0},
    }


def normalize_rgb_channel(
    values: np.ndarray,
    min_value: float,
    max_value: float,
    *,
    invert: bool = False,
    gamma: float = 1.0,
) -> np.ndarray:
    if max_value <= min_value:
        raise ValueError(f"Invalid range: min={min_value}, max={max_value}")

    scaled = (values - min_value) / (max_value - min_value)
    scaled = np.clip(scaled, 0.0, 1.0)
    if invert:
        scaled = 1.0 - scaled
    if gamma != 1.0:
        scaled = np.power(scaled, 1.0 / gamma)
    return scaled.astype(np.float32)


def _to_celsius(values_kelvin: np.ndarray) -> np.ndarray:
    return (values_kelvin - np.float32(273.15)).astype(np.float32)


def _web_mercator_to_lon_lat(x_coords: np.ndarray, y_coords: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    lon = np.degrees(x_coords / WEB_MERCATOR_RADIUS_METERS).astype(np.float32)
    lat = np.degrees(np.arctan(np.sinh(y_coords / WEB_MERCATOR_RADIUS_METERS))).astype(np.float32)
    return lon, lat


def compute_solar_zenith_angle(latitude_deg: np.ndarray, longitude_deg: np.ndarray, timestamp: datetime) -> np.ndarray:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    else:
        timestamp = timestamp.astimezone(UTC)

    day_of_year = timestamp.timetuple().tm_yday
    hour_utc = timestamp.hour + (timestamp.minute / 60.0) + (timestamp.second / 3600.0)
    fractional_year = (2.0 * np.pi / 365.0) * (day_of_year - 1 + ((hour_utc - 12.0) / 24.0))
    declination = (
        0.006918
        - 0.399912 * np.cos(fractional_year)
        + 0.070257 * np.sin(fractional_year)
        - 0.006758 * np.cos(2.0 * fractional_year)
        + 0.000907 * np.sin(2.0 * fractional_year)
        - 0.002697 * np.cos(3.0 * fractional_year)
        + 0.00148 * np.sin(3.0 * fractional_year)
    )
    equation_of_time = 229.18 * (
        0.000075
        + 0.001868 * np.cos(fractional_year)
        - 0.032077 * np.sin(fractional_year)
        - 0.014615 * np.cos(2.0 * fractional_year)
        - 0.040849 * np.sin(2.0 * fractional_year)
    )

    true_solar_time_minutes = (hour_utc * 60.0) + equation_of_time + (4.0 * longitude_deg)
    true_solar_time_minutes = np.mod(true_solar_time_minutes, 1440.0)
    hour_angle_deg = (true_solar_time_minutes / 4.0) - 180.0
    hour_angle_rad = np.radians(hour_angle_deg)
    latitude_rad = np.radians(latitude_deg)

    cos_zenith = (
        np.sin(latitude_rad) * np.sin(declination)
        + np.cos(latitude_rad) * np.cos(declination) * np.cos(hour_angle_rad)
    )
    cos_zenith = np.clip(cos_zenith, -1.0, 1.0)
    return np.degrees(np.arccos(cos_zenith)).astype(np.float32)


def compute_true_color_night_blend(
    c07_kelvin: np.ndarray,
    latitude_deg: np.ndarray,
    longitude_deg: np.ndarray,
    timestamp: datetime,
) -> tuple[np.ndarray, np.ndarray]:
    solar_zenith = compute_solar_zenith_angle(latitude_deg, longitude_deg, timestamp)
    night_weight = normalize_rgb_channel(
        solar_zenith,
        TRUE_COLOR_TERMINATOR_START_DEGREES,
        TRUE_COLOR_TERMINATOR_END_DEGREES,
    )
    night_luminance = normalize_rgb_channel(c07_kelvin, 190.0, 300.0, invert=True)
    night_rgb = np.dstack((night_luminance, night_luminance, night_luminance)).astype(np.float32)
    return night_rgb, night_weight.astype(np.float32)


def _colorize(values: np.ndarray, thresholds: np.ndarray, colors: np.ndarray) -> np.ndarray:
    safe = np.clip(values, thresholds[0], thresholds[-1])
    red = np.interp(safe, thresholds, colors[:, 0])
    green = np.interp(safe, thresholds, colors[:, 1])
    blue = np.interp(safe, thresholds, colors[:, 2])
    return np.dstack((red, green, blue)).astype(np.float32)


def _rgb_to_rgba(rgb: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
    rgb_uint8 = np.clip(np.nan_to_num(rgb, nan=0.0) * 255.0, 0.0, 255.0).astype(np.uint8)
    alpha = np.where(valid_mask, 255, 0).astype(np.uint8)
    return np.dstack((rgb_uint8, alpha))


def _load_goes_ir_colormap(colormaps_file: Path) -> tuple[np.ndarray, np.ndarray]:
    raw = json.loads(colormaps_file.read_text())
    sources = raw if isinstance(raw, list) else [raw]
    for source in sources:
        for cmap in source.get("colormaps", []):
            if str(cmap.get("name", "")) != "GOES_IR":
                continue
            thresholds = np.array([item["value"] for item in cmap["thresholds"]], dtype=np.float32)
            colors = np.array([item["rgb"] for item in cmap["thresholds"]], dtype=np.float32) / 255.0
            return thresholds, colors
    raise ValueError(f"GOES_IR colormap not found in {colormaps_file}")


def _get_recipe(recipe_key: str) -> GoesRGBRecipe:
    recipe = GOES_RGB_RECIPES.get(recipe_key)
    if recipe is None:
        raise ValueError(f"Unknown GOES RGB recipe: {recipe_key}")
    return recipe


def _list_channel_files(channel_dir: Path) -> list[tuple[datetime, Path]]:
    if not channel_dir.exists():
        return []

    entries = []
    for candidate in channel_dir.iterdir():
        if not candidate.is_file() or candidate.suffix.lower() == ".idx":
            continue
        try:
            timestamp = extract_timestamp(candidate.name).astimezone(UTC)
        except Exception:
            continue
        entries.append((timestamp, candidate))

    entries.sort(key=lambda item: item[0])
    return entries


def _pick_nearest_file(files: list[tuple[datetime, Path]], target_timestamp: datetime) -> tuple[Path, float]:
    chosen_timestamp, chosen_path = min(files, key=lambda item: abs((item[0] - target_timestamp).total_seconds()))
    delta_minutes = abs((chosen_timestamp - target_timestamp).total_seconds()) / 60.0
    return chosen_path, delta_minutes


def select_recipe_channel_files(
    recipe_key: str,
    files_by_channel: dict[str, list[tuple[datetime, Path]]],
    *,
    max_offset_minutes: float = 20.0,
    requested_timestamp: datetime | None = None,
) -> dict[str, Any] | None:
    recipe = _get_recipe(recipe_key)

    missing_channels = [channel_id for channel_id in recipe.required_channels if not files_by_channel.get(channel_id)]
    if missing_channels:
        io_manager.write_warning(
            f"Skipping {recipe.display_name}: missing staged files for {', '.join(sorted(missing_channels))}"
        )
        return None

    target_timestamp = requested_timestamp
    if target_timestamp is None:
        latest_per_channel = [files_by_channel[channel_id][-1][0] for channel_id in recipe.required_channels]
        target_timestamp = min(latest_per_channel)

    selected_files: dict[str, Path] = {}
    for channel_id in recipe.required_channels:
        selected_path, delta_minutes = _pick_nearest_file(files_by_channel[channel_id], target_timestamp)
        if delta_minutes > max_offset_minutes:
            io_manager.write_warning(
                f"Skipping {recipe.display_name}: {channel_id} nearest file is {delta_minutes:.1f} minutes from "
                f"target timestamp {target_timestamp.isoformat()} (max {max_offset_minutes:.1f})"
            )
            return None
        selected_files[channel_id] = selected_path

    return {
        "recipe": recipe,
        "recipe_key": recipe.key,
        "timestamp": target_timestamp,
        "timestamp_iso": target_timestamp.replace(tzinfo=None).isoformat(),
        "selected_files": selected_files,
    }


def prepare_goes_rgb_render(layer_config: dict[str, Any], *, max_offset_minutes: float = 20.0) -> dict[str, Any] | None:
    recipe_key = str(layer_config.get("recipe_key", "")).strip()
    source_root = Path(layer_config.get("filepath") or fs.GOES_ABI_RADC_DIR)

    channel_dir_names = {spec.channel_id: spec.outdir.name for spec in get_abi_radc_channel_specs() if spec.channel_id}
    recipe = _get_recipe(recipe_key)
    files_by_channel: dict[str, list[tuple[datetime, Path]]] = {}

    for channel_id in recipe.required_channels:
        channel_dir_name = channel_dir_names.get(channel_id)
        if not channel_dir_name:
            files_by_channel[channel_id] = []
            continue
        files_by_channel[channel_id] = _list_channel_files(source_root / channel_dir_name)

    prepared = select_recipe_channel_files(
        recipe_key,
        files_by_channel,
        max_offset_minutes=max_offset_minutes,
    )
    if prepared is None:
        return None

    prepared["source_root"] = source_root
    return prepared


def _align_to_reference_grid(datasets: dict[str, Any]) -> dict[str, np.ndarray]:
    reference_channel = "C13" if "C13" in datasets else min(
        datasets,
        key=lambda key: int(datasets[key]["unknown"].sizes["y"]) * int(datasets[key]["unknown"].sizes["x"]),
    )
    ref_da = datasets[reference_channel]["unknown"]
    ref_x = ref_da["x"]
    ref_y = ref_da["y"]

    aligned: dict[str, np.ndarray] = {}
    for channel_id, ds in datasets.items():
        da = ds["unknown"]
        if da.sizes == ref_da.sizes and np.array_equal(da["x"].values, ref_x.values) and np.array_equal(da["y"].values, ref_y.values):
            aligned[channel_id] = np.asarray(da.values, dtype=np.float32)
            continue

        interp_da = da.interp(x=ref_x, y=ref_y, method="linear")
        aligned[channel_id] = np.asarray(interp_da.values, dtype=np.float32)

    return aligned


def _reference_lon_lat_grids(datasets: dict[str, Any]) -> tuple[np.ndarray, np.ndarray]:
    reference_channel = "C13" if "C13" in datasets else min(
        datasets,
        key=lambda key: int(datasets[key]["unknown"].sizes["y"]) * int(datasets[key]["unknown"].sizes["x"]),
    )
    ref_da = datasets[reference_channel]["unknown"]
    x_coords = np.asarray(ref_da["x"].values, dtype=np.float64)
    y_coords = np.asarray(ref_da["y"].values, dtype=np.float64)
    grid_x, grid_y = np.meshgrid(x_coords, y_coords)
    return _web_mercator_to_lon_lat(grid_x, grid_y)


def compute_goes_rgb_product(
    recipe_key: str,
    channel_data: dict[str, np.ndarray],
    *,
    goes_ir_thresholds: np.ndarray,
    goes_ir_colors: np.ndarray,
    true_color_gamma: float = 2.2,
    timestamp: datetime | None = None,
    latitude_deg: np.ndarray | None = None,
    longitude_deg: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    recipe = _get_recipe(recipe_key)

    if recipe.key == "true_color":
        c01 = channel_data["C01"]
        c02 = channel_data["C02"]
        c03 = channel_data["C03"]
        c07 = channel_data["C07"]
        green_syn = 0.45 * c02 + 0.10 * c03 + 0.45 * c01
        day_rgb = np.dstack(
            (
                normalize_rgb_channel(c02, 0.0, 1.0, gamma=true_color_gamma),
                normalize_rgb_channel(green_syn, 0.0, 1.0, gamma=true_color_gamma),
                normalize_rgb_channel(c01, 0.0, 1.0, gamma=true_color_gamma),
            )
        ).astype(np.float32)
        if timestamp is None or latitude_deg is None or longitude_deg is None:
            raise ValueError("True Color RGB requires timestamp, latitude_deg, and longitude_deg for terminator blending")
        night_rgb, night_weight = compute_true_color_night_blend(c07, latitude_deg, longitude_deg, timestamp)
        rgb = (day_rgb * (1.0 - night_weight[..., None])) + (night_rgb * night_weight[..., None])
        mask = np.isfinite(c01) & np.isfinite(c02) & np.isfinite(c03) & np.isfinite(c07)
        return rgb, mask

    if recipe.key == "airmass":
        c08_c = _to_celsius(channel_data["C08"])
        c10_c = _to_celsius(channel_data["C10"])
        c12_c = _to_celsius(channel_data["C12"])
        c13_c = _to_celsius(channel_data["C13"])
        rgb = np.dstack(
            (
                normalize_rgb_channel(c08_c - c10_c, -26.2, 0.6),
                normalize_rgb_channel(c12_c - c13_c, -43.2, 6.7),
                normalize_rgb_channel(c08_c, -64.65, -29.25, invert=True),
            )
        ).astype(np.float32)
        mask = np.isfinite(c08_c) & np.isfinite(c10_c) & np.isfinite(c12_c) & np.isfinite(c13_c)
        return rgb, mask

    if recipe.key == "nighttime_microphysics":
        c07_c = _to_celsius(channel_data["C07"])
        c13_c = _to_celsius(channel_data["C13"])
        c15_c = _to_celsius(channel_data["C15"])
        rgb = np.dstack(
            (
                normalize_rgb_channel(c15_c - c13_c, -6.7, 2.6),
                normalize_rgb_channel(c13_c - c07_c, -3.1, 5.2),
                normalize_rgb_channel(c13_c, -29.6, 19.5),
            )
        ).astype(np.float32)
        mask = np.isfinite(c15_c) & np.isfinite(c13_c) & np.isfinite(c07_c)
        return rgb, mask

    if recipe.key == "day_cloud_phase":
        c02 = channel_data["C02"]
        c05 = channel_data["C05"]
        c13_c = _to_celsius(channel_data["C13"])
        rgb = np.dstack(
            (
                normalize_rgb_channel(c13_c, -53.5, 7.5, invert=True),
                normalize_rgb_channel(c02 * 100.0, 0.0, 78.0),
                normalize_rgb_channel(c05 * 100.0, 1.0, 59.0),
            )
        ).astype(np.float32)
        mask = np.isfinite(c13_c) & np.isfinite(c02) & np.isfinite(c05)
        return rgb, mask

    if recipe.key == "simple_water_vapor":
        c08_c = _to_celsius(channel_data["C08"])
        c10_c = _to_celsius(channel_data["C10"])
        c13_c = _to_celsius(channel_data["C13"])
        rgb = np.dstack(
            (
                normalize_rgb_channel(c13_c, -70.86, 5.81, invert=True),
                normalize_rgb_channel(c08_c, -58.49, -30.48, invert=True),
                normalize_rgb_channel(c10_c, -28.03, -12.12, invert=True),
            )
        ).astype(np.float32)
        mask = np.isfinite(c13_c) & np.isfinite(c08_c) & np.isfinite(c10_c)
        return rgb, mask

    if recipe.key == "sandwich":
        c02 = channel_data["C02"]
        c13_k = channel_data["C13"]
        c13_c = _to_celsius(c13_k)
        vis = normalize_rgb_channel(c02, 0.0, 1.0, gamma=true_color_gamma)
        vis_rgb = np.dstack((vis, vis, vis)).astype(np.float32)
        ir_rgb = _colorize(c13_k, goes_ir_thresholds, goes_ir_colors)
        overlay_alpha = normalize_rgb_channel(c13_c, -70.0, -10.0, invert=True)
        overlay_alpha = np.where(c13_c < -5.0, overlay_alpha, 0.0).astype(np.float32) * 0.9
        rgb = (vis_rgb * (1.0 - overlay_alpha[..., None])) + (ir_rgb * overlay_alpha[..., None])
        mask = np.isfinite(c02) & np.isfinite(c13_k)
        return rgb.astype(np.float32), mask

    raise ValueError(f"Unhandled GOES RGB recipe: {recipe.key}")


def compose_goes_rgb(
    prepared: dict[str, Any],
    *,
    web_mercator_shape: tuple[int, int],
    web_mercator_transform: Any,
    true_color_gamma: float = 2.2,
) -> tuple[np.ndarray, dict[str, Any]] | None:
    datasets: dict[str, Any] = {}
    for channel_id, file_path in prepared["selected_files"].items():
        ds = load_goes_abi_render_dataset(file_path, _layer_config_for_channel(channel_id))
        if ds is None:
            io_manager.write_warning(f"Skipping {prepared['recipe'].display_name}: failed to load {channel_id} from {file_path}")
            return None

        ds = reproject_goes_abi_to_web_mercator(
            ds,
            shape=web_mercator_shape,
            transform=web_mercator_transform,
            resampling=Resampling.bilinear,
        )
        if ds is None:
            io_manager.write_warning(
                f"Skipping {prepared['recipe'].display_name}: failed to reproject {channel_id} to EPSG:3857"
            )
            return None
        datasets[channel_id] = ds

    aligned_channel_data = _align_to_reference_grid(datasets)
    latitude_deg, longitude_deg = _reference_lon_lat_grids(datasets)
    goes_ir_thresholds, goes_ir_colors = _load_goes_ir_colormap(Path(fs.GUI_COLORMAP_JSON))
    rgb, valid_mask = compute_goes_rgb_product(
        prepared["recipe_key"],
        aligned_channel_data,
        goes_ir_thresholds=goes_ir_thresholds,
        goes_ir_colors=goes_ir_colors,
        true_color_gamma=true_color_gamma,
        timestamp=prepared["timestamp"],
        latitude_deg=latitude_deg,
        longitude_deg=longitude_deg,
    )
    rgba = _rgb_to_rgba(rgb, valid_mask)
    metadata = {
        "recipe_key": prepared["recipe_key"],
        "recipe_name": prepared["recipe"].display_name,
        "timestamp_iso": prepared["timestamp_iso"],
        "selected_files": {channel_id: str(path) for channel_id, path in prepared["selected_files"].items()},
        "projection": "EPSG:3857",
        "shape": list(web_mercator_shape),
    }
    return rgba, metadata

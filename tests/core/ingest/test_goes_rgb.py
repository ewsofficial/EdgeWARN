from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from EWMRS.render import goes_rgb


def test_normalize_rgb_channel_supports_inversion_and_gamma():
    values = np.array([0.0, 0.5, 1.0], dtype=np.float32)
    normalized = goes_rgb.normalize_rgb_channel(values, 0.0, 1.0, invert=True, gamma=2.0)

    assert normalized.dtype == np.float32
    assert np.isclose(normalized[0], 1.0)
    assert np.isclose(normalized[-1], 0.0)
    assert 0.70 < normalized[1] < 0.71


def test_select_recipe_channel_files_uses_latest_common_target_and_nearest_inputs(tmp_path):
    files_by_channel = {
        "C01": [
            (datetime(2026, 4, 21, 18, 0, tzinfo=UTC), tmp_path / "c01_old.nc"),
            (datetime(2026, 4, 21, 18, 10, tzinfo=UTC), tmp_path / "c01_new.nc"),
        ],
        "C02": [
            (datetime(2026, 4, 21, 18, 9, tzinfo=UTC), tmp_path / "c02.nc"),
        ],
        "C03": [
            (datetime(2026, 4, 21, 18, 11, tzinfo=UTC), tmp_path / "c03.nc"),
        ],
        "C07": [
            (datetime(2026, 4, 21, 18, 8, tzinfo=UTC), tmp_path / "c07.nc"),
        ],
    }

    prepared = goes_rgb.select_recipe_channel_files("true_color", files_by_channel, max_offset_minutes=5.0)

    assert prepared is not None
    assert prepared["timestamp_iso"] == "2026-04-21T18:08:00"
    assert prepared["selected_files"]["C01"].name == "c01_new.nc"
    assert prepared["selected_files"]["C02"].name == "c02.nc"
    assert prepared["selected_files"]["C03"].name == "c03.nc"
    assert prepared["selected_files"]["C07"].name == "c07.nc"


def test_select_recipe_channel_files_skips_recipe_when_offset_exceeds_limit(tmp_path):
    files_by_channel = {
        "C08": [(datetime(2026, 4, 21, 18, 0, tzinfo=UTC), tmp_path / "c08.nc")],
        "C10": [(datetime(2026, 4, 21, 18, 0, tzinfo=UTC), tmp_path / "c10.nc")],
        "C12": [(datetime(2026, 4, 21, 18, 30, tzinfo=UTC), tmp_path / "c12.nc")],
        "C13": [(datetime(2026, 4, 21, 18, 0, tzinfo=UTC), tmp_path / "c13.nc")],
    }

    prepared = goes_rgb.select_recipe_channel_files("airmass", files_by_channel, max_offset_minutes=10.0)

    assert prepared is None


def test_compute_true_color_recipe_builds_expected_rgb_triplet():
    c01 = np.full((1, 1), 0.1, dtype=np.float32)
    c02 = np.full((1, 1), 0.6, dtype=np.float32)
    c03 = np.full((1, 1), 0.2, dtype=np.float32)
    c07 = np.full((1, 1), 300.0, dtype=np.float32)

    rgb, mask = goes_rgb.compute_goes_rgb_product(
        "true_color",
        {"C01": c01, "C02": c02, "C03": c03, "C07": c07},
        goes_ir_thresholds=np.array([180.0, 330.0], dtype=np.float32),
        goes_ir_colors=np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], dtype=np.float32),
        true_color_gamma=1.0,
        timestamp=datetime(2026, 6, 21, 18, 0, tzinfo=UTC),
        latitude_deg=np.array([[35.0]], dtype=np.float32),
        longitude_deg=np.array([[-97.0]], dtype=np.float32),
    )

    expected_green = 0.45 * 0.6 + 0.10 * 0.2 + 0.45 * 0.1
    assert mask[0, 0]
    assert np.isclose(rgb[0, 0, 0], 0.6)
    assert np.isclose(rgb[0, 0, 1], expected_green)
    assert np.isclose(rgb[0, 0, 2], 0.1)


def test_compute_true_color_recipe_fades_to_c07_at_night():
    c01 = np.full((1, 1), 0.1, dtype=np.float32)
    c02 = np.full((1, 1), 0.6, dtype=np.float32)
    c03 = np.full((1, 1), 0.2, dtype=np.float32)
    c07 = np.full((1, 1), 220.0, dtype=np.float32)

    rgb, mask = goes_rgb.compute_goes_rgb_product(
        "true_color",
        {"C01": c01, "C02": c02, "C03": c03, "C07": c07},
        goes_ir_thresholds=np.array([180.0, 330.0], dtype=np.float32),
        goes_ir_colors=np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], dtype=np.float32),
        true_color_gamma=1.0,
        timestamp=datetime(2026, 6, 21, 6, 0, tzinfo=UTC),
        latitude_deg=np.array([[35.0]], dtype=np.float32),
        longitude_deg=np.array([[-97.0]], dtype=np.float32),
    )

    expected_night = goes_rgb.normalize_rgb_channel(np.array([[220.0]], dtype=np.float32), 190.0, 300.0, invert=True)[0, 0]
    assert mask[0, 0]
    assert np.allclose(rgb[0, 0], np.array([expected_night, expected_night, expected_night], dtype=np.float32), atol=1e-3)


def test_compute_true_color_recipe_partially_blends_near_terminator():
    c01 = np.full((1, 1), 0.2, dtype=np.float32)
    c02 = np.full((1, 1), 0.5, dtype=np.float32)
    c03 = np.full((1, 1), 0.3, dtype=np.float32)
    c07 = np.full((1, 1), 230.0, dtype=np.float32)

    timestamp = datetime(2026, 3, 21, 0, 0, tzinfo=UTC)
    latitude = np.array([[0.0]], dtype=np.float32)
    longitude = np.array([[90.0]], dtype=np.float32)
    solar_zenith = goes_rgb.compute_solar_zenith_angle(latitude, longitude, timestamp)[0, 0]
    night_weight = goes_rgb.normalize_rgb_channel(
        np.array([[solar_zenith]], dtype=np.float32),
        goes_rgb.TRUE_COLOR_TERMINATOR_START_DEGREES,
        goes_rgb.TRUE_COLOR_TERMINATOR_END_DEGREES,
    )[0, 0]

    rgb, _ = goes_rgb.compute_goes_rgb_product(
        "true_color",
        {"C01": c01, "C02": c02, "C03": c03, "C07": c07},
        goes_ir_thresholds=np.array([180.0, 330.0], dtype=np.float32),
        goes_ir_colors=np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], dtype=np.float32),
        true_color_gamma=1.0,
        timestamp=timestamp,
        latitude_deg=latitude,
        longitude_deg=longitude,
    )

    assert 0.0 < solar_zenith < 100.0
    assert 0.0 < night_weight < 1.0
    assert not np.allclose(rgb[0, 0], np.array([0.5, 0.35, 0.2], dtype=np.float32))


def test_compute_sandwich_recipe_uses_ir_overlay_when_clouds_are_cold():
    c02 = np.full((1, 1), 0.8, dtype=np.float32)
    c13 = np.full((1, 1), 200.0, dtype=np.float32)

    rgb, mask = goes_rgb.compute_goes_rgb_product(
        "sandwich",
        {"C02": c02, "C13": c13},
        goes_ir_thresholds=np.array([180.0, 330.0], dtype=np.float32),
        goes_ir_colors=np.array([[1.0, 0.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float32),
        true_color_gamma=1.0,
    )

    assert mask[0, 0]
    assert not np.allclose(rgb[0, 0], np.array([0.8, 0.8, 0.8], dtype=np.float32))


def test_non_true_color_recipe_does_not_require_solar_geometry_inputs():
    rgb, mask = goes_rgb.compute_goes_rgb_product(
        "simple_water_vapor",
        {
            "C08": np.full((2, 2), 230.0, dtype=np.float32),
            "C10": np.full((2, 2), 245.0, dtype=np.float32),
            "C13": np.full((2, 2), 250.0, dtype=np.float32),
        },
        goes_ir_thresholds=np.array([180.0, 330.0], dtype=np.float32),
        goes_ir_colors=np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]], dtype=np.float32),
    )

    assert rgb.shape == (2, 2, 3)
    assert mask.shape == (2, 2)
    assert np.all(mask)


def test_prepare_goes_rgb_batch_uses_shared_timestamp_for_all_recipes(tmp_path):
    files_by_channel = {
        "C01": [(datetime(2026, 4, 21, 18, 2, tzinfo=UTC), tmp_path / "c01.nc")],
        "C02": [(datetime(2026, 4, 21, 18, 3, tzinfo=UTC), tmp_path / "c02.nc")],
        "C03": [(datetime(2026, 4, 21, 18, 4, tzinfo=UTC), tmp_path / "c03.nc")],
        "C05": [(datetime(2026, 4, 21, 18, 5, tzinfo=UTC), tmp_path / "c05.nc")],
        "C07": [(datetime(2026, 4, 21, 18, 1, tzinfo=UTC), tmp_path / "c07.nc")],
        "C08": [(datetime(2026, 4, 21, 18, 6, tzinfo=UTC), tmp_path / "c08.nc")],
        "C10": [(datetime(2026, 4, 21, 18, 7, tzinfo=UTC), tmp_path / "c10.nc")],
        "C12": [(datetime(2026, 4, 21, 18, 8, tzinfo=UTC), tmp_path / "c12.nc")],
        "C13": [(datetime(2026, 4, 21, 18, 9, tzinfo=UTC), tmp_path / "c13.nc")],
        "C15": [(datetime(2026, 4, 21, 18, 10, tzinfo=UTC), tmp_path / "c15.nc")],
    }

    original_specs = goes_rgb.get_abi_radc_channel_specs
    original_list_channel_files = goes_rgb._list_channel_files
    try:
        goes_rgb.get_abi_radc_channel_specs = lambda: [
            type("Spec", (), {"channel_id": channel_id, "outdir": type("OutDir", (), {"name": channel_id})()})()
            for channel_id in files_by_channel
        ]
        goes_rgb._list_channel_files = lambda channel_dir: files_by_channel[channel_dir.name]

        prepared = goes_rgb.prepare_goes_rgb_batch(
            [
                {"recipe_key": "true_color", "filepath": tmp_path, "name": "GOES_RGB_TrueColor"},
                {"recipe_key": "airmass", "filepath": tmp_path, "name": "GOES_RGB_Airmass"},
            ]
        )
    finally:
        goes_rgb.get_abi_radc_channel_specs = original_specs
        goes_rgb._list_channel_files = original_list_channel_files

    assert prepared is not None
    assert prepared["timestamp_iso"] == "2026-04-21T18:01:00"
    assert len(prepared["recipes"]) == 2
    assert prepared["recipes"][0]["timestamp_iso"] == "2026-04-21T18:01:00"
    assert prepared["recipes"][1]["timestamp_iso"] == "2026-04-21T18:01:00"
    assert sorted(prepared["selected_files"]) == ["C01", "C02", "C03", "C07", "C08", "C10", "C12", "C13"]

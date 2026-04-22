from datetime import datetime, timezone

from common.pipeline.goes_readiness import check_local_glm_ready, check_local_goes_ready


def test_check_local_goes_ready_requires_configured_abi_source(tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)

    abi_dir = tmp_path / "VisibleRed"
    abi_dir.mkdir(parents=True)
    (abi_dir / "OR_ABI-L1b-RadC-M6C02_G19_s20261090010000.nc").write_bytes(b"abi")

    ready, path = check_local_goes_ready(
        dt,
        specs=[{"name": "GOES_ABI_C02_Reflectance", "filepath": abi_dir}],
    )

    assert ready is True
    assert path is not None


def test_check_local_goes_ready_accepts_goes_scan_window_covering_cycle_time(tmp_path):
    dt = datetime(2026, 4, 22, 14, 28, tzinfo=timezone.utc)

    abi_dir = tmp_path / "VisibleRed"
    abi_dir.mkdir(parents=True)
    abi_file = (
        abi_dir
        / "OR_ABI-L1b-RadC-M6C02_G19_s20261121426178_e20261121428551_c20261121428578.nc"
    )
    abi_file.write_bytes(b"abi")

    ready, path = check_local_goes_ready(
        dt,
        specs=[{"name": "GOES_ABI_C02_Reflectance", "filepath": abi_dir}],
    )

    assert ready is True
    assert path == str(abi_file)


def test_check_local_goes_ready_does_not_treat_glm_only_as_ready(tmp_path):
    dt = datetime(2026, 4, 19, 0, 10, tzinfo=timezone.utc)

    glm_dir = tmp_path / "GLM"
    glm_dir.mkdir(parents=True)
    (glm_dir / "OR_GLM-L2-LCFA_G19_s20261090010000.nc").write_bytes(b"glm")

    ready, path = check_local_goes_ready(
        dt,
        specs=[{"name": "GOES_ABI_C02_Reflectance", "filepath": tmp_path / "MissingABI"}],
    )
    glm_ready, glm_path = check_local_glm_ready(
        dt,
        specs=[{"outdir": glm_dir, "is_glm": True}],
    )

    assert ready is False
    assert path is None
    assert glm_ready is True
    assert glm_path is not None

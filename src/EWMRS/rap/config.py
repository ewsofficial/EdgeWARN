"""Configuration for EWMRS RAP Uint16Array conversion."""

from __future__ import annotations

import util.file as fs

UINT16_NODATA = 65535
UINT16_VALID_MAX = 65534


def get_rap_uint16_layers() -> list[dict]:
    """Return RAP layers converted to one Uint16Array-compatible file each."""
    return [
        {
            "name": "RAP_Temperature_2m",
            "short_names": ["2t", "t", "t2m"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_Temperature_2m",
            "description": "RAP 2 meter temperature",
        },
        {
            "name": "RAP_Dewpoint_2m",
            "short_names": ["2d", "d2m", "dpt"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_Dewpoint_2m",
            "description": "RAP 2 meter dewpoint temperature",
        },
        {
            "name": "RAP_UWind_10m",
            "short_names": ["10u", "u10", "u"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_UWind_10m",
            "description": "RAP 10 meter u-component wind",
        },
        {
            "name": "RAP_VWind_10m",
            "short_names": ["10v", "v10", "v"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_VWind_10m",
            "description": "RAP 10 meter v-component wind",
        },
    ]
